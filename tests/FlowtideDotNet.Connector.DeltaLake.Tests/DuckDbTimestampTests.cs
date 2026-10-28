// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Apache.Arrow;
using Apache.Arrow.Types;
using DuckDB.NET.Data;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;
using System.Globalization;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DuckDbTimestampTests : IDisposable
    {
        private readonly string _root = Path.Combine(Path.GetTempPath(), "duckts-" + ScratchOwners.ShortId());

        public DuckDbTimestampTests()
        {
            Directory.CreateDirectory(_root);
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(_root, true);
            }
            catch (IOException)
            {
            }
        }

        [DuckDbExtensionFact]
        public async Task DuckDbReadsAUtcInsertExactly()
        {
            var storage = Files.Of.LocalDisk(_root);
            await using var stream = new DeltaLakeSinkStream(nameof(DuckDbReadsAUtcInsertExactly), storage);
            stream.Generate(10);
            await stream.StartStream(@"
                CREATE TABLE test (
                    orderkey INT,
                    orderdate TIMESTAMP
                );

                INSERT INTO test
                SELECT orderkey, orderdate FROM orders
            ");
            await WaitForVersion(storage, "test", stream, 0);

            var expected = stream.Orders.OrderBy(x => x.OrderKey).Select(x => $"{x.OrderKey} {new DateTimeOffset(x.Orderdate, TimeSpan.Zero).ToUnixTimeMilliseconds()}");
            Assert.Equal(expected, Rows("SELECT orderkey, epoch_ms(orderdate) FROM probe ORDER BY orderkey"));
        }

        [DuckDbExtensionFact]
        public async Task DuckDbReadsAMicrosecondRewriteExactly()
        {
            var micros = new long[] { 1_000_001, 1_700_000_000_123_456, -1_500, 86_400_000_000 };
            var schema = new Schema.Builder()
                .Field(new Field("id", Int64Type.Default, true))
                .Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true))
                .Build();
            // The batch the file is written from disposes both arrays
            var source = RewriteCopyTests.Write(schema, new Int64Array.Builder().AppendRange(new long[] { 0, 1, 2, 3 }).Build(),
                new TimestampArray(new TimestampType(TimeUnit.Microsecond, "UTC"), new ArrowBuffer.Builder<long>().AppendRange(micros).Build(), ArrowBuffer.Empty, micros.Length, 0, 0));
            var table = new Delta.StructType(new[]
            {
                new Delta.StructField("id", new Delta.LongType(), true, new Dictionary<string, object>()),
                new Delta.StructField("ts", new Delta.TimestampType(), true, new Dictionary<string, object>())
            });
            var rewritten = await RewriteCopyTests.Rewrite(source, table, 0);

            // Another engine's file, then our rewrite of it without row 0
            var storage = Files.Of.LocalDisk(_root);
            await WriteBytes(storage, "/test/source.parquet", source);
            await WriteBytes(storage, "/test/rewritten.parquet", rewritten.File);
            var schemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}";
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(schemaString), new DeltaAction() { Add = new DeltaAddAction() { Path = "source.parquet", Size = source.Length, DataChange = true, PartitionValues = new Dictionary<string, string>() } });
            await WriteCommit(storage, "test", 1,
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "source.parquet", DataChange = true, DeletionTimestamp = 1 } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "rewritten.parquet", Size = rewritten.File.Length, DataChange = true, PartitionValues = new Dictionary<string, string>(), Statistics = rewritten.Statistics.GetRawText() } });

            Assert.Equal(micros.Skip(1).Select((x, i) => $"{i + 1} {x}"), Rows("SELECT id, epoch_us(ts) FROM probe ORDER BY id"));
        }

        private static async Task WriteBytes(IFileStorage storage, string path, byte[] bytes)
        {
            using var write = await storage.OpenWrite(path);
            await write.WriteAsync(bytes);
        }

        // Rows of the table at its head, read by DuckDB from the cache only
        private List<string> Rows(string query)
        {
            using var connection = new DuckDBConnection("DataSource=:memory:;autoinstall_known_extensions=false;autoload_known_extensions=false");
            connection.Open();
            using var command = connection.CreateCommand();
            lock (DuckDbInstallLock)
            {
                command.CommandText = $"SET extension_directory = '{DuckDbTimeTravelProbe.ExtensionDirectory.Replace("\\", "/")}'; LOAD delta;";
                command.ExecuteNonQuery();
            }
            command.CommandText = $"ATTACH '{Path.GetFullPath(Path.Combine(_root, "test")).Replace("\\", "/")}' AS probe (TYPE delta)";
            command.ExecuteNonQuery();
            command.CommandText = query;
            using var reader = command.ExecuteReader();
            var rows = new List<string>();
            while (reader.Read())
            {
                rows.Add($"{Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture)} {Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture)}");
            }
            return rows;
        }
    }
}

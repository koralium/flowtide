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
using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using Stowage;
using System.Globalization;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DuckDbDeletionVectorTests : IDisposable
    {
        private readonly string _root = Path.Join(Path.GetTempPath(), "duckdv-" + ScratchOwners.ShortId());

        public DuckDbDeletionVectorTests()
        {
            Directory.CreateDirectory(_root);
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(_root, true);
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
                // A folder the system still holds is left in temp
            }
        }

        [DuckDbExtensionFact]
        public async Task DuckDbReadsACommitsSharedFile()
        {
            var storage = Files.Of.LocalDisk(_root);
            await using var stream = new DeltaLakeSinkStream(nameof(DuckDbReadsACommitsSharedFile), storage, options => options.MaxFileSizeBytes = 1_500);
            stream.WaitForUpdateDoesNotRequireDataChange();
            for (int i = 0; i < 600; i++)
            {
                stream.AddOrUpdateUser(new User() { UserKey = i, FirstName = $"first name of {i * 7919}" });
            }
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            var deleted = new[] { 0, 100, 200, 300 };
            stream.DeleteUsers(deleted.Select(x => new User() { UserKey = x }).ToList());
            await WaitForVersion(storage, "test", stream, 1);

            var vectors = (await ReadCommitActions(storage, "test", 1)).Where(x => x.Add?.DeletionVector != null).Select(x => x.Add!.DeletionVector!).ToList();
            Assert.Equal(4, vectors.Count);
            Assert.Single(vectors.Select(x => x.AbsolutePath).Distinct());
            Assert.Equal(4, vectors.Select(x => x.Offset).Distinct().Count());
            var expected = Enumerable.Range(0, 600).Except(deleted).Select(x => $"{x} first name of {x * 7919}");
            Assert.Equal(expected, Rows("SELECT userkey, name FROM probe ORDER BY userkey"));
        }

        // The real writer past the limit, two files to read
        [DuckDbExtensionFact]
        public async Task DuckDbReadsAFileNearTheLimit()
        {
            const int files = 130;
            const int rows = 50_000;
            var storage = Files.Of.LocalDisk(_root);
            var schema = new Schema.Builder().Field(new Field("v", Int64Type.Default, true)).Build();
            var table = new Delta.StructType(new[] { new Delta.StructField("v", new Delta.LongType(), true, new Dictionary<string, object>()) });
            var deletes = new Dictionary<int, ModifiableDeleteVector>();
            var records = new Dictionary<int, DeltaFileRecord>();
            var footers = new Dictionary<int, long>();
            var added = new List<DeltaAction>() { ProtocolWith("deletionVectors"), Metadata("{\"type\":\"struct\",\"fields\":[{\"name\":\"v\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}") };
            long expectedCount = 0;
            long expectedSum = 0;
            for (int file = 0; file < files; file++)
            {
                var first = (long)file * rows;
                var bytes = RewriteCopyTests.Write(schema, new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(x => first + x)).Build());
                using (var write = await storage.OpenWrite($"/test/data{file}.parquet"))
                {
                    await write.WriteAsync(bytes);
                }
                var path = $"data{file}.parquet";
                added.Add(new DeltaAction() { Add = new DeltaAddAction() { Path = path, Size = bytes.Length, DataChange = true, PartitionValues = new Dictionary<string, string>(), Statistics = $"{{\"numRecords\":{rows}}}" } });

                // Under a tenth of the rows, a pattern per file
                var vector = new ModifiableDeleteVector(EmptyDeleteVector.Instance);
                var positions = new HashSet<long>(Enumerable.Range(0, 4_950).Select(x => x * 10L + file % 10));
                foreach (var position in positions.OrderBy(x => x))
                {
                    vector.Add(position);
                }
                deletes[file] = vector;
                records[file] = new DeltaFileRecord() { Path = path, Size = bytes.Length, PartitionValues = new Dictionary<string, string>(), Statistics = $"{{\"numRecords\":{rows}}}" };
                footers[file] = rows;
                for (long x = 0; x < rows; x++)
                {
                    if (!positions.Contains(x))
                    {
                        expectedCount++;
                        expectedSum += (first + x) * (file + 1);
                    }
                }
            }
            await WriteCommit(storage, "test", 0, added.ToArray());

            var options = new JsonSerializerOptions();
            options.Converters.Add(new Internal.Delta.Stats.DeltaStatisticsConverter(table));
            var overlay = new CatalogOverlay(1, PruningLayout.Create(table, System.Array.Empty<string>(), 8), options, null);
            var actions = new List<DeltaAction>();
            var created = new List<string>();
            var output = new DeleteOutput(storage, "/test", "test", table, false, true, null, _ => false, _ => Task.CompletedTask);
            await output.WriteAsync(deletes, records, footers, actions, 1000, overlay, created);
            Assert.Equal(2, created.Count);
            await WriteCommit(storage, "test", 1, actions.ToArray());

            // Weighted per file, so swapped vectors change the sum
            Assert.Equal(new[] { $"{expectedCount} {expectedSum}" }, Rows($"SELECT count(*), sum(v * (v // {rows} + 1)) FROM probe"));
        }

        // The head's rows, by DuckDB from the cache only
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
            command.CommandText = $"ATTACH '{Path.GetFullPath(Path.Join(_root, "test")).Replace("\\", "/")}' AS probe (TYPE delta)";
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

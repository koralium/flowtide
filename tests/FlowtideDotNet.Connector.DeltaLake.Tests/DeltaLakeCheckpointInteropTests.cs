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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using ParquetSharp;
using Stowage;
using System.Text.Json;
using Xunit.Abstractions;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeCheckpointInteropTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _root;

        public DeltaLakeCheckpointInteropTests(ITestOutputHelper output)
        {
            _output = output;
            _root = Path.Join(Directory.GetCurrentDirectory(), "interop_" + Guid.NewGuid().ToString("N"));
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

        private string LogPath(string table) => Path.Join(_root, table, "_delta_log");

        // A table written by the sink with inserts and deletes over the given versions, and its expected row count
        private async Task<int> CreateTable(string table, int versions)
        {
            var storage = Files.Of.LocalDisk(_root);
            await using var stream = new DeltaLakeSinkStream($"interop_{table}_{Guid.NewGuid():N}", storage, options => options.CheckpointInterval = 0);
            stream.Generate(20);
            await stream.StartStream(UserInsert.Replace("test", table));
            await WaitForVersion(storage, table, stream, 0);
            for (int v = 1; v < versions; v++)
            {
                stream.DeleteUser(stream.Users[v]);
                stream.Generate(3);
                await WaitForVersion(storage, table, stream, v);
            }
            return stream.Users.Count;
        }

        // The oracle's content at the version as records, through the new writer, then the hint
        private async Task WriteCheckpoint(string table, long version, string? marker, Func<DeltaAction, DeltaAction>? edit = null)
        {
            var storage = Files.Of.LocalDisk(_root);
            var lines = await CheckpointOracle.FullHistory(storage, table, version, DateTimeOffset.UtcNow);
            var path = Path.Join(LogPath(table), $"{version:D20}.checkpoint.parquet");
            long actions;
            using (var writer = new CheckpointFileWriter(path, new DiskLedger(), new JobMemory(), Compression.Zstd, marker))
            {
                foreach (var line in lines)
                {
                    var action = JsonSerializer.Deserialize<DeltaAction>(line)!;
                    foreach (var record in TestRecords.FromAction(edit?.Invoke(action) ?? action))
                    {
                        writer.Append(record);
                    }
                }
                actions = writer.Close().Actions;
            }
            await File.WriteAllTextAsync(Path.Join(LogPath(table), "_last_checkpoint"), JsonSerializer.Serialize(new LastCheckpointInfo() { Version = version, Size = actions }));
        }

        private void DeleteJsonThrough(string table, long version)
        {
            for (long v = 0; v <= version; v++)
            {
                File.Delete(Path.Join(LogPath(table), $"{v:D20}.json"));
            }
        }

        // A connection with the delta extension loaded, set up outside any assertion that expects a failure
        private DuckDB.NET.Data.DuckDBConnection OpenDuckDb()
        {
            var connection = new DuckDB.NET.Data.DuckDBConnection("DataSource=:memory:");
            connection.Open();
            var extensions = Path.Join(Directory.GetCurrentDirectory(), "duckdb_extensions").Replace("\\", "/");
            Directory.CreateDirectory(extensions);
            DeltaTestKit.LoadDuckDbDelta(connection, Environment.GetEnvironmentVariable("FLOWTIDE_DUCKDB_FRESH") == "1" ? $"{extensions}_fresh_{Guid.NewGuid():N}" : extensions);
            if (Environment.GetEnvironmentVariable("FLOWTIDE_DUCKDB_FRESH") == "1")
            {
                using var version = connection.CreateCommand();
                version.CommandText = "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'delta'";
                _output.WriteLine($"delta extension {version.ExecuteScalar()}");
            }
            return connection;
        }

        // Every row DuckDB reads from the table, as sorted text, so a wrong row is caught and not only a wrong count
        private List<string> DuckDbRows(string table)
        {
            using var connection = OpenDuckDb();
            return DuckDbRows(connection, table);
        }

        private List<string> DuckDbRows(DuckDB.NET.Data.DuckDBConnection connection, string table)
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT * FROM delta_scan('{Path.GetFullPath(Path.Join(_root, table)).Replace("\\", "/")}')";
            using var reader = command.ExecuteReader();
            var rows = new List<string>();
            while (reader.Read())
            {
                var values = new string[reader.FieldCount];
                for (int i = 0; i < values.Length; i++)
                {
                    values[i] = reader.IsDBNull(i) ? "null" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture)!;
                }
                rows.Add(string.Join("|", values));
            }
            rows.Sort(StringComparer.Ordinal);
            return rows;
        }

        [Fact]
        public async Task DuckDbReadsCheckpointWithJsonDeleted()
        {
            var rows = await CreateTable("plain", 4);
            var expected = DuckDbRows("plain");
            Assert.Equal(rows, expected.Count);
            await WriteCheckpoint("plain", 3, null);
            DeleteJsonThrough("plain", 3);

            Assert.Equal(expected, DuckDbRows("plain"));
            var table = (await DeltaTransactionReader.ReadTable(Files.Of.LocalDisk(_root), "plain"))!;
            Assert.Equal(3, table.Version);
        }

        [Fact]
        public async Task DuckDbAcceptsMarkerAndArrowSchema()
        {
            var rows = await CreateTable("marked", 3);
            var expected = DuckDbRows("marked");
            Assert.Equal(rows, expected.Count);
            var marker = new CoverageMarker(2, 1, 1, new CoverageGap(1, new[] { "remove", "txn" }, 5), false).ToJson();
            await WriteCheckpoint("marked", 2, marker);
            DeleteJsonThrough("marked", 2);
            using (var file = new ParquetFileReader(Path.Join(LogPath("marked"), $"{2:D20}.checkpoint.parquet")))
            {
                Assert.Equal(marker, file.FileMetaData.KeyValueMetadata[CheckpointSchema.CoverageMarkerKey]);
                Assert.Contains("ARROW:schema", file.FileMetaData.KeyValueMetadata.Keys);
            }

            Assert.Equal(expected, DuckDbRows("marked"));
        }

        [Fact]
        public async Task DuckDbFailsOnATornNewestCheckpoint()
        {
            var rows = await CreateTable("torn", 3);
            await WriteCheckpoint("torn", 2, null);
            using var connection = OpenDuckDb();
            // The intact checkpoint reads, so the failure below is the tear and not the setup
            Assert.Equal(rows, DuckDbRows(connection, "torn").Count);
            var path = Path.Join(LogPath("torn"), $"{2:D20}.checkpoint.parquet");
            var bytes = await File.ReadAllBytesAsync(path);
            await File.WriteAllBytesAsync(path, bytes.Take(bytes.Length / 2).ToArray());

            // A torn newest checkpoint is an error for kernel readers, with no fallback
            Assert.ThrowsAny<DuckDB.NET.Data.DuckDBException>(() => DuckDbRows(connection, "torn"));
        }

        [Fact]
        public async Task DuckDbReadsDvAndRowTrackingCheckpoint()
        {
            var rows = await CreateTable("rowtracking", 4);
            var expected = DuckDbRows("rowtracking");
            Assert.Equal(rows, expected.Count);
            Assert.Contains(await CheckpointOracle.FullHistory(Files.Of.LocalDisk(_root), "rowtracking", 3, DateTimeOffset.UtcNow), x => x.Contains("\"deletionVector\":{"));
            var rowId = 0L;
            await WriteCheckpoint("rowtracking", 3, null, action =>
            {
                if (action.Protocol != null)
                {
                    action.Protocol.WriterFeatures = action.Protocol.WriterFeatures!.Concat(new[] { "rowTracking", "domainMetadata" }).Distinct().ToList();
                }
                if (action.Add != null)
                {
                    action.Add.BaseRowId = rowId;
                    action.Add.DefaultRowCommitVersion = 1;
                    rowId += 1000;
                }
                if (action.Remove != null)
                {
                    action.Remove.BaseRowId = 7;
                    action.Remove.DefaultRowCommitVersion = 1;
                }
                return action;
            });
            DeleteJsonThrough("rowtracking", 3);

            Assert.Equal(expected, DuckDbRows("rowtracking"));
        }

        [Fact]
        public async Task DuckDbReadsAVersionZeroCheckpoint()
        {
            var rows = await CreateTable("zero", 1);
            var expected = DuckDbRows("zero");
            Assert.Equal(rows, expected.Count);
            await WriteCheckpoint("zero", 0, null);
            DeleteJsonThrough("zero", 0);

            Assert.Equal(expected, DuckDbRows("zero"));
        }

        [Fact]
        public async Task DuckDbAndOurReaderIgnoreOrphanTempFiles()
        {
            var rows = await CreateTable("orphans", 3);
            var expected = DuckDbRows("orphans");
            Assert.Equal(rows, expected.Count);
            await WriteCheckpoint("orphans", 2, null);
            await File.WriteAllBytesAsync(Path.Join(LogPath("orphans"), $".flowtide-{Guid.NewGuid():N}.checkpoint.tmp"), new byte[100]);
            await File.WriteAllTextAsync(Path.Join(LogPath("orphans"), $".flowtide-{Guid.NewGuid():N}.last_checkpoint.tmp"), "{\"version\":99,\"size\":1}");

            Assert.Equal(expected, DuckDbRows("orphans"));
            var table = (await DeltaTransactionReader.ReadTable(Files.Of.LocalDisk(_root), "orphans"))!;
            Assert.Equal(2, table.Version);
            Assert.Equal(2, table.StartCheckpointVersion);
        }
    }
}

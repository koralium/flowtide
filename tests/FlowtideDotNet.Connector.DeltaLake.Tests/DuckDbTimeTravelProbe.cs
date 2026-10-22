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

using DuckDB.NET.Data;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using ParquetSharp;
using Stowage;
using System.Globalization;
using System.Runtime.InteropServices;
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// A fact that runs only when the delta extension for the pinned DuckDB engine is already cached; it never installs or downloads.
    /// </summary>
    public sealed class DuckDbExtensionFactAttribute : FactAttribute
    {
        public DuckDbExtensionFactAttribute()
        {
            if (!File.Exists(DuckDbTimeTravelProbe.CachedExtension))
            {
                Skip = "release gate: cached delta extension unavailable";
            }
        }
    }

    public class DuckDbTimeTravelProbe : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);

        private readonly ITestOutputHelper _output;
        private readonly string _root = Path.Combine(Path.GetTempPath(), "duck-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();

        public DuckDbTimeTravelProbe(ITestOutputHelper output)
        {
            _output = output;
            Directory.CreateDirectory(_root);
        }

        public void Dispose()
        {
            _owners.Release();
            try
            {
                Directory.Delete(_root, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        // The engine DuckDB.NET pins, which names the cache folder
        internal static string EngineVersion
        {
            get
            {
                var version = typeof(DuckDBConnection).Assembly.GetName().Version!;
                return $"v{version.Major}.{version.Minor}.{version.Build}";
            }
        }

        internal static string ExtensionDirectory => Path.Join(AppContext.BaseDirectory, "duckdb_extensions");

        private static string Platform => (OperatingSystem.IsWindows() ? "windows" : OperatingSystem.IsMacOS() ? "osx" : "linux") + "_" + (RuntimeInformation.ProcessArchitecture == Architecture.Arm64 ? "arm64" : "amd64");

        internal static string CachedExtension => Path.Join(ExtensionDirectory, EngineVersion, Platform, "delta.duckdb_extension");

        // A data file of the user schema; returns its length
        private static long WriteData(string path, params long[] keys)
        {
            using (var file = new ParquetFileWriter(path, new ParquetSharp.Column[] { new ParquetSharp.Column<long?>("userkey"), new ParquetSharp.Column<string>("name") }))
            {
                using (var rowGroup = file.AppendRowGroup())
                {
                    using (var writer = rowGroup.NextColumn().LogicalWriter<long?>())
                    {
                        writer.WriteBatch(keys.Select(x => (long?)x).ToArray());
                    }
                    using (var writer = rowGroup.NextColumn().LogicalWriter<string>())
                    {
                        writer.WriteBatch(keys.Select(Name).ToArray());
                    }
                }
                file.Close();
            }
            return new FileInfo(path).Length;
        }

        private static string Name(long key) => $"user-{key}";

        private static string Row(long key) => $"{key}|{Name(key)}";

        // Commits that add and remove data files, a distinct row set per version, with one of our checkpoints between them
        private async Task<List<List<string>>> WriteTable(IFileStorage storage)
        {
            var folder = Path.Combine(_root, "t");
            Directory.CreateDirectory(folder);
            var files = new Dictionary<string, long[]>()
            {
                ["a.parquet"] = new long[] { 1, 2, 3 },
                ["b.parquet"] = new long[] { 4, 5 },
                ["c.parquet"] = new long[] { 6 },
                ["d.parquet"] = new long[] { 7, 8 },
                ["e.parquet"] = new long[] { 9 }
            };
            var sizes = files.ToDictionary(x => x.Key, x => WriteData(Path.Combine(folder, x.Key), x.Value));
            DeltaAction Add(string path) => new DeltaAction()
            {
                Add = new DeltaAddAction() { Path = path, PartitionValues = new Dictionary<string, string>(), Size = sizes[path], ModificationTime = s_now.ToUnixTimeMilliseconds(), DataChange = true }
            };
            DeltaAction Remove(string path) => new DeltaAction()
            {
                Remove = new DeltaRemoveFileAction() { Path = path, DeletionTimestamp = s_now.ToUnixTimeMilliseconds(), DataChange = true, Size = sizes[path] }
            };
            var versions = new List<DeltaAction[]>()
            {
                new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("a.parquet") },
                new[] { Add("b.parquet") },
                new[] { Remove("a.parquet") },
                new[] { Add("c.parquet") },
                new[] { Remove("b.parquet"), Add("d.parquet") },
                new[] { Add("e.parquet") }
            };
            var live = new List<string>();
            var expected = new List<List<string>>();
            for (int version = 0; version < versions.Count; version++)
            {
                await DeltaTestKit.WriteOwnCommit(storage, "t", version, 0, s_now.ToUnixTimeMilliseconds() + version, versions[version]);
                foreach (var action in versions[version])
                {
                    if (action.Add != null)
                    {
                        live.Add(action.Add.Path!);
                    }
                    if (action.Remove != null)
                    {
                        live.Remove(action.Remove.Path!);
                    }
                }
                expected.Add(live.SelectMany(x => files[x]).Order().Select(Row).ToList());
                if (version == 4)
                {
                    await BuildCheckpoint(storage, version);
                }
            }
            return expected;
        }

        private async Task BuildCheckpoint(IFileStorage storage, long version)
        {
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_root, ScratchOwners.ShortId())));
            using (var scratch = owner.CreateJob())
            {
                var result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = "t",
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Clock = new FakeTimeProvider(s_now)
                }, new BuildRequest() { Version = version });
                Assert.Equal(BuildStatus.Completed, result.Status);
            }
            _owners.Release(owner);
        }

        private static object? Scalar(DuckDBConnection connection, string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            return command.ExecuteScalar();
        }

        private static List<string> Rows(DuckDBConnection connection, string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            using var reader = command.ExecuteReader();
            var rows = new List<string>();
            while (reader.Read())
            {
                rows.Add($"{Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture)}|{reader.GetValue(1)}");
            }
            return rows;
        }

        [DuckDbExtensionFact]
        public async Task InstalledDuckDbReadsDistinctVersionsWithoutDownloads()
        {
            var storage = Files.Of.LocalDisk(_root);
            var expected = await WriteTable(storage);
            Assert.Equal(expected.Count, expected.Select(x => string.Join(",", x)).Distinct().Count());

            // Automatic installation and loading off; the extension comes only from the cache by an explicit LOAD
            using var connection = new DuckDBConnection("DataSource=:memory:;autoinstall_known_extensions=false;autoload_known_extensions=false");
            connection.Open();
            Assert.Equal(false, Scalar(connection, "SELECT current_setting('autoinstall_known_extensions')"));
            Assert.Equal(false, Scalar(connection, "SELECT current_setting('autoload_known_extensions')"));
            lock (DeltaTestKit.DuckDbInstallLock)
            {
                using var load = connection.CreateCommand();
                load.CommandText = $"SET extension_directory = '{ExtensionDirectory.Replace("\\", "/")}'; LOAD delta;";
                load.ExecuteNonQuery();
            }
            var engine = Scalar(connection, "SELECT library_version FROM pragma_version()");
            var source = Scalar(connection, "SELECT source_id FROM pragma_version()");
            var extension = Scalar(connection, "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'delta' AND loaded");
            _output.WriteLine($"DuckDB {engine} ({source}), delta extension {extension}, from {CachedExtension}");
            Assert.Equal(EngineVersion, engine);
            Assert.NotNull(extension);

            using (var attach = connection.CreateCommand())
            {
                attach.CommandText = $"ATTACH '{Path.GetFullPath(Path.Combine(_root, "t")).Replace("\\", "/")}' AS probe (TYPE delta)";
                attach.ExecuteNonQuery();
            }
            for (int version = 0; version < expected.Count; version++)
            {
                Assert.Equal(expected[version], Rows(connection, $"SELECT userkey, name FROM probe AT (VERSION => {version}) ORDER BY userkey"));
            }
        }
    }
}

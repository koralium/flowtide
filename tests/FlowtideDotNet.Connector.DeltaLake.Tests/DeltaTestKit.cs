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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Logging;
using ParquetSharp;
using Stowage;
using System.Collections.Concurrent;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    internal static class DeltaTestKit
    {
        public const string UserSchema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"userkey\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";

        public const string UserInsert = @"
            CREATE TABLE test (
                userkey INT,
                name STRING
            );

            INSERT INTO test
            SELECT userKey AS userkey, firstName AS name FROM users
        ";

        public static string CommitPath(string table, long version) => $"/{table}/_delta_log/{version:D20}.json";

        public static async Task WaitForVersion(IFileStorage storage, string table, FlowtideTestStream stream, long version, TimeSpan? timeout = null)
        {
            var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromMinutes(2));
            while (!await CommitExists(storage, table, version))
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"Version {version} of {table} was not written in time");
                }
                await stream.SchedulerTick();
                await Task.Delay(50);
            }
        }

        /// <summary>
        /// Reads the table with a new source stream until it holds the expected rows; counting updates hangs when one wait sees two versions.
        /// </summary>
        public static async Task AssertReaderResult<T>(string testName, IFileStorage storage, string tableName, IEnumerable<T> expected)
        {
            var rows = expected.ToList();
            var stream = new DeltaLakeTestStream(testName + "_compare", storage);
            try
            {
                await stream.StartStream($"INSERT INTO result SELECT * FROM {tableName}");
                // Includes the new reader's initial load, which a loaded machine slows
                var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(3);
                while (true)
                {
                    await stream.SchedulerTick();
                    await Task.Delay(10);
                    try
                    {
                        stream.AssertCurrentDataEqual(rows);
                        return;
                    }
                    catch (Exception) when (DateTime.UtcNow < deadline)
                    {
                    }
                }
            }
            finally
            {
                await stream.DisposeAsync();
            }
        }

        // Local disk answers Exists by opening the file, which fails while the sink still writes it
        private static async Task<bool> CommitExists(IFileStorage storage, string table, long version)
        {
            try
            {
                return await storage.Exists(CommitPath(table, version));
            }
            catch (IOException)
            {
                return false;
            }
        }

        // Triggers checkpoints while waiting, for versions that need more checkpoints than new data brings
        public static async Task WaitForVersionCheckpointing(IFileStorage storage, string table, FlowtideTestStream stream, long version)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (!await CommitExists(storage, table, version))
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"Version {version} of {table} was not written in time");
                }
                await stream.SchedulerTick();
                await stream.TriggerCheckpoint();
                await Task.Delay(50);
            }
        }

        // Each wait sees one more completed checkpoint, so every earlier checkpoint also finished its commit, the stream must count checkpoints without data
        public static async Task RunCheckpoints(FlowtideTestStream stream, int count)
        {
            for (int i = 0; i <= count; i++)
            {
                _ = stream.TriggerCheckpoint();
                await stream.WaitForUpdate();
            }
        }

        // The checkpoint notification comes before the sink publishes, an awaited checkpoint has published
        public static async Task SettlePublications(FlowtideTestStream stream)
        {
            await RunCheckpoints(stream, 3);
            await stream.TriggerCheckpoint();
        }

        /// <summary>
        /// Waits until the hint names a checkpoint at or above <paramref name="minVersion"/> whose file exists, and returns the hinted version.
        /// With <paramref name="content"/>, the hinted checkpoint must also pass that check.
        /// </summary>
        public static async Task<long> WaitForCheckpoint(IFileStorage storage, string table, FlowtideTestStream stream, long minVersion, TimeSpan? timeout = null, Func<long, Task<bool>>? content = null)
        {
            var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromMinutes(2));
            while (true)
            {
                try
                {
                    var text = await storage.ReadText($"/{table}/_delta_log/_last_checkpoint");
                    var hint = text == null ? null : JsonSerializer.Deserialize<LastCheckpointInfo>(text);
                    if (hint != null && hint.Version >= minVersion
                        && await storage.Exists($"/{table}/_delta_log/{hint.Version:D20}.checkpoint.parquet")
                        && (content == null || await content(hint.Version)))
                    {
                        return hint.Version;
                    }
                }
                catch (Exception e) when (e is IOException || e is JsonException)
                {
                    // Caught mid-write
                }
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"No checkpoint at or above {minVersion} of {table} was written in time");
                }
                // Checkpoints are built in the background, which the stream's manual budget runs only when ticked
                if (stream is DeltaLakeSinkStream delta)
                {
                    await delta.Maintenance.TickOnceAsync();
                }
                await stream.SchedulerTick();
                await Task.Delay(50);
            }
        }

        // DuckDB moves a downloaded extension into its folder, so parallel installs into one shared folder are serialized
        internal static readonly object DuckDbInstallLock = new object();

        public static void LoadDuckDbDelta(DuckDB.NET.Data.DuckDBConnection connection, string extensionsDir)
        {
            lock (DuckDbInstallLock)
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"SET extension_directory = '{extensionsDir}'; INSTALL delta; LOAD delta;";
                command.ExecuteNonQuery();
            }
        }

        public static async Task<byte[]> ReadBytes(IFileStorage storage, string path)
        {
            using var read = await storage.OpenRead(path);
            Assert.NotNull(read);
            using var memory = new MemoryStream();
            await read.CopyToAsync(memory);
            return memory.ToArray();
        }

        // A published checkpoint's records as canonical lines, read without the reader model that maps a null map to an empty one
        public static async Task<List<string>> CheckpointLines(IFileStorage storage, string table, long version)
        {
            var path = Path.Combine(Path.GetTempPath(), $"checkpoint-lines-{Guid.NewGuid():N}.parquet");
            try
            {
                await File.WriteAllBytesAsync(path, await ReadBytes(storage, $"/{table}/_delta_log/{version:D20}.checkpoint.parquet"));
                var memory = new JobMemory();
                var scan = CheckpointPageScan.Scan(path, memory);
                var records = new List<byte[]>();
                using (var reader = new BoundedCheckpointReader(path, scan, memory))
                {
                    reader.Read(record => records.Add(record.ToArray()));
                }
                memory.Release(scan.ModelBytes);
                return CheckpointOracle.Canonical(records.Select(x => CheckpointRecord.ToAction(x)));
            }
            finally
            {
                File.Delete(path);
            }
        }

        // Codec of every column chunk in the file
        public static async Task<List<Compression>> ReadCompressions(IFileStorage storage, string path)
        {
            var bytes = await ReadBytes(storage, path);
            using var input = new MemoryStream(bytes);
            using var reader = new ParquetFileReader(input);
            var codecs = new List<Compression>();
            for (int g = 0; g < reader.FileMetaData.NumRowGroups; g++)
            {
                using var rowGroup = reader.RowGroup(g);
                for (int c = 0; c < rowGroup.MetaData.NumColumns; c++)
                {
                    codecs.Add(rowGroup.MetaData.GetColumnChunkMetaData(c).Compression);
                }
            }
            return codecs;
        }

        public static async Task<List<DeltaAction>> ReadCommitActions(IFileStorage storage, string table, long version)
        {
            var text = System.Text.Encoding.UTF8.GetString(await ReadBytes(storage, CommitPath(table, version)));
            return text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(line => JsonSerializer.Deserialize<DeltaAction>(line)!)
                .ToList();
        }

        // Each version's files by content and deleted rows, not by name
        public static async Task<List<string>> TableHistory(IFileStorage storage, string table, long lastVersion)
        {
            var hashes = new Dictionary<string, string>();
            var history = new List<string>();
            for (long version = 0; version <= lastVersion; version++)
            {
                var entries = new List<string>();
                foreach (var action in await ReadCommitActions(storage, table, version))
                {
                    if (action.Add != null)
                    {
                        hashes.TryAdd(action.Add.Path!, Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(await ReadBytes(storage, $"/{table}/{action.Add.Path}"))));
                        entries.Add($"add {hashes[action.Add.Path!]} dv {await Deleted(storage, table, action.Add.DeletionVector)} {action.Add.Statistics}");
                    }
                    if (action.Remove != null)
                    {
                        entries.Add($"remove {hashes[action.Remove.Path!]} dv {await Deleted(storage, table, action.Remove.DeletionVector)}");
                    }
                    if (action.Cdc != null)
                    {
                        entries.Add($"cdc {Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(await ReadBytes(storage, $"/{table}/{action.Cdc.Path}")))}");
                    }
                }
                entries.Sort(StringComparer.Ordinal);
                history.Add($"v{version}: {string.Join(" | ", entries)}");
            }
            return history;
        }

        private static async Task<string> Deleted(IFileStorage storage, string table, DeletionVector? vector)
        {
            return vector == null ? "" : string.Join(",", await DeletionVectorReader.ReadDeletionVector(storage, new IOPath(table), vector));
        }

        public static Task WriteCommit(IFileStorage storage, string table, long version, params DeltaAction[] actions)
        {
            return DeltaTransactionWriter.WriteCommit(storage, table, version, actions.ToList());
        }

        // A commit our sink would write: a leading commitInfo with a stage id, the adoption epoch and the timestamp
        public static Task WriteOwnCommit(IFileStorage storage, string table, long version, long adoptedAt, long timestamp, params DeltaAction[] actions)
        {
            var commitInfo = new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = $"stage-{version}", AdoptedAt = adoptedAt, Timestamp = timestamp, Operation = "WRITE" } };
            return DeltaTransactionWriter.WriteCommit(storage, table, version, actions.Prepend(commitInfo).ToList());
        }

        // The properties a configured builder produces; the builder is disposed
        public static WriterProperties Properties(Func<WriterPropertiesBuilder, WriterPropertiesBuilder> configure)
        {
            using var builder = new WriterPropertiesBuilder();
            return configure(builder).Build();
        }

        public static DeltaAction Protocol()
        {
            return new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 2 } };
        }

        private static readonly HashSet<string> s_readerWriterFeatures = new HashSet<string>()
        {
            "columnMapping", "deletionVectors", "timestampNtz", "v2Checkpoint", "catalogManaged", "vacuumProtocolCheck",
            "typeWidening", "typeWidening-preview", "variantType", "variantType-preview"
        };

        // Reader 3 and writer 7 with the named writer features; the reader features are the ones the spec shares with readers
        public static DeltaAction ProtocolWith(params string[] writerFeatures)
        {
            return new DeltaAction()
            {
                Protocol = new DeltaProtocolAction()
                {
                    MinReaderVersion = 3,
                    MinWriterVersion = 7,
                    ReaderFeatures = writerFeatures.Where(s_readerWriterFeatures.Contains).ToList(),
                    WriterFeatures = writerFeatures.ToList()
                }
            };
        }

        public static DeltaAction Metadata(string schema, Dictionary<string, string>? configuration = null)
        {
            return new DeltaAction()
            {
                MetaData = new DeltaMetadataAction()
                {
                    Id = "table-id",
                    SchemaString = schema,
                    PartitionColumns = new List<string>(),
                    Format = new DeltaMetadataFormat() { Provider = "parquet", Options = new Dictionary<string, string>() },
                    Configuration = configuration ?? new Dictionary<string, string>()
                }
            };
        }
    }

    // Collects warnings and errors from a stream
    internal sealed class TestLogCollector : ILoggerProvider, ILogger
    {
        private readonly ConcurrentQueue<(LogLevel Level, string Message)> _entries = new ConcurrentQueue<(LogLevel, string)>();

        public IReadOnlyCollection<string> Warnings => _entries.Where(x => x.Level == LogLevel.Warning).Select(x => x.Message).ToArray();

        public IReadOnlyCollection<string> Errors => _entries.Where(x => x.Level >= LogLevel.Error).Select(x => x.Message).ToArray();

        public ILogger CreateLogger(string categoryName) => this;

        public void Dispose()
        {
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
            {
                _entries.Enqueue((logLevel, exception == null ? formatter(state, exception) : $"{formatter(state, exception)} {exception}"));
            }
        }
    }

    /// <summary>
    /// The scratch owners a test created, released at its teardown so their folders can be deleted.
    /// </summary>
    internal sealed class ScratchOwners
    {
        private readonly List<ScratchDirectory> _owners = new List<ScratchDirectory>();

        // A short folder name, since native Parquet opens fail on Windows past 260 characters
        public static string ShortId() => Guid.NewGuid().ToString("N").Substring(0, 8);

        public ScratchDirectory Add(ScratchDirectory owner)
        {
            lock (_owners)
            {
                _owners.Add(owner);
            }
            return owner;
        }

        // Deletes what the owner's deletion hook kept, then releases its lock
        public void Release(ScratchDirectory owner)
        {
            lock (_owners)
            {
                _owners.Remove(owner);
            }
            owner.DeleteFailsForTests = null;
            owner.RetryPending();
            owner.ReleaseLockForTests();
        }

        // Releases every owner the test still holds
        public void Release()
        {
            ScratchDirectory[] owners;
            lock (_owners)
            {
                owners = _owners.ToArray();
            }
            foreach (var owner in owners)
            {
                Release(owner);
            }
        }
    }
}

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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
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

        public static async Task WaitForCheckpoint(IFileStorage storage, string table, FlowtideTestStream stream, long version)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (!await storage.Exists($"/{table}/_delta_log/{version:D20}.checkpoint.parquet") || !await storage.Exists($"/{table}/_delta_log/_last_checkpoint"))
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"Checkpoint {version} of {table} was not written in time");
                }
                await stream.SchedulerTick();
                await Task.Delay(50);
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

        public static Task WriteCommit(IFileStorage storage, string table, long version, params DeltaAction[] actions)
        {
            return DeltaTransactionWriter.WriteCommit(storage, table, version, actions.ToList());
        }

        public static DeltaAction Protocol()
        {
            return new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 2 } };
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
}

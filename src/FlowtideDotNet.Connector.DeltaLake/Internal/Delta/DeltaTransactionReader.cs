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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.CheckpointReading;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Microsoft.Extensions.Logging;
using Stowage;
using System.Globalization;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    internal static class DeltaTransactionReader
    {
        public const string DeltaLogDirName = "_delta_log/";
        private const int VersionDigits = 20;
        private const string CommitSuffix = ".json";
        private const string ClassicCheckpointSuffix = ".checkpoint.parquet";

        /// <summary>
        /// Reads the snapshot at maxVersion, where 0 is the oldest readable version, and returns null when the table has no log.
        /// </summary>
        public static async Task<DeltaTable?> ReadTable(IFileStorage storage, IOPath tableName, long maxVersion = long.MaxValue, DeltaReadOptions? options = null)
        {
            return await ReadTable(storage, tableName, await ListLog(storage, tableName), maxVersion, options);
        }

        /// <summary>
        /// Reads from a listing the caller already has, so it is not listed again.
        /// </summary>
        internal static async Task<DeltaTable?> ReadTable(IFileStorage storage, IOPath tableName, LogListing log, long maxVersion = long.MaxValue, DeltaReadOptions? options = null)
        {
            options ??= DeltaReadOptions.Default;

            if (log.Head < 0)
            {
                if (log.HasOtherVersionFiles)
                {
                    throw new DeltaVersionNotAvailableException($"Delta table {tableName} only has log files this reader does not support, such as UUID-named or multi-part checkpoints.");
                }
                return null;
            }

            if (maxVersion == 0)
            {
                return await ReadOldestVersion(storage, tableName, log, options);
            }

            var target = Math.Min(maxVersion, log.Head);
            return await ReadVersion(storage, tableName, log, target, options);
        }

        private static async Task<DeltaTable> ReadVersion(IFileStorage storage, IOPath tableName, LogListing log, long target, DeltaReadOptions options)
        {
            // Every commit from suffixStart up to the target exists
            long suffixStart = target + 1;
            while (suffixStart > 0 && log.Commits.ContainsKey(suffixStart - 1))
            {
                suffixStart--;
            }

            Exception? lastFailure = null;
            foreach (var checkpoint in log.Checkpoints)
            {
                if (checkpoint.Version > target)
                {
                    continue;
                }
                // Older checkpoints need an even longer run of commits
                if (checkpoint.Version + 1 < suffixStart)
                {
                    break;
                }
                var replay = new LogReplay(options.SkipTombstones);
                var failure = await TryReplayCheckpoint(storage, checkpoint, replay, options);
                if (failure == null)
                {
                    await ReplayCommits(storage, log, checkpoint.Version + 1, target, replay);
                    failure = replay.CheckpointSchemaFailure();
                    if (failure == null)
                    {
                        return replay.ToTable(target, checkpoint.Version);
                    }
                    ReportCheckpointFailure(checkpoint, failure, options);
                }
                lastFailure = failure;
            }

            if (suffixStart > 0)
            {
                throw new DeltaVersionNotAvailableException($"Version {target} of delta table {tableName} cannot be reconstructed, commit {suffixStart - 1} is missing and no usable checkpoint at version {suffixStart - 1} or later exists.", lastFailure);
            }
            var fromStart = new LogReplay(options.SkipTombstones);
            await ReplayCommits(storage, log, 0, target, fromStart);
            return fromStart.ToTable(target, null);
        }

        private static async Task<DeltaTable> ReadOldestVersion(IFileStorage storage, IOPath tableName, LogListing log, DeltaReadOptions options)
        {
            if (log.Commits.ContainsKey(0))
            {
                return await ReadVersion(storage, tableName, log, 0, options);
            }

            // After metadata cleanup the oldest version is the oldest usable checkpoint
            Exception? lastFailure = null;
            for (int i = log.Checkpoints.Count - 1; i >= 0; i--)
            {
                var checkpoint = log.Checkpoints[i];
                var replay = new LogReplay(options.SkipTombstones);
                var failure = await TryReplayCheckpoint(storage, checkpoint, replay, options);
                if (failure == null)
                {
                    failure = replay.CheckpointSchemaFailure();
                    if (failure == null)
                    {
                        return replay.ToTable(checkpoint.Version, checkpoint.Version);
                    }
                    ReportCheckpointFailure(checkpoint, failure, options);
                }
                lastFailure = failure;
            }
            throw new DeltaVersionNotAvailableException($"Delta table {tableName} has neither a first commit nor a usable classic checkpoint.", lastFailure);
        }

        // Null when the checkpoint is usable, otherwise the reason it is not
        private static async Task<Exception?> TryReplayCheckpoint(IFileStorage storage, LogTransactionFile checkpoint, LogReplay replay, DeltaReadOptions options)
        {
            var entry = checkpoint.IOEntry;
            Exception? failure = null;
            try
            {
                var reader = new ParquetCheckpointReader();
                await foreach (var action in reader.ReadCheckpointFile(storage, entry, includeRemoves: !options.SkipTombstones))
                {
                    replay.Apply(action, fromCheckpoint: true);
                }
                if (!replay.HasMetadataAndProtocol)
                {
                    throw new CheckpointUnusableException($"Checkpoint {entry.Path.Full} has no protocol or no metadata.");
                }
            }
            // No cancellation token reaches this read, a storage timeout surfaces as a cancellation and must fall back too
            catch (Exception e)
            {
                failure = e;
            }

            if (failure != null)
            {
                ReportCheckpointFailure(checkpoint, failure, options);
            }
            return failure;
        }

        // The checkpoint is tried again on every read, a failure can be transient
        private static void ReportCheckpointFailure(LogTransactionFile checkpoint, Exception failure, DeltaReadOptions options)
        {
            if (options.Logger != null)
            {
                var path = checkpoint.IOEntry.Path.Full;
                var firstReport = options.ReportedCheckpointFailures?.Add(path) ?? true;
                options.Logger.Log(firstReport ? LogLevel.Warning : LogLevel.Debug, failure, "Delta checkpoint {checkpoint} could not be used, an older checkpoint or the commits are read instead", path);
            }
        }

        private static async Task ReplayCommits(IFileStorage storage, LogListing log, long from, long to, LogReplay replay)
        {
            for (long version = from; version <= to; version++)
            {
                var commit = log.Commits[version];
                using var logData = await storage.OpenRead(commit.IOEntry.Path);

                if (logData == null)
                {
                    throw new DeltaVersionNotAvailableException($"Commit {commit.IOEntry.Path.Full} was removed while it was read.");
                }

                using var textReader = new StreamReader(logData);

                string? line;
                while ((line = await textReader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }
                    var action = JsonSerializer.Deserialize<DeltaAction>(line);
                    if (action == null)
                    {
                        throw new Exception("Failed to deserialize action");
                    }
                    replay.Apply(action, fromCheckpoint: false);
                }
            }
        }

        /// <summary>
        /// Snapshot state built by replaying actions in log order, following the spec's action reconciliation.
        /// </summary>
        private sealed class LogReplay
        {
            private readonly Dictionary<DeltaFileKey, DeltaAddAction> _adds = new Dictionary<DeltaFileKey, DeltaAddAction>();
            private readonly Dictionary<DeltaFileKey, DeltaRemoveFileAction>? _tombstones;
            private readonly Dictionary<string, DeltaTransactionAction> _transactions = new Dictionary<string, DeltaTransactionAction>();
            private readonly Dictionary<string, DeltaDomainMetadataAction> _domains = new Dictionary<string, DeltaDomainMetadataAction>();
            private DeltaMetadataAction? _metadata;
            private bool _metadataFromCheckpoint;
            private DeltaProtocolAction? _protocol;
            private DeltaMetadataAction? _parsedSchemaSource;
            private StructType? _parsedSchema;

            public LogReplay(bool skipTombstones)
            {
                _tombstones = skipTombstones ? null : new Dictionary<DeltaFileKey, DeltaRemoveFileAction>();
            }

            public bool HasMetadataAndProtocol => _metadata != null && _protocol != null;

            // A checkpoint schema that cannot be parsed rules out the checkpoint, unless a later commit replaced the metadata
            public Exception? CheckpointSchemaFailure()
            {
                if (!_metadataFromCheckpoint)
                {
                    return null;
                }
                try
                {
                    ParseSchema();
                    return null;
                }
                catch (Exception e)
                {
                    return e;
                }
            }

            // Parsed once per metadata action
            public StructType ParseSchema()
            {
                if (_metadata == null)
                {
                    throw new Exception("No metadata found for the table");
                }
                if (_parsedSchema != null && ReferenceEquals(_parsedSchemaSource, _metadata))
                {
                    return _parsedSchema;
                }

                var schemaJsonOptions = new JsonSerializerOptions();
                schemaJsonOptions.Converters.Add(new TypeConverter());
                var schema = JsonSerializer.Deserialize<SchemaBaseType>(_metadata.SchemaString!, schemaJsonOptions);

                if (schema!.Type != SchemaType.Struct)
                {
                    throw new Exception("Schema type must be struct");
                }

                _parsedSchemaSource = _metadata;
                _parsedSchema = (schema as StructType)!;
                return _parsedSchema;
            }

            public void Apply(DeltaAction action, bool fromCheckpoint)
            {
                if (action.Add != null)
                {
                    // Checkpoint adds are the table content, so they always count as data
                    if (fromCheckpoint)
                    {
                        action.Add.DataChange = true;
                    }
                    var key = action.Add.GetKey();
                    _adds[key] = action.Add;
                    _tombstones?.Remove(key);
                }
                if (action.Remove != null)
                {
                    var key = action.Remove.GetKey();
                    _adds.Remove(key);
                    if (_tombstones != null)
                    {
                        _tombstones[key] = action.Remove;
                    }
                }
                if (action.MetaData != null)
                {
                    _metadata = action.MetaData;
                    _metadataFromCheckpoint = fromCheckpoint;
                }
                if (action.Protocol != null)
                {
                    _protocol = action.Protocol;
                }
                // The latest txn per application wins, versions are not assumed to grow
                if (action.Txn?.AppId != null)
                {
                    _transactions[action.Txn.AppId] = action.Txn;
                }
                if (action.DomainMetadata?.Domain != null)
                {
                    _domains[action.DomainMetadata.Domain] = action.DomainMetadata;
                }
            }

            public DeltaTable ToTable(long version, long? startCheckpointVersion)
            {
                if (_metadata == null)
                {
                    throw new Exception("No metadata found for the table");
                }
                if (_protocol == null)
                {
                    throw new Exception("No protocol found for the table");
                }

                var schema = ParseSchema();

                // Removed domains only suppress older ones
                var domains = new Dictionary<string, DeltaDomainMetadataAction>();
                foreach (var domain in _domains)
                {
                    if (!domain.Value.Removed)
                    {
                        domains.Add(domain.Key, domain.Value);
                    }
                }

                var tombstones = _tombstones != null ? _tombstones.Values.ToList() : new List<DeltaRemoveFileAction>();
                return new DeltaTable(_metadata, _protocol, _adds.Values.ToList(), schema, version, tombstones, _transactions, domains, startCheckpointVersion);
            }
        }

        public static async Task<DeltaCommit?> ReadVersionCommit(IFileStorage storage, IOPath table, long version)
        {
            var fileName = $"{version.ToString("D20")}.json";

            var deltaLogDir = table.Combine(DeltaLogDirName);
            if (!await storage.Exists(new IOPath(deltaLogDir, fileName)))
            {
                return null;
            }

            using var commitData = await storage.OpenRead(new IOPath(deltaLogDir, fileName));
            // Removed between the check and the read
            if (commitData == null)
            {
                return null;
            }

            using var textReader = new StreamReader(commitData);

            var line = await textReader.ReadLineAsync();

            List<DeltaAddAction> addedFiles = new List<DeltaAddAction>();
            List<DeltaRemoveFileAction> removedFiles = new List<DeltaRemoveFileAction>();
            List<DeltaCdcAction> cdcActions = new List<DeltaCdcAction>();

            DeltaMetadataAction? metadata = null;
            DeltaCommitInfoAction? commitInfo = null;

            while (line != null)
            {

                var action = JsonSerializer.Deserialize<DeltaAction>(line);
                if (action == null)
                {
                    throw new Exception("Failed to deserialize action");
                }

                if (action.Add != null)
                {
                    addedFiles.Add(action.Add);
                }
                if (action.Remove != null)
                {
                    removedFiles.Add(action.Remove);
                }
                if (action.MetaData != null)
                {
                    metadata = action.MetaData;
                }
                if (action.Cdc != null)
                {
                    cdcActions.Add(action.Cdc);
                }
                if (action.CommitInfo != null)
                {
                    commitInfo = action.CommitInfo;
                }

                line = await textReader.ReadLineAsync();
                if (line == null)
                {
                    break;
                }
            }

            return new DeltaCommit(addedFiles, removedFiles, cdcActions, metadata, commitInfo);
        }

        private static DeltaBaseAction? ToGenericAction(DeltaAction action)
        {
            if (action.Add != null)
            {
                return action.Add;
            }
            else if (action.MetaData != null)
            {
                return action.MetaData;
            }
            else if (action.Protocol != null)
            {
                return action.Protocol;
            }
            else if (action.CommitInfo != null)
            {
                return action.CommitInfo;
            }

            return null;
        }

        /// <summary>
        /// Files in the log directory whose names start with a dot, such as staged commits.
        /// </summary>
        public static async Task<IReadOnlyList<IOEntry>> ListHiddenLogFiles(IFileStorage storage, IOPath tableName)
        {
            var files = await storage.Ls(tableName.Combine(DeltaLogDirName));
            return files.Where(x => !x.Path.IsFolder && x.Name.StartsWith('.')).ToList();
        }

        /// <summary>
        /// The commits and classic checkpoints of the log, ordered by version.
        /// </summary>
        public static async Task<IReadOnlyList<LogTransactionFile>> ReadTransactionLog(IFileStorage storage, IOPath tableName)
        {
            var log = await ListLog(storage, tableName);
            return log.Commits.Values.Concat(log.Checkpoints).OrderBy(x => x.Version).ThenBy(x => x.IsCheckpoint ? 0 : 1).ToList();
        }

        // A log compaction {start}.{end}.compacted.json covers up to its second version
        private static long CoveredVersion(string name, long version)
        {
            if (name.Length > 41 && name[20] == '.' && long.TryParse(name.AsSpan(21, 20), out var end))
            {
                return Math.Max(version, end);
            }
            return version;
        }

        internal static async Task<LogListing> ListLog(IFileStorage storage, IOPath tableName)
        {
            var log = new LogListing();
            var files = await storage.Ls(tableName.Combine(DeltaLogDirName));
            foreach (var file in files)
            {
                if (file.Path.IsFolder || !TryGetVersion(file.Name, out var version))
                {
                    continue;
                }

                if (HasSuffix(file.Name, CommitSuffix))
                {
                    log.Commits[version] = new LogTransactionFile(file.Name, false, true, version, file);
                }
                else if (HasSuffix(file.Name, ClassicCheckpointSuffix))
                {
                    log.Checkpoints.Add(new LogTransactionFile(file.Name, true, false, version, file));
                }
                else
                {
                    // UUID-named or multi-part checkpoints, log compactions and checksums still prove the table exists
                    log.HasOtherVersionFiles = true;
                    log.MaxOtherVersion = Math.Max(log.MaxOtherVersion, CoveredVersion(file.Name, version));
                    continue;
                }
                log.Head = Math.Max(log.Head, version);
            }
            log.Checkpoints.Sort((left, right) => right.Version.CompareTo(left.Version));
            return log;
        }

        // The suffix directly follows the version digits
        private static bool HasSuffix(string name, string suffix)
        {
            return name.Length == VersionDigits + suffix.Length && name.EndsWith(suffix, StringComparison.Ordinal);
        }

        private static bool TryGetVersion(string name, out long version)
        {
            version = 0;
            if (name.Length <= VersionDigits || name[VersionDigits] != '.')
            {
                return false;
            }
            for (int i = 0; i < VersionDigits; i++)
            {
                if (!char.IsAsciiDigit(name[i]))
                {
                    return false;
                }
            }
            return long.TryParse(name.AsSpan(0, VersionDigits), NumberStyles.None, CultureInfo.InvariantCulture, out version);
        }

        /// <summary>
        /// The commits and classic checkpoints of one listing of the log.
        /// </summary>
        internal sealed class LogListing
        {
            public Dictionary<long, LogTransactionFile> Commits { get; } = new Dictionary<long, LogTransactionFile>();

            // Newest first
            public List<LogTransactionFile> Checkpoints { get; } = new List<LogTransactionFile>();

            public long Head { get; set; } = -1;

            public bool HasOtherVersionFiles { get; set; }

            // The highest version the other version files cover, -1 without any
            public long MaxOtherVersion { get; set; } = -1;
        }
    }
}

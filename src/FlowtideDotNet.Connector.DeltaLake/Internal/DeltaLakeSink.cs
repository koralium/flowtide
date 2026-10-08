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
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using Stowage;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaLakePendingCommit
    {
        public long Version { get; set; }

        public string StagedFile { get; set; } = string.Empty;

        /// <summary>
        /// Null for commits staged before stage ids existed.
        /// </summary>
        public string? StageId { get; set; }

        public long? Length { get; set; }

        /// <summary>
        /// The checkpoint that staged the commit, it is published once that checkpoint is committed.
        /// </summary>
        public long? CheckpointId { get; set; }
    }

    internal class DeltaLakeSink : WriteBaseOperator
    {
        private const int MaxRowsPerFile = 10_000_000;

        /// <summary>
        /// Called with the table name before the temporary tree is committed.
        /// </summary>
        internal static Action<string>? TemporaryTreeCommitHookForTests;

        /// <summary>
        /// Run by table name on the sink's thread once the catalog holds the published head, before it is used.
        /// </summary>
        internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Func<DeltaSinkCatalog, Task>> CatalogHooksForTests = new System.Collections.Concurrent.ConcurrentDictionary<string, Func<DeltaSinkCatalog, Task>>();

        private readonly DeltaLakeOptions _options;
        private readonly WriteRelation _writeRelation;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTree;
        private string _tableName;
        private IOPath _tablePath;

        private IObjectState<bool>? _firstInsertDone;
        private IObjectState<DeltaLakePendingCommit>? _pendingCommit;
        // True while the committed temporary tree may hold rows that wait for a publication
        private IObjectState<bool>? _bufferDurable;
        // The staged bytes and catalog changes of this run's pending commit
        private CommitBytes? _stagedBytes;
        private CatalogOverlay? _stagedOverlay;
        private DeltaSinkCatalog? _catalog;
        private ProbeBatch? _probes;
        private bool _halted;
        private string? _haltReason;
        private bool _gaugesCreated;
        private List<string> _createdFiles = new List<string>();
        private readonly HashSet<string> _checkpointSkipReasons = new HashSet<string>();
        private readonly HashSet<string> _reportedCheckpointFailures = new HashSet<string>();
        private readonly HashSet<string> _unknownCodecs = new HashSet<string>();
        private string _fileExtension = ".parquet";

        public DeltaLakeSink(DeltaLakeOptions options, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this._options = options;
            this._writeRelation = writeRelation;
            _tableName = string.Join("/", writeRelation.NamedObject.Names);
            _tablePath = _tableName;

            if (writeRelation.TableSchema.Struct != null)
            {
                for (int i = 0; i < writeRelation.TableSchema.Struct.Types.Count; i++)
                {
                    if (writeRelation.TableSchema.Struct.Types[i].Type == Substrait.Type.SubstraitType.Any)
                    {
                        var columnName = writeRelation.TableSchema.Names[i];
                        throw new NotSupportedException($"Delta Lake Sink does not support columns of type Any, destination: '{_tableName}', columnName: '{columnName}'");
                    }
                }
            }
        }

        public override string DisplayName => $"DeltaLakeSink({_tableName})";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _temporaryTree = await stateManagerClient.GetOrCreateTree("temporary", new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(_writeRelation.OutputLength),
                KeySerializer = new ColumnStoreSerializer(_writeRelation.OutputLength, MemoryAllocator),
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                MemoryAllocator = MemoryAllocator,
                UseByteBasedPageSizes = true,
            });
            _firstInsertDone = await stateManagerClient.GetOrCreateObjectStateAsync<bool>("isFirstInsert");
            _pendingCommit = await stateManagerClient.GetOrCreateObjectStateAsync<DeltaLakePendingCommit>("pendingCommit");
            _bufferDurable = await stateManagerClient.GetOrCreateObjectStateAsync<bool>("bufferDurable");

            // Vertices are reused across restarts
            _stagedBytes = null;
            _stagedOverlay = null;
            _halted = false;
            SetHealth(true);
            _catalog?.Dispose();
            _catalog = await DeltaSinkCatalog.Open(stateManagerClient, _options, _tablePath, _writeRelation.TableSchema.Names, MemoryAllocator, Logger);
            _probes = null;

            // Meters outlive restarts
            if (!_gaugesCreated)
            {
                _gaugesCreated = true;
                Metrics.CreateObservableGauge("delta_catalog_files", () => _catalog?.LiveFiles ?? 0);
                Metrics.CreateObservableGauge("delta_catalog_pruning_columns", () => _catalog?.ScanLayout.Columns.Count ?? 0);
                Metrics.CreateObservableGauge("delta_catalog_pruning_bytes", () => _catalog?.PruningBytes ?? 0);
                Metrics.CreateObservableGauge("delta_catalog_tree_mode", () => _catalog?.TreeMode == true ? 1 : 0);
                Metrics.CreateObservableGauge("delta_catalog_rotations", () => _catalog?.Rotations ?? 0);
                Metrics.CreateObservableGauge("delta_catalog_records_migrated", () => _catalog?.RecordsMigrated ?? 0);
                Metrics.CreateObservableGauge("delta_catalog_bytes_migrated", () => _catalog?.BytesMigrated ?? 0);
                Metrics.CreateObservableGauge("delta_catalog_last_slice_ms", () => _catalog?.LastSliceTime.TotalMilliseconds ?? 0);
                Metrics.CreateObservableGauge("delta_catalog_last_clear_ms", () => _catalog?.LastClearTime.TotalMilliseconds ?? 0);
            }
        }

        public override ValueTask DisposeAsync()
        {
            _catalog?.Dispose();
            _catalog = null;
            return base.DisposeAsync();
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_firstInsertDone != null);
            Debug.Assert(_pendingCommit != null);
            Debug.Assert(_bufferDurable != null);
            Debug.Assert(_catalog != null);

            // Another table may need the memory, applied here so a halted sink still gives it up
            _catalog.ApplyRevocation();

            // Stop skipped the commit, rows wait in the tree.
            if (_pendingCommit.Value == null)
            {
                await SaveData();
            }
            else
            {
                await KeepWaitingRows();
            }
            await _firstInsertDone.Commit();
            await _pendingCommit.Commit();
            await _bufferDurable.Commit();
        }

        // Rows that wait for an unpublished commit must survive a restart, the checkpoint already moved the sources past them
        private async Task KeepWaitingRows()
        {
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_bufferDurable != null);

            if (_bufferDurable.Value || await HasRows())
            {
                TemporaryTreeCommitHookForTests?.Invoke(_tableName);
                await _temporaryTree.Commit();
                _bufferDurable.Value = true;
            }
        }

        private async Task<bool> HasRows()
        {
            Debug.Assert(_temporaryTree != null);

            using var iterator = _temporaryTree.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {
                if (page.Values.Data.Count > 0)
                {
                    return true;
                }
            }
            return false;
        }

        // Empties the tree after its rows were staged
        private async Task ClearRows()
        {
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_bufferDurable != null);

            if (!_bufferDurable.Value)
            {
                await _temporaryTree.Clear();
                return;
            }

            // Clear would orphan the committed pages, deletes release them
            while (true)
            {
                EventBatchData? keys = null;
                using (var iterator = _temporaryTree.CreateIterator())
                {
                    await iterator.SeekFirst();
                    await foreach (var page in iterator)
                    {
                        if (page.Keys.Data.Count > 0)
                        {
                            var columns = new IColumn[page.Keys.Data.Columns.Count];
                            for (int i = 0; i < columns.Length; i++)
                            {
                                columns[i] = page.Keys.Data.Columns[i].Copy(MemoryAllocator);
                            }
                            keys = new EventBatchData(columns);
                            break;
                        }
                    }
                }
                if (keys == null)
                {
                    break;
                }
                using (keys)
                {
                    for (int i = 0; i < keys.Count; i++)
                    {
                        var key = new ColumnRowReference() { referenceBatch = keys, RowIndex = i };
                        await _temporaryTree.Delete(in key);
                    }
                }
            }
            TemporaryTreeCommitHookForTests?.Invoke(_tableName);
            await _temporaryTree.Commit();
            _bufferDurable.Value = false;
        }

        public override async Task CommitVersion(long version)
        {
            Debug.Assert(_pendingCommit != null);

            var pendingCommit = _pendingCommit.Value;
            if (pendingCommit == null || !IsPublishable(pendingCommit, version))
            {
                return;
            }

            // The first attempt of a commit staged in this run follows the listing that chose its version
            var checkSuccessor = _stagedBytes == null || _halted;
            var result = await DeltaTransactionWriter.PublishCommit(_options.StorageLocation, _tablePath, pendingCommit, _stagedBytes, checkSuccessor);
            if (result.Outcome == PublishOutcome.FailedClosed)
            {
                Halt(result.Reason!);
                return;
            }
            if (_halted)
            {
                _halted = false;
                SetHealth(true);
                Logger.LogInformation("Delta table {table} published version {version}, the sink continues", _tableName, pendingCommit.Version);
            }
            // Applied by the next SaveData, never while OnRecieve runs
            if (_stagedOverlay != null)
            {
                _catalog!.MarkPublished(_stagedOverlay);
                _stagedOverlay = null;
            }
            _stagedBytes = null;

            if (_options.CheckpointInterval > 0 && pendingCommit.Version > 0 && (pendingCommit.Version % _options.CheckpointInterval == 0))
            {
                try
                {
                    // The checkpoint writer does not write tombstones, so they are not read
                    var currentTableState = await DeltaTransactionReader.ReadTable(_options.StorageLocation, _tablePath, options: new DeltaReadOptions() { SkipTombstones = true, Logger = Logger, ReportedCheckpointFailures = _reportedCheckpointFailures });
                    if (currentTableState != null)
                    {
                        var skipReason = CheckpointSkipReason(currentTableState);
                        if (skipReason == null)
                        {
                            var codec = ResolveCodec(currentTableState.Metadata.Configuration);
                            await DeltaCheckpointWriter.WriteCheckpoint(_options.StorageLocation, _tablePath, currentTableState, codec.WriterProperties);
                        }
                        else if (_checkpointSkipReasons.Add(skipReason))
                        {
                            Logger.LogWarning("Delta table {table} {reason}, no checkpoint is written", _tableName, skipReason);
                        }
                    }
                }
                catch (Exception e)
                {
                    // Checkpoint is optional, next interval tries again.
                    Logger.LogWarning(e, "Failed to write delta checkpoint for version {version} of table {table}", pendingCommit.Version, _tableName);
                }
            }

            _pendingCommit.Value = null;
        }

        /// <summary>
        /// A stop drain cycle stages after the stop checkpoint, its rows are replayed after the restart and must not be published.
        /// </summary>
        internal static bool IsPublishable(DeltaLakePendingCommit pending, long version)
        {
            return !pending.CheckpointId.HasValue || pending.CheckpointId.Value <= version;
        }

        // The sink keeps buffering rows, a later checkpoint publishes once the conflict is gone
        private void Halt(string reason)
        {
            // Logged again only when the reason changes
            if (_halted && reason == _haltReason)
            {
                return;
            }
            _halted = true;
            _haltReason = reason;
            SetHealth(false);
            Logger.LogError("Delta table {table} stopped publishing: {reason}. Rows are kept and publishing is tried again at every checkpoint. Remove the conflicting commit file while no later version exists, or reset the stream state, which writes every row again: INSERT OVERWRITE replaces the table, INSERT INTO appends to it", _tableName, reason);
        }

        private DeltaParquetCodec ResolveCodec(IReadOnlyDictionary<string, string>? configuration)
        {
            var codec = DeltaParquetCompression.Resolve(configuration, _options.CompressionCodec, out var unknownValue);
            if (unknownValue != null && _unknownCodecs.Add(unknownValue))
            {
                Logger.LogWarning("Delta table {table} has the unsupported compression codec {codec}, zstd is used instead", _tableName, unknownValue);
            }
            return codec;
        }

        // Null when the checkpoint writer can write this snapshot without losing anything
        private static string? CheckpointSkipReason(DeltaTable table)
        {
            // The checkpoint writer cannot keep txn or domainMetadata actions yet, a checkpoint would drop them
            if (table.Transactions.Count > 0 || table.DomainMetadata.Count > 0)
            {
                return "has txn or domainMetadata actions that checkpoints cannot keep yet";
            }
            // The spec requires numRecords on every file with a deletion vector
            foreach (var file in table.AddFiles)
            {
                if (file.DeletionVector != null && !HasRecordCount(file.Statistics))
                {
                    return "has files with deletion vectors but no record count";
                }
            }
            return null;
        }

        // A top level, non negative integer numRecords
        private static bool HasRecordCount(string? statistics)
        {
            if (statistics == null)
            {
                return false;
            }
            try
            {
                using var document = JsonDocument.Parse(statistics);
                return document.RootElement.ValueKind == JsonValueKind.Object &&
                    document.RootElement.TryGetProperty("numRecords", out var recordCount) &&
                    recordCount.ValueKind == JsonValueKind.Number &&
                    recordCount.TryGetInt64(out var value) &&
                    value >= 0;
            }
            catch (JsonException)
            {
                return false;
            }
        }

        private async Task SaveData()
        {
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_firstInsertDone != null);
            Debug.Assert(_pendingCommit != null);
            Debug.Assert(_catalog != null);

            await _catalog.ApplyPublished();
            if (_catalog.IsReady && CatalogHooksForTests.TryGetValue(_tableName, out var hook))
            {
                await hook(_catalog);
            }
            var readOptions = new DeltaReadOptions() { SkipTombstones = true, Logger = Logger, ReportedCheckpointFailures = _reportedCheckpointFailures };
            // A first read lists the log itself
            var validated = false;
            if (!_catalog.IsReady)
            {
                await _catalog.Validate(readOptions);
                validated = true;
            }
            var overwrite = !_firstInsertDone.Value && _writeRelation.Overwrite;
            if (_catalog.Header != null && !overwrite && !await HasRows())
            {
                await ClearRows();
                _firstInsertDone.Value = true;
                await _catalog.Maintain();
                return;
            }
            // Before the version, the codec or any file is chosen, another writer may have moved the head
            if (!validated)
            {
                await _catalog.Validate(readOptions);
            }
            var table = _catalog.Header;

            using var iterator = _temporaryTree.CreateIterator();
            await iterator.SeekFirst();

            long nextVersion = 0;
            DeltaTable? newHeader = null;
            List<DeltaAction> actions = new List<DeltaAction>();
            var currentTime = _options.TimeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            var stageId = Guid.NewGuid().ToString("N");
            _createdFiles = new List<string>();
            long adoptedAt;

            bool changeDataEnabled = false;
            bool deletionVectorEnabled = false;
            bool columnMappingEnabled = false;

            int maxColumnId = 0;

            StructType? schema;
            IReadOnlyDictionary<string, string>? configuration;
            if (table == null)
            {
                changeDataEnabled = _options.WriteChangeDataOnNewTables;
                deletionVectorEnabled = _options.EnableDeletionVectorsOnNewTables;
                columnMappingEnabled = _options.EnableColumnMappingOnNewTables;

                // Create schema
                schema = SubstraitTypeToDeltaType.GetSchema(_writeRelation.TableSchema, ref maxColumnId, columnMappingEnabled);

                var jsonOptions = new JsonSerializerOptions();
                jsonOptions.Converters.Add(new TypeConverter());
                var schemaString = JsonSerializer.Serialize(schema as SchemaBaseType, jsonOptions);

                adoptedAt = 0;
                actions.Add(new DeltaAction()
                {
                    CommitInfo = new DeltaCommitInfoAction()
                    {
                        StageId = stageId,
                        Timestamp = currentTime,
                        AdoptedAt = adoptedAt,
                        CreatedFiles = _createdFiles,
                        Data = new Dictionary<string, object>()
                        {
                            { "operation", "CREATE TABLE" }
                        }
                    }
                });

                var tableConfiguration = new Dictionary<string, string>();
                configuration = tableConfiguration;

                if (changeDataEnabled)
                {
                    tableConfiguration.Add("delta.enableChangeDataFeed", "true");
                }
                if (deletionVectorEnabled)
                {
                    tableConfiguration.Add("delta.enableDeletionVectors", "true");
                }
                if (columnMappingEnabled)
                {
                    tableConfiguration.Add("delta.columnMapping.mode", "name");
                    tableConfiguration.Add("delta.columnMapping.maxColumnId", maxColumnId.ToString());
                }

                var metadataAction = new DeltaMetadataAction()
                {
                    Id = Guid.NewGuid().ToString(),
                    SchemaString = schemaString,
                    Configuration = tableConfiguration,
                    Format = new DeltaMetadataFormat()
                    {
                        Provider = "parquet",
                        Options = new Dictionary<string, string>()
                    },
                    PartitionColumns = new List<string>(),
                    CreatedTime = currentTime
                };
                actions.Add(new DeltaAction()
                {
                    MetaData = metadataAction
                });

                var writerFeatures = new List<string>();
                if (deletionVectorEnabled)
                {
                    writerFeatures.Add("deletionVectors");
                }
                if (changeDataEnabled)
                {
                    writerFeatures.Add("changeDataFeed");
                }

                var readerFeatures = new List<string>();
                if (deletionVectorEnabled)
                {
                    readerFeatures.Add("deletionVectors");
                }
                if (columnMappingEnabled)
                {
                    writerFeatures.Add("columnMapping");
                    readerFeatures.Add("columnMapping");
                }

                var protocolAction = new DeltaProtocolAction()
                {
                    MinReaderVersion = 3,
                    MinWriterVersion = 7,
                    ReaderFeatures = readerFeatures,
                    WriterFeatures = writerFeatures
                };
                actions.Add(new DeltaAction()
                {
                    Protocol = protocolAction
                });
                newHeader = new DeltaTable(metadataAction, protocolAction, new List<DeltaAddAction>(), schema, 0, System.Array.Empty<DeltaRemoveFileAction>(), new Dictionary<string, DeltaTransactionAction>(), new Dictionary<string, DeltaDomainMetadataAction>(), null);
            }
            else
            {
                schema = table.Schema;
                configuration = table.Metadata.Configuration;
                nextVersion = _catalog.Head + 1;
                changeDataEnabled = table.ChangeDataEnabled;

                adoptedAt = _catalog.AdoptedAt;
                actions.Add(new DeltaAction()
                {
                    CommitInfo = new DeltaCommitInfoAction()
                    {
                        StageId = stageId,
                        Timestamp = currentTime,
                        AdoptedAt = adoptedAt,
                        CreatedFiles = _createdFiles,
                        Data = new Dictionary<string, object>() { { "operation", "WRITE" } }
                    }
                });

                if (table.PartitionColumns.Count > 0)
                {
                    throw new NotImplementedException("Partition columns are not implemented yet");
                }

            }

            var overlay = _catalog.BeginCommit(nextVersion, adoptedAt, newHeader);
            if (table != null && overwrite)
            {
                await foreach (var (_, record) in _catalog.ScanFiles())
                {
                    actions.Add(new DeltaAction()
                    {
                        Remove = new DeltaRemoveFileAction()
                        {
                            Path = record.Path,
                            DeletionVector = record.DeletionVector,
                            DataChange = true,
                            DeletionTimestamp = currentTime,
                            Stats = record.Statistics,
                            Size = record.Size,
                            PartitionValues = record.PartitionValues,
                        }
                    });
                }
                overlay.RemoveAll = true;
                _catalog.RestartIds();
            }

            var codec = ResolveCodec(configuration);
            _fileExtension = codec.FileExtension;
            var writer = new ParquetSharpWriter(schema, _writeRelation.TableSchema.Names, writerProperties: codec.WriterProperties);
            // Only used to compare deleted rows, never written
            var deleteWriter = new ParquetSharpWriter(schema, _writeRelation.TableSchema.Names);

            ParquetSharpWriter? cdcWriter = default;
            if (changeDataEnabled)
            {
                cdcWriter = new ParquetSharpWriter(schema, _writeRelation.TableSchema.Names, isCdcWriter: true, writerProperties: codec.WriterProperties);
                cdcWriter.NewBatch();
            }

            writer.NewBatch();
            deleteWriter.NewBatch();

            // Flag that tracks if any delete was written, if this is false, no cdc file is required even if it is enabled.
            bool deleteWritten = false;

            // Deleted rows of the current batch and their probes, matched against the catalog per batch
            var pendingDeletes = new List<RowToDelete>();
            _probes ??= new ProbeBatch(_catalog.ScanLayout);
            _probes.Clear();
            _probes.SetLayout(_catalog.ScanLayout);
            var fileDeleteVectors = new Dictionary<int, ModifiableDeleteVector>();
            var touchedFiles = new Dictionary<int, DeltaFileRecord>();
            await foreach (var page in iterator)
            {
                for (int i = 0; i < page.Values.Data.Count; i++)
                {
                    var weight = page.Values.Data[i];
                    if (weight < 0)
                    {
                        deleteWritten = true;
                        int deleteIndex = deleteWriter.WrittenCount;
                        var rowRef = new ColumnRowReference() { referenceBatch = page.Keys.Data, RowIndex = i };
                        deleteWriter.AddRow(rowRef);
                        if (cdcWriter != null)
                        {
                            // One change row per deleted copy, rolled per copy so a large weight stays bounded
                            for (int v = 0; v > weight; v--)
                            {
                                cdcWriter.AddRow(rowRef, true);
                                if (IsFull(cdcWriter))
                                {
                                    await WriteNewCdcFile(cdcWriter, actions, currentTime);
                                }
                            }
                        }

                        pendingDeletes.Add(new RowToDelete()
                        {
                            DeleteIndex = deleteIndex,
                            Weight = weight
                        });
                        _probes.Add(rowRef);
                    }
                    else
                    {
                        // One row per copy, rolled per copy so a large weight stays bounded
                        var rowRef = new ColumnRowReference() { referenceBatch = page.Keys.Data, RowIndex = i };
                        for (int v = 0; v < weight; v++)
                        {
                            writer.AddRow(rowRef);
                            if (IsFull(writer))
                            {
                                await WriteNewFile(writer, actions, currentTime, overlay);
                            }
                            if (cdcWriter != null)
                            {
                                cdcWriter.AddRow(rowRef);
                                if (IsFull(cdcWriter))
                                {
                                    await WriteNewCdcFile(cdcWriter, actions, currentTime);
                                }
                            }
                        }
                    }
                }

                // If we found more than 100k deletes, handle them and and scan files and update deletion vectors
                if (deleteWriter.WrittenCount >= 100_000)
                {
                    if (table == null)
                    {
                        throw new InvalidOperationException("Table should not be null when delete is found");
                    }
                    using (var deleteBatch = deleteWriter.GetRecordBatch())
                    {
                        await HandleDeletedRows(pendingDeletes, overwrite, table, fileDeleteVectors, touchedFiles, deleteBatch);
                    }
                    deleteWriter.NewBatch();
                }
            }

            if (deleteWriter.WrittenCount > 0)
            {
                if (table == null)
                {
                    throw new InvalidOperationException("Table should not be null when delete is found");
                }
                using (var deleteBatch = deleteWriter.GetRecordBatch())
                {
                    await HandleDeletedRows(pendingDeletes, overwrite, table, fileDeleteVectors, touchedFiles, deleteBatch);
                }
            }

            await WriteDeleteFiles(fileDeleteVectors, touchedFiles, table, actions, currentTime, writer, overlay);

            if (writer.WrittenCount > 0)
            {
                await WriteNewFile(writer, actions, currentTime, overlay);
            }
            if (cdcWriter != null)
            {
                if (deleteWritten)
                {
                    if (cdcWriter.WrittenCount > 0)
                    {
                        await WriteNewCdcFile(cdcWriter, actions, currentTime);
                    }
                }
                else
                {
                    // Remove written cdc files since there was no delete, the rows are included in the add actions
                    await RemoveWrittenCdcFiles(actions);
                }
            }

            if (table == null || HasChanges(actions))
            {
                // Published in CommitVersion once the version is final.
                var staged = await DeltaTransactionWriter.StageCommit(_options.StorageLocation, _tablePath, nextVersion, actions);
                _pendingCommit.Value = new DeltaLakePendingCommit()
                {
                    Version = nextVersion,
                    StagedFile = staged.FileName,
                    StageId = staged.StageId,
                    Length = staged.Length,
                    CheckpointId = CurrentCheckpointId
                };
                _stagedBytes = staged.Bytes;
                _stagedOverlay = overlay;
            }

            // Last thing we do is clear the temporary tree, if the write fails we might need the tree again to recompute the files
            await ClearRows();
            // Set that the first insert is done, this is used to determine if we need to write delete files for overwrite writes
            _firstInsertDone.Value = true;
            await _catalog.Maintain();
        }

        // A commit without file, metadata or protocol actions is not written
        internal static bool HasChanges(List<DeltaAction> actions)
        {
            foreach (var action in actions)
            {
                if (action.Add != null || action.Remove != null || action.Cdc != null || action.MetaData != null || action.Protocol != null || action.DomainMetadata != null)
                {
                    return true;
                }
            }
            return false;
        }

        private async Task RemoveWrittenCdcFiles(List<DeltaAction> actions)
        {
            for (int i = 0; i < actions.Count; i++)
            {
                var action = actions[i];
                if (action.Cdc != null)
                {
                    await _options.StorageLocation.Rm(_tablePath.Combine(action.Cdc.Path));
                    _createdFiles.Remove(action.Cdc.Path!);
                    actions.RemoveAt(i);
                    i--;
                }
            }
        }

        private async Task WriteDeleteFiles(
            Dictionary<int, ModifiableDeleteVector> fileDeleteVectors,
            Dictionary<int, DeltaFileRecord> touchedFiles,
            DeltaTable? table,
            List<DeltaAction> actions,
            long currentTime,
            ParquetSharpWriter writer,
            CatalogOverlay overlay)
        {
            foreach (var deleteFile in fileDeleteVectors)
            {
                if (table == null)
                {
                    throw new InvalidOperationException("Table should not be null when delete is found");
                }
                var existingFile = touchedFiles[deleteFile.Key];
                actions.Add(new DeltaAction()
                {
                    Remove = new DeltaRemoveFileAction()
                    {
                        Path = existingFile.Path,
                        DeletionVector = existingFile.DeletionVector,
                        DataChange = true,
                        DeletionTimestamp = currentTime,
                        Stats = existingFile.Statistics,
                        Size = existingFile.Size,
                        PartitionValues = existingFile.PartitionValues
                    }
                });

                var deletePercentage = (double)deleteFile.Value.Cardinality / (double)(existingFile.NumRecords ?? 0);

                // Use delete vectors if it is enabled, there is file statistics and the percentage deleted is less than 10%.
                if (table.DeleteVectorEnabled && !double.IsNaN(deletePercentage) && deletePercentage < 0.1)
                {
                    var roaringBitmap = deleteFile.Value.ToRoaringBitmapArray();

                    // Write delete vector here to file
                    var (deletePath, z85string) = DeletionVectorWriter.GenerateDestination();

                    _createdFiles.Add(deletePath);
                    var (_, dataSize) = await DeletionVectorWriter.WriteDeletionVector(_options.StorageLocation, _tablePath, deletePath, roaringBitmap);

                    var reAdded = existingFile.ToAdd().WithDeletionVector(new DeletionVector()
                    {
                        Cardinality = roaringBitmap.Cardinality,
                        Offset = 1,
                        StorageType = "u",
                        PathOrInlineDv = z85string,
                        SizeInBytes = dataSize
                    });
                    actions.Add(new DeltaAction()
                    {
                        Add = reAdded
                    });
                    // Same file and bounds, only the record changes
                    overlay.Updates.Add((deleteFile.Key, DeltaFileRecord.FromAdd(reAdded, existingFile.NumRecords)));
                }
                else
                {
                    await writer.CopyFrom(_options.StorageLocation, _tablePath, existingFile.Path, deleteFile.Value, IsFull, () => WriteNewFile(writer, actions, currentTime, overlay));
                    overlay.Removes.Add(deleteFile.Key);
                }
            }
        }

        // Error messages only, enumerates each column up to the row
        private static string DescribeRow(RecordBatch batch, int index)
        {
            var values = new List<string>(batch.ColumnCount);
            for (int c = 0; c < batch.ColumnCount; c++)
            {
                var array = batch.Column(c);
                try
                {
                    values.Add(array.IsNull(index) ? "null" : array switch
                    {
                        Decimal128Array decimals => decimals.GetValue(index)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        System.Collections.IEnumerable items => items.Cast<object?>().ElementAt(index)?.ToString() ?? "null",
                        _ => array.Data.DataType.Name
                    });
                }
                catch (Exception)
                {
                    values.Add(array.Data.DataType.Name);
                }
            }
            return $"{{{string.Join(",", values)}}}";
        }

        // One pass over the catalog for the batch, then a scan of every candidate file
        private async Task HandleDeletedRows(
            List<RowToDelete> rows,
            bool overwrite,
            DeltaTable? table,
            Dictionary<int, ModifiableDeleteVector> fileDeleteVectors,
            Dictionary<int, DeltaFileRecord> touchedFiles,
            RecordBatch deleteBatch)
        {
            Debug.Assert(_catalog != null);
            Debug.Assert(_probes != null);

            if (table == null)
            {
                throw new InvalidOperationException("Table should not be null when delete is found");
            }
            // An overwrite removes every file, a delete in the same commit finds none
            var candidates = overwrite ? new Dictionary<int, List<int>>() : await _catalog.FindCandidates(_probes);
            var matched = new bool[rows.Count];
            foreach (var probes in candidates.Values)
            {
                foreach (var probe in probes)
                {
                    matched[probe] = true;
                }
            }
            for (int i = 0; i < matched.Length; i++)
            {
                if (!matched[i])
                {
                    throw new InvalidOperationException($"Could not find any data file that contains the row {DescribeRow(deleteBatch, rows[i].DeleteIndex)}");
                }
            }

            var missing = candidates.Keys.Where(x => !touchedFiles.ContainsKey(x)).ToList();
            foreach (var (id, record) in await _catalog.GetFiles(missing))
            {
                touchedFiles[id] = record;
            }
            var comparer = RecordBatchComparer.Create(table.Schema);
            foreach (var (id, probes) in candidates)
            {
                var toFind = new List<RowToDelete>(probes.Count);
                foreach (var probe in probes)
                {
                    toFind.Add(rows[probe]);
                }
                await ScanDataFileForRows(table, toFind, id, touchedFiles[id], fileDeleteVectors, deleteBatch, comparer);
            }
            rows.Clear();
            _probes.Clear();
        }

        private async Task WriteNewCdcFile(ParquetSharpWriter cdcWriter, List<DeltaAction> actions, long currentTime)
        {
            string addFilePath = $"_change_data/cdc-00000-{Guid.NewGuid().ToString()}{_fileExtension}";

            _createdFiles.Add(addFilePath);
            var fileSize = await cdcWriter.WriteData(_options.StorageLocation, _tablePath, addFilePath);
            actions.Add(new DeltaAction()
            {
                Cdc = new DeltaCdcAction()
                {
                    // Change files never change table data (spec)
                    DataChange = false,
                    PartitionValues = new Dictionary<string, string>(),
                    Path = addFilePath,
                    Size = fileSize
                }
            });
            cdcWriter.NewBatch();
        }

        // The row cap bounds buffering when a byte estimate is wrong
        private bool IsFull(ParquetSharpWriter writer)
        {
            return writer.WrittenBytes >= _options.MaxFileSizeBytes || writer.WrittenCount >= MaxRowsPerFile;
        }

        private async Task WriteNewFile(ParquetSharpWriter writer, List<DeltaAction> actions, long currentTime, CatalogOverlay overlay)
        {
            string addFilePath = $"part-00000-{Guid.NewGuid().ToString()}{_fileExtension}";

            var stats = writer.GetStatistics();
            var statsString = JsonSerializer.Serialize(stats, overlay.StatisticsOptions);

            _createdFiles.Add(addFilePath);
            var fileSize = await writer.WriteData(_options.StorageLocation, _tablePath, addFilePath);
            var add = new DeltaAddAction()
            {
                Path = addFilePath,
                PartitionValues = new Dictionary<string, string>(),
                Size = fileSize,
                ModificationTime = currentTime,
                DataChange = true,
                Statistics = statsString
            };
            actions.Add(new DeltaAction()
            {
                Add = add
            });
            overlay.Add(_catalog!.AllocateId(), add);
            writer.NewBatch();
        }

        private async Task ScanDataFileForRows(
            DeltaTable table,
            List<RowToDelete> toFind,
            int fileId,
            DeltaFileRecord file,
            Dictionary<int, ModifiableDeleteVector> deleteVectors,
            RecordBatch deleteBatch,
            RecordBatchComparer comparer)
        {
            ParquetSharpReader reader = new ParquetSharpReader();
            reader.Initialize(table, _writeRelation.TableSchema.Names);

            IDeleteVector? deleteVector;
            if (file.DeletionVector != null)
            {
                deleteVector = await DeletionVectorReader.ReadDeletionVector(_options.StorageLocation, _tablePath, file.DeletionVector);
            }
            else
            {
                deleteVector = EmptyDeleteVector.Instance;
            }

            // If a modified delete vector already exist, use it instead
            if (deleteVectors.TryGetValue(fileId, out var claimed))
            {
                deleteVector = claimed;
            }

            if (reader.Fields == null)
            {
                throw new InvalidOperationException("Fields should not be null");
            }

            // Open file without deletion vector, it will be used when finding rows
            var iterator = reader.ReadDataFileArrowFormat(_options.StorageLocation, _tablePath, file.Path);

            int globalOffset = 0;
            await foreach (var batch in iterator)
            {
                using (batch)
                {
                    for (int i = 0; i < toFind.Count; i++)
                    {
                        var row = toFind[i];
                        bool resolved = false;
                        int searchFrom = 0;
                        // A weight of -k needs k matches, copies can share a batch
                        while (!resolved)
                        {
                            // Another file may have found the row already
                            lock (row.Lock)
                            {
                                if (row.Weight == 0)
                                {
                                    resolved = true;
                                    break;
                                }
                            }
                            int index = comparer.FindOccurance(row.DeleteIndex, deleteBatch, batch, globalOffset, deleteVector, searchFrom);
                            if (index < 0)
                            {
                                break;
                            }
                            searchFrom = index + 1;
                            // Positions claimed in this scan are checked per match, not per scanned row
                            if (claimed != null && claimed.Contains(index + globalOffset))
                            {
                                continue;
                            }
                            lock (row.Lock)
                            {
                                if (row.Weight == 0)
                                {
                                    resolved = true;
                                    break;
                                }
                                row.Weight++;
                                resolved = row.Weight == 0;
                            }
                            lock (deleteVectors)
                            {
                                if (claimed == null)
                                {
                                    claimed = new ModifiableDeleteVector(deleteVector);
                                    deleteVectors.Add(fileId, claimed);
                                }
                                claimed.Add(index + globalOffset);
                            }
                        }
                        if (resolved)
                        {
                            toFind.RemoveAt(i);
                            i--;
                        }
                    }
                    globalOffset += batch.Length;
                }
                // Nothing left to find in this file
                if (toFind.Count == 0)
                {
                    break;
                }
            }
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_temporaryTree != null);

            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var rowRef = new ColumnRowReference() { referenceBatch = msg.Data.EventBatchData, RowIndex = i };
                await _temporaryTree.RMWNoResult(in rowRef, msg.Data.Weights[i], (input, current, exists) =>
                {
                    if (exists)
                    {
                        var newWeight = current + input;
                        if (newWeight == 0)
                        {
                            return (0, GenericWriteOperation.Delete);
                        }
                        return (newWeight, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
            }
        }
    }
}

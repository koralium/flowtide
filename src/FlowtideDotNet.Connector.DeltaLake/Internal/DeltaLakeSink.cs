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
    }

    internal class DeltaLakeSink : WriteBaseOperator
    {
        private const int MaxRowsPerFile = 10_000_000;

        private readonly DeltaLakeOptions _options;
        private readonly WriteRelation _writeRelation;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTree;
        private string _tableName;
        private IOPath _tablePath;

        private IObjectState<bool>? _firstInsertDone;
        private IObjectState<DeltaLakePendingCommit>? _pendingCommit;
        private readonly HashSet<string> _checkpointSkipReasons = new HashSet<string>();
        private readonly HashSet<string> _reportedCheckpointFailures = new HashSet<string>();

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
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_firstInsertDone != null);
            Debug.Assert(_pendingCommit != null);

            // Stop skipped the commit, rows wait in the tree.
            if (_pendingCommit.Value == null)
            {
                await SaveData();
            }
            await _firstInsertDone.Commit();
            await _pendingCommit.Commit();
        }

        public override async Task CommitVersion(long version)
        {
            Debug.Assert(_pendingCommit != null);

            var pendingCommit = _pendingCommit.Value;
            if (pendingCommit == null)
            {
                return;
            }

            await DeltaTransactionWriter.PublishCommit(_options.StorageLocation, _tablePath, pendingCommit.Version, pendingCommit.StagedFile);

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
                            await DeltaCheckpointWriter.WriteCheckpoint(_options.StorageLocation, _tablePath, currentTableState);
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

            using var iterator = _temporaryTree.CreateIterator();
            await iterator.SeekFirst();

            var table = await DeltaTransactionReader.ReadTable(_options.StorageLocation, _tablePath, options: new DeltaReadOptions() { SkipTombstones = true, Logger = Logger, ReportedCheckpointFailures = _reportedCheckpointFailures });

            long nextVersion = 0;
            List<DeltaAction> actions = new List<DeltaAction>();
            var currentTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            bool changeDataEnabled = false;
            bool deletionVectorEnabled = false;
            bool columnMappingEnabled = false;

            int maxColumnId = 0;

            StructType? schema;
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

                actions.Add(new DeltaAction()
                {
                    CommitInfo = new DeltaCommitInfoAction()
                    {
                        Data = new Dictionary<string, object>()
                        {
                            { "operation", "CREATE TABLE" }
                        }
                    }
                });

                var tableConfiguration = new Dictionary<string, string>();

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

                actions.Add(new DeltaAction()
                {
                    Protocol = new DeltaProtocolAction()
                    {
                        MinReaderVersion = 3,
                        MinWriterVersion = 7,
                        ReaderFeatures = readerFeatures,
                        WriterFeatures = writerFeatures
                    }
                });
            }
            else
            {
                schema = table.Schema;
                nextVersion = table.Version + 1;
                changeDataEnabled = table.ChangeDataEnabled;

                actions.Add(new DeltaAction()
                {
                    CommitInfo = new DeltaCommitInfoAction()
                    {
                        Data = new Dictionary<string, object>() { { "operation", "WRITE" } }
                    }
                });

                if (table.PartitionColumns.Count > 0)
                {
                    throw new NotImplementedException("Partition columns are not implemented yet");
                }

                if (!_firstInsertDone.Value && _writeRelation.Overwrite)
                {
                    table.AddFiles.ForEach(x =>
                    {
                        actions.Add(new DeltaAction()
                        {
                            Remove = new DeltaRemoveFileAction()
                            {
                                Path = x.Path,
                                DeletionVector = x.DeletionVector,
                                DataChange = true,
                                DeletionTimestamp = currentTime,
                                Stats = x.Statistics,
                                Size = x.Size,
                                PartitionValues = x.PartitionValues,
                            }
                        });
                    });
                    table.AddFiles.Clear();
                    table.Files.Clear();
                }
            }

            var writer = new ParquetSharpWriter(schema, _writeRelation.TableSchema.Names);
            var deleteWriter = new ParquetSharpWriter(schema, _writeRelation.TableSchema.Names);

            ParquetSharpWriter? cdcWriter = default;
            if (changeDataEnabled)
            {
                cdcWriter = new ParquetSharpWriter(schema, _writeRelation.TableSchema.Names, isCdcWriter: true);
                cdcWriter.NewBatch();
            }

            writer.NewBatch();
            deleteWriter.NewBatch();

            // Flag that tracks if any delete was written, if this is false, no cdc file is required even if it is enabled.
            bool deleteWritten = false;

            Dictionary<string, List<RowToDelete>> rowsToDeleteByFile = new Dictionary<string, List<RowToDelete>>();
            Dictionary<string, ModifiableDeleteVector> fileDeleteVectors = new Dictionary<string, ModifiableDeleteVector>();
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

                        var rowToDelete = new RowToDelete()
                        {
                            DeleteIndex = deleteIndex,
                            Weight = weight
                        };

                        bool foundFile = false;
                        for (int f = 0; f < table!.Files.Count; f++)
                        {
                            var file = table.Files[f];
                            if (file.CanBeInFile(rowRef, _writeRelation.TableSchema.Names))
                            {
                                foundFile = true;
                                if (!rowsToDeleteByFile.TryGetValue(file.Action.Path!, out var deleteRowList))
                                {
                                    deleteRowList = new List<RowToDelete>();
                                    rowsToDeleteByFile.Add(file.Action.Path!, deleteRowList);
                                }
                                deleteRowList.Add(rowToDelete);
                            }
                        }
                        if (!foundFile)
                        {
                            throw new InvalidOperationException($"Could not find any data file that contains the row {rowRef}");
                        }
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
                                await WriteNewFile(writer, actions, currentTime, schema);
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
                        await HandleDeletedRows(rowsToDeleteByFile, table, fileDeleteVectors, deleteBatch);
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
                    await HandleDeletedRows(rowsToDeleteByFile, table, fileDeleteVectors, deleteBatch);
                }
            }

            await WriteDeleteFiles(fileDeleteVectors, table, actions, currentTime, writer, schema);

            if (writer.WrittenCount > 0)
            {
                await WriteNewFile(writer, actions, currentTime, schema);
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

            // Published in CommitVersion once the version is final.
            var stagedFile = await DeltaTransactionWriter.StageCommit(_options.StorageLocation, _tablePath, nextVersion, actions);
            _pendingCommit.Value = new DeltaLakePendingCommit()
            {
                Version = nextVersion,
                StagedFile = stagedFile
            };

            // Last thing we do is clear the temporary tree, if the write fails we might need the tree again to recompute the files
            await _temporaryTree.Clear();
            // Set that the first insert is done, this is used to determine if we need to write delete files for overwrite writes
            _firstInsertDone.Value = true;
        }

        private async Task RemoveWrittenCdcFiles(List<DeltaAction> actions)
        {
            for (int i = 0; i < actions.Count; i++)
            {
                var action = actions[i];
                if (action.Cdc != null)
                {
                    await _options.StorageLocation.Rm(_tablePath.Combine(action.Cdc.Path));
                    actions.RemoveAt(i);
                    i--;
                }
            }
        }

        private async Task WriteDeleteFiles(
            Dictionary<string, ModifiableDeleteVector> fileDeleteVectors,
            DeltaTable? table,
            List<DeltaAction> actions,
            long currentTime,
            ParquetSharpWriter writer,
            StructType schema)
        {
            foreach (var deleteFile in fileDeleteVectors)
            {
                if (table == null)
                {
                    throw new InvalidOperationException("Table should not be null when delete is found");
                }
                var existingFile = table.Files.First(x => x.Action.Path == deleteFile.Key);
                actions.Add(new DeltaAction()
                {
                    Remove = new DeltaRemoveFileAction()
                    {
                        Path = deleteFile.Key,
                        DeletionVector = existingFile.Action.DeletionVector,
                        DataChange = true,
                        DeletionTimestamp = currentTime,
                        Stats = existingFile.Action.Statistics,
                        Size = existingFile.Action.Size,
                        PartitionValues = existingFile.Action.PartitionValues
                    }
                });

                var deletePercentage = (double)deleteFile.Value.Cardinality / (double)existingFile.Statistics.NumRecords;

                // Use delete vectors if it is enabled, there is file statistics and the percentage deleted is less than 10%.
                if (table.DeleteVectorEnabled && !double.IsNaN(deletePercentage) && deletePercentage < 0.1)
                {
                    var roaringBitmap = deleteFile.Value.ToRoaringBitmapArray();

                    // Write delete vector here to file
                    var (deletePath, z85string) = DeletionVectorWriter.GenerateDestination();

                    var (_, dataSize) = await DeletionVectorWriter.WriteDeletionVector(_options.StorageLocation, _tablePath, deletePath, roaringBitmap);

                    actions.Add(new DeltaAction()
                    {
                        Add = existingFile.Action.WithDeletionVector(new DeletionVector()
                        {
                            Cardinality = roaringBitmap.Cardinality,
                            Offset = 1,
                            StorageType = "u",
                            PathOrInlineDv = z85string,
                            SizeInBytes = dataSize
                        })
                    });
                }
                else
                {
                    await writer.CopyFrom(_options.StorageLocation, _tablePath, existingFile.Action.Path!, deleteFile.Value, IsFull, () => WriteNewFile(writer, actions, currentTime, schema));
                }
            }
        }

        private async Task HandleDeletedRows(
            Dictionary<string, List<RowToDelete>> rowsToDeleteByFile,
            DeltaTable table,
            Dictionary<string, ModifiableDeleteVector> fileDeleteVectors,
            RecordBatch deleteBatch)
        {
            var comparer = RecordBatchComparer.Create(table.Schema);
            foreach (var fileWithPossibleDelete in rowsToDeleteByFile)
            {
                // This can be made into tasks later on
                var file = table!.AddFiles.First(x => x.Path == fileWithPossibleDelete.Key);
                await ScanDataFileForRows(table, fileWithPossibleDelete.Value, file, fileDeleteVectors, deleteBatch, comparer);
            }
            rowsToDeleteByFile.Clear();
        }

        private async Task WriteNewCdcFile(ParquetSharpWriter cdcWriter, List<DeltaAction> actions, long currentTime)
        {
            string addFilePath = $"_change_data/cdc-00000-{Guid.NewGuid().ToString()}.snappy.parquet";

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

        private async Task WriteNewFile(ParquetSharpWriter writer, List<DeltaAction> actions, long currentTime, StructType schema)
        {
            string addFilePath = $"part-00000-{Guid.NewGuid().ToString()}.snappy.parquet";

            var stats = writer.GetStatistics();

            JsonSerializerOptions jsonOptions = new JsonSerializerOptions();
            jsonOptions.Converters.Add(new DeltaStatisticsConverter(schema));
            var statsString = JsonSerializer.Serialize(stats, jsonOptions);

            var fileSize = await writer.WriteData(_options.StorageLocation, _tablePath, addFilePath);
            actions.Add(new DeltaAction()
            {
                Add = new DeltaAddAction()
                {
                    Path = addFilePath,
                    PartitionValues = new Dictionary<string, string>(),
                    Size = fileSize,
                    ModificationTime = currentTime,
                    DataChange = true,
                    Statistics = statsString
                }
            });
            writer.NewBatch();
        }

        private async Task ScanDataFileForRows(
            DeltaTable table,
            List<RowToDelete> toFind,
            DeltaAddAction file,
            Dictionary<string, ModifiableDeleteVector> deleteVectors,
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
            if (deleteVectors.TryGetValue(file.Path!, out var claimed))
            {
                deleteVector = claimed;
            }

            if (reader.Fields == null)
            {
                throw new InvalidOperationException("Fields should not be null");
            }

            // Open file without deletion vector, it will be used when finding rows
            var iterator = reader.ReadDataFileArrowFormat(_options.StorageLocation, _tablePath, file.Path!);

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
                                    deleteVectors.Add(file.Path!, claimed);
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

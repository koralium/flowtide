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
using Stowage;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaLakeSink : WriteBaseOperator
    {
        private readonly DeltaLakeOptions _options;
        private readonly WriteRelation _writeRelation;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTree;
        private string _tableName;
        private IOPath _tablePath;

        public DeltaLakeSink(DeltaLakeOptions options, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this._options = options;
            this._writeRelation = writeRelation;
            _tableName = string.Join("/", writeRelation.NamedObject.Names);
            _tablePath = _tableName;
        }

        public override string DisplayName => "DeltaLakeSink";

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
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            return SaveData();
        }

        private async Task SaveData()
        {
            Debug.Assert(_temporaryTree != null);
            using var iterator = _temporaryTree.CreateIterator();
            await iterator.SeekFirst();

            var table = await DeltaTransactionReader.ReadTable(_options.StorageLocation, _tablePath);

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
                deletionVectorEnabled = table.DeleteVectorEnabled;

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
                            cdcWriter.AddRow(rowRef, true);
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
                        // Make sure to handle duplicate rows
                        for (int v = 0; v < weight; v++)
                        {
                            var rowRef = new ColumnRowReference() { referenceBatch = page.Keys.Data, RowIndex = i };
                            writer.AddRow(rowRef);
                            if (cdcWriter != null)
                            {
                                cdcWriter.AddRow(rowRef);
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

                // Write max 100k rows per file for now, a user must call optimize in another framework to increase the file size
                if (writer.WrittenCount >= 100_000)
                {
                    await WriteNewFile(writer, actions, currentTime, schema);
                }
                if (cdcWriter != null && cdcWriter.WrittenCount >= 100_000)
                {
                    await WriteNewCdcFile(cdcWriter, actions, currentTime);
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

            await WriteDeleteFiles(fileDeleteVectors, table, actions, currentTime, writer);

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

            await DeltaTransactionWriter.WriteCommit(_options.StorageLocation, _tablePath, nextVersion, actions);

            // Last thing we do is clear the temporary tree, if the write fails we might need the tree again to recompute the files
            await _temporaryTree.Clear();
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
            ParquetSharpWriter writer)
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

                    var fileSize = await DeletionVectorWriter.WriteDeletionVector(_options.StorageLocation, _tablePath, deletePath, roaringBitmap);

                    actions.Add(new DeltaAction()
                    {
                        Add = new DeltaAddAction()
                        {
                            Path = deleteFile.Key,
                            Size = existingFile.Action.Size,
                            Statistics = existingFile.Action.Statistics,
                            PartitionValues = existingFile.Action.PartitionValues,
                            DataChange = existingFile.Action.DataChange,
                            ModificationTime = currentTime,
                            DeletionVector = new DeletionVector()
                            {
                                Cardinality = deleteFile.Value.Cardinality,
                                Offset = 1,
                                StorageType = "u",
                                PathOrInlineDv = z85string,
                                SizeInBytes = fileSize
                            }
                        }
                    });
                }
                else
                {
                    await writer.CopyFrom(_options.StorageLocation, _tablePath, existingFile.Action.Path!, deleteFile.Value);
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
                    DataChange = true,
                    PartitionValues = new Dictionary<string, string>(),
                    Path = addFilePath,
                    Size = fileSize
                }
            });
            cdcWriter.NewBatch();
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
            if (deleteVectors.TryGetValue(file.Path!, out var vector))
            {
                deleteVector = vector;
            }

            if (reader.Fields == null)
            {
                throw new InvalidOperationException("Fields should not be null");
            }

            // Open file without deletion vector, it will be used when finding rows
            var iterator = reader.ReadDataFileArrowFormat(_options.StorageLocation, _tablePath, file.Path!);

            await foreach (var batch in iterator)
            {
                for (int i = 0; i < toFind.Count; i++)
                {
                    // Lock to see if the row has been found in another file
                    lock (toFind[i].Lock)
                    {
                        if (toFind[i].Weight == 0)
                        {
                            toFind.RemoveAt(i);
                            i--;
                            continue;
                        }
                    }
                    int index = comparer.FindOccurance(toFind[i].DeleteIndex, deleteBatch, batch, 0, deleteVector);
                    if (index >= 0)
                    {
                        lock (toFind[i].Lock)
                        {
                            // See if the row has been found in another file
                            if (toFind[i].Weight == 0)
                            {
                                toFind.RemoveAt(i);
                                i--;
                                continue;
                            }
                            // If not increase the weight until we reach 0
                            toFind[i].Weight++;
                            // check if the object has been removed completely
                            if (toFind[i].Weight == 0)
                            {
                                toFind.RemoveAt(i);
                                i--;
                            }
                        }
                        lock (deleteVectors)
                        {
                            if (!deleteVectors.TryGetValue(file.Path!, out var modifiedVector))
                            {
                                modifiedVector = new ModifiableDeleteVector(deleteVector);
                                deleteVectors.Add(file.Path!, modifiedVector);
                            }
                            modifiedVector.Add(index);
                        }
                    }
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

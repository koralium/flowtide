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

using FlowtideDotNet.Base;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Options;
using Stowage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static SqlParser.Ast.Privileges;

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

            StructType? schema;
            if (table == null)
            {
                // Create schema
                schema = SubstraitTypeToDeltaType.GetSchema(_writeRelation.TableSchema);

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

                actions.Add(new DeltaAction()
                {
                    MetaData = new DeltaMetadataAction()
                    {
                        Id = Guid.NewGuid().ToString(),
                        SchemaString = schemaString,
                        Configuration = new Dictionary<string, string>(),
                        Format = new DeltaMetadataFormat()
                        {
                            Provider = "parquet",
                            Options = new Dictionary<string, string>()
                        },
                        PartitionColumns = new List<string>(),
                        CreatedTime = currentTime
                    }
                });
                actions.Add(new DeltaAction()
                {
                    Protocol = new DeltaProtocolAction()
                    {
                        MinReaderVersion = 3,
                        MinWriterVersion = 7,
                        ReaderFeatures = new List<string>() { "deletionVectors" },
                        WriterFeatures = new List<string>() { "deletionVectors" }
                    }
                });
            }
            else
            {
                schema = table.Schema;
                nextVersion = table.Version + 1;
                actions.Add(new DeltaAction()
                {
                    CommitInfo = new DeltaCommitInfoAction()
                    {
                        Data = new Dictionary<string, object>() { { "operation", "WRITE" } }
                    }
                });
            }

            var writer = new ParquetSharpWriter(schema, _writeRelation.TableSchema.Names);

            writer.NewBatch();

            Dictionary<string, List<RowToDelete>> rowsToDeleteByFile = new Dictionary<string, List<RowToDelete>>();

            Dictionary<string, ModifiableDeleteVector> fileDeleteVectors = new Dictionary<string, ModifiableDeleteVector>();
            List<LeafNode<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>> negativeWeightPages = new List<LeafNode<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>>();
            await foreach(var page in iterator)
            {
                bool addedToNegativeWeights = false;

                for (int i = 0; i < page.Values.Data.Count; i++)
                {
                    var weight = page.Values.Data[i];
                    if (weight < 0)
                    {
                        if (!addedToNegativeWeights)
                        {
                            addedToNegativeWeights = true;

                            if (!page.CurrentPage.TryRent())
                            {
                                throw new InvalidOperationException("Could not rent page");
                            }

                            // Add the page to negative weight pages if the page has negative weights
                            negativeWeightPages.Add(page.CurrentPage);

                            var rowToDelete = new RowToDelete()
                            {
                                RowReference = new ColumnRowReference() { referenceBatch = page.Keys.Data, RowIndex = i },
                                Weight = weight
                            };

                            for (int f = 0; f < table!.Files.Count; f++)
                            {
                                var file = table.Files[f];
                                if (file.CanBeInFile(rowToDelete.RowReference, _writeRelation.TableSchema.Names))
                                {
                                    if (!rowsToDeleteByFile.TryGetValue(file.Action.Path!, out var deleteRowList))
                                    {
                                        deleteRowList = new List<RowToDelete>();
                                        rowsToDeleteByFile.Add(file.Action.Path!, deleteRowList);
                                    }
                                    deleteRowList.Add(rowToDelete);
                                }
                            }

                        }
                    }
                    else
                    {
                        // Make sure to handle duplicate rows
                        for (int v = 0; v < weight; v++)
                        {
                            writer.AddRow(new ColumnRowReference() { referenceBatch = page.Keys.Data, RowIndex = i});
                        }
                    }
                }

                // How many pages of data that should be processed at the same time when searching existing data
                if (negativeWeightPages.Count == 100)
                {
                    if (table == null)
                    {
                        throw new InvalidOperationException("Table should not be null when delete is found");
                    }
                    await HandleDeletedRows(rowsToDeleteByFile, table, fileDeleteVectors, negativeWeightPages);
                }

                // Write max 100k rows per file for now, a user must call optimize in another framework to increase the file size
                if (writer.WrittenCount >= 100_000)
                {
                    await WriteNewFile(writer, actions, currentTime, schema);
                }
            }

            if (negativeWeightPages.Count > 0)
            {
                if (table == null)
                {
                    throw new InvalidOperationException("Table should not be null when delete is found");
                }
                await HandleDeletedRows(rowsToDeleteByFile, table, fileDeleteVectors, negativeWeightPages);
            }

            await WriteDeleteFiles(fileDeleteVectors, table, actions, currentTime, writer);

            if (writer.WrittenCount > 0)
            {
                await WriteNewFile(writer, actions, currentTime, schema);
            }

            await DeltaTransactionWriter.WriteCommit(_options.StorageLocation, _tablePath, nextVersion, actions);

            // Last thing we do is clear the temporary tree, if the write fails we might need the tree again to recompute the files
            await _temporaryTree.Clear();
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
            List<LeafNode<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>> negativeWeightPages)
        {
            foreach (var fileWithPossibleDelete in rowsToDeleteByFile)
            {
                // This can be made into tasks later on
                var file = table!.AddFiles.First(x => x.Path == fileWithPossibleDelete.Key);
                await ScanDataFileForRows(table, fileWithPossibleDelete.Value, file, fileDeleteVectors);
            }

            foreach (var negativePage in negativeWeightPages)
            {
                negativePage.Return();
            }
            negativeWeightPages.Clear();
            rowsToDeleteByFile.Clear();
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

        private async Task ScanDataFileForRows(DeltaTable table, List<RowToDelete> toFind, DeltaAddAction file, Dictionary<string, ModifiableDeleteVector> deleteVectors)
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
            var iterator = reader.ReadDataFile(_options.StorageLocation, _tablePath, file.Path!, EmptyDeleteVector.Instance, default, MemoryAllocator);
            
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
                    int index = FindRowInBatch.FindRow(toFind[i].RowReference, batch.data, deleteVector, reader.Fields);
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

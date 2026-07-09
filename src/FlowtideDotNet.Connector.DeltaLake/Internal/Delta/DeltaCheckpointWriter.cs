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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using Stowage;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    internal class LastCheckpointInfo
    {
        [JsonPropertyName("version")]
        public long Version { get; set; }

        [JsonPropertyName("size")]
        public long Size { get; set; }
    }

    internal static class DeltaCheckpointWriter
    {
        public static async Task WriteCheckpoint(IFileStorage storage, IOPath tablePath, DeltaTable table)
        {
            var nextVersion = table.Version;
            var addActions = table.AddFiles;
            var metadata = table.Metadata;
            var protocol = table.Protocol;

            var recordBatch = BuildCheckpointRecordBatch(protocol, metadata, addActions);

            var checkpointFileName = $"{nextVersion.ToString("D20")}.checkpoint.parquet";
            var checkpointFilePath = tablePath.Combine(DeltaTransactionWriter.DeltaLogDirName).Combine(checkpointFileName);

            using (var stream = await storage.OpenWrite(checkpointFilePath))
            {
                if (stream == null)
                {
                    throw new Exception($"Failed to open stream for writing checkpoint: {checkpointFilePath}");
                }

                using (var deltaStream = new DeltaWriteStream(stream))
                {
                    using (var writer = new ParquetSharp.Arrow.FileWriter(deltaStream, recordBatch.Schema))
                    {
                        writer.WriteRecordBatch(recordBatch);
                        writer.Close();
                    }
                }
            }

            // Now write the _last_checkpoint file
            var lastCheckpointFilePath = tablePath.Combine(DeltaTransactionWriter.DeltaLogDirName).Combine("_last_checkpoint");

            var lastCheckpointContent = JsonSerializer.Serialize(new LastCheckpointInfo
            {
                Version = nextVersion,
                Size = recordBatch.Length
            });

            using (var stream = await storage.OpenWrite(lastCheckpointFilePath))
            {
                if (stream == null)
                {
                    throw new Exception($"Failed to open stream for writing _last_checkpoint: {lastCheckpointFilePath}");
                }

                using (var writer = new StreamWriter(stream))
                {
                    await writer.WriteAsync(lastCheckpointContent);
                }
            }
        }

        private static RecordBatch BuildCheckpointRecordBatch(DeltaProtocolAction protocol, DeltaMetadataAction metadata, IReadOnlyList<DeltaAddAction> addActions)
        {
            int totalRows = 2 + addActions.Count;
            int protocolRowIndex = 0;
            int metadataRowIndex = 1;
            int addRowStart = 2;

            var txnArray = BuildTxnStructArray(totalRows);
            var addArray = BuildAddStructArray(addActions, addRowStart, totalRows);
            var removeArray = BuildRemoveStructArray(totalRows);
            var metadataArray = BuildMetadataStructArray(metadata, metadataRowIndex, totalRows);
            var protocolArray = BuildProtocolStructArray(protocol, protocolRowIndex, totalRows);

            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("txn", txnArray.Data.DataType, true))
                .Field(new Field("add", addArray.Data.DataType, true))
                .Field(new Field("remove", removeArray.Data.DataType, true))
                .Field(new Field("metaData", metadataArray.Data.DataType, true))
                .Field(new Field("protocol", protocolArray.Data.DataType, true))
                .Build();

            return new RecordBatch(schema, new IArrowArray[]
            {
                txnArray,
                addArray,
                removeArray,
                metadataArray,
                protocolArray
            }, totalRows);
        }

        private static MapArray BuildStringMapArray(IEnumerable<Dictionary<string, string>?> maps, int totalRows)
        {
            var keyBuilder = new StringArray.Builder();
            var valueBuilder = new StringArray.Builder();
            var offsetBuilder = new ArrowBuffer.Builder<int>();
            var nullBitmap = new ArrowBuffer.BitmapBuilder();

            int currentOffset = 0;
            offsetBuilder.Append(currentOffset);

            int nullCount = 0;
            foreach (var map in maps)
            {
                if (map == null)
                {
                    nullBitmap.Append(false);
                    nullCount++;
                    offsetBuilder.Append(currentOffset);
                }
                else
                {
                    nullBitmap.Append(true);
                    foreach (var kvp in map)
                    {
                        keyBuilder.Append(kvp.Key);
                        valueBuilder.Append(kvp.Value);
                    }
                    currentOffset += map.Count;
                    offsetBuilder.Append(currentOffset);
                }
            }

            var keyArray = keyBuilder.Build();
            var valueArray = valueBuilder.Build();

            var structType = new StructType(new[]
            {
                new Field("key", new StringType(), false),
                new Field("value", new StringType(), true)
            });

            var structArray = new StructArray(structType, keyArray.Length, new IArrowArray[] { keyArray, valueArray }, ArrowBuffer.Empty, 0, 0);
            var mapType = new MapType(new StringType(), new StringType(), keySorted: false);

            return new MapArray(mapType, totalRows, offsetBuilder.Build(), structArray, nullBitmap.Build(), nullCount);
        }

        private static ListArray BuildStringListArray(IEnumerable<IEnumerable<string>?> lists, int totalRows)
        {
            var valueBuilder = new StringArray.Builder();
            var offsetBuilder = new ArrowBuffer.Builder<int>();
            var nullBitmap = new ArrowBuffer.BitmapBuilder();

            int currentOffset = 0;
            offsetBuilder.Append(currentOffset);

            int nullCount = 0;
            foreach (var list in lists)
            {
                if (list == null)
                {
                    nullBitmap.Append(false);
                    nullCount++;
                    offsetBuilder.Append(currentOffset);
                }
                else
                {
                    nullBitmap.Append(true);
                    int count = 0;
                    foreach (var item in list)
                    {
                        valueBuilder.Append(item);
                        count++;
                    }
                    currentOffset += count;
                    offsetBuilder.Append(currentOffset);
                }
            }

            var valueArray = valueBuilder.Build();
            var listType = new ListType(new StringType());

            return new ListArray(listType, totalRows, offsetBuilder.Build(), valueArray, nullBitmap.Build(), nullCount);
        }

        private static StructArray BuildDeletionVectorStructArray(IEnumerable<DeletionVector?> deletionVectors, int totalRows)
        {
            var storageTypeBuilder = new StringArray.Builder();
            var pathOrInlineDvBuilder = new StringArray.Builder();
            var offsetBuilder = new Int32Array.Builder();
            var sizeInBytesBuilder = new Int32Array.Builder();
            var cardinalityBuilder = new Int32Array.Builder();
            var nullBitmap = new ArrowBuffer.BitmapBuilder();

            int nullCount = 0;
            foreach (var dv in deletionVectors)
            {
                if (dv == null)
                {
                    nullBitmap.Append(false);
                    nullCount++;
                    storageTypeBuilder.AppendNull();
                    pathOrInlineDvBuilder.AppendNull();
                    offsetBuilder.AppendNull();
                    sizeInBytesBuilder.AppendNull();
                    cardinalityBuilder.AppendNull();
                }
                else
                {
                    nullBitmap.Append(true);
                    storageTypeBuilder.Append(dv.StorageType);
                    pathOrInlineDvBuilder.Append(dv.PathOrInlineDv);
                    if (dv.Offset.HasValue)
                    {
                        offsetBuilder.Append((int)dv.Offset.Value);
                    }
                    else
                    {
                        offsetBuilder.AppendNull();
                    }
                    sizeInBytesBuilder.Append(dv.SizeInBytes);
                    cardinalityBuilder.Append((int)dv.Cardinality);
                }
            }

            var fields = new List<Field>
            {
                new Field("storageType", new StringType(), true),
                new Field("pathOrInlineDv", new StringType(), true),
                new Field("offset", new Int32Type(), true),
                new Field("sizeInBytes", new Int32Type(), true),
                new Field("cardinality", new Int32Type(), true)
            };
            var structType = new StructType(fields);
            var children = new IArrowArray[]
            {
                storageTypeBuilder.Build(),
                pathOrInlineDvBuilder.Build(),
                offsetBuilder.Build(),
                sizeInBytesBuilder.Build(),
                cardinalityBuilder.Build()
            };

            return new StructArray(structType, totalRows, children, nullBitmap.Build(), nullCount);
        }

        private static StructArray BuildFormatStructArray(IEnumerable<DeltaMetadataFormat?> formats, int totalRows)
        {
            var providerBuilder = new StringArray.Builder();
            var optionsList = new List<Dictionary<string, string>?>();
            var nullBitmap = new ArrowBuffer.BitmapBuilder();

            int nullCount = 0;
            foreach (var f in formats)
            {
                if (f == null)
                {
                    nullBitmap.Append(false);
                    nullCount++;
                    providerBuilder.AppendNull();
                    optionsList.Add(null);
                }
                else
                {
                    nullBitmap.Append(true);
                    providerBuilder.Append(f.Provider);
                    optionsList.Add(f.Options);
                }
            }

            var providerArray = providerBuilder.Build();
            var optionsArray = BuildStringMapArray(optionsList, totalRows);

            var fields = new List<Field>
            {
                new Field("provider", new StringType(), true),
                new Field("options", optionsArray.Data.DataType, true)
            };
            var structType = new StructType(fields);
            var children = new IArrowArray[]
            {
                providerArray,
                optionsArray
            };

            return new StructArray(structType, totalRows, children, nullBitmap.Build(), nullCount);
        }

        private static StructArray BuildTxnStructArray(int totalRows)
        {
            var appIdBuilder = new StringArray.Builder();
            var versionBuilder = new Int64Array.Builder();
            var lastUpdatedBuilder = new Int64Array.Builder();
            var nullBitmap = new ArrowBuffer.BitmapBuilder();

            for (int i = 0; i < totalRows; i++)
            {
                nullBitmap.Append(false);
                appIdBuilder.AppendNull();
                versionBuilder.AppendNull();
                lastUpdatedBuilder.AppendNull();
            }

            var fields = new List<Field>
            {
                new Field("appId", new StringType(), true),
                new Field("version", new Int64Type(), true),
                new Field("lastUpdated", new Int64Type(), true)
            };
            var structType = new StructType(fields);
            var children = new IArrowArray[]
            {
                appIdBuilder.Build(),
                versionBuilder.Build(),
                lastUpdatedBuilder.Build()
            };

            return new StructArray(structType, totalRows, children, nullBitmap.Build(), totalRows);
        }

        private static StructArray BuildRemoveStructArray(int totalRows)
        {
            var pathBuilder = new StringArray.Builder();
            var deletionTimestampBuilder = new Int64Array.Builder();
            var dataChangeBuilder = new BooleanArray.Builder();
            var extendedFileMetadataBuilder = new BooleanArray.Builder();
            var sizeBuilder = new Int64Array.Builder();
            var statsBuilder = new StringArray.Builder();
            var nullBitmap = new ArrowBuffer.BitmapBuilder();

            var partitionValuesList = new List<Dictionary<string, string>?>();
            var deletionVectorList = new List<DeletionVector?>();

            for (int i = 0; i < totalRows; i++)
            {
                nullBitmap.Append(false);
                pathBuilder.AppendNull();
                deletionTimestampBuilder.AppendNull();
                dataChangeBuilder.AppendNull();
                extendedFileMetadataBuilder.AppendNull();
                sizeBuilder.AppendNull();
                statsBuilder.AppendNull();
                partitionValuesList.Add(null);
                deletionVectorList.Add(null);
            }

            var partitionValuesArray = BuildStringMapArray(partitionValuesList, totalRows);
            var deletionVectorArray = BuildDeletionVectorStructArray(deletionVectorList, totalRows);

            var fields = new List<Field>
            {
                new Field("path", new StringType(), true),
                new Field("deletionTimestamp", new Int64Type(), true),
                new Field("dataChange", new BooleanType(), true),
                new Field("extendedFileMetadata", new BooleanType(), true),
                new Field("partitionValues", partitionValuesArray.Data.DataType, true),
                new Field("size", new Int64Type(), true),
                new Field("stats", new StringType(), true),
                new Field("deletionVector", deletionVectorArray.Data.DataType, true)
            };
            var structType = new StructType(fields);
            var children = new IArrowArray[]
            {
                pathBuilder.Build(),
                deletionTimestampBuilder.Build(),
                dataChangeBuilder.Build(),
                extendedFileMetadataBuilder.Build(),
                partitionValuesArray,
                sizeBuilder.Build(),
                statsBuilder.Build(),
                deletionVectorArray
            };

            return new StructArray(structType, totalRows, children, nullBitmap.Build(), totalRows);
        }

        private static StructArray BuildMetadataStructArray(DeltaMetadataAction metadata, int metadataRowIndex, int totalRows)
        {
            var idBuilder = new StringArray.Builder();
            var nameBuilder = new StringArray.Builder();
            var descriptionBuilder = new StringArray.Builder();
            var schemaStringBuilder = new StringArray.Builder();
            var createdTimeBuilder = new Int64Array.Builder();
            var nullBitmap = new ArrowBuffer.BitmapBuilder();

            var formatsList = new List<DeltaMetadataFormat?>();
            var partitionColumnsList = new List<IEnumerable<string>?>();
            var configurationsList = new List<Dictionary<string, string>?>();

            for (int i = 0; i < totalRows; i++)
            {
                if (i == metadataRowIndex)
                {
                    nullBitmap.Append(true);
                    idBuilder.Append(metadata.Id);
                    nameBuilder.Append(metadata.Name);
                    descriptionBuilder.Append(metadata.Description);
                    schemaStringBuilder.Append(metadata.SchemaString);
                    createdTimeBuilder.Append(metadata.CreatedTime);

                    formatsList.Add(metadata.Format);
                    partitionColumnsList.Add(metadata.PartitionColumns);
                    configurationsList.Add(metadata.Configuration);
                }
                else
                {
                    nullBitmap.Append(false);
                    idBuilder.AppendNull();
                    nameBuilder.AppendNull();
                    descriptionBuilder.AppendNull();
                    schemaStringBuilder.AppendNull();
                    createdTimeBuilder.AppendNull();

                    formatsList.Add(null);
                    partitionColumnsList.Add(null);
                    configurationsList.Add(null);
                }
            }

            var formatArray = BuildFormatStructArray(formatsList, totalRows);
            var partitionColumnsArray = BuildStringListArray(partitionColumnsList, totalRows);
            var configurationArray = BuildStringMapArray(configurationsList, totalRows);

            var fields = new List<Field>
            {
                new Field("id", new StringType(), true),
                new Field("name", new StringType(), true),
                new Field("description", new StringType(), true),
                new Field("format", formatArray.Data.DataType, true),
                new Field("schemaString", new StringType(), true),
                new Field("partitionColumns", partitionColumnsArray.Data.DataType, true),
                new Field("configuration", configurationArray.Data.DataType, true),
                new Field("createdTime", new Int64Type(), true)
            };
            var structType = new StructType(fields);
            var children = new IArrowArray[]
            {
                idBuilder.Build(),
                nameBuilder.Build(),
                descriptionBuilder.Build(),
                formatArray,
                schemaStringBuilder.Build(),
                partitionColumnsArray,
                configurationArray,
                createdTimeBuilder.Build()
            };

            return new StructArray(structType, totalRows, children, nullBitmap.Build(), totalRows - 1);
        }

        private static StructArray BuildProtocolStructArray(DeltaProtocolAction protocol, int protocolRowIndex, int totalRows)
        {
            var minReaderVersionBuilder = new Int32Array.Builder();
            var minWriterVersionBuilder = new Int32Array.Builder();
            var nullBitmap = new ArrowBuffer.BitmapBuilder();

            var readerFeaturesList = new List<IEnumerable<string>?>();
            var writerFeaturesList = new List<IEnumerable<string>?>();

            for (int i = 0; i < totalRows; i++)
            {
                if (i == protocolRowIndex)
                {
                    nullBitmap.Append(true);
                    minReaderVersionBuilder.Append(protocol.MinReaderVersion);
                    minWriterVersionBuilder.Append(protocol.MinWriterVersion);
                    readerFeaturesList.Add(protocol.ReaderFeatures);
                    writerFeaturesList.Add(protocol.WriterFeatures);
                }
                else
                {
                    nullBitmap.Append(false);
                    minReaderVersionBuilder.AppendNull();
                    minWriterVersionBuilder.AppendNull();
                    readerFeaturesList.Add(null);
                    writerFeaturesList.Add(null);
                }
            }

            var readerFeaturesArray = BuildStringListArray(readerFeaturesList, totalRows);
            var writerFeaturesArray = BuildStringListArray(writerFeaturesList, totalRows);

            var fields = new List<Field>
            {
                new Field("minReaderVersion", new Int32Type(), true),
                new Field("minWriterVersion", new Int32Type(), true),
                new Field("readerFeatures", readerFeaturesArray.Data.DataType, true),
                new Field("writerFeatures", writerFeaturesArray.Data.DataType, true)
            };
            var structType = new StructType(fields);
            var children = new IArrowArray[]
            {
                minReaderVersionBuilder.Build(),
                minWriterVersionBuilder.Build(),
                readerFeaturesArray,
                writerFeaturesArray
            };

            return new StructArray(structType, totalRows, children, nullBitmap.Build(), totalRows - 1);
        }

        private static StructArray BuildAddStructArray(IReadOnlyList<DeltaAddAction> addActions, int startRowIndex, int totalRows)
        {
            var pathBuilder = new StringArray.Builder();
            var sizeBuilder = new Int64Array.Builder();
            var modificationTimeBuilder = new Int64Array.Builder();
            var dataChangeBuilder = new BooleanArray.Builder();
            var statsBuilder = new StringArray.Builder();
            var nullBitmap = new ArrowBuffer.BitmapBuilder();

            var partitionValuesList = new List<Dictionary<string, string>?>();
            var tagsList = new List<Dictionary<string, string>?>();
            var deletionVectorList = new List<DeletionVector?>();

            for (int i = 0; i < totalRows; i++)
            {
                if (i >= startRowIndex)
                {
                    var add = addActions[i - startRowIndex];
                    nullBitmap.Append(true);
                    pathBuilder.Append(add.Path);
                    sizeBuilder.Append(add.Size);
                    modificationTimeBuilder.Append(add.ModificationTime);
                    dataChangeBuilder.Append(add.DataChange);
                    statsBuilder.Append(add.Statistics);

                    partitionValuesList.Add(add.PartitionValues);
                    tagsList.Add(add.Tags);
                    deletionVectorList.Add(add.DeletionVector);
                }
                else
                {
                    nullBitmap.Append(false);
                    pathBuilder.AppendNull();
                    sizeBuilder.AppendNull();
                    modificationTimeBuilder.AppendNull();
                    dataChangeBuilder.AppendNull();
                    statsBuilder.AppendNull();

                    partitionValuesList.Add(null);
                    tagsList.Add(null);
                    deletionVectorList.Add(null);
                }
            }

            var partitionValuesArray = BuildStringMapArray(partitionValuesList, totalRows);
            var tagsArray = BuildStringMapArray(tagsList, totalRows);
            var deletionVectorArray = BuildDeletionVectorStructArray(deletionVectorList, totalRows);

            var fields = new List<Field>
            {
                new Field("path", new StringType(), true),
                new Field("partitionValues", partitionValuesArray.Data.DataType, true),
                new Field("size", new Int64Type(), true),
                new Field("modificationTime", new Int64Type(), true),
                new Field("dataChange", new BooleanType(), true),
                new Field("stats", new StringType(), true),
                new Field("tags", tagsArray.Data.DataType, true),
                new Field("deletionVector", deletionVectorArray.Data.DataType, true)
            };
            var structType = new StructType(fields);
            var children = new IArrowArray[]
            {
                pathBuilder.Build(),
                partitionValuesArray,
                sizeBuilder.Build(),
                modificationTimeBuilder.Build(),
                dataChangeBuilder.Build(),
                statsBuilder.Build(),
                tagsArray,
                deletionVectorArray
            };

            return new StructArray(structType, totalRows, children, nullBitmap.Build(), startRowIndex);
        }
    }
}

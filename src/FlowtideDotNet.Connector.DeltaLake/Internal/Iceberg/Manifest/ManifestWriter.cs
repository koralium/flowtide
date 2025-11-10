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

using Avro;
using Avro.Generic;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Manifest
{
    internal class ManifestWriter
    {
        public ManifestWriter()
        {
            var partitions = RecordSchema.Create("partition", new List<Field>());


            var lowerBoundsSchema = MapSchema.CreateMap(PrimitiveSchema.Create(Schema.Type.Bytes));

            var dataFileSchema = RecordSchema.Create("data_file", new List<Field>()
            {
                new Field(PrimitiveSchema.Create(Schema.Type.Int), "content", 0, customProperties: new PropertyMap()
                {
                    {"field-id", "134" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.String), "file_path", 1, customProperties: new PropertyMap()
                {
                    {"field-id", "100" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.String), "file_format", 2, customProperties: new PropertyMap()
                {
                    {"field-id", "101" }
                }),
                new Field(partitions, "partition", 3, customProperties: new PropertyMap()
                {
                    {"field-id", "102" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Long), "record_count", 4, customProperties: new PropertyMap()
                {
                    {"field-id", "103" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Long), "file_size_in_bytes", 5, customProperties: new PropertyMap()
                {
                    {"field-id", "104" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Long), "column_sizes", 6, customProperties: new PropertyMap()
                {
                    {"field-id", "108" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Long), "value_counts", 7, customProperties: new PropertyMap()
                {
                    {"field-id", "109" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Long), "null_value_counts", 8, customProperties: new PropertyMap()
                {
                    {"field-id", "110" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Long), "nan_value_counts", 9, customProperties: new PropertyMap()
                {
                    {"field-id", "137" }
                }),
                new Field(lowerBoundsSchema, "lower_bounds", 10, customProperties: new PropertyMap()
                {
                    {"field-id", "125" }
                }),
                new Field(lowerBoundsSchema, "upper_bounds", 11, customProperties: new PropertyMap()
                {
                    {"field-id", "128" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Bytes), "key_metadata", 12, customProperties: new PropertyMap()
                {
                    {"field-id", "111" }
                }),
                new Field(ArraySchema.Create(PrimitiveSchema.Create(Schema.Type.Long)), "split_offsets", 13, customProperties: new PropertyMap()
                {
                    {"field-id", "132" }
                }),
                new Field(ArraySchema.Create(PrimitiveSchema.Create(Schema.Type.Long)), "equality_ids", 14, customProperties: new PropertyMap()
                {
                    {"field-id", "135" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Int), "sort_order_id", 15, customProperties: new PropertyMap()
                {
                    {"field-id", "140" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Long), "first_row_id", 16, customProperties: new PropertyMap()
                {
                    {"field-id", "142" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.String), "referenced_data_file", 17, customProperties: new PropertyMap()
                {
                    {"field-id", "143" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Long), "content_offset", 18, customProperties: new PropertyMap()
                {
                    {"field-id", "144" }
                }),
                new Field(PrimitiveSchema.Create(Schema.Type.Long), "content_size_in_bytes", 19, customProperties: new PropertyMap()
                {
                    {"field-id", "145" }
                }),

            });

            var manifestSchema = RecordSchema.Create("manifest_entry", new List<Field>()
            {
                new Field(Avro.PrimitiveSchema.Create(Schema.Type.Int), "status", 0, customProperties: new PropertyMap()
                {
                    {"field-id", "0" }
                }),
                new Field(Avro.PrimitiveSchema.Create(Schema.Type.Long), "snapshot_id", 1, customProperties: new PropertyMap()
                {
                    {"field-id", "1" }
                }),
                new Field(Avro.PrimitiveSchema.Create(Schema.Type.Long), "sequence_number", 2, customProperties: new PropertyMap()
                {
                    {"field-id", "3" }
                }),
                new Field(Avro.PrimitiveSchema.Create(Schema.Type.Long), "file_sequence_number", 3, customProperties: new PropertyMap()
                {
                    {"field-id", "4" }
                }),
                new Field(dataFileSchema, "data_file", 2, customProperties: new PropertyMap()
                {
                    {"field-id", "2" }
                }),
            });

            //new GenericRecord(manifestSchema).Add("data_file", ;
        }

        public void AddDataFile(
            Delta.Schema.Types.StructType schema, 
            DeltaAddAction addAction, 
            DeltaStatistics stats,
            long snapshotId)
        {
            var record = new GenericRecord(null);
            record.Add("content", 0);
            record.Add("file_path", addAction.Path);
            record.Add("file_format", "parquet");

            GenericRecord partitionRecord = new GenericRecord(null);
            record.Add("partition", partitionRecord);
            record.Add("record_count", stats.NumRecords);
            record.Add("file_size_in_bytes", addAction.Size);

            
            Dictionary<string, long> nullValueCount = new Dictionary<string, long>();
            Dictionary<string, byte[]> lowerBounds = new Dictionary<string, byte[]>();
            Dictionary<string, byte[]> maxBounds = new Dictionary<string, byte[]>();

            if (stats.ValueComparers != null)
            {
                foreach (var val in stats.ValueComparers)
                {
                    var field = schema.Fields.First(x => x.PhysicalName == val.Key);
                    var fieldId = field.FieldId!.Value.ToString();
                    if (val.Value.NullCount.HasValue)
                    {
                        nullValueCount.Add(fieldId, val.Value.NullCount.Value);
                    }
                    var minValue = val.Value.GetMinValueIcebergBinary();
                    if (minValue != null)
                    {
                        lowerBounds.Add(fieldId, minValue);
                    }
                    var maxValue = val.Value.GetMaxValueIcebergBinary();
                    if (maxValue != null)
                    {
                        maxBounds.Add(fieldId, maxValue);
                    }
                }
            }
            

            record.Add("null_value_counts", nullValueCount);
            record.Add("lower_bounds", lowerBounds);
            record.Add("upper_bounds", maxBounds);

            var manifestEntry = new ManifestEntry()
            {
                Status = 1,
                SnapshotId = snapshotId,
                DataFile = new DataFile()
                {
                    Content = 0,
                    FilePath = addAction.Path,
                    FileFormat = "parquet",
                    Partition = new Dictionary<string, object>(),
                    RecordCount = stats.NumRecords,
                    FileSizeInBytes = addAction.Size,
                    NullValueCounts = nullValueCount,
                    LowerBounds = lowerBounds,
                    UpperBounds = maxBounds
                }
            };
            var datumWriter = new Avro.Generic.GenericDatumWriter<ManifestEntry>(default);
            var writer = Avro.File.DataFileWriter<ManifestEntry>.OpenWriter(datumWriter, "");
            writer.Append(manifestEntry);

            
        }
    }
}

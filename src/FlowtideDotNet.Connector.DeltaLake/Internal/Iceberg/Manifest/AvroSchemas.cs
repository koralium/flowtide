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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Manifest
{
    internal static class AvroSchemas
    {
        public static readonly Schema IntSchema = PrimitiveSchema.Create(Schema.Type.Int);
        public static readonly Schema LongSchema = PrimitiveSchema.Create(Schema.Type.Long);
        public static readonly Schema NullSchema = PrimitiveSchema.Create(Schema.Type.Null);
        public static readonly Schema BinarySchema = PrimitiveSchema.Create(Schema.Type.Bytes);
        public static readonly Schema StringSchema = PrimitiveSchema.Create(Schema.Type.String);
        public static readonly Schema BooleanSchema = PrimitiveSchema.Create(Schema.Type.Boolean);

        public static readonly Schema NullIntSchema = UnionSchema.Create([NullSchema, IntSchema]);
        public static readonly Schema NullLongSchema = UnionSchema.Create([NullSchema, LongSchema]);
        public static readonly Schema bytesNullableSchema = UnionSchema.Create([NullSchema, BinarySchema]);
        public static readonly Schema stringNullableSchema = UnionSchema.Create([NullSchema, StringSchema]);
        public static readonly Schema nullBooleanSchema = UnionSchema.Create([NullSchema, BooleanSchema]);
        public static readonly Schema nullBinarySchema = UnionSchema.Create([NullSchema, BinarySchema]);

        public static Schema columnSizesSchema = UnionSchema.Create([NullSchema, ArraySchema.Create(RecordSchema.Create("k117_v118", new List<Field>()
        {
            new Field(IntSchema, "key", 0, customProperties: new PropertyMap()
            {
                { "field-id", "117" }
            }),
            new Field(LongSchema, "value", 1, customProperties: new PropertyMap()
            {
                { "field-id", "118" }
            }),
        }))]);

        public static Schema valueCountsSchema = UnionSchema.Create([NullSchema, ArraySchema.Create(RecordSchema.Create("k119_v120", new List<Field>()
        {
            new Field(IntSchema, "key", 0, customProperties: new PropertyMap()
            {
                { "field-id", "119" }
            }),
            new Field(LongSchema, "value", 1, customProperties: new PropertyMap()
            {
                { "field-id", "120" }
            }),
        }))]);

        public static RecordSchema NullValueCountSchema = RecordSchema.Create("k121_v122", new List<Field>()
        {
            new Field(IntSchema, "key", 0, customProperties: new PropertyMap()
            {
                { "field-id", "121" }
            }),
            new Field(LongSchema, "value", 1, customProperties: new PropertyMap()
            {
                { "field-id", "122" }
            }),
        });

        public static Schema nullValueCountsSchema = UnionSchema.Create([NullSchema, ArraySchema.Create(NullValueCountSchema)]);

        public static Schema nanValueCountsSchema = UnionSchema.Create([NullSchema, ArraySchema.Create(RecordSchema.Create("k138_v139", new List<Field>()
        {
            new Field(IntSchema, "key", 0, customProperties: new PropertyMap()
            {
                { "field-id", "138" }
            }),
            new Field(LongSchema, "value", 1, customProperties: new PropertyMap()
            {
                { "field-id", "139" }
            }),
        }))]);

        public static RecordSchema lowerBoundSchema = RecordSchema.Create("k126_v127", new List<Field>()
        {
            new Field(IntSchema, "key", 0, customProperties: new PropertyMap()
            {
                { "field-id", "126" }
            }),
            new Field(BinarySchema, "value", 1, customProperties: new PropertyMap()
            {
                { "field-id", "127" }
            }),
        });

        public static Schema lowerBoundsSchema = UnionSchema.Create([NullSchema, ArraySchema.Create(lowerBoundSchema)]);

        public static RecordSchema UpperBoundSchema = RecordSchema.Create("k129_v130", new List<Field>()
        {
            new Field(IntSchema, "key", 0, customProperties: new PropertyMap()
            {
                { "field-id", "129" }
            }),
            new Field(BinarySchema, "value", 1, customProperties: new PropertyMap()
            {
                { "field-id", "130" }
            }),
        });

        public static Schema upperBoundsSchema = UnionSchema.Create([NullSchema, ArraySchema.Create(UpperBoundSchema)]);

        public static Schema splitOffsetsSchema = UnionSchema.Create([NullSchema, ArraySchema.Create(LongSchema, customAttributes: new PropertyMap()
        {
            { "element-id", "133"}
        })]);

        public static Schema equalityIdsSchema = UnionSchema.Create([NullSchema, ArraySchema.Create(IntSchema, customAttributes: new PropertyMap()
        {
            { "element-id", "136"}
        })]);

        public static RecordSchema CreateDataFileSchema(Schema partitionSchema)
        {
            return RecordSchema.Create("data_file", new List<Field>()
            {
                new Field(IntSchema, "content", 0, customProperties: new PropertyMap()
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
                new Field(partitionSchema, "partition", 3, customProperties: new PropertyMap()
                {
                    {"field-id", "102" }
                }),
                new Field(LongSchema, "record_count", 4, customProperties: new PropertyMap()
                {
                    {"field-id", "103" }
                }),
                new Field(LongSchema, "file_size_in_bytes", 5, customProperties: new PropertyMap()
                {
                    {"field-id", "104" }
                }),
                new Field(columnSizesSchema, "column_sizes", 6, customProperties: new PropertyMap()
                {
                    {"field-id", "108" }
                }),
                new Field(valueCountsSchema, "value_counts", 7, customProperties: new PropertyMap()
                {
                    {"field-id", "109" }
                }),
                new Field(nullValueCountsSchema, "null_value_counts", 8, customProperties: new PropertyMap()
                {
                    {"field-id", "110" }
                }),
                new Field(nanValueCountsSchema, "nan_value_counts", 9, customProperties: new PropertyMap()
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
                new Field(bytesNullableSchema, "key_metadata", 12, customProperties: new PropertyMap()
                {
                    {"field-id", "111" }
                }),
                new Field(splitOffsetsSchema, "split_offsets", 13, customProperties: new PropertyMap()
                {
                    {"field-id", "132" }
                }),
                new Field(equalityIdsSchema, "equality_ids", 14, customProperties: new PropertyMap()
                {
                    {"field-id", "135" }
                }),
                new Field(NullIntSchema, "sort_order_id", 15, customProperties: new PropertyMap()
                {
                    {"field-id", "140" }
                }),
                new Field(NullLongSchema, "first_row_id", 16, customProperties: new PropertyMap()
                {
                    {"field-id", "142" }
                }),
                new Field(stringNullableSchema, "referenced_data_file", 17, customProperties: new PropertyMap()
                {
                    {"field-id", "143" }
                }),
                new Field(NullLongSchema, "content_offset", 18, customProperties: new PropertyMap()
                {
                    {"field-id", "144" }
                }),
                new Field(NullLongSchema, "content_size_in_bytes", 19, customProperties: new PropertyMap()
                {
                    {"field-id", "145" }
                }),
            });
        }

        public static RecordSchema CreateManifestEntrySchema(RecordSchema dataFileSchema)
        {
            return RecordSchema.Create("manifest_entry", new List<Field>()
            {
                new Field(IntSchema, "status", 0, customProperties: new PropertyMap()
                {
                    {"field-id", "0" }
                }),
                new Field(LongSchema, "snapshot_id", 1, customProperties: new PropertyMap()
                {
                    {"field-id", "1" }
                }),
                new Field(NullLongSchema, "sequence_number", 2, customProperties: new PropertyMap()
                {
                    {"field-id", "3" }
                }),
                new Field(NullLongSchema, "file_sequence_number", 3, customProperties: new PropertyMap()
                {
                    {"field-id", "4" }
                }),
                new Field(dataFileSchema, "data_file", 4, customProperties: new PropertyMap()
                {
                    {"field-id", "2" }
                }),
            });
        }

        public static RecordSchema FieldSummarySchema = RecordSchema.Create("field_summary", new List<Field>()
        {
            new Field(BooleanSchema, "contains_null", 0, customProperties: new PropertyMap()
            {
                { "field-id", "509" }
            }),
            new Field(nullBooleanSchema, "contains_nan", 0, customProperties: new PropertyMap()
            {
                { "field-id", "518" }
            }),
            new Field(nullBinarySchema, "lower_bound", 0, customProperties: new PropertyMap()
            {
                { "field-id", "510" }
            }),
            new Field(nullBinarySchema, "upper_bound", 0, customProperties: new PropertyMap()
            {
                { "field-id", "511" }
            }),
        });

        public static Schema ManifestListPartitionsArraySchema = ArraySchema.Create(FieldSummarySchema, new PropertyMap()
        {
            { "element-id", "508" }
        });

        public static Schema ManifestListPartitionsSchema = UnionSchema.Create([NullSchema, ManifestListPartitionsArraySchema]);

        public static RecordSchema ManifestListSchema = RecordSchema.Create("manifest_file", new List<Field>()
        {
            new Field(StringSchema, "manifest_path", 0, customProperties: new PropertyMap()
            {
                { "field-id", "500" }
            }),
            new Field(LongSchema, "manifest_length", 1, customProperties: new PropertyMap()
            {
                { "field-id", "501" }
            }),
            new Field(IntSchema, "partition_spec_id", 2, customProperties: new PropertyMap()
            {
                { "field-id", "502" }
            }),
            new Field(IntSchema, "content", 3, customProperties: new PropertyMap()
            {
                { "field-id", "517" }
            }),
            new Field(LongSchema, "sequence_number", 4, customProperties: new PropertyMap()
            {
                { "field-id", "515" }
            }),
            new Field(LongSchema, "min_sequence_number", 5, customProperties: new PropertyMap()
            {
                { "field-id", "516" }
            }),
            new Field(LongSchema, "added_snapshot_id", 6, customProperties: new PropertyMap()
            {
                { "field-id", "503" }
            }),
            new Field(IntSchema, "added_files_count", 7, customProperties: new PropertyMap()
            {
                { "field-id", "504" }
            }),
            new Field(IntSchema, "existing_files_count", 8, customProperties: new PropertyMap()
            {
                { "field-id", "505" }
            }),
            new Field(IntSchema, "deleted_files_count", 9, customProperties: new PropertyMap()
            {
                { "field-id", "506" }
            }),
            new Field(LongSchema, "added_rows_count", 10, customProperties: new PropertyMap()
            {
                { "field-id", "512" }
            }),
            new Field(LongSchema, "existing_rows_count", 11, customProperties: new PropertyMap()
            {
                { "field-id", "513" }
            }),
            new Field(LongSchema, "deleted_rows_count", 12, customProperties: new PropertyMap()
            {
                { "field-id", "514" }
            }),
            new Field(ManifestListPartitionsSchema, "partitions", 13, customProperties: new PropertyMap()
            {
                { "field-id", "507" }
            }),
            new Field(nullBinarySchema, "key_metadata", 14, customProperties: new PropertyMap()
            {
                { "field-id", "519" }
            }),
            new Field(NullLongSchema, "first_row_id", 15, customProperties: new PropertyMap()
            {
                { "field-id", "520" }
            })
        });
    }
}

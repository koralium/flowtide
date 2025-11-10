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

using Avro.Reflect;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Manifest
{
    internal class DataFile
    {
        [AvroField("content")]
        public int Content { get; set; }

        [AvroField("file_path")]
        public string? FilePath { get; set; }

        [AvroField("file_format")]
        public string? FileFormat { get; set; }

        [AvroField("partition")]
        public Dictionary<string, object>? Partition { get; set; }

        [AvroField("record_count")]
        public long RecordCount { get; set; }

        [AvroField("file_size_in_bytes")]
        public long FileSizeInBytes { get; set; }

        [AvroField("column_sizes")]
        public Dictionary<string, long>? ColumnSizes { get; set; }

        [AvroField("value_counts")]
        public Dictionary<string, long>? ValueCounts { get; set; }

        [AvroField("null_value_counts")]
        public Dictionary<string, long>? NullValueCounts { get; set; }

        [AvroField("lower_bounds")]
        public Dictionary<string, byte[]>? LowerBounds { get; set; }

        [AvroField("upper_bounds")]
        public Dictionary<string, byte[]>? UpperBounds { get; set; }

        [AvroField("key_metadata")]
        public byte[]? KeyMetadata { get; set; }

        [AvroField("split_offsets")]
        public List<long>? SplitOffsets { get; set; }
    }
}

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
    internal class ManifestEntry
    {
        [AvroField("status")]
        public int Status { get; set; }

        [AvroField("snapshot_id")]
        public long SnapshotId { get; set; }

        [AvroField("sequence_number")]
        public long SequenceNumber { get; set; }

        [AvroField("file_sequence_number")]
        public long FileSequenceNumber { get; set; }

        [AvroField("data_file")]
        public DataFile? DataFile { get; set; }
    }
}

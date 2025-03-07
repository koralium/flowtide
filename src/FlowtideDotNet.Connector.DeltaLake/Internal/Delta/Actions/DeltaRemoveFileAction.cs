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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions
{
    internal class DeltaRemoveFileAction
    {
        [JsonPropertyName("path")]
        public string? Path { get; set; }

        [JsonPropertyName("deletionTimestamp")]
        public long? DeletionTimestamp { get; set; }

        [JsonPropertyName("dataChange")]
        public bool DataChange { get; set; }

        [JsonPropertyName("extendedFileMetadata")]
        public bool ExtendedFileMetadata { get; set; }

        [JsonPropertyName("partitionValues")]
        public Dictionary<string, string>? PartitionValues { get; set; }

        [JsonPropertyName("size")]
        public long? Size { get; set; }

        [JsonPropertyName("stats")]
        public string? Stats { get; set; }

        [JsonPropertyName("tags")]
        public Dictionary<string, string>? Tags { get; set; }

        [JsonPropertyName("baseRowId")]
        public long? BaseRowId { get; set; }

        [JsonPropertyName("defaultRowCommitVersion")]
        public long? DefaultRowCommitVersion { get; set; }

        [JsonPropertyName("deletionVector")]
        public DeletionVector? DeletionVector { get; set; }

        public DeltaFileKey GetKey()
        {
            return new DeltaFileKey(Path!, DeletionVector?.UniqueId);
        }
    }
}

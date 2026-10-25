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

using System.Text.Json.Serialization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// The _last_checkpoint hint; it never carries checkpointSchema, so readers always use the footer's schema.
    /// </summary>
    internal class LastCheckpointInfo
    {
        [JsonPropertyName("version")]
        public long Version { get; set; }

        // Actions in the checkpoint, one per row
        [JsonPropertyName("size")]
        public long Size { get; set; }

        [JsonPropertyName("sizeInBytes")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? SizeInBytes { get; set; }

        [JsonPropertyName("numOfAddFiles")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? NumOfAddFiles { get; set; }

        // Read from other writers' hints; this writer never sets it
        [JsonPropertyName("parts")]
        [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
        public long? Parts { get; set; }
    }
}

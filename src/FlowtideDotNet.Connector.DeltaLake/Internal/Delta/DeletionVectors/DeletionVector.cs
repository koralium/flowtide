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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    internal class DeletionVector
    {
        [JsonPropertyName("storageType")]
        public string? StorageType { get; set; }

        [JsonPropertyName("pathOrInlineDv")]
        public string? PathOrInlineDv { get; set; }

        [JsonPropertyName("offset")]
        public long? Offset { get; set; }

        [JsonPropertyName("sizeInBytes")]
        public int SizeInBytes { get; set; }

        [JsonPropertyName("cardinality")]
        public long Cardinality { get; set; }

        [JsonIgnore]
        public string UniqueId => GetUniqueId();

        [JsonIgnore]
        public string? AbsolutePath => GetAbsolutePath();

        private string GetUniqueId()
        {
            if (Offset == null)
            {
                return $"{StorageType}{PathOrInlineDv}";
            }
            return $"{StorageType}{PathOrInlineDv}@{Offset}";
        }

        public string? GetAbsolutePath()
        {
            if (StorageType == "p")
            {
                return PathOrInlineDv!;
            }
            if (StorageType == "u")
            {
                var guid = Z85.DecodeToGuid(PathOrInlineDv!);
                var deletionVectorPath = "deletion_vector_" + guid.ToString() + ".bin";
                return deletionVectorPath;
            }
            return null;
        }
    }
}

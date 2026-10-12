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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions
{
    internal class DeltaAddAction : DeltaBaseAction
    {
        [JsonPropertyName("path")]
        public string? Path { get; set; }

        [JsonPropertyName("partitionValues")]
        public Dictionary<string, string>? PartitionValues { get; set; }

        [JsonPropertyName("size")]
        public long Size { get; set; }

        [JsonPropertyName("modificationTime")]
        public long ModificationTime { get; set; }

        [JsonPropertyName("dataChange")]
        public bool DataChange { get; set; }

        [JsonPropertyName("stats")]
        public string? Statistics { get; set; }

        [JsonPropertyName("tags")]
        public Dictionary<string, string>? Tags { get; set; }

        [JsonPropertyName("baseRowId")]
        public long? BaseRowId { get; set; }

        [JsonPropertyName("defaultRowCommitVersion")]
        public long? DefaultRowCommitVersion { get; set; }

        [JsonPropertyName("clusteringProvider")]
        public string? ClusteringProvider { get; set; }

        [JsonPropertyName("deletionVector")]
        public DeletionVector? DeletionVector { get; set; }

        private static readonly JsonSerializerOptions s_statisticsOptions = new JsonSerializerOptions()
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        /// <summary>
        /// The same physical file with a new deletion vector.
        /// </summary>
        public DeltaAddAction WithDeletionVector(DeletionVector deletionVector)
        {
            return new DeltaAddAction()
            {
                Path = Path,
                PartitionValues = PartitionValues,
                Size = Size,
                ModificationTime = ModificationTime,
                // Removing rows is a data change
                DataChange = true,
                Statistics = WithLooseBounds(Statistics),
                Tags = Tags,
                BaseRowId = BaseRowId,
                DefaultRowCommitVersion = DefaultRowCommitVersion,
                ClusteringProvider = ClusteringProvider,
                DeletionVector = deletionVector
            };
        }

        // Bounds may belong to deleted rows
        private static string? WithLooseBounds(string? statistics)
        {
            if (statistics == null || JsonNode.Parse(statistics) is not JsonObject node)
            {
                return statistics;
            }
            node["tightBounds"] = false;
            return node.ToJsonString(s_statisticsOptions);
        }

        public DeltaFileKey GetKey()
        {
            return new DeltaFileKey(Path!, DeletionVector?.UniqueId);
        }
    }
}

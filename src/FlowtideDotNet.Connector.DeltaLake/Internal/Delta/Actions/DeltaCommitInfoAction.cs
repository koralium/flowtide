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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions
{
    internal class DeltaCommitInfoAction : DeltaBaseAction
    {
        /// <summary>
        /// Written first in the line, a torn commit proves it is ours only when it holds the whole id.
        /// </summary>
        [JsonPropertyName("flowtide.stageId")]
        [JsonConverter(typeof(LenientStringConverter))]
        public string? StageId { get; set; }

        [JsonPropertyName("timestamp")]
        [JsonConverter(typeof(LenientInt64Converter))]
        public long? Timestamp { get; set; }

        /// <summary>
        /// The first version from which only stage id writing Flowtide sinks have committed.
        /// </summary>
        [JsonPropertyName("flowtide.adoptedAt")]
        [JsonConverter(typeof(LenientInt64Converter))]
        public long? AdoptedAt { get; set; }

        /// <summary>
        /// Data, change data and deletion vector files this commit created.
        /// </summary>
        [JsonPropertyName("flowtide.createdFiles")]
        [JsonConverter(typeof(LenientStringListConverter))]
        public List<string>? CreatedFiles { get; set; }

        [JsonExtensionData]
        public Dictionary<string, object>? Data { get; set; }
    }
}

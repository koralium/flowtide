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

using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace FlowtideDotNet.Connector.StarRocks.Internal.HttpApi
{
    internal class HttpResultRow
    {
        [JsonPropertyName("connectionId")]
        public long? ConnectionId { get; set; }

        [JsonPropertyName("meta")]
        public List<MetadataField>? Meta { get; set; }

        [JsonPropertyName("data")]
        [JsonConverter(typeof(DataRowConverter))]
        public List<object>? Data { get; set; }

        [JsonPropertyName("statistics")]
        public Statistics? Statistics { get; set; }

        [MemberNotNullWhen(true, nameof(ConnectionId))]
        public bool IsConnectionId => ConnectionId != null;

        [MemberNotNullWhen(true, nameof(Meta))]
        public bool IsMetadata => Meta != null;

        [MemberNotNullWhen(true, nameof(Data))]
        public bool IsData => Data != null;

        [MemberNotNullWhen(true, nameof(Statistics))]
        public bool IsStatistics => Statistics != null;
    }
}

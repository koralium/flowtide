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

namespace FlowtideDotNet.Connector.StarRocks.Internal.HttpApi
{
    internal class StreamLoadInfo
    {
        [JsonPropertyName("TxnId")]
        public long? TxnId { get; set; }

        [JsonPropertyName("Label")]
        public string? Label { get; set; }

        [JsonPropertyName("Status")]
        public string? Status { get; set; }

        [JsonPropertyName("Message")]
        public string? Message { get; set; }

        [JsonPropertyName("NumberTotalRows")]
        public long? NumberTotalRows { get; set; }

        [JsonPropertyName("NumberLoadedRows")]
        public long? NumberLoadedRows { get; set; }

        [JsonPropertyName("NumberFilteredRows")]
        public long? NumberFilteredRows { get; set; }

        [JsonPropertyName("NumberUnselectedRows")]
        public long? NumberUnselectedRows { get; set; }

        [JsonPropertyName("LoadBytes")]
        public long? LoadBytes { get; set; }

        [JsonPropertyName("LoadTimeMs")]
        public long? LoadTimeMs { get; set; }

        [JsonPropertyName("BeginTxnTimeMs")]
        public long? BeginTxnTimeMs { get; set; }

        [JsonPropertyName("StreamLoadPlanTimeMs")]
        public long? StreamLoadPlanTimeMs { get; set; }

        [JsonPropertyName("ReadDataTimeMs")]
        public long? ReadDataTimeMs { get; set; }

        [JsonPropertyName("WriteDataTimeMs")]
        public long? WriteDataTimeMs { get; set; }

        [JsonPropertyName("CommitAndPublishTimeMs")]
        public long? CommitAndPublishTimeMs { get; set; }
    }

}

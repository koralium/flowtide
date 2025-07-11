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
    internal class DeltaAction
    {
        [JsonPropertyName("txn")]
        public DeltaTransactionAction? Txn { get; set; }

        [JsonPropertyName("add")]
        public DeltaAddAction? Add { get; set; }

        [JsonPropertyName("remove")]
        public DeltaRemoveFileAction? Remove { get; set; }

        [JsonPropertyName("metaData")]
        public DeltaMetadataAction? MetaData { get; set; }

        [JsonPropertyName("protocol")]
        public DeltaProtocolAction? Protocol { get; set; }

        [JsonPropertyName("commitInfo")]
        public DeltaCommitInfoAction? CommitInfo { get; set; }

        [JsonPropertyName("cdc")]
        public DeltaCdcAction? Cdc { get; set; }
    }
}

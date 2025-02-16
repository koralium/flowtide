using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

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

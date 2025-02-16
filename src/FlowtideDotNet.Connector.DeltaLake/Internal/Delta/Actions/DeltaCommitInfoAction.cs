using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions
{
    internal class DeltaCommitInfoAction : DeltaBaseAction
    {
        [JsonExtensionData]
        public Dictionary<string, object>? Data { get; set; }
    }
}

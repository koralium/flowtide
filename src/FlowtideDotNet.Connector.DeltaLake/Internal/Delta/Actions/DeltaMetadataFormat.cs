using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions
{
    internal class DeltaMetadataFormat
    {
        [JsonPropertyName("provider")]
        public string? Provider { get; set; }

        [JsonPropertyName("options")]
        public Dictionary<string, string>? Options { get; set; }
    }
}

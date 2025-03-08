using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions
{
    internal class DeltaProtocolAction : DeltaBaseAction
    {
        [JsonPropertyName("minReaderVersion")]
        public int MinReaderVersion { get; set; }

        [JsonPropertyName("minWriterVersion")]
        public int MinWriterVersion { get; set; }

        [JsonPropertyName("readerFeatures")]
        public IReadOnlyList<string>? ReaderFeatures { get; set; }

        [JsonPropertyName("writerFeatures")]
        public IReadOnlyList<string>? WriterFeatures { get; set; }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class StructField
    {
        [JsonPropertyName("name")]
        public string Name { get; }

        [JsonPropertyName("type")]
        public SchemaBaseType Type { get; }

        [JsonPropertyName("nullable")]
        public bool Nullable { get; }

        [JsonPropertyName("metadata")]
        public IReadOnlyDictionary<string, object> Metadata { get; }

        public StructField(string name, SchemaBaseType type, bool nullable, IReadOnlyDictionary<string, object> metadata)
        {
            Name = name;
            Type = type;
            Nullable = nullable;
            Metadata = metadata;
        }
    }
}

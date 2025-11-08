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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class StructField : IEquatable<StructField>
    {
        [JsonPropertyName("name")]
        public string Name { get; }

        [JsonPropertyName("type")]
        public SchemaBaseType Type { get; }

        [JsonPropertyName("nullable")]
        public bool Nullable { get; }

        [JsonPropertyName("metadata")]
        public IReadOnlyDictionary<string, object> Metadata { get; }

        [JsonIgnore]
        public int? FieldId { get; set; } // Used only for iceberg compatibility

        public StructField(string name, SchemaBaseType type, bool nullable, IReadOnlyDictionary<string, object> metadata)
        {
            Name = name;
            Type = type;
            Nullable = nullable;
            Metadata = metadata;
        }

        public bool Equals(StructField? other)
        {
            if (other == null)
            {
                return false;
            }

            return Name == other.Name &&
                   Type.Equals(other.Type) &&
                   Nullable == other.Nullable &&
                   Metadata.SequenceEqual(other.Metadata);
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as StructField);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name, Type, Nullable);
        }
    }
}

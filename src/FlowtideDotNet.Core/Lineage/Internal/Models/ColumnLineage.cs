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

namespace FlowtideDotNet.Core.Lineage.Internal.Models
{
    internal class ColumnLineage : IEquatable<ColumnLineage>
    {
        [JsonPropertyName("_producer")]
        public string Producer => "https://github.com/koralium/flowtide";

        [JsonPropertyName("_schemaURL")]
        public string SchemaURL => "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json";

        [JsonPropertyName("fields")]
        public IReadOnlyDictionary<string, ColumnLineageField> Fields { get; }

        [JsonPropertyName("dataset")]
        public IReadOnlyList<LineageInputField> Dataset { get; }

        public ColumnLineage(
            IReadOnlyDictionary<string, ColumnLineageField> fields,
            IReadOnlyList<LineageInputField> datasetFields)
        {
            Fields = fields;
            Dataset = datasetFields;
        }

        public bool Equals(ColumnLineage? other)
        {
            // Compare fields
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;

            if (Fields.Count != other.Fields.Count)
            {
                return false;
            }

            foreach(var kv in Fields)
            {
                if (!other.Fields.TryGetValue(kv.Key, out var otherFields))
                {
                    return false;
                }
                var fields = kv.Value;
                if (fields.InputFields.Count != otherFields.InputFields.Count)
                {
                    return false;
                }
                for (int i = 0; i < fields.InputFields.Count; i++)
                {
                    if (!fields.InputFields[i].Equals(otherFields.InputFields[i]))
                    {
                        return false;
                    }
                }
            }

            if (Dataset.Count != other.Dataset.Count)
            {
                return false;
            }

            for (int i = 0; i < Dataset.Count; i++)
            {
                if (!Dataset[i].Equals(other.Dataset[i]))
                {
                    return false;
                }
            }

            return true;
        }
    }
}

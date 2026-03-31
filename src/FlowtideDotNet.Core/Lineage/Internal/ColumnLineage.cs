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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class ColumnLineage : IEquatable<ColumnLineage>
    {
        [JsonPropertyName("fields")]
        public IReadOnlyDictionary<string, IReadOnlyList<LineageInputField>> Fields { get; }

        [JsonPropertyName("dataset")]
        public IReadOnlyList<LineageInputField> Dataset { get; }

        public ColumnLineage(
            IReadOnlyDictionary<string, IReadOnlyList<LineageInputField>> fields,
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
                if (fields.Count != otherFields.Count)
                {
                    return false;
                }
                for (int i = 0; i < fields.Count; i++)
                {
                    if (!fields[i].Equals(otherFields[i]))
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

﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
    internal class StructType : SchemaBaseType, IEquatable<StructType>
    {
        public override SchemaType Type => SchemaType.Struct;

        [JsonPropertyName("fields")]
        public IReadOnlyList<StructField> Fields { get; set; }

        public StructType(IReadOnlyList<StructField> fields)
        {
            Fields = fields;
        }

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitStructType(this);
        }

        public bool Equals(StructType? other)
        {
            if (other == null)
            {
                return false;
            }

            if (Fields.Count != other.Fields.Count)
            {
                return false;
            }

            for (int i = 0; i < Fields.Count; i++)
            {
                if (!Fields[i].Equals(other.Fields[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as StructType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}

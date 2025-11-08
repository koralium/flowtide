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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class MapType : SchemaBaseType, IEquatable<MapType>
    {
        public override SchemaType Type => SchemaType.Map;

        public SchemaBaseType KeyType { get; }

        public SchemaBaseType ValueType { get; }

        public bool ValueContainsNull { get; }

        /// <summary>
        /// Only for iceberg compatibility
        /// </summary>
        public int? KeyId { get; set; }

        /// <summary>
        /// Only for iceberg compatibility
        /// </summary>
        public int? ValueId { get; set; }

        public MapType(SchemaBaseType keyType, SchemaBaseType valueType, bool valueContainsNull)
        {
            KeyType = keyType;
            ValueType = valueType;
            ValueContainsNull = valueContainsNull;
        }

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitMapType(this);
        }

        public bool Equals(MapType? other)
        {
            if (other is null)
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return KeyType.Equals(other.KeyType) && ValueType.Equals(other.ValueType) && ValueContainsNull == other.ValueContainsNull;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as MapType);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(KeyType, ValueType, ValueContainsNull);
        }
    }
}

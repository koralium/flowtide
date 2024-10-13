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
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Type
{
    public class MapType : SubstraitBaseType, IEquatable<MapType>
    {
        public override SubstraitType Type => SubstraitType.Map;

        public SubstraitBaseType KeyType { get; }

        public SubstraitBaseType ValueType { get; }

        public MapType(SubstraitBaseType keyType, SubstraitBaseType valueType)
        {
            KeyType = keyType;
            ValueType = valueType;
        }

        public override bool Equals(object? obj)
        {
            return obj is MapType @struct &&
                Equals(@struct);
        }

        public bool Equals(MapType? other)
        {
            return other != null &&
                Equals(KeyType, other.KeyType) &&
                Equals(ValueType, other.ValueType);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(KeyType);
            code.Add(ValueType);
            return code.ToHashCode();
        }

        public static bool operator ==(MapType? left, MapType? right)
        {
            return EqualityComparer<MapType>.Default.Equals(left, right);
        }

        public static bool operator !=(MapType? left, MapType? right)
        {
            return !(left == right);
        }
    }
}

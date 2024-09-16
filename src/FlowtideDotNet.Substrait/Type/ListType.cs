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
    public class ListType : SubstraitBaseType, IEquatable<ListType>
    {
        public ListType(SubstraitBaseType valueType)
        {
            ValueType = valueType;
        }

        public override SubstraitType Type => SubstraitType.List;

        public SubstraitBaseType ValueType { get; }

        public bool Equals(ListType? other)
        {
            return other != null &&
                Equals(ValueType, other.ValueType);
        }

        public override bool Equals(object? obj)
        {
            return obj is ListType list &&
                Equals(list);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(ValueType);
            return code.ToHashCode();
        }

        public static bool operator ==(ListType? left, ListType? right)
        {
            return EqualityComparer<ListType>.Default.Equals(left, right);
        }

        public static bool operator !=(ListType? left, ListType? right)
        {
            return !(left == right);
        }
    }
}

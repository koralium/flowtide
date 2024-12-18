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


namespace FlowtideDotNet.Substrait.Type
{
    public sealed class Struct : IEquatable<Struct>
    {
        public required List<SubstraitBaseType> Types { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is Struct @struct &&
                Equals(@struct);
        }

        public bool Equals(Struct? other)
        {
            return other != null &&
                Types.SequenceEqual(other.Types);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            if (Types != null)
            {
                foreach (var type in Types)
                {
                    code.Add(type);
                }
            }
            return code.ToHashCode();
        }

        public static bool operator ==(Struct? left, Struct? right)
        {
            return EqualityComparer<Struct>.Default.Equals(left, right);
        }

        public static bool operator !=(Struct? left, Struct? right)
        {
            return !(left == right);
        }
    }
}

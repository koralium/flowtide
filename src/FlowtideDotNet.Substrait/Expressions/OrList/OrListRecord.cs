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


namespace FlowtideDotNet.Substrait.Expressions
{
    public sealed class OrListRecord : IEquatable<OrListRecord>
    {
        public required List<Expression> Fields { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is OrListRecord record &&
                   Equals(record);
        }

        public bool Equals(OrListRecord? other)
        {
            if (other == null)
            {
                return false;
            }
            return Fields.SequenceEqual(other.Fields);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();

            foreach (var field in Fields)
            {
                code.Add(field);
            }

            return code.ToHashCode();
        }

        public static bool operator ==(OrListRecord? left, OrListRecord? right)
        {
            return EqualityComparer<OrListRecord>.Default.Equals(left, right);
        }

        public static bool operator !=(OrListRecord? left, OrListRecord? right)
        {
            return !(left == right);
        }
    }
}

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

using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Substrait
{
    public sealed class Plan : IEquatable<Plan>
    {
        public required List<Relation> Relations { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is Plan plan &&
                   Equals(plan);
        }

        public bool Equals(Plan? other)
        {
            return other != null &&
                   Relations.SequenceEqual(other.Relations);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            foreach (var relation in Relations)
            {
                code.Add(relation);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(Plan? left, Plan? right)
        {
            return EqualityComparer<Plan>.Default.Equals(left, right);
        }

        public static bool operator !=(Plan? left, Plan? right)
        {
            return !(left == right);
        }
    }
}

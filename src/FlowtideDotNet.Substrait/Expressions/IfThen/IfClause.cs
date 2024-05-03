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


namespace FlowtideDotNet.Substrait.Expressions.IfThen
{
    public sealed class IfClause : IEquatable<IfClause>
    {
        public required Expression If { get; set; }

        public required Expression Then { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is IfClause clause &&
                   Equals(clause);
        }

        public bool Equals(IfClause? other)
        {
            if (other == null)
            {
                return false;
            }
            return Equals(If, other.If) &&
                   Equals(Then, other.Then);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(If, Then);
        }

        public static bool operator ==(IfClause? left, IfClause? right)
        {
            return EqualityComparer<IfClause>.Default.Equals(left, right);
        }

        public static bool operator !=(IfClause? left, IfClause? right)
        {
            return !(left == right);
        }
    }
}

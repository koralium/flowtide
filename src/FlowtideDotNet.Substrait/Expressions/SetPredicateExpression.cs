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

namespace FlowtideDotNet.Substrait.Expressions
{
    public sealed class SetPredicateExpression : Expression, IEquatable<SetPredicateExpression>
    {
        public required Relation Relation { get; set; }

        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            return visitor.VisitSetPredicateExpression(this, state)!;
        }

        public override Expression Clone()
        {
            throw new NotImplementedException("Cloning SetPredicateExpression is not supported.");
        }

        public override bool Equals(object? obj)
        {
            return obj is SetPredicateExpression expression &&
                   Equals(expression);
        }

        public bool Equals(SetPredicateExpression? other)
        {
            return other != null &&
                   Equals(Relation, other.Relation);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Relation);
        }

        public static bool operator ==(SetPredicateExpression? left, SetPredicateExpression? right)
        {
            return EqualityComparer<SetPredicateExpression>.Default.Equals(left, right);
        }

        public static bool operator !=(SetPredicateExpression? left, SetPredicateExpression? right)
        {
            return !(left == right);
        }
    }
}

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


using static Substrait.Protobuf.Expression.Types;

namespace FlowtideDotNet.Substrait.Expressions.Literals
{
    public sealed class NumericLiteral : Literal, IEquatable<NumericLiteral>
    {
        public override LiteralType Type => LiteralType.Numeric;

        public decimal Value { get; set; }

        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            return visitor.VisitNumericLiteral(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is NumericLiteral literal &&
                   Equals(literal);
        }

        public bool Equals(NumericLiteral? other)
        {
            if (other == null)
            {
                return false;
            }
            return Value == other.Value;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Type, Value);
        }

        public static bool operator ==(NumericLiteral? left, NumericLiteral? right)
        {
            return EqualityComparer<NumericLiteral>.Default.Equals(left, right);
        }

        public static bool operator !=(NumericLiteral? left, NumericLiteral? right)
        {
            return !(left == right);
        }
    }
}

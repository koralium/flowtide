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
    public class ArrayLiteral : Literal, IEquatable<ArrayLiteral>
    {
        public override LiteralType Type => LiteralType.Array;

        public required List<Expression> Expressions { get; set; }

        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            return visitor.VisitArrayLiteral(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is ArrayLiteral literal &&
                   Equals(literal);
        }

        public bool Equals(ArrayLiteral? other)
        {
            if (other == null)
            {
                return false;
            }
            return Expressions.SequenceEqual(other.Expressions);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Type);
            foreach(var expr in Expressions)
            {
                code.Add(expr);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(ArrayLiteral? left, ArrayLiteral? right)
        {
            return EqualityComparer<ArrayLiteral>.Default.Equals(left, right);
        }

        public static bool operator !=(ArrayLiteral? left, ArrayLiteral? right)
        {
            return !(left == right);
        }
    }
}

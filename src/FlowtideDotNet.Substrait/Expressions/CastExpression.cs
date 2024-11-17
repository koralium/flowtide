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

using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Expressions
{
    public sealed class CastExpression : Expression, IEquatable<CastExpression>
    {
        public required Expression Expression { get; set; }

        public required SubstraitBaseType Type { get; set; }

        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            return visitor.VisitCastExpression(this, state)!;
        }

        public override bool Equals(object? obj)
        {
            return obj is CastExpression expression &&
                   Equals(expression);
        }

        public bool Equals(CastExpression? other)
        {
            return other != null &&
                   Equals(Expression, other.Expression) &&
                   Equals(Type, other.Type);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Expression, Type);
        }

        public static bool operator ==(CastExpression? left, CastExpression? right)
        {
            return EqualityComparer<CastExpression>.Default.Equals(left, right);
        }

        public static bool operator !=(CastExpression? left, CastExpression? right)
        {
            return !(left == right);
        }
    }
}

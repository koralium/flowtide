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


namespace FlowtideDotNet.Substrait.Expressions
{
    public sealed class ListNestedExpression : NestedExpression, IEquatable<ListNestedExpression>
    {
        public required List<Expression> Values { get; set; }
        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            return visitor.VisitListNestedExpression(this, state)!;
        }

        public override bool Equals(object? obj)
        {
            return obj is ListNestedExpression expression &&
                   Equals(expression);
        }

        public bool Equals(ListNestedExpression? other)
        {
            return other != null &&
                   Values.SequenceEqual(other.Values);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            foreach (var value in Values)
            {
                code.Add(value);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(ListNestedExpression? left, ListNestedExpression? right)
        {
            return EqualityComparer<ListNestedExpression>.Default.Equals(left, right);
        }

        public static bool operator !=(ListNestedExpression? left, ListNestedExpression? right)
        {
            return !(left == right);
        }
    }
}

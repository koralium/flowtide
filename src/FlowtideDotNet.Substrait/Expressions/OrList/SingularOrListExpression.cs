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
    public sealed class SingularOrListExpression : Expression, IEquatable<SingularOrListExpression>
    {
        public required Expression Value { get; set; }

        public required List<Expression> Options { get; set; }

        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            return visitor.VisitSingularOrList(this, state)!;
        }

        public override bool Equals(object? obj)
        {
            return obj is SingularOrListExpression expression &&
                   Equals(expression);
        }

        public bool Equals(SingularOrListExpression? other)
        {
            return other != null &&
                   Equals(Value, other.Value) &&
                   Options.SequenceEqual(other.Options);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Value);

            foreach (var option in Options)
            {
                code.Add(option);
            }

            return code.ToHashCode();
        }

        public static bool operator ==(SingularOrListExpression? left, SingularOrListExpression? right)
        {
            return EqualityComparer<SingularOrListExpression>.Default.Equals(left, right);
        }

        public static bool operator !=(SingularOrListExpression? left, SingularOrListExpression? right)
        {
            return !(left == right);
        }
    }
}

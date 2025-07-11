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
    public sealed class MultiOrListExpression : Expression, IEquatable<MultiOrListExpression>
    {
        public required List<Expression> Value { get; set; }

        public required List<OrListRecord> Options { get; set; }

        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            throw new NotImplementedException();
        }

        public override bool Equals(object? obj)
        {
            return obj is MultiOrListExpression expression &&
                   Equals(expression);
        }

        public bool Equals(MultiOrListExpression? other)
        {
            if (other == null)
            {
                return false;
            }
            return Value.SequenceEqual(other.Value) &&
                   Options.SequenceEqual(other.Options);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();

            foreach (var value in Value)
            {
                code.Add(value);
            }

            foreach (var option in Options)
            {
                code.Add(option);
            }

            return code.ToHashCode();
        }

        public static bool operator ==(MultiOrListExpression? left, MultiOrListExpression? right)
        {
            return EqualityComparer<MultiOrListExpression>.Default.Equals(left, right);
        }

        public static bool operator !=(MultiOrListExpression? left, MultiOrListExpression? right)
        {
            return !(left == right);
        }
    }
}

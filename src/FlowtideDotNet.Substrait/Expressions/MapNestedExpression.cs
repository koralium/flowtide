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
    public class MapNestedExpression : NestedExpression, IEquatable<MapNestedExpression>
    {
        public required List<KeyValuePair<Expression, Expression>> KeyValues { get; set; }

        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            return visitor.VisitMapNestedExpression(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is MapNestedExpression expression &&
                   Equals(expression);
        }

        public bool Equals(MapNestedExpression? other)
        {
            return other != null &&
                   KeyValues.SequenceEqual(other.KeyValues);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            foreach(var keyValue in KeyValues)
            {
                code.Add(keyValue);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(MapNestedExpression? left, MapNestedExpression? right)
        {
            return EqualityComparer<MapNestedExpression>.Default.Equals(left, right);
        }

        public static bool operator !=(MapNestedExpression? left, MapNestedExpression? right)
        {
            return !(left == right);
        }
    }
}

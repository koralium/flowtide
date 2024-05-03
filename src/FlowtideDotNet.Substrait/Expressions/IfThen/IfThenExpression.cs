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
    public class IfThenExpression : Expression, IEquatable<IfThenExpression>
    {
        /// <summary>
        /// Contains all the if clauses, evaulated top to bottom.
        /// </summary>
        public required List<IfClause> Ifs { get; set; }

        /// <summary>
        /// The else clause if no if clauses match
        /// </summary>
        public Expression? Else { get; set; }

        public override TOutput Accept<TOutput, TState>(ExpressionVisitor<TOutput, TState> visitor, TState state)
        {
            return visitor.VisitIfThen(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is IfThenExpression expression &&
                   Equals(expression);
        }

        public bool Equals(IfThenExpression? other)
        {
            if (other == null)
            {
                return false;
            }
            return Ifs.SequenceEqual(other.Ifs) &&
                   Equals(Else, other.Else);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            foreach(var ifClause in Ifs)
            {
                code.Add(ifClause);
            }
            code.Add(Else);
            return code.ToHashCode();
        }

        public static bool operator ==(IfThenExpression? left, IfThenExpression? right)
        {
            return EqualityComparer<IfThenExpression>.Default.Equals(left, right);
        }

        public static bool operator !=(IfThenExpression? left, IfThenExpression? right)
        {
            return !(left == right);
        }
    }
}

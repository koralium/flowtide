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

using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Substrait.Relations
{
    public class JoinRelation : Relation, IEquatable<JoinRelation>
    {
        public JoinType Type { get; set; }

        public required Relation Left { get; set; }

        public required Relation Right { get; set; }

        public Expression? Expression { get; set; }

        public Expression? PostJoinFilter { get; set; }

        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit.Count;
                }
                return Left.OutputLength + Right.OutputLength;
            }
        }

        public bool IsFieldFromLeft(int index)
        {
            if (index < Left.OutputLength)
            {
                return true;
            }
            return false;
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitJoinRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is JoinRelation relation &&
                Equals(relation);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Type);
            code.Add(Left);
            code.Add(Right);
            code.Add(Expression);
            code.Add(PostJoinFilter);
            return code.ToHashCode();
        }

        public bool Equals(JoinRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Type, other.Type) &&
                Equals(Left, other.Left) &&
                Equals(Right, other.Right) &&
                Equals(Expression, other.Expression) &&
                Equals(PostJoinFilter, other.PostJoinFilter);
        }

        public static bool operator ==(JoinRelation? left, JoinRelation? right)
        {
            return EqualityComparer<JoinRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(JoinRelation? left, JoinRelation? right)
        {
            return !(left == right);
        }
    }
}

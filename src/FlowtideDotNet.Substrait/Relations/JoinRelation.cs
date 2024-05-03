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
    public class JoinRelation : Relation
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
            if (obj is JoinRelation relation)
            {
                return EmitEquals(relation.Emit) &&
                   Equals(Type, relation.Type) &&
                   Equals(Left, relation.Left) &&
                   Equals(Right, relation.Right) &&
                   Equals(Expression, relation.Expression) &&
                   Equals(PostJoinFilter, relation.PostJoinFilter);
            }
            return false;
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            if (Emit != null)
            {
                foreach (var emit in Emit)
                {
                    code.Add(emit);
                }
            }
            code.Add(Type);
            code.Add(Left);
            code.Add(Right);
            code.Add(Expression);
            code.Add(PostJoinFilter);
            return code.ToHashCode();
        }
    }
}

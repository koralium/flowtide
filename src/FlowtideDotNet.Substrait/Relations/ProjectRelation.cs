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
    public sealed class ProjectRelation : Relation, IEquatable<ProjectRelation>
    {
        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit!.Count;
                }
                // Expressions are appended, so if emit is not set it is input + extra columns
                return Input.OutputLength + Expressions.Count;
            }
        }

        public required Relation Input { get; set; }
        
        public required List<Expression> Expressions { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitProjectRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is ProjectRelation relation &&
                Equals(relation);
        }

        public bool Equals(ProjectRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                Expressions.SequenceEqual(other.Expressions);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Input);
            foreach(var expression in Expressions)
            {
                code.Add(expression);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(ProjectRelation? left, ProjectRelation? right)
        {
            return EqualityComparer<ProjectRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(ProjectRelation? left, ProjectRelation? right)
        {
            return !(left == right);
        }
    }
}

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


namespace FlowtideDotNet.Substrait.Relations
{
    public class PlanRelation : Relation, IEquatable<PlanRelation>
    {
        public override int OutputLength => Root.OutputLength;

        public required RootRelation Root { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitPlanRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is PlanRelation relation &&
                Equals(relation);
        }

        public bool Equals(PlanRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Root, other.Root);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Root);
            return code.ToHashCode();
        }

        public static bool operator ==(PlanRelation? left, PlanRelation? right)
        {
            return EqualityComparer<PlanRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(PlanRelation? left, PlanRelation? right)
        {
            return !(left == right);
        }
    }
}

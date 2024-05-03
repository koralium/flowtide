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
    public sealed class IterationRelation : Relation, IEquatable<IterationRelation>
    {
        public override int OutputLength
        {
            get
            {
                return LoopPlan.OutputLength;
            }
        }

        public Relation? Input { get; set; }

        public required string IterationName { get; set; }

        /// <summary>
        /// The loop plan
        /// </summary>
        public required Relation LoopPlan { get; set; }

        public Expression? SkipIterateCondition { get; set; }

        public int? MaxIterations { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitIterationRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is IterationRelation relation &&
                Equals(relation);
        }

        public bool Equals(IterationRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                Equals(IterationName, other.IterationName) &&
                Equals(LoopPlan, other.LoopPlan) &&
                Equals(SkipIterateCondition, other.SkipIterateCondition) &&
                Equals(MaxIterations, other.MaxIterations);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            if (Input != null)
            {
                code.Add(Input);
            }
            code.Add(IterationName);
            code.Add(LoopPlan);
            if (SkipIterateCondition != null)
            {
                code.Add(SkipIterateCondition);
            }
            if (MaxIterations != null)
            {
                code.Add(MaxIterations);
            }
            return code.ToHashCode();
        }

        public static bool operator ==(IterationRelation? left, IterationRelation? right)
        {
            return EqualityComparer<IterationRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(IterationRelation? left, IterationRelation? right)
        {
            return !(left == right);
        }
    }
}

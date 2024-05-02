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
    public class IterationRelation : Relation
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
            if (obj is IterationRelation relation)
            {
                return EmitEquals(relation.Emit) &&
                   Equals(Input, relation.Input) &&
                   Equals(IterationName, relation.IterationName) &&
                   Equals(LoopPlan, relation.LoopPlan) &&
                   Equals(SkipIterateCondition, relation.SkipIterateCondition) &&
                   Equals(MaxIterations, relation.MaxIterations);
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
    }
}

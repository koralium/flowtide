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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    class EmitLengthValidatorVisitor : OptimizerBaseVisitor
    {
        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            if (aggregateRelation.EmitSet && aggregateRelation.Input.EmitSet &&
                aggregateRelation.Emit.Count > (aggregateRelation.Input.Emit.Count + aggregateRelation.Measures?.Count ?? 0))
            {
                Assert.Fail();
            }
            return base.VisitAggregateRelation(aggregateRelation, state);
        }

        public override Relation VisitBufferRelation(BufferRelation bufferRelation, object state)
        {
            if (bufferRelation.EmitSet && bufferRelation.Input.EmitSet &&
                bufferRelation.Emit.Count > bufferRelation.Input.Emit.Count)
            {
                Assert.Fail();
            }
            return base.VisitBufferRelation(bufferRelation, state);
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            if (filterRelation.EmitSet && filterRelation.Input.EmitSet &&
                filterRelation.Emit.Count > filterRelation.Input.Emit.Count)
            {
                Assert.Fail();
            }
            return base.VisitFilterRelation(filterRelation, state);
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            if (joinRelation.OutputLength > joinRelation.Left.OutputLength + joinRelation.Right.OutputLength)
            {
                Assert.Fail($"Join output length {joinRelation.OutputLength} bigger than input left: {joinRelation.Left.OutputLength} right {joinRelation.Right.OutputLength}");
            }
            return base.VisitJoinRelation(joinRelation, state);
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
        {
            if (mergeJoinRelation.OutputLength > mergeJoinRelation.Left.OutputLength + mergeJoinRelation.Right.OutputLength)
            {
                Assert.Fail();
            }
            return base.VisitMergeJoinRelation(mergeJoinRelation, state);
        }

        public override Relation VisitNormalizationRelation(NormalizationRelation normalizationRelation, object state)
        {
            if (normalizationRelation.OutputLength > normalizationRelation.Input.OutputLength)
            {
                Assert.Fail();
            }
            return base.VisitNormalizationRelation(normalizationRelation, state);
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            if (projectRelation.OutputLength > (projectRelation.Input.OutputLength + projectRelation.Expressions.Count))
            {
                if (projectRelation.EmitSet)
                {
                    if (projectRelation.Emit.Distinct().Count() > (projectRelation.OutputLength + projectRelation.Expressions.Count))
                    {
                        Assert.Fail();
                    }
                }
                else
                {
                    Assert.Fail();
                }
            }
            return base.VisitProjectRelation(projectRelation, state);
        }

        public override Relation VisitSetRelation(SetRelation setRelation, object state)
        {
            if (setRelation.Inputs.Any(x => setRelation.OutputLength > x.OutputLength))
            {
                Assert.Fail();
            }
            return base.VisitSetRelation(setRelation, state);
        }

        public override Relation VisitSortRelation(SortRelation sortRelation, object state)
        {
            if (sortRelation.OutputLength > sortRelation.Input.OutputLength)
            {
                Assert.Fail();
            }
            return base.VisitSortRelation(sortRelation, state);
        }

        public override Relation VisitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, object state)
        {
            if (tableFunctionRelation.OutputLength > tableFunctionRelation.Input?.OutputLength)
            {
                Assert.Fail();
            }
            return base.VisitTableFunctionRelation(tableFunctionRelation, state);
        }
    }
}

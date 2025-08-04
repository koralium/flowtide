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

using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.WatermarkOutput
{
    internal class WatermarkOutputVisitor : RelationVisitor<Relation, bool>
    {

        public void Run(Plan plan, bool[] referenceMap)
        {
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                var relation = plan.Relations[i];
                this.Visit(relation, referenceMap[i]);
            }
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, bool state)
        {
            // We will not change output mode if an aggregate relation is found
            return aggregateRelation;
        }

        public override Relation VisitBufferRelation(BufferRelation bufferRelation, bool state)
        {
            Visit(bufferRelation.Input, state);
            return bufferRelation;
        }

        public override Relation VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, bool state)
        {
            Visit(consistentPartitionWindowRelation.Input, state);
            return consistentPartitionWindowRelation;
        }

        public override Relation VisitFetchRelation(FetchRelation fetchRelation, bool state)
        {
            return fetchRelation;
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, bool state)
        {
            Visit(filterRelation.Input, state);
            return filterRelation;
        }

        public override Relation VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, bool state)
        {
            return iterationReferenceReadRelation;
        }

        public override Relation VisitIterationRelation(IterationRelation iterationRelation, bool state)
        {
            return iterationRelation;
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, bool state)
        {
            if (!state)
            {
                return joinRelation;
            }
            if (joinRelation.Type == JoinType.Left)
            {
                Visit(joinRelation.Left, state);
            }
            else if (joinRelation.Type == JoinType.Right)
            {
                Visit(joinRelation.Right, state);
            }
            else if (joinRelation.Type == JoinType.Inner)
            {
                Visit(joinRelation.Left, state);
            }
            return joinRelation;
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, bool state)
        {
            if (!state)
            {
                return mergeJoinRelation;
            }
            if (mergeJoinRelation.Type == JoinType.Left)
            {
                Visit(mergeJoinRelation.Left, state);
            }
            else if (mergeJoinRelation.Type == JoinType.Right)
            {
                Visit(mergeJoinRelation.Right, state);
            }
            else if (mergeJoinRelation.Type == JoinType.Inner)
            {
                Visit(mergeJoinRelation.Left, state);
            }
            return mergeJoinRelation;
        }

        public override Relation VisitNormalizationRelation(NormalizationRelation normalizationRelation, bool state)
        {
            Visit(normalizationRelation.Input, state);
            return normalizationRelation;
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, bool state)
        {
            Visit(projectRelation.Input, state);
            return projectRelation;
        }

        public override Relation VisitWriteRelation(WriteRelation writeRelation, bool state)
        {
            Visit(writeRelation.Input, true);
            return writeRelation;
        }

        public override Relation VisitPlanRelation(PlanRelation planRelation, bool state)
        {
            Visit(planRelation.Root, state);
            return planRelation;
        }

        public override Relation VisitExchangeRelation(ExchangeRelation exchangeRelation, bool state)
        {
            Visit(exchangeRelation.Input, state);
            return exchangeRelation;
        }

        public override Relation VisitPullExchangeReferenceRelation(PullExchangeReferenceRelation pullExchangeReferenceRelation, bool state)
        {
            return pullExchangeReferenceRelation;
        }

        public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, bool state)
        {
            return referenceRelation;
        }

        public override Relation VisitRootRelation(RootRelation rootRelation, bool state)
        {
            Visit(rootRelation.Input, state);
            return rootRelation;
        }

        public override Relation VisitSetRelation(SetRelation setRelation, bool state)
        {
            if (setRelation.Operation == SetOperation.UnionAll || setRelation.Operation == SetOperation.UnionDistinct)
            {
                foreach (var input in setRelation.Inputs)
                {
                    Visit(input, state);
                }
            }
            
            return setRelation;
        }

        public override Relation VisitReadRelation(ReadRelation readRelation, bool state)
        {
            if (state)
            {
                readRelation.Hint.Optimizations.Properties.TryAdd("WATERMARK_OUTPUT_MODE", "ON_EACH_BATCH");
            }
            return readRelation;
        }

        public override Relation VisitSortRelation(SortRelation sortRelation, bool state)
        {
            return sortRelation;
        }

        public override Relation VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, bool state)
        {
            return standardOutputExchangeReferenceRelation;
        }

        public override Relation VisitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, bool state)
        {
            // Skip table functions for now for pushdown
            return tableFunctionRelation;
        }

        public override Relation VisitSubStreamRootRelation(SubStreamRootRelation subStreamRootRelation, bool state)
        {
            Visit(subStreamRootRelation.Input, state);
            return subStreamRootRelation;
        }

        public override Relation VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, bool state)
        {
            return virtualTableReadRelation;
        }

        public override Relation VisitTopNRelation(TopNRelation topNRelation, bool state)
        {
            return topNRelation;
        }
    }
}

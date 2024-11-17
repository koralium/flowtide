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

using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Modifier
{
    internal abstract class BaseModifierVisitor : RelationVisitor<Relation, object?>
    {

        public override Relation VisitReadRelation(ReadRelation readRelation, object? state)
        {
            return readRelation;
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object? state)
        {
            filterRelation.Input = Visit(filterRelation.Input, state);
            return filterRelation;
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object? state)
        {
            joinRelation.Left = Visit(joinRelation.Left, state);
            joinRelation.Right = Visit(joinRelation.Right, state);
            return joinRelation;
        }

        public override Relation VisitNormalizationRelation(NormalizationRelation normalizationRelation, object? state)
        {
            normalizationRelation.Input = Visit(normalizationRelation.Input, state);
            return normalizationRelation;
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object? state)
        {
            projectRelation.Input = Visit(projectRelation.Input, state);
            return projectRelation;
        }

        public override Relation VisitWriteRelation(WriteRelation writeRelation, object? state)
        {
            writeRelation.Input = Visit(writeRelation.Input, state);
            return writeRelation;
        }

        public override Relation VisitPlanRelation(PlanRelation planRelation, object? state)
        {
            return planRelation;
        }

        public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, object? state)
        {
            return referenceRelation;
        }

        public override Relation VisitRootRelation(RootRelation rootRelation, object? state)
        {
            rootRelation.Input = Visit(rootRelation.Input, state);
            return rootRelation;
        }

        public override Relation VisitSetRelation(SetRelation setRelation, object? state)
        {
            for (int i = 0; i < setRelation.Inputs.Count; i++)
            {
                setRelation.Inputs[i] = Visit(setRelation.Inputs[i], state);
            }
            return setRelation;
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object? state)
        {
            mergeJoinRelation.Left = Visit(mergeJoinRelation.Left, state);
            mergeJoinRelation.Right = Visit(mergeJoinRelation.Right, state);
            return mergeJoinRelation;
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object? state)
        {
            aggregateRelation.Input = Visit(aggregateRelation.Input, state);
            return aggregateRelation;
        }

        public override Relation VisitIterationRelation(IterationRelation iterationRelation, object? state)
        {
            if (iterationRelation.Input != null)
            {
                iterationRelation.Input = Visit(iterationRelation.Input, state);
            }
            iterationRelation.LoopPlan = Visit(iterationRelation.LoopPlan, state);

            return iterationRelation;
        }

        public override Relation VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, object? state)
        {
            return iterationReferenceReadRelation;
        }

        public override Relation VisitUnwrapRelation(UnwrapRelation unwrapRelation, object? state)
        {
            unwrapRelation.Input = Visit(unwrapRelation.Input, state);
            return unwrapRelation;
        }

        public override Relation VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, object? state)
        {
            return virtualTableReadRelation;
        }

        public override Relation VisitBufferRelation(BufferRelation bufferRelation, object? state)
        {
            bufferRelation.Input = Visit(bufferRelation.Input, state);
            return bufferRelation;
        }

        public override Relation VisitFetchRelation(FetchRelation fetchRelation, object? state)
        {
            fetchRelation.Input = Visit(fetchRelation.Input, state);
            return fetchRelation;
        }

        public override Relation VisitSortRelation(SortRelation sortRelation, object? state)
        {
            sortRelation.Input = Visit(sortRelation.Input, state);
            return sortRelation;
        }

        public override Relation VisitTopNRelation(TopNRelation topNRelation, object? state)
        {
            topNRelation.Input = Visit(topNRelation.Input, state);
            return topNRelation;
        }

        public override Relation VisitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, object? state)
        {
            if (tableFunctionRelation.Input != null)
            {
                tableFunctionRelation.Input = Visit(tableFunctionRelation.Input, state);
            }
            
            return tableFunctionRelation;
        }

        public override Relation VisitSubStreamRootRelation(SubStreamRootRelation subStreamRootRelation, object? state)
        {
            subStreamRootRelation.Input = Visit(subStreamRootRelation.Input, state);
            return subStreamRootRelation;
        }

        public override Relation VisitExchangeRelation(ExchangeRelation exchangeRelation, object? state)
        {
            exchangeRelation.Input = Visit(exchangeRelation.Input, state);
            return exchangeRelation;
        }

        public override Relation VisitPullExchangeReferenceRelation(PullExchangeReferenceRelation pullExchangeReferenceRelation, object? state)
        {
            return pullExchangeReferenceRelation;
        }

        public override Relation VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, object? state)
        {
            return standardOutputExchangeReferenceRelation;
        }
    }
}

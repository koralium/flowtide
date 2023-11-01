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
    public class RelationVisitor<TReturn, TState>
    {
        public virtual TReturn Visit(Relation relation, TState state)
        {
            return relation.Accept(this, state);
        }

        public virtual TReturn VisitReadRelation(ReadRelation readRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitFilterRelation(FilterRelation filterRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitJoinRelation(JoinRelation joinRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitPlanRelation(PlanRelation planRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitProjectRelation(ProjectRelation projectRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitRootRelation(RootRelation rootRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitSetRelation(SetRelation setRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitWriteRelation(WriteRelation writeRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitReferenceRelation(ReferenceRelation referenceRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitNormalizationRelation(NormalizationRelation normalizationRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitUnwrapRelation(UnwrapRelation unwrapRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitAggregateRelation(AggregateRelation aggregateRelation, TState state)
        {
            return default;
        }
        
        public virtual TReturn VisitIterationRelation(IterationRelation iterationRelation, TState state)
        {
            return default;
        }

        public virtual TReturn VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, TState state)
        {
            return default;
        }
    }
}

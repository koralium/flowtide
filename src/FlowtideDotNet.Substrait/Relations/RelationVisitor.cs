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
            throw new NotImplementedException("Read relation is not implemented");
        }

        public virtual TReturn VisitFilterRelation(FilterRelation filterRelation, TState state)
        {
            throw new NotImplementedException("Filter relation is not implemented");
        }

        public virtual TReturn VisitJoinRelation(JoinRelation joinRelation, TState state)
        {
            throw new NotImplementedException("Join relation is not implemented");
        }

        public virtual TReturn VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, TState state)
        {
            throw new NotImplementedException("Merge join relation is not implemented");
        }

        public virtual TReturn VisitPlanRelation(PlanRelation planRelation, TState state)
        {
            throw new NotImplementedException("Plan relation is not implemented");
        }

        public virtual TReturn VisitProjectRelation(ProjectRelation projectRelation, TState state)
        {
            throw new NotImplementedException("Project relation is not implemented");
        }

        public virtual TReturn VisitRootRelation(RootRelation rootRelation, TState state)
        {
            throw new NotImplementedException("Root relation is not implemented");
        }

        public virtual TReturn VisitSetRelation(SetRelation setRelation, TState state)
        {
            throw new NotImplementedException("Set relation is not implemented");
        }

        public virtual TReturn VisitWriteRelation(WriteRelation writeRelation, TState state)
        {
            throw new NotImplementedException("Write relation is not implemented");
        }

        public virtual TReturn VisitReferenceRelation(ReferenceRelation referenceRelation, TState state)
        {
            throw new NotImplementedException("Reference relation is not implemented");
        }

        public virtual TReturn VisitNormalizationRelation(NormalizationRelation normalizationRelation, TState state)
        {
            throw new NotImplementedException("Normalization relation is not implemented");
        }

        public virtual TReturn VisitUnwrapRelation(UnwrapRelation unwrapRelation, TState state)
        {
            throw new NotImplementedException("Unwrap relation is not implemented");
        }

        public virtual TReturn VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, TState state)
        {
            throw new NotImplementedException("Virtual table read relation is not implemented");
        }

        public virtual TReturn VisitAggregateRelation(AggregateRelation aggregateRelation, TState state)
        {
            throw new NotImplementedException("Aggregate relation is not implemented");
        }
        
        public virtual TReturn VisitIterationRelation(IterationRelation iterationRelation, TState state)
        {
            throw new NotImplementedException("Iteration relation is not implemented");
        }

        public virtual TReturn VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, TState state)
        {
            throw new NotImplementedException("Iteration reference read relation is not implemented");
        }

        public virtual TReturn VisitBufferRelation(BufferRelation bufferRelation, TState state)
        {
            throw new NotImplementedException("Buffer relation is not implemented");
        }
    }
}

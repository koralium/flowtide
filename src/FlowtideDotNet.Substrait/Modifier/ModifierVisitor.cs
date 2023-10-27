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

namespace FlowtideDotNet.Substrait.Modifier
{
    internal class ModifierVisitor : RelationVisitor<Relation, object>
    {
        private readonly Dictionary<string, ReferenceInfo> subPlanNameToId;

        public ModifierVisitor(Dictionary<string, ReferenceInfo> subPlanNameToId)
        {
            this.subPlanNameToId = subPlanNameToId;
        }

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            if (subPlanNameToId.TryGetValue(readRelation.NamedTable.DotSeperated, out var referenceInfo))
            {
                return new ReferenceRelation()
                {
                    ReferenceOutputLength = referenceInfo.length,
                    RelationId = referenceInfo.index
                };
            }
            return readRelation;
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            filterRelation.Input = Visit(filterRelation.Input, state);
            return filterRelation;
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            joinRelation.Left = Visit(joinRelation.Left, state);
            joinRelation.Right = Visit(joinRelation.Right, state);
            return joinRelation;
        }

        public override Relation VisitNormalizationRelation(NormalizationRelation normalizationRelation, object state)
        {
            normalizationRelation.Input = Visit(normalizationRelation.Input, state);
            return normalizationRelation;
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            projectRelation.Input = Visit(projectRelation.Input, state);
            return projectRelation;
        }

        public override Relation VisitWriteRelation(WriteRelation writeRelation, object state)
        {
            writeRelation.Input = Visit(writeRelation.Input, state);
            return writeRelation;
        }

        public override Relation VisitPlanRelation(PlanRelation planRelation, object state)
        {
            return planRelation;
        }

        public override Relation VisitReferenceRelation(ReferenceRelation referenceRelation, object state)
        {
            return referenceRelation;
        }

        public override Relation VisitRootRelation(RootRelation rootRelation, object state)
        {
            rootRelation.Input = Visit(rootRelation.Input, state);
            return rootRelation;
        }

        public override Relation VisitSetRelation(SetRelation setRelation, object state)
        {
            return setRelation;
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
        {
            mergeJoinRelation.Left = Visit(mergeJoinRelation.Left, state);
            mergeJoinRelation.Right = Visit(mergeJoinRelation.Right, state);
            return mergeJoinRelation;
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            aggregateRelation.Input = Visit(aggregateRelation.Input, state);
            return aggregateRelation;
        }
    }
}

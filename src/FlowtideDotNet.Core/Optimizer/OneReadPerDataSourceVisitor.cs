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

namespace FlowtideDotNet.Core.Optimizer
{
    internal class OneReadPerDataSourceVisitor : OptimizerBaseVisitor
    {
        Dictionary<string, int> tableToRelationId = new Dictionary<string, int>();
        List<Relation> readRelations = new List<Relation>();
        int relationIdCounter;

        public override Plan VisitPlan(Plan plan)
        {
            relationIdCounter = plan.Relations.Count;
            plan = base.VisitPlan(plan);
            plan.Relations.AddRange(readRelations);
            return plan;
        }

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            if (readRelation.Filter == null && !readRelation.EmitSet)
            {
                if (tableToRelationId.TryGetValue(readRelation.NamedTable.DotSeperated, out var relationId))
                {
                    // Check if new columns must be selected, must update the normalization operator
                    return new ReferenceRelation()
                    {
                        RelationId = relationId
                    };
                }
                else
                {
                    relationId = relationIdCounter++;
                    readRelations.Add(readRelation);
                    tableToRelationId.Add(readRelation.NamedTable.DotSeperated, relationId);
                    return new ReferenceRelation()
                    {
                        RelationId = relationId
                    };   
                }   
            }
            return base.VisitReadRelation(readRelation, state);
        }
    }
}

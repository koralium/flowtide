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

using FlowtideDotNet.Substrait.Modifier;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait
{
    public class PlanModifier
    {
        private List<Plan> _rootPlans;
        private Dictionary<string, Plan> _subplans;
        private List<string> _writeToTables;

        public PlanModifier()
        {
            _rootPlans = new List<Plan>();
            _subplans = new Dictionary<string, Plan>();
            _writeToTables = new List<string>();
        }

        /// <summary>
        /// Add a plan that can be referenced using a read relation.
        /// This sub plan can include its own write relations.
        /// But it can also be used in the rest of the plan to read the data.
        /// </summary>
        /// <param name="viewName"></param>
        /// <param name="plan"></param>
        /// <returns></returns>
        public PlanModifier AddPlanAsView(string viewName, Plan plan)
        {
            _subplans.Add(viewName, plan);
            return this;
        }

        public PlanModifier AddRootPlan(Plan plan)
        {
            _rootPlans.Add(plan);
            return this;
        }

        [Obsolete("Use inserts with SQL instead")]
        public PlanModifier WriteToTable(string tableName) 
        {
            _writeToTables.Add(tableName);
            return this;
        }

        public Plan Modify()
        {
            if (_rootPlans.Count == 0)
            {
                throw new InvalidOperationException("No root plan has been added.");
            }
            // Convert here.
            Plan newPlan = new Plan
            {
                Relations = new List<FlowtideDotNet.Substrait.Relations.Relation>()
            };
            Dictionary<string, ReferenceInfo> subPlanNameToId = new Dictionary<string, ReferenceInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var subplan in _subplans)
            {
                Dictionary<int, int> oldRelationToNewMap = new Dictionary<int, int>();
                var referenceRemapVisitor = new ReferenceRemapVisitor(oldRelationToNewMap);

                bool containsRootRelation = subplan.Value.Relations.Any(x => x is RootRelation);
                // TODO: Must remap reference relations from sub plans to their new id.
                for (int i = 0; i < subplan.Value.Relations.Count; i++)
                {
                    var relation = subplan.Value.Relations[i];
                    if (relation is RootRelation rootRelation)
                    {
                        // Check if a view has a writerelation, if so that needs to be moved out into a different relation
                        // with reference to the select statement.
                        if (rootRelation.Input is WriteRelation writeRelation)
                        {
                            var selectPlan = writeRelation.Input;
                            var relationId = newPlan.Relations.Count;
                            subPlanNameToId.Add(subplan.Key, new ReferenceInfo(relationId, selectPlan.OutputLength));
                            newPlan.Relations.Add(selectPlan);

                            writeRelation.Input = new ReferenceRelation()
                            {
                                ReferenceOutputLength = selectPlan.OutputLength,
                                RelationId = relationId
                            };
                            newPlan.Relations.Add(writeRelation);
                        }
                        else
                        {
                            var relationId = newPlan.Relations.Count;
                            oldRelationToNewMap.Add(i, relationId);
                            subPlanNameToId.Add(subplan.Key, new ReferenceInfo(relationId, rootRelation.Input.OutputLength));
                            referenceRemapVisitor.Visit(rootRelation.Input, default);
                            newPlan.Relations.Add(rootRelation.Input);
                        }
                    }
                    else
                    {
                        var relationId = newPlan.Relations.Count;
                        oldRelationToNewMap.Add(i, relationId);
                        if (!containsRootRelation)
                        {
                            subPlanNameToId.Add(subplan.Key, new ReferenceInfo(relationId, relation.OutputLength));
                        }
                        referenceRemapVisitor.Visit(relation, default);
                        newPlan.Relations.Add(relation);
                    }
                }
            }
            var modifierVisitor = new ModifierVisitor(subPlanNameToId);

            for (int i = 0; i < newPlan.Relations.Count; i++)
            {
                var relation = newPlan.Relations[i];
                if (relation is RootRelation rootRelation)
                {
                    var modified = modifierVisitor.Visit(rootRelation.Input, default);
                    newPlan.Relations[i] = modified;
                }
                else
                {
                    var modified = modifierVisitor.Visit(relation, default);
                    newPlan.Relations[i] = modified;
                }
            }


            var rootRelationId = -1;
            RootRelation? oldRootRel = null;
            foreach (var rootPlan in _rootPlans)
            {
                Dictionary<int, int> oldRelationToNewMap = new Dictionary<int, int>();
                for (int i = 0; i < rootPlan.Relations.Count; i++)
                {
                    oldRelationToNewMap.Add(i, i + newPlan.Relations.Count);
                }
                var referenceRemapVisitor = new ReferenceRemapVisitor(oldRelationToNewMap);
                for (int i = 0; i < rootPlan.Relations.Count; i++)
                {
                    var relation = rootPlan.Relations[i];
                    referenceRemapVisitor.Visit(relation, default);
                    if (relation is RootRelation rootRelation)
                    {
                        oldRootRel = rootRelation;
                        var modified = modifierVisitor.Visit(rootRelation.Input, default);
                        //var modified = rootRelation.Input;
                        rootRelationId = newPlan.Relations.Count;
                        newPlan.Relations.Add(modified);
                    }
                    else
                    {
                        var modified = modifierVisitor.Visit(relation, default);
                        newPlan.Relations.Add(modified);
                    }
                }
            }


            foreach (var write in _writeToTables)
            {
                if (oldRootRel == null)
                {
                    throw new InvalidOperationException("No root relation found");
                }
                newPlan.Relations.Add(new RootRelation()
                {
                    Names = oldRootRel.Names,
                    Input = new WriteRelation()
                    {
                        TableSchema = new FlowtideDotNet.Substrait.Type.NamedStruct()
                        {
                            Names = oldRootRel.Names,
                            Struct = new FlowtideDotNet.Substrait.Type.Struct()
                            {
                                Types = oldRootRel.Names.Select(x =>
                                {
                                    return (SubstraitBaseType)new AnyType()
                                    {
                                        Nullable = true
                                    };
                                }).ToList()
                            }
                        },
                        NamedObject = new FlowtideDotNet.Substrait.Type.NamedTable()
                        {
                            Names = new List<string>()
                            {
                                write
                            }
                        },
                        Input = new ReferenceRelation()
                        {
                            ReferenceOutputLength = oldRootRel.OutputLength,
                            RelationId = rootRelationId
                        }
                    }
                });
            }

            return newPlan;
        }
    }
}

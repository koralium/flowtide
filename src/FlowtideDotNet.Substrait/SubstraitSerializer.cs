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

using Protobuf = Substrait.Protobuf;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Substrait
{
    internal class SubstraitSerializer
    {
        private class SerializerVisitorState
        {
            public SerializerVisitorState(Protobuf.Rel? previous, Protobuf.Plan root)
            {
                Previous = previous;
                Root = root;
            }

            public Protobuf.Rel? Previous { get; }
            public Protobuf.Plan Root { get; }
        }

        private class SerializerVisitor : RelationVisitor<object, SerializerVisitorState>
        {
            public override object VisitPlanRelation(PlanRelation planRelation, SerializerVisitorState state)
            {
                return base.VisitPlanRelation(planRelation, state);
            }

            public override object VisitRootRelation(RootRelation rootRelation, SerializerVisitorState state)
            {
                return base.VisitRootRelation(rootRelation, state);
            }

            public override object VisitProjectRelation(ProjectRelation projectRelation, SerializerVisitorState state)
            {
                var rel = new Protobuf.ProjectRel();
                rel.Common = new Protobuf.RelCommon();
                return base.VisitProjectRelation(projectRelation, state);
            }

            public override object VisitWriteRelation(WriteRelation writeRelation, SerializerVisitorState state)
            {
                var writeRel = new Protobuf.WriteRel();
                
                if (writeRelation.TableSchema != null)
                {
                    writeRel.TableSchema = new Protobuf.NamedStruct();
                    writeRel.TableSchema.Names.AddRange(writeRelation.TableSchema.Names);
                }
                if (writeRelation.Emit != null)
                {
                    
                }

                return base.VisitWriteRelation(writeRelation, state);
            }
        }

        public void Serialize(Plan plan)
        {
            var rootPlan = new Protobuf.Plan();

            var visitor = new SerializerVisitor();
            foreach (var relation in plan.Relations)
            {
                rootPlan.Relations.Add(new Protobuf.PlanRel()
                {
                    Root = new Protobuf.RelRoot()
                    {

                    }
                });
            }
            
            visitor.Visit(plan.Relations.First(), new SerializerVisitorState(null, rootPlan));
            rootPlan.Relations.Add(new Protobuf.PlanRel()
            {
                //Root = new Protobuf.RelRoot()
                //{
                //    Input = new Protobuf.Rel()
                //    {

                //    }
                //}
            });
        }
    }
}

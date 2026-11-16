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
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Collections.Generic;

namespace FlowtideDotNet.Connector.SpiceDB.Tests
{
    public class SpiceDbToFlowtideTests
    {
        [Fact]
        public void ParseParenthesisInPermission()
        {
            var schema = @"
                definition user {}
    
                definition document {
                    relation viewer: user
                    relation editor: user
                    permission can_view = (viewer + editor)
                }
            ";

            var plan = SpiceDbToFlowtide.Convert(schema, "document", "can_view", "spicedb");

            var expectedPlan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new RootRelation()
                    {
                        Names = ["subject_type", "subject_id", "subject_relation", "relation", "resource_type", "resource_id"],
                        Input = new SetRelation()
                        {
                            Operation = SetOperation.UnionAll,
                            Inputs = new List<Relation>()
                            {
                                new ProjectRelation()
                                {
                                    Emit = [0,1,2 ,6, 4, 5],
                                    Expressions = new List<Substrait.Expressions.Expression>()
                                    {
                                        new StringLiteral()
                                        {
                                            Value = "can_view"
                                        }
                                    },
                                      Input = new FilterRelation()
                                      {
                                          Condition = new ScalarFunction()
                                          {
                                              ExtensionUrn = FunctionsBoolean.Urn,
                                              ExtensionName = FunctionsBoolean.And,
                                              Arguments = new List<Expression>()
                                              {
                                                  new ScalarFunction()
                                                  {
                                                      ExtensionUrn = FunctionsComparison.Urn,
                                                      ExtensionName = FunctionsComparison.Equal,
                                                      Arguments = new List<Expression>()
                                                      {
                                                          new DirectFieldReference()
                                                          {
                                                              ReferenceSegment = new StructReferenceSegment()
                                                              {
                                                                  Field = 0
                                                              }
                                                          },
                                                          new StringLiteral()
                                                          {
                                                              Value = "user"
                                                          }
                                                      }
                                                  },
                                                  new ScalarFunction()
                                                  {
                                                      ExtensionUrn = FunctionsComparison.Urn,
                                                      ExtensionName = FunctionsComparison.Equal,
                                                      Arguments = new List<Expression>()
                                                      {
                                                          new DirectFieldReference()
                                                          {
                                                              ReferenceSegment = new StructReferenceSegment()
                                                              {
                                                                  Field = 3
                                                              }
                                                          },
                                                          new StringLiteral()
                                                          {
                                                              Value = "viewer"
                                                          }
                                                      }
                                                  },
                                                  new ScalarFunction()
                                                  {
                                                      ExtensionUrn = FunctionsComparison.Urn,
                                                      ExtensionName = FunctionsComparison.Equal,
                                                      Arguments = new List<Expression>()
                                                      {
                                                          new DirectFieldReference()
                                                          {
                                                              ReferenceSegment = new StructReferenceSegment()
                                                              {
                                                                  Field = 4
                                                              }
                                                          },
                                                          new StringLiteral()
                                                          {
                                                              Value = "document"
                                                          }
                                                      }
                                                  }
                                              }
                                          },
                                          Input = new ReadRelation()
                                          {
                                              BaseSchema = new Substrait.Type.NamedStruct()
                                              {
                                                  Names = ["subject_type", "subject_id", "subject_relation", "relation", "resource_type", "resource_id"],
                                                  Struct = new Substrait.Type.Struct()
                                                  {
                                                      Types = new List<Substrait.Type.SubstraitBaseType>()
                                                      {
                                                          new StringType(),
                                                          new StringType(),
                                                          new StringType(),
                                                          new StringType(),
                                                          new StringType(),
                                                          new StringType()
                                                      }
                                                  }
                                              },
                                              NamedTable = new Substrait.Type.NamedTable()
                                              {
                                                  Names = ["spicedb"]
                                              }
                                          }
                                      }
                                },
                                new ProjectRelation()
                                {
                                    Emit = [0,1,2 ,6, 4, 5],
                                    Expressions = new List<Substrait.Expressions.Expression>()
                                    {
                                        new StringLiteral()
                                        {
                                            Value = "can_view"
                                        }
                                    },
                                      Input = new FilterRelation()
                                      {
                                          Condition = new ScalarFunction()
                                          {
                                              ExtensionUrn = FunctionsBoolean.Urn,
                                              ExtensionName = FunctionsBoolean.And,
                                              Arguments = new List<Expression>()
                                              {
                                                  new ScalarFunction()
                                                  {
                                                      ExtensionUrn = FunctionsComparison.Urn,
                                                      ExtensionName = FunctionsComparison.Equal,
                                                      Arguments = new List<Expression>()
                                                      {
                                                          new DirectFieldReference()
                                                          {
                                                              ReferenceSegment = new StructReferenceSegment()
                                                              {
                                                                  Field = 0
                                                              }
                                                          },
                                                          new StringLiteral()
                                                          {
                                                              Value = "user"
                                                          }
                                                      }
                                                  },
                                                  new ScalarFunction()
                                                  {
                                                      ExtensionUrn = FunctionsComparison.Urn,
                                                      ExtensionName = FunctionsComparison.Equal,
                                                      Arguments = new List<Expression>()
                                                      {
                                                          new DirectFieldReference()
                                                          {
                                                              ReferenceSegment = new StructReferenceSegment()
                                                              {
                                                                  Field = 3
                                                              }
                                                          },
                                                          new StringLiteral()
                                                          {
                                                              Value = "editor"
                                                          }
                                                      }
                                                  },
                                                  new ScalarFunction()
                                                  {
                                                      ExtensionUrn = FunctionsComparison.Urn,
                                                      ExtensionName = FunctionsComparison.Equal,
                                                      Arguments = new List<Expression>()
                                                      {
                                                          new DirectFieldReference()
                                                          {
                                                              ReferenceSegment = new StructReferenceSegment()
                                                              {
                                                                  Field = 4
                                                              }
                                                          },
                                                          new StringLiteral()
                                                          {
                                                              Value = "document"
                                                          }
                                                      }
                                                  }
                                              }
                                          },
                                          Input = new ReadRelation()
                                          {
                                              BaseSchema = new Substrait.Type.NamedStruct()
                                              {
                                                  Names = ["subject_type", "subject_id", "subject_relation", "relation", "resource_type", "resource_id"],
                                                  Struct = new Substrait.Type.Struct()
                                                  {
                                                      Types = new List<Substrait.Type.SubstraitBaseType>()
                                                      {
                                                          new StringType(),
                                                          new StringType(),
                                                          new StringType(),
                                                          new StringType(),
                                                          new StringType(),
                                                          new StringType()
                                                      }
                                                  }
                                              },
                                              NamedTable = new Substrait.Type.NamedTable()
                                              {
                                                  Names = ["spicedb"]
                                              }
                                          }
                                      }
                                },
                            }
                        }
                    }
                }
            };

            Assert.Equal(expectedPlan, plan);
        }

        private const string SharedRecursionSchema = @"
            definition user {}

            definition organization {
                relation member: user
                relation parent: organization
                permission can_view = member + parent->can_view
            }

            definition project {
                relation organization: organization
                relation owner: organization
                permission can_view = organization->can_view + owner->can_view
                permission org_can_view = organization->can_view
            }
        ";

        [Fact]
        public void SameRecursivePermissionReachedTwice()
        {
            var plan = SpiceDbToFlowtide.Convert(SharedRecursionSchema, "project", "can_view", "spicedb");

            var (loops, reads) = CollectIterations(plan);

            // Identical loops share the plain name
            Assert.Equal(2, loops.Count);
            Assert.All(loops, loop => Assert.Equal("organization_can_view", loop.IterationName));
            Assert.Equal(loops[0], loops[1]);
            Assert.NotEmpty(reads);
            // Each read binds to its own enclosing loop
            Assert.All(reads, read => Assert.Equal(read.Scope[0], read.Name));
        }

        [Fact]
        public void DifferentPermissionsReachingSameLoopGetIdenticalLoops()
        {
            var organizationPlan = SpiceDbToFlowtide.Convert(SharedRecursionSchema, "organization", "can_view", "spicedb");
            var projectPlan = SpiceDbToFlowtide.Convert(SharedRecursionSchema, "project", "org_can_view", "spicedb");

            var organizationLoop = Assert.Single(CollectIterations(organizationPlan).Loops);
            var projectLoop = Assert.Single(CollectIterations(projectPlan).Loops);

            Assert.Equal(organizationLoop, projectLoop);
        }

        private static (List<IterationRelation> Loops, List<(string Name, string[] Scope)> Reads) CollectIterations(Plan plan)
        {
            var loops = new List<IterationRelation>();
            var reads = new List<(string Name, string[] Scope)>();
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                CollectIterations(plan.Relations[i], new Stack<string>(), loops, reads);
            }
            return (loops, reads);
        }

        private static void CollectIterations(Relation relation, Stack<string> scope, List<IterationRelation> loops, List<(string Name, string[] Scope)> reads)
        {
            switch (relation)
            {
                case RootRelation root:
                    CollectIterations(root.Input, scope, loops, reads);
                    break;
                case IterationRelation iteration:
                    loops.Add(iteration);
                    if (iteration.Input != null)
                    {
                        CollectIterations(iteration.Input, scope, loops, reads);
                    }
                    scope.Push(iteration.IterationName);
                    CollectIterations(iteration.LoopPlan, scope, loops, reads);
                    scope.Pop();
                    break;
                case IterationReferenceReadRelation read:
                    reads.Add((read.IterationName, scope.ToArray()));
                    break;
                case FilterRelation filter:
                    CollectIterations(filter.Input, scope, loops, reads);
                    break;
                case ProjectRelation project:
                    CollectIterations(project.Input, scope, loops, reads);
                    break;
                case JoinRelation join:
                    CollectIterations(join.Left, scope, loops, reads);
                    CollectIterations(join.Right, scope, loops, reads);
                    break;
                case SetRelation set:
                    foreach (var input in set.Inputs)
                    {
                        CollectIterations(input, scope, loops, reads);
                    }
                    break;
                case ReadRelation:
                case ReferenceRelation:
                    break;
                default:
                    throw new NotSupportedException(relation.GetType().Name);
            }
        }
    }
}

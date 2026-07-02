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
                                              ExtensionUri = FunctionsBoolean.Uri,
                                              ExtensionName = FunctionsBoolean.And,
                                              Arguments = new List<Expression>()
                                              {
                                                  new ScalarFunction()
                                                  {
                                                      ExtensionUri = FunctionsComparison.Uri,
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
                                                      ExtensionUri = FunctionsComparison.Uri,
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
                                                      ExtensionUri = FunctionsComparison.Uri,
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
                                              ExtensionUri = FunctionsBoolean.Uri,
                                              ExtensionName = FunctionsBoolean.And,
                                              Arguments = new List<Expression>()
                                              {
                                                  new ScalarFunction()
                                                  {
                                                      ExtensionUri = FunctionsComparison.Uri,
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
                                                      ExtensionUri = FunctionsComparison.Uri,
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
                                                      ExtensionUri = FunctionsComparison.Uri,
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
    }
}

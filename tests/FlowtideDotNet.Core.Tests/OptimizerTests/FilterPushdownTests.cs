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

using FlowtideDotNet.Core.Optimizer.FIlterPushdown;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.DataType;

namespace FlowtideDotNet.Core.Tests.OptimizerTests
{
    public class FilterPushdownTests
    {
        [Fact]
        public void TestPushFilterThroughInnerJoinNoEmitWholeConditionLeft()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new FilterRelation()
                    {
                        Condition = new ScalarFunction()
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
                                    Value = "test"
                                }
                            }
                        },
                        Input = new JoinRelation()
                        {
                            Type = JoinType.Inner,
                            Expression = new ScalarFunction()
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
                                    new DirectFieldReference()
                                    {
                                        ReferenceSegment = new StructReferenceSegment()
                                        {
                                            Field = 2
                                        }
                                    }
                                }
                            },
                            Left = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            },
                            Right = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        }
                    }
                }
            };

            var optimizedPlan = FilterPushdown.Optimize(plan);

            var expectedPlan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new JoinRelation()
                    {
                        Type = JoinType.Inner,
                        Expression = new ScalarFunction()
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
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 2
                                    }
                                }
                            }
                        },
                        Left = new FilterRelation()
                        {
                            Condition = new ScalarFunction()
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
                                        Value = "test"
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        },
                        Right = new ReadRelation()
                        {
                            NamedTable = new Substrait.Type.NamedTable()
                            {
                                Names = new List<string>(){ "table2" }
                            },
                            BaseSchema = new Substrait.Type.NamedStruct()
                            {
                                Names = new List<string>(){ "c1", "c2"}
                            }
                        }
                    }
                }
            };

            optimizedPlan.Should().BeEquivalentTo(expectedPlan,
                opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void TestPushFilterThroughInnerJoinNoEmitWholeConditionRight()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new FilterRelation()
                    {
                        Condition = new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 2
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = "test"
                                }
                            }
                        },
                        Input = new JoinRelation()
                        {
                            Type = JoinType.Inner,
                            Expression = new ScalarFunction()
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
                                    new DirectFieldReference()
                                    {
                                        ReferenceSegment = new StructReferenceSegment()
                                        {
                                            Field = 2
                                        }
                                    }
                                }
                            },
                            Left = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            },
                            Right = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        }
                    }
                }
            };

            var optimizedPlan = FilterPushdown.Optimize(plan);

            var expectedPlan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new JoinRelation()
                    {
                        Type = JoinType.Inner,
                        Expression = new ScalarFunction()
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
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 2
                                    }
                                }
                            }
                        },
                        Right = new FilterRelation()
                        {
                            Condition = new ScalarFunction()
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
                                        Value = "test"
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        },
                        Left = new ReadRelation()
                        {
                            NamedTable = new Substrait.Type.NamedTable()
                            {
                                Names = new List<string>(){ "table1" }
                            },
                            BaseSchema = new Substrait.Type.NamedStruct()
                            {
                                Names = new List<string>(){ "c1", "c2"}
                            }
                        }
                    }
                }
            };

            optimizedPlan.Should().BeEquivalentTo(expectedPlan,
                opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void TestPushFilterThroughJoinWithEmitOnJoinWholeConditionLeft()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new FilterRelation()
                    {
                        Condition = new ScalarFunction()
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
                                    Value = "test"
                                }
                            }
                        },
                        Input = new JoinRelation()
                        {
                            Type = JoinType.Inner,
                            Emit = new List<int>(){ 2 }, 
                            Expression = new ScalarFunction()
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
                                    new DirectFieldReference()
                                    {
                                        ReferenceSegment = new StructReferenceSegment()
                                        {
                                            Field = 2
                                        }
                                    }
                                }
                            },
                            Left = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            },
                            Right = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        }
                    }
                }
            };

            var optimizedPlan = FilterPushdown.Optimize(plan);

            var expectedPlan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new JoinRelation()
                    {
                        Type = JoinType.Inner,
                        Emit = new List<int>(){ 2 },
                        Expression = new ScalarFunction()
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
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 2
                                    }
                                }
                            }
                        },
                        Right = new FilterRelation()
                        {
                            Condition = new ScalarFunction()
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
                                        Value = "test"
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        },
                        Left = new ReadRelation()
                        {
                            NamedTable = new Substrait.Type.NamedTable()
                            {
                                Names = new List<string>(){ "table1" }
                            },
                            BaseSchema = new Substrait.Type.NamedStruct()
                            {
                                Names = new List<string>(){ "c1", "c2"}
                            }
                        }
                    }
                }
            };

            optimizedPlan.Should().BeEquivalentTo(expectedPlan,
                opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void TestPushFilterThroughLeftJoinNoEmitWholeConditionLeft()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new FilterRelation()
                    {
                        Condition = new ScalarFunction()
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
                                    Value = "test"
                                }
                            }
                        },
                        Input = new JoinRelation()
                        {
                            Type = JoinType.Left,
                            Expression = new ScalarFunction()
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
                                    new DirectFieldReference()
                                    {
                                        ReferenceSegment = new StructReferenceSegment()
                                        {
                                            Field = 2
                                        }
                                    }
                                }
                            },
                            Left = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            },
                            Right = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        }
                    }
                }
            };

            var optimizedPlan = FilterPushdown.Optimize(plan);

            var expectedPlan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new JoinRelation()
                    {
                        Type = JoinType.Left,
                        Expression = new ScalarFunction()
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
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 2
                                    }
                                }
                            }
                        },
                        Left = new FilterRelation()
                        {
                            Condition = new ScalarFunction()
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
                                        Value = "test"
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        },
                        Right = new ReadRelation()
                        {
                            NamedTable = new Substrait.Type.NamedTable()
                            {
                                Names = new List<string>(){ "table2" }
                            },
                            BaseSchema = new Substrait.Type.NamedStruct()
                            {
                                Names = new List<string>(){ "c1", "c2"}
                            }
                        }
                    }
                }
            };

            optimizedPlan.Should().BeEquivalentTo(expectedPlan,
                opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }


        [Fact]
        public void TestPushFilterThroughLeftJoinNoEmitWholeConditionRight()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new FilterRelation()
                    {
                        Condition = new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 2
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = "test"
                                }
                            }
                        },
                        Input = new JoinRelation()
                        {
                            Type = JoinType.Left,
                            Expression = new ScalarFunction()
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
                                    new DirectFieldReference()
                                    {
                                        ReferenceSegment = new StructReferenceSegment()
                                        {
                                            Field = 2
                                        }
                                    }
                                }
                            },
                            Left = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            },
                            Right = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        }
                    }
                }
            };

            var optimizedPlan = FilterPushdown.Optimize(plan);

            var expectedPlan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new FilterRelation()
                    {
                        Condition = new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 2
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = "test"
                                }
                            }
                        },
                        Input = new JoinRelation()
                        {
                            Type = JoinType.Left,
                            Expression = new ScalarFunction()
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
                                    new DirectFieldReference()
                                    {
                                        ReferenceSegment = new StructReferenceSegment()
                                        {
                                            Field = 2
                                        }
                                    }
                                }
                            },
                            Left = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            },
                            Right = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        }
                    }
                }
            };

            optimizedPlan.Should().BeEquivalentTo(expectedPlan,
                opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void TestPushFilterThroughInnerJoinNoEmitWholeAndExpressionLeftAndRight()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new FilterRelation()
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
                                            Value = "test"
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
                                                Field = 2
                                            }
                                        },
                                        new StringLiteral()
                                        {
                                            Value = "test"
                                        }
                                    }
                                }
                            }
                        },
                        Input = new JoinRelation()
                        {
                            Type = JoinType.Inner,
                            Expression = new ScalarFunction()
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
                                    new DirectFieldReference()
                                    {
                                        ReferenceSegment = new StructReferenceSegment()
                                        {
                                            Field = 2
                                        }
                                    }
                                }
                            },
                            Left = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            },
                            Right = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        }
                    }
                }
            };

            var optimizedPlan = FilterPushdown.Optimize(plan);

            var expectedPlan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new JoinRelation()
                    {
                        Type = JoinType.Inner,
                        Expression = new ScalarFunction()
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
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 2
                                    }
                                }
                            }
                        },
                        Left = new FilterRelation()
                        {
                            Condition = new ScalarFunction()
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
                                        Value = "test"
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        },
                        Right = new FilterRelation()
                        {
                            Condition = new ScalarFunction()
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
                                        Value = "test"
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        }
                    }
                }
            };

            optimizedPlan.Should().BeEquivalentTo(expectedPlan,
                opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }


        [Fact]
        public void TestPushFilterThroughLeftJoinNoEmitWholeAndExpressionLeftAndRight()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new FilterRelation()
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
                                            Value = "test"
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
                                                Field = 2
                                            }
                                        },
                                        new StringLiteral()
                                        {
                                            Value = "test"
                                        }
                                    }
                                }
                            }
                        },
                        Input = new JoinRelation()
                        {
                            Type = JoinType.Left,
                            Expression = new ScalarFunction()
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
                                    new DirectFieldReference()
                                    {
                                        ReferenceSegment = new StructReferenceSegment()
                                        {
                                            Field = 2
                                        }
                                    }
                                }
                            },
                            Left = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            },
                            Right = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        }
                    }
                }
            };

            var optimizedPlan = FilterPushdown.Optimize(plan);

            var expectedPlan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new FilterRelation()
                    {
                        Condition = new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 2
                                    }
                                },
                                new StringLiteral()
                                {
                                    Value = "test"
                                }
                            }
                        },
                        Input = new JoinRelation()
                        {
                            Type = JoinType.Left,
                            Expression = new ScalarFunction()
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
                                    new DirectFieldReference()
                                    {
                                        ReferenceSegment = new StructReferenceSegment()
                                        {
                                            Field = 2
                                        }
                                    }
                                }
                            },
                            Left = new FilterRelation()
                            {
                                Condition = new ScalarFunction()
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
                                            Value = "test"
                                        }
                                    }
                                },
                                Input = new ReadRelation()
                                {
                                    NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table1" }
                                },
                                    BaseSchema = new Substrait.Type.NamedStruct()
                                    {
                                        Names = new List<string>(){ "c1", "c2"}
                                    }
                                }
                            },
                            Right = new ReadRelation()
                            {
                                NamedTable = new Substrait.Type.NamedTable()
                                {
                                    Names = new List<string>(){ "table2" }
                                },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "c1", "c2"}
                                }
                            }
                        }
                    }
                    
                }
            };

            optimizedPlan.Should().BeEquivalentTo(expectedPlan,
                opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }
    }
}

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

using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using FluentAssertions;

namespace FlowtideDotNet.Substrait.Tests
{
    public class SqlTests
    {
        private SqlPlanBuilder builder;
        public SqlTests()
        {
            builder = new SqlPlanBuilder();
        }

        [Fact]
        public void TestCreateTable()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );
            ");

            var exists = builder._tablesMetadata.TryGetTable("testtable", out var table);
            Assert.True(exists);

            table.Should().BeEquivalentTo(
                new TableMetadata("testtable", new List<string>() { "c1", "c2" })
                , opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectOnly()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT c1, c2 FROM testtable
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2,3},
                            Expressions = new List<Expression>()
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
                                        Field = 1
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectWithFilterEqualsString()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT c1, c2 FROM testtable
                WHERE c1 = 'test'
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2,3},
                            Expressions = new List<Expression>()
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
                                        Field = 1
                                    }
                                }
                            },
                            Input = new FilterRelation(){
                                Input = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct() {
                                        Names = new List<string>() { "c1", "c2" },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                        }
                                    },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                },
                                Condition = new ScalarFunction()
                                {
                                    ExtensionName = FunctionsComparison.Equal,
                                    ExtensionUri = FunctionsComparison.Uri,
                                    Arguments = new List<Expression>()
                                    {
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment(){ Field = 0 }
                                        },
                                        new StringLiteral(){ Value = "test" }
                                    }
                                }
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        /// <summary>
        /// Check that selecting from view actually uses the reference
        /// </summary>
        [Fact]
        public void SelectFromView()
        {
            builder.Sql(@"
            CREATE TABLE testtable (
                c1 any
            );
            
            CREATE VIEW testview AS
            SELECT c1 FROM testtable;

            SELECT c1 FROM testview;
            ");

            var plan = builder.GetPlan();
            plan.Should().BeEquivalentTo(new Plan()
            {
                Relations = new List<Relations.Relation>()
                {
                    new ProjectRelation()
                    {
                        Emit = new List<int>(){ 1 },
                        Expressions = new List<Expressions.Expression>()
                        {
                            new DirectFieldReference()
                            {
                                ReferenceSegment = new StructReferenceSegment()
                                {
                                    Field = 0
                                }
                            }
                        },
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct(){
                                Names = new List<string>() { "c1" },
                                Struct = new Type.Struct()
                                {
                                    Types = new List<Type.SubstraitBaseType>(){ new AnyType() }
                                }
                            },
                            NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                        }
                    },
                    new ProjectRelation()
                    {
                        Emit = new List<int>(){ 1 },
                        Expressions = new List<Expressions.Expression>()
                        {
                            new DirectFieldReference()
                            {
                                ReferenceSegment = new StructReferenceSegment()
                                {
                                    Field = 0
                                }
                            }
                        },
                        Input = new ReferenceRelation()
                        {
                            ReferenceOutputLength = 1,
                            RelationId = 0
                        }
                    }
                }
            }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void LeftJoinSimple()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                CREATE TABLE other (
                    c1 any,
                    c2 any
                );

                SELECT t.c1, o.c2 FROM testtable t
                LEFT JOIN other o
                ON t.c1 = o.c1
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){4, 5},
                            Expressions = new List<Expression>()
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
                                        Field = 3
                                    }
                                }
                            },
                            Input = new JoinRelation(){
                                Type = JoinType.Left,
                                Left = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct(){
                                        Names = new List<string>() { "c1", "c2" },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                        }
                                    },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                },
                                Right = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct(){
                                        Names = new List<string>() { "c1", "c2" },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                        }
                                    },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "other" }}
                                },
                                Expression = new ScalarFunction()
                                {
                                    ExtensionName = FunctionsComparison.Equal,
                                    ExtensionUri = FunctionsComparison.Uri,
                                    Arguments = new List<Expression>()
                                    {
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment(){ Field = 0 }
                                        },
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment(){ Field = 2 }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void InnerJoinSimple()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                CREATE TABLE other (
                    c1 any,
                    c2 any
                );

                SELECT t.c1, o.c2 FROM testtable t
                INNER JOIN other o
                ON t.c1 = o.c1
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){4, 5},
                            Expressions = new List<Expression>()
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
                                        Field = 3
                                    }
                                }
                            },
                            Input = new JoinRelation(){
                                Type = JoinType.Inner,
                                Left = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                },
                                Right = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "other" }}
                                },
                                Expression = new ScalarFunction()
                                {
                                    ExtensionName = FunctionsComparison.Equal,
                                    ExtensionUri = FunctionsComparison.Uri,
                                    Arguments = new List<Expression>()
                                    {
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment(){ Field = 0 }
                                        },
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment(){ Field = 2 }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }


        [Fact]
        public void UnionTwoTables()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                CREATE TABLE other (
                    c1 any,
                    c2 any
                );

                SELECT c1, c2 FROM testtable
                UNION
                SELECT c1, c2 from other
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new SetRelation()
                        {
                            Operation = SetOperation.UnionAll,
                            Inputs = new List<Relation>()
                            {
                                new ProjectRelation()
                                {
                                    Emit = new List<int>(){2, 3},
                                    Expressions = new List<Expression>()
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
                                                Field = 1
                                            }
                                        }
                                    },
                                    Input = new ReadRelation()
                                    {
                                        BaseSchema = new Type.NamedStruct() {
                                            Names = new List<string>() { "c1", "c2" },
                                            Struct = new Type.Struct()
                                            {
                                                Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                            }
                                        },
                                        NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                    }
                                },
                                new ProjectRelation()
                                {
                                    Emit = new List<int>(){2, 3},
                                    Expressions = new List<Expression>()
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
                                                Field = 1
                                            }
                                        }
                                    },
                                    Input = new ReadRelation()
                                    {
                                        BaseSchema = new Type.NamedStruct() {
                                            Names = new List<string>() { "c1", "c2" },
                                            Struct = new Type.Struct()
                                            {
                                                Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                            }
                                        },
                                        NamedTable = new Type.NamedTable(){Names = new List<string> { "other" }}
                                    }
                                }
                            }
                        },

                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void StringConcat()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT c1 || c2 || 'test' FROM testtable
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2},
                            Expressions = new List<Expression>()
                            {
                                new ScalarFunction()
                                {
                                    ExtensionUri = FunctionsString.Uri,
                                    ExtensionName = FunctionsString.Concat,
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
                                                Field = 1
                                            }
                                        },
                                        new StringLiteral()
                                        {
                                            Value = "test"
                                        }
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct() {
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void CaseWhenThenElse()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT CASE WHEN c1 = 'test' THEN 1 WHEN c1 = 'test2' THEN 2 ELSE 3 END as case FROM testtable
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2},
                            Expressions = new List<Expression>()
                            {
                                new IfThenExpression()
                                {
                                    Ifs = new List<IfClause>()
                                    {
                                        new IfClause()
                                        {
                                            If = new ScalarFunction()
                                            {
                                                ExtensionName = FunctionsComparison.Equal,
                                                ExtensionUri = FunctionsComparison.Uri,
                                                Arguments = new List<Expression>()
                                                {
                                                    new DirectFieldReference()
                                                    {
                                                        ReferenceSegment = new StructReferenceSegment(){ Field = 0 }
                                                    },
                                                    new StringLiteral()
                                                    {
                                                        Value = "test"
                                                    }
                                                }
                                            },
                                            Then = new NumericLiteral(){Value = 1 }
                                        },
                                        new IfClause()
                                        {
                                            If = new ScalarFunction()
                                            {
                                                ExtensionName = FunctionsComparison.Equal,
                                                ExtensionUri = FunctionsComparison.Uri,
                                                Arguments = new List<Expression>()
                                                {
                                                    new DirectFieldReference()
                                                    {
                                                        ReferenceSegment = new StructReferenceSegment(){ Field = 0 }
                                                    },
                                                    new StringLiteral()
                                                    {
                                                        Value = "test2"
                                                    }
                                                }
                                            },
                                            Then = new NumericLiteral(){Value = 2 }
                                        }
                                    },
                                    Else = new NumericLiteral(){ Value = 3 }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void CoalesceTest()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT coalesce(c1, c2, 1) FROM testtable
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2},
                            Expressions = new List<Expression>()
                            {
                                new ScalarFunction()
                                {
                                    ExtensionUri = FunctionsComparison.Uri,
                                    ExtensionName = FunctionsComparison.Coalesce,
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
                                                Field = 1
                                            }
                                        },
                                        new NumericLiteral(){ Value = 1 }
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectBoolLiteral()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT true, false FROM testtable
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2,3},
                            Expressions = new List<Expression>()
                            {
                                new BoolLiteral()
                                {
                                    Value = true
                                },
                                new BoolLiteral()
                                {
                                    Value = false
                                }
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectNullLiteral()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT null FROM testtable
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2},
                            Expressions = new List<Expression>()
                            {
                                new NullLiteral()
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void WhereIsNotNull()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT c1 FROM testtable
                WHERE c1 is not null
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2},
                            Expressions = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 0
                                    }
                                }
                            },
                            Input = new FilterRelation(){
                                Condition = new ScalarFunction()
                                {
                                    ExtensionUri = FunctionsComparison.Uri,
                                    ExtensionName = FunctionsComparison.IsNotNull,
                                    Arguments = new List<Expression>()
                                    {
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment()
                                            {
                                                Field = 0
                                            }
                                        }
                                    }
                                },
                                Input = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct() {
                                        Names = new List<string>() { "c1", "c2" },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                        }
                                    },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                }
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void QuotaUsageSpecialWord()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    [key] any
                );

                SELECT c1, [key] FROM testtable
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2,3},
                            Expressions = new List<Expression>()
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
                                        Field = 1
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "key" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void CreateTableWithNoType()
        {
            var b1 = new SqlPlanBuilder();
            b1.Sql(@"
                CREATE TABLE test (
                    c1 amy,
                    c2 any
                );

                SELECT c1, c2 FROM test
            ");

            var b2 = new SqlPlanBuilder();
            b2.Sql(@"
                CREATE TABLE test (
                    c1,
                    c2
                );

                SELECT c1, c2 FROM test
            ");

            var b1plan = b1.GetPlan();
            var b2plan = b2.GetPlan();

            b2plan.Should().BeEquivalentTo(b1plan, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        private sealed class TestTableProvider : ITableProvider
        {
            public bool TryGetTableInformation(string tableName, out TableMetadata? tableMetadata)
            {
                if (tableName.Equals("testtable", StringComparison.OrdinalIgnoreCase))
                {
                    tableMetadata = new TableMetadata("testtable", new List<string>() { "c1", "c2" });
                    return true;
                }
                tableMetadata = default;
                return false;
            }
        }

        [Fact]
        public void TableProviderTest()
        {
            SqlPlanBuilder tmpBuilder = new SqlPlanBuilder();
            tmpBuilder.AddTableProvider(new TestTableProvider());
            tmpBuilder.Sql(@"
                SELECT c1, c2 FROM testtable
            ");

            var plan = tmpBuilder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2,3},
                            Expressions = new List<Expression>()
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
                                        Field = 1
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectAggregate()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT count(*) FROM testtable
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){1},
                            Expressions = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 0
                                    }
                                }
                            },
                            Input = new AggregateRelation()
                            {
                                Input = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct(){
                                        Names = new List<string>() { "c1", "c2" },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                        }
                                    },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                },
                                Measures = new List<AggregateMeasure>()
                                {
                                    new AggregateMeasure()
                                    {
                                        Measure = new AggregateFunction()
                                        {
                                            ExtensionUri = FunctionsAggregateGeneric.Uri,
                                            ExtensionName = FunctionsAggregateGeneric.Count,
                                            Arguments = new List<Expression>()
                                        }
                                    }
                                }
                            }   
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectAggregateWithGroup()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT c1, count(*) FROM testtable
                GROUP BY c1
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2,3},
                            Expressions = new List<Expression>()
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
                                        Field = 1
                                    }
                                }
                            },
                            Input = new AggregateRelation()
                            {
                                Input = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct(){
                                        Names = new List<string>() { "c1", "c2" },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                        }
                                    },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                },
                                Groupings = new List<AggregateGrouping>()
                                {
                                    new AggregateGrouping()
                                    {
                                        GroupingExpressions = new List<Expression>()
                                        {
                                            new DirectFieldReference()
                                            {
                                                ReferenceSegment = new StructReferenceSegment()
                                                {
                                                    Field = 0
                                                }
                                            }
                                        }
                                    }
                                },
                                Measures = new List<AggregateMeasure>()
                                {
                                    new AggregateMeasure()
                                    {
                                        Measure = new AggregateFunction()
                                        {
                                            ExtensionUri = FunctionsAggregateGeneric.Uri,
                                            ExtensionName = FunctionsAggregateGeneric.Count,
                                            Arguments = new List<Expression>()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectWithAddition()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT c1 + 1 FROM testtable
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2},
                            Expressions = new List<Expression>()
                            {
                                new ScalarFunction()
                                {
                                    ExtensionUri = FunctionsArithmetic.Uri,
                                    ExtensionName = FunctionsArithmetic.Add,
                                    Arguments = new List<Expression>()
                                    {
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment()
                                            {
                                                Field = 0
                                            }
                                        },
                                        new NumericLiteral()
                                        {
                                            Value = 1
                                        }
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void TestMultipleInserts()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                INSERT INTO output
                SELECT c1 FROM testtable;

                INSERT INTO output
                SELECT c1 FROM testtable;
            ");

            var plan = builder.GetPlan();
        }

        [Fact]
        public void SelectWithWildcard()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT * FROM testtable
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2, 3},
                            Expressions = new List<Expression>()
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
                                        Field = 1
                                    }
                                },
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectWithWildcardAliasOnTable()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT * FROM testtable t
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2, 3},
                            Expressions = new List<Expression>()
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
                                        Field = 1
                                    }
                                },
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectWithQualifiedWildcard()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT t.* FROM testtable t
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){2, 3},
                            Expressions = new List<Expression>()
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
                                        Field = 1
                                    }
                                },
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Type.NamedStruct(){
                                    Names = new List<string>() { "c1", "c2" },
                                    Struct = new Type.Struct()
                                    {
                                        Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                    }
                                },
                                NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectWithQualifiedWildcardWithJoin()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT t.* FROM testtable t
                LEFT JOIN testtable t2 ON t.c1 = t2.c1
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){4, 5},
                            Expressions = new List<Expression>()
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
                                        Field = 1
                                    }
                                },
                            },
                            Input = new JoinRelation()
                            {
                                Type = JoinType.Left,
                                Expression = new ScalarFunction()
                                {
                                    ExtensionUri = FunctionsComparison.Uri,
                                    ExtensionName = FunctionsComparison.Equal,
                                    Arguments = new List<Expression>
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
                                    BaseSchema = new Type.NamedStruct(){
                                        Names = new List<string>() { "c1", "c2" },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                        }
                                    },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                },
                                Right = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct(){
                                        Names = new List<string>() { "c1", "c2" },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                        }
                                    },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                }
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }

        [Fact]
        public void SelectWithWildcardWithJoin()
        {
            builder.Sql(@"
                CREATE TABLE testtable (
                    c1 any,
                    c2 any
                );

                SELECT * FROM testtable t
                LEFT JOIN testtable t2 ON t.c1 = t2.c1
            ");

            var plan = builder.GetPlan();

            plan.Should().BeEquivalentTo(
                new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){4, 5, 6, 7},
                            Expressions = new List<Expression>()
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
                                        Field = 1
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 2
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 3
                                    }
                                },
                            },
                            Input = new JoinRelation()
                            {
                                Type = JoinType.Left,
                                Expression = new ScalarFunction()
                                {
                                    ExtensionUri = FunctionsComparison.Uri,
                                    ExtensionName = FunctionsComparison.Equal,
                                    Arguments = new List<Expression>
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
                                    BaseSchema = new Type.NamedStruct(){
                                        Names = new List<string>() { "c1", "c2" },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                        }
                                    },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                },
                                Right = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct(){
                                        Names = new List<string>() { "c1", "c2" },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>(){ new AnyType(), new AnyType() }
                                        }
                                    },
                                    NamedTable = new Type.NamedTable(){Names = new List<string> { "testtable" }}
                                }
                            }
                        }
                    }
                }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes());
        }
    }
}

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
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Tests
{
    public class SerializeTests
    {
        [Fact]
        public void SerializeWriteAndRead()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any);
                create table table2 (b any);
                
                insert into out
                select lower(a) FROM table1 t1
                LEFT JOIN table2 t2 ON t1.a = t2.b;
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeTopNRelation()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any);
                
                insert into out
                select TOP (1) a FROM table1 t1 ORDER BY a
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeTableFunctionInFrom()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                insert into out
                select a FROM UNNEST(list(1,2,3)) a
            ");
            var plan = sqlPlanBuilder.GetPlan();

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeTableFunctionInJoin()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any);

                insert into out
                select t.a FROM table1 t
                LEFT JOIN UNNEST(t.a) c ON c = 123
            ");
            var plan = sqlPlanBuilder.GetPlan();

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeSubstreamRootRelation()
        {
            var plan = new Plan()
            {
                Relations = new List<Relations.Relation>()
                {
                    new SubStreamRootRelation()
                    {
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["a"]
                            }
                        },
                        Name = "1",
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeListNestedExpression()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new ProjectRelation()
                    {
                        Expressions = [new ListNestedExpression() { Values = [new StringLiteral() { Value = "abc" }] }],
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["a"]
                            }
                        },
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeIterations()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
            CREATE TABLE users (userKey INT, firstName STRING, lastName STRING, managerKey INT);
            with user_manager_cte AS (
                SELECT 
                    userKey, 
                    firstName,
                    lastName,
                    managerKey,
                    null as ManagerFirstName,
                    1 as level
                FROM users
                WHERE managerKey is null
                UNION ALL
                SELECT 
                    u.userKey, 
                    u.firstName,
                    u.lastName,
                    u.managerKey,
                    umc.firstName as ManagerFirstName,
                    level + 1 as level 
                FROM users u
                INNER JOIN user_manager_cte umc ON umc.userKey = u.managerKey
            )
            INSERT INTO output
            SELECT userKey, firstName, lastName, managerKey, ManagerFirstName, level
            FROM user_manager_cte
            ");

            var plan = sqlPlanBuilder.GetPlan();

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeCastExpression()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new ProjectRelation()
                    {
                        Expressions = [new CastExpression() { Expression = new StringLiteral() { Value = "abc" }, Type = new Int64Type() }],
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["a"]
                            }
                        },
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeMapNestedExpression()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new ProjectRelation()
                    {
                        Expressions = [new MapNestedExpression() {
                            KeyValues = new List<KeyValuePair<Expression, Expression>>(){
                                new KeyValuePair<Expression, Expression>(new StringLiteral() { Value = "key" }, new StringLiteral() { Value = "value" })
                            }
                        }],
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["a"]
                            }
                        },
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeTop()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any);

                insert into out
                select TOP (1000) a FROM table1
                ORDER BY a;
            ");
            var plan = sqlPlanBuilder.GetPlan();

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeWindowFunction()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                insert into out
                select a, SUM(b) OVER (PARTITION BY a ORDER BY b ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as sum_b FROM table1;
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeMergeJoin()
        {
            var plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new MergeJoinRelation()
                    {
                        Left = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["a"]
                            }
                        },
                        Right = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["a"]
                            }
                        },
                        LeftKeys = new List<FieldReference>()
                        {
                             new DirectFieldReference()
                             {
                                 ReferenceSegment = new StructReferenceSegment()
                                 {
                                     Field = 0
                                 }
                             }
                        },
                        RightKeys = new List<FieldReference>()
                        {
                            new DirectFieldReference()
                             {
                                 ReferenceSegment = new StructReferenceSegment()
                                 {
                                     Field = 1
                                 }
                             }
                        },
                        Type = JoinType.Inner
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeOrList()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new ProjectRelation()
                    {
                        Expressions = [
                            new SingularOrListExpression(){
                                Options = new List<Expression>(){
                                    new StringLiteral() { Value = "a" },
                                    new StringLiteral() { Value = "b" },
                                },
                                Value = new StringLiteral() { Value = "a" }
                            }
                            ],
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["a"]
                            }
                        },
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void TestExchangeRelationWithScatter()
        {
            var plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new ExchangeRelation()
                    {
                        ExchangeKind = new ScatterExchangeKind()
                        {
                            Fields = new List<FieldReference>()
                            {
                                new DirectFieldReference()
                                 {
                                     ReferenceSegment = new StructReferenceSegment()
                                     {
                                         Field = 1
                                     }
                                 }
                            }
                        },
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["a"]
                            }
                        },
                        Targets = new List<ExchangeTarget>()
                        {
                            new StandardOutputExchangeTarget()
                            {
                                PartitionIds = new List<int>(){ 0, 1, 2, 3 }
                            }
                        },
                        PartitionCount = 4
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void TestStandardOutputReference()
        {
            var plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new ExchangeRelation()
                    {
                        ExchangeKind = new ScatterExchangeKind()
                        {
                            Fields = new List<FieldReference>()
                            {
                                new DirectFieldReference()
                                 {
                                     ReferenceSegment = new StructReferenceSegment()
                                     {
                                         Field = 1
                                     }
                                 }
                            }
                        },
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["a"]
                            }
                        },
                        Targets = new List<ExchangeTarget>()
                        {
                            new StandardOutputExchangeTarget()
                            {
                                PartitionIds = new List<int>(){ 0, 1, 2, 3 }
                            }
                        },
                        PartitionCount = 4
                    },
                    new StandardOutputExchangeReferenceRelation()
                    {
                        RelationId = 0,
                        TargetId = 1,
                        ReferenceOutputLength = 1
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void TestSerializeAggregate()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                insert into out
                select a, sum(b) as sum_b FROM table1 GROUP BY a;
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void TestSerializeBufferRelation()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new BufferRelation()
                    {
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["a"]
                            }
                        },
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }


        /// <summary>
        /// This will add a named struct type in the write relation.
        /// This test makes sure it can be serialized and deserialized
        /// </summary>
        [Fact]
        public void TestNamedStruct()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                insert into out
                select a, b, named_struct('hello', a, 'world', b) as my_struct FROM table1;
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        private void AssertPlanCanSerializeDeserialize(Plan plan)
        {
            var json = SubstraitSerializer.SerializeToJson(plan);
            var deserializedPlan = SubstraitDeserializer.DeserializeFromJson(json);
            Assert.Equal(plan, deserializedPlan);
        }
    }
}

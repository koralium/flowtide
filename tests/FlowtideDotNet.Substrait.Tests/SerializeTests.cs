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

using FlowtideDotNet.Substrait.Exceptions;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System.Globalization;
using System.Numerics;
using Protobuf = global::Substrait.Protobuf;

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
        public void SerializeScalarFunctionWithOptions()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new ProjectRelation()
                    {
                        Expressions = [new ScalarFunction()
                        {
                            ExtensionUrn = FunctionsString.Urn,
                            ExtensionName = FunctionsString.Substring,
                            Arguments = [new StringLiteral() { Value = "a" }],
                            Options = new SortedList<string, string>() { { "negative_start", "WRAP_FROM_END" } }
                        }],
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct() { Names = ["a"] },
                            NamedTable = new Type.NamedTable() { Names = ["a"] }
                        },
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeAggregateFunctionWithOptions()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new AggregateRelation()
                    {
                        Groupings = new List<AggregateGrouping>(),
                        Measures = new List<AggregateMeasure>()
                        {
                            new AggregateMeasure()
                            {
                                Measure = new AggregateFunction()
                                {
                                    ExtensionUrn = FunctionsArithmetic.Urn,
                                    ExtensionName = FunctionsArithmetic.Sum,
                                    Arguments = [new StringLiteral() { Value = "a" }],
                                    Options = new SortedList<string, string>() { { "NULL_TREATMENT", "IGNORE_NULLS" } }
                                }
                            }
                        },
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct() { Names = ["a"] },
                            NamedTable = new Type.NamedTable() { Names = ["a"] }
                        },
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeWindowFunctionWithOptions()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                insert into out
                select a, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) as rn FROM table1;
            ");
            var plan = sqlPlanBuilder.GetPlan();

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            windowRelation.WindowFunctions[0].Options = new SortedList<string, string>()
            {
                { "max_row_number", "1" }
            };

            var json = SubstraitSerializer.SerializeToJson(plan);
            var deserializedPlan = SubstraitDeserializer.DeserializeFromJson(json);

            var deserializedWindowRelation = FindWindowRelation(deserializedPlan.Relations[deserializedPlan.Relations.Count - 1]);
            Assert.NotNull(deserializedWindowRelation.WindowFunctions[0].Options);
            Assert.Equal("1", deserializedWindowRelation.WindowFunctions[0].Options!["max_row_number"]);
        }

        private static ConsistentPartitionWindowRelation FindWindowRelation(Relation relation)
        {
            switch (relation)
            {
                case ConsistentPartitionWindowRelation windowRelation:
                    return windowRelation;
                case FilterRelation filterRelation:
                    return FindWindowRelation(filterRelation.Input);
                case ProjectRelation projectRelation:
                    return FindWindowRelation(projectRelation.Input);
                case WriteRelation writeRelation:
                    return FindWindowRelation(writeRelation.Input);
                case RootRelation rootRelation:
                    return FindWindowRelation(rootRelation.Input);
                default:
                    throw new InvalidOperationException($"No window relation found under {relation.GetType().Name}");
            }
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

        /// <summary>
        /// AggregateFunction.OutputType (Substrait output_type) must survive a serialize/deserialize round
        /// trip. It is intentionally excluded from AggregateFunction.Equals, so assert it explicitly rather
        /// than relying on plan equality. sum0 depends on it to emit a correctly-typed zero for empty groups.
        /// </summary>
        [Fact]
        public void SerializeAggregatePreservesMeasureOutputType()
        {
            var plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new AggregateRelation()
                    {
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct() { Names = ["a"] },
                            NamedTable = new Type.NamedTable() { Names = ["a"] }
                        },
                        Measures = new List<AggregateMeasure>()
                        {
                            new AggregateMeasure()
                            {
                                Measure = new AggregateFunction()
                                {
                                    ExtensionUrn = FunctionsArithmetic.Urn,
                                    ExtensionName = FunctionsArithmetic.Sum0,
                                    Arguments = new List<Expression>()
                                    {
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment() { Field = 0 }
                                        }
                                    },
                                    OutputType = new Int64Type()
                                }
                            }
                        }
                    }
                }
            };

            var json = SubstraitSerializer.SerializeToJson(plan);
            var deserialized = SubstraitDeserializer.DeserializeFromJson(json);

            var measure = ((AggregateRelation)deserialized.Relations[0]).Measures![0].Measure;
            Assert.NotNull(measure.OutputType);
            Assert.Equal(SubstraitType.Int64, measure.OutputType!.Type);
        }

        [Fact]
        public void TestSerializeReadRelationWithRowStore()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new ReadRelation()
                    {
                        BaseSchema = new Type.NamedStruct()
                        {
                            Names = ["a", "b", "c"]
                        },
                        NamedTable = new Type.NamedTable()
                        {
                            Names = ["t"]
                        },
                        RowStore = new RowStoreDefinition()
                        {
                            Name = "t_rows",
                            StoredColumns = [1, 2]
                        }
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
            var deserialized = SubstraitDeserializer.DeserializeFromJson(SubstraitSerializer.SerializeToJson(plan));
            Assert.Equal(2, deserialized.Relations[0].OutputLength);
        }

        [Fact]
        public void ARowStoreIsPartOfThePlanWithoutHints()
        {
            Plan Read(RowStoreDefinition? rowStore)
            {
                return new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct() { Names = ["a", "b", "c"] },
                            NamedTable = new Type.NamedTable() { Names = ["t"] },
                            RowStore = rowStore
                        }
                    }
                };
            }
            var plain = SubstraitSerializer.SerializeToJson(Read(null), includeHints: false);
            var stored = SubstraitSerializer.SerializeToJson(Read(new RowStoreDefinition() { Name = "rows", StoredColumns = [1] }), includeHints: false);
            var storedOther = SubstraitSerializer.SerializeToJson(Read(new RowStoreDefinition() { Name = "rows", StoredColumns = [2] }), includeHints: false);

            Assert.NotEqual(plain, stored);
            Assert.NotEqual(stored, storedOther);
        }

        [Fact]
        public void ANormalizationStoreAndAFetchArePartOfThePlanWithoutHints()
        {
            Plan Stored(int storedColumn, int fetchedColumn)
            {
                return new Plan()
                {
                    Relations = new List<Relation>()
                    {
                        new FetchRequiredColumnsRelation()
                        {
                            Fetches = [new RowStoreFetch() { ReferenceColumn = 2, StoreName = "rows", Columns = [fetchedColumn] }],
                            Input = new NormalizationRelation()
                            {
                                KeyIndex = [0],
                                RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [storedColumn, 3] },
                                Input = new ReadRelation()
                                {
                                    BaseSchema = new Type.NamedStruct() { Names = ["a", "b", "c", "d"] },
                                    NamedTable = new Type.NamedTable() { Names = ["t"] }
                                }
                            }
                        }
                    }
                };
            }
            var plan = SubstraitSerializer.SerializeToJson(Stored(1, 0), includeHints: false);

            Assert.NotEqual(plan, SubstraitSerializer.SerializeToJson(Stored(2, 0), includeHints: false));
            Assert.NotEqual(plan, SubstraitSerializer.SerializeToJson(Stored(1, 1), includeHints: false));
            Assert.Equal(plan, SubstraitSerializer.SerializeToJson(Stored(1, 0), includeHints: false));
        }

        [Fact]
        public void AnotherEnhancementOnAReadIsIgnored()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new ReadRelation()
                    {
                        BaseSchema = new Type.NamedStruct() { Names = ["a", "b"] },
                        NamedTable = new Type.NamedTable() { Names = ["t"] }
                    }
                }
            };
            var serialized = SubstraitSerializer.Serialize(plan);
            serialized.Relations[0].Rel.Read.AdvancedExtension = new Protobuf.AdvancedExtension()
            {
                Enhancement = Google.Protobuf.WellKnownTypes.Any.Pack(new CustomProtobuf.WriteRelationPrimaryKeys())
            };

            var read = (ReadRelation)new SubstraitDeserializer().Deserialize(serialized).Relations[0];

            Assert.Null(read.RowStore);
            Assert.Equal(2, read.OutputLength);
        }

        [Fact]
        public void TestSerializeNormalizationRelationWithRowStore()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new NormalizationRelation()
                    {
                        KeyIndex = [0],
                        RowStore = new RowStoreDefinition()
                        {
                            Name = "n_rows",
                            StoredColumns = [2]
                        },
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a", "b", "c"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["t"]
                            }
                        },
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void TestSerializeFetchRequiredColumnsRelation()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new FetchRequiredColumnsRelation()
                    {
                        Fetches =
                        [
                            new RowStoreFetch()
                            {
                                ReferenceColumn = 1,
                                StoreName = "rows",
                                Columns = [1, 0]
                            }
                        ],
                        Emit = [0, 3, 2],
                        Input = new ReadRelation()
                        {
                            BaseSchema = new Type.NamedStruct()
                            {
                                Names = ["a", "b", "c"]
                            },
                            NamedTable = new Type.NamedTable()
                            {
                                Names = ["t"]
                            },
                            RowStore = new RowStoreDefinition()
                            {
                                Name = "rows",
                                StoredColumns = [1, 2]
                            }
                        }
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
            var fetch = (FetchRequiredColumnsRelation)plan.Relations[0];
            Assert.Equal(3, fetch.OutputLength);
            Assert.Equal(2, fetch.Input.OutputLength);
            fetch.Emit = null;
            Assert.Equal(4, fetch.OutputLength);
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

        [Fact]
        public void TestSerializeCheckRelationWithGuardsTagsAndEmit()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new CheckRelation()
                    {
                        Input = CheckInputRead(),
                        Checks = new List<CheckDefinition>()
                        {
                            new CheckDefinition()
                            {
                                Condition = new ScalarFunction()
                                {
                                    ExtensionUrn = FunctionsComparison.Urn,
                                    ExtensionName = FunctionsComparison.Equal,
                                    Arguments = new List<Expression>() { CheckField(0), CheckField(1) }
                                },
                                Message = "a {a} differs from b {b}",
                                Tags = new List<CheckTag>()
                                {
                                    new CheckTag() { Key = "a", Value = CheckField(0) },
                                    new CheckTag() { Key = "b", Value = CheckField(1) }
                                },
                                Guards = new List<CheckGuard>()
                                {
                                    new CheckGuard() { Expression = CheckField(0), Kind = CheckGuardKind.IsTrue },
                                    new CheckGuard() { Expression = new BoolLiteral() { Value = false }, Kind = CheckGuardKind.IsNotTrue },
                                    new CheckGuard() { Expression = CheckField(1), Kind = CheckGuardKind.IsNull }
                                }
                            },
                            new CheckDefinition()
                            {
                                Condition = CheckField(1),
                                Message = "second",
                                Tags = new List<CheckTag>(),
                                Guards = new List<CheckGuard>()
                            },
                            new CheckDefinition()
                            {
                                Condition = CheckField(0),
                                Message = "",
                                Tags = new List<CheckTag>() { new CheckTag() { Key = "a", Value = CheckField(0) } },
                                Guards = new List<CheckGuard>()
                            }
                        },
                        Emit = new List<int>() { 1, 0 }
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void TestSerializeCheckRelationWithoutEmit()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new CheckRelation()
                    {
                        Input = CheckInputRead(),
                        Checks = new List<CheckDefinition>()
                        {
                            new CheckDefinition()
                            {
                                Condition = CheckField(0),
                                Message = "failed",
                                Tags = new List<CheckTag>(),
                                Guards = new List<CheckGuard>()
                                {
                                    new CheckGuard() { Expression = CheckField(1), Kind = CheckGuardKind.IsTrue }
                                }
                            }
                        }
                    }
                }
            };

            var json = SubstraitSerializer.SerializeToJson(plan);
            Assert.Contains("type.googleapis.com/flowtide.CheckRelation", json);
            AssertPlanCanSerializeDeserialize(plan);
        }

        private static ReadRelation CheckInputRead()
        {
            return new ReadRelation()
            {
                BaseSchema = new Type.NamedStruct()
                {
                    Names = ["a", "b"]
                },
                NamedTable = new Type.NamedTable()
                {
                    Names = ["t"]
                }
            };
        }

        private static DirectFieldReference CheckField(int index)
        {
            return new DirectFieldReference()
            {
                ReferenceSegment = new StructReferenceSegment()
                {
                    Field = index
                }
            };
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

        [Fact]
        public void TestListType()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                insert into out
                select a, b, list(a, b) as my_list FROM table1;
            "
            );
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void TestMapType()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                insert into out
                select a, b, map(a, b) as my_list FROM table1;
            "
            );
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void LargeIntegerNumber()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                INSERT INTO output
            SELECT 
                16094592000000000 AS nr
            FROM table1
            "); 
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void TimestampType()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                INSERT INTO output
            SELECT 
                CAST(16094592000000000 AS TIMESTAMP) as nr
            FROM table1
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void FetchRelation()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                INSERT INTO output
                SELECT TOP 1 a
                FROM table1
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void BroadcastExchange()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                CREATE VIEW dataview WITH(DISTRIBUTED = true) AS
                SELECT a FROM table1;

                INSERT INTO outputtable
                SELECT* FROM dataview
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void HexValue()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                INSERT INTO output 
                SELECT hex FROM 
                (
                    VALUES 
                    (0x544F2041)
                ) t(hex)
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void InsertOverwrite()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                INSERT OVERWRITE outputtable
                SELECT * FROM table1
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        /// <summary>
        /// A pre declared distributed view plan (exchange relation with standard output targets
        /// and standard output exchange references) round trips.
        /// </summary>
        [Fact]
        public void SerializeDistributedViewPlan()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                CREATE TABLE users (userkey any);

                CREATE VIEW read_users WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
                SELECT userkey FROM users;

                INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 0);
                INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 1);
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        /// <summary>
        /// The substream distribution relations, substream roots, exchange relations with
        /// substream and pull bucket targets and substream and pull exchange references, round
        /// trip. These are produced by the distributed plan modifier when a plan is split into
        /// substreams and must serialize for the plan version hash.
        /// </summary>
        [Fact]
        public void SerializeSubstreamDistributionRelations()
        {
            var read = new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { "users" } },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "userkey" },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>() { new AnyType() { Nullable = true } }
                    }
                }
            };

            var scatterField = new DirectFieldReference()
            {
                ReferenceSegment = new StructReferenceSegment() { Field = 0 }
            };

            var exchange = new ExchangeRelation()
            {
                Input = read,
                PartitionCount = 2,
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = new List<FieldReference>() { scatterField }
                },
                Targets = new List<ExchangeTarget>()
                {
                    new StandardOutputExchangeTarget() { PartitionIds = new List<int>() { 0 } },
                    new SubstreamExchangeTarget() { PartitionIds = new List<int>() { 1 }, SubstreamName = "sub1", ExchangeTargetId = 5 },
                    new PullBucketExchangeTarget() { PartitionIds = new List<int>(), ExchangeTargetId = 7 }
                }
            };

            var plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new SubStreamRootRelation() { Name = "sub0", Input = exchange },
                    new SubStreamRootRelation()
                    {
                        Name = "sub1",
                        Input = new SubstreamExchangeReferenceRelation()
                        {
                            SubStreamName = "sub0",
                            ExchangeTargetId = 5,
                            ReferenceOutputLength = 1
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "sub2",
                        Input = new PullExchangeReferenceRelation()
                        {
                            SubStreamName = "sub0",
                            ExchangeTargetId = 7,
                            ReferenceOutputLength = 1
                        }
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        /// <summary>
        /// References can point at relations that appear LATER in the plan: the distributed
        /// plan modifier appends hoisted exchange and lane relations after the sinks that
        /// consume them. Deserialization must resolve the referenced relation's output length
        /// even when the reference is visited before its target relation exists, for both
        /// plain references and exchange output references.
        /// </summary>
        [Fact]
        public void SerializeForwardReferences()
        {
            var read = new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { "users" } },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "userkey" },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>() { new AnyType() { Nullable = true } }
                    }
                }
            };

            var exchange = new ExchangeRelation()
            {
                Input = read,
                PartitionCount = 2,
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = new List<FieldReference>()
                    {
                        new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment() { Field = 0 }
                        }
                    }
                },
                Targets = new List<ExchangeTarget>()
                {
                    new StandardOutputExchangeTarget() { PartitionIds = new List<int>() { 0 } },
                    new StandardOutputExchangeTarget() { PartitionIds = new List<int>() { 1 } }
                }
            };

            var plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    // Both consumers come FIRST and reference relations that only appear
                    // later in the plan, the shape the distributed plan modifier produces.
                    new SubStreamRootRelation()
                    {
                        Name = "sub1",
                        Input = new StandardOutputExchangeReferenceRelation()
                        {
                            RelationId = 2,
                            TargetId = 0,
                            ReferenceOutputLength = 1
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "sub1",
                        Input = new ReferenceRelation()
                        {
                            RelationId = 3,
                            ReferenceOutputLength = 1
                        }
                    },
                    new SubStreamRootRelation() { Name = "sub0", Input = exchange },
                    read
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void InsertWithPrimaryKeyDeclaration()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                INSERT INTO outputtable PRIMARY KEY (b, a)
                SELECT * FROM table1
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);

            var json = SubstraitSerializer.SerializeToJson(plan);
            var deserializedPlan = SubstraitDeserializer.DeserializeFromJson(json);
            var writeRelation = Assert.IsType<WriteRelation>(deserializedPlan.Relations[0]);
            Assert.Equal(new List<string>() { "b", "a" }, writeRelation.PrimaryKeyNames);
        }

        /// <summary>
        /// The json is hashed, existing plans must keep their hash.
        /// </summary>
        [Fact]
        public void InsertWithoutPrimaryKeyDeclarationDoesNotChangeJson()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                INSERT INTO outputtable
                SELECT * FROM table1
            ");
            var plan = sqlPlanBuilder.GetPlan();

            var json = SubstraitSerializer.SerializeToJson(plan);

            Assert.DoesNotContain("WriteRelationPrimaryKeys", json);
            Assert.DoesNotContain("advancedExtension", json);

            var writeRelation = Assert.IsType<WriteRelation>(SubstraitDeserializer.DeserializeFromJson(json).Relations[0]);
            Assert.Null(writeRelation.PrimaryKeyNames);
        }


        [Fact]
        public void TestSerializeListAggNamedStructAggregate()
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
                create table table1 (a any, b any);
                insert into out
                select a, list_agg(named_struct('b', b)) as list FROM table1 GROUP BY a;
            ");
            var plan = sqlPlanBuilder.GetPlan();
            AssertPlanCanSerializeDeserialize(plan);
        }

        private static ReadRelation ReadOf(params string[] names)
        {
            return new ReadRelation()
            {
                BaseSchema = new Type.NamedStruct()
                {
                    Names = names.ToList()
                },
                NamedTable = new Type.NamedTable()
                {
                    Names = ["a"]
                }
            };
        }

        private static Plan ProjectionOf(params Expression[] expressions)
        {
            return new Plan()
            {
                Relations = new List<Relation>()
                {
                    new ProjectRelation()
                    {
                        Expressions = expressions.ToList(),
                        Input = ReadOf("a", "b")
                    }
                }
            };
        }

        private static DirectFieldReference Reference(ReferenceSegment segment)
        {
            return new DirectFieldReference()
            {
                ReferenceSegment = segment
            };
        }

        [Fact]
        public void SerializeReadRelationWithFilter()
        {
            var read = ReadOf("a", "b");
            read.Filter = Reference(new StructReferenceSegment() { Field = 1 });
            read.Emit = [0];

            AssertPlanCanSerializeDeserialize(new Plan() { Relations = new List<Relation>() { read } });
        }

        // Join pushdown puts is_not_null on reads
        [Fact]
        public void SerializeReadRelationWithFunctionFilter()
        {
            var read = ReadOf("a", "b");
            read.Filter = new ScalarFunction()
            {
                ExtensionUrn = FunctionsBoolean.Urn,
                ExtensionName = FunctionsBoolean.And,
                Arguments = new List<Expression>()
                {
                    new ScalarFunction()
                    {
                        ExtensionUrn = FunctionsComparison.Urn,
                        ExtensionName = FunctionsComparison.IsNotNull,
                        Arguments = new List<Expression>() { Reference(new StructReferenceSegment() { Field = 0 }) }
                    },
                    new ScalarFunction()
                    {
                        ExtensionUrn = FunctionsComparison.Urn,
                        ExtensionName = FunctionsComparison.LessThan,
                        Arguments = new List<Expression>()
                        {
                            Reference(new StructReferenceSegment() { Field = 0 }),
                            new NumericLiteral() { Value = 10 }
                        }
                    }
                }
            };
            read.Emit = [1];

            AssertPlanCanSerializeDeserialize(new Plan() { Relations = new List<Relation>() { read } });
        }

        [Fact]
        public void SerializeRootRelation()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new RootRelation()
                    {
                        Input = ReadOf("a", "b"),
                        Names = ["first", "second"]
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void RootRelationWithEmitIsRefused()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new RootRelation()
                    {
                        Input = ReadOf("a", "b"),
                        Names = ["second"],
                        Emit = [1]
                    }
                }
            };

            Assert.Throws<NotSupportedException>(() => SubstraitSerializer.SerializeToJson(plan));
        }

        [Fact]
        public void SerializeNormalizationRelation()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new NormalizationRelation()
                    {
                        Input = ReadOf("a", "b", "c"),
                        KeyIndex = [0, 2],
                        Filter = Reference(new StructReferenceSegment() { Field = 1 }),
                        Emit = [2, 0]
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeNormalizationRelationWithoutFilterAndEmit()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new NormalizationRelation()
                    {
                        Input = ReadOf("a", "b"),
                        KeyIndex = [1]
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        [Fact]
        public void SerializeNestedReferences()
        {
            var structInStruct = Reference(new StructReferenceSegment()
            {
                Field = 0,
                Child = new StructReferenceSegment() { Field = 3 }
            });
            var mapKeyInStruct = Reference(new StructReferenceSegment()
            {
                Field = 1,
                Child = new MapKeyReferenceSegment() { Key = "key" }
            });
            var threeLevels = Reference(new StructReferenceSegment()
            {
                Field = 1,
                Child = new MapKeyReferenceSegment()
                {
                    Key = "key",
                    Child = new StructReferenceSegment() { Field = 2 }
                }
            });
            var mapKeyFirst = Reference(new MapKeyReferenceSegment()
            {
                Key = "top",
                Child = new StructReferenceSegment() { Field = 4 }
            });

            AssertPlanCanSerializeDeserialize(ProjectionOf(structInStruct, mapKeyInStruct, threeLevels, mapKeyFirst));
        }

        [Fact]
        public void NestedPathsSerializeDifferently()
        {
            var first = ProjectionOf(Reference(new StructReferenceSegment()
            {
                Field = 0,
                Child = new StructReferenceSegment() { Field = 1 }
            }));
            var second = ProjectionOf(Reference(new StructReferenceSegment()
            {
                Field = 0,
                Child = new StructReferenceSegment() { Field = 2 }
            }));
            var noChild = ProjectionOf(Reference(new StructReferenceSegment() { Field = 0 }));

            var firstJson = SubstraitSerializer.SerializeToJson(first);
            Assert.NotEqual(firstJson, SubstraitSerializer.SerializeToJson(second));
            Assert.NotEqual(firstJson, SubstraitSerializer.SerializeToJson(noChild));
        }

        [Fact]
        public void SerializeMergeJoinKeyWithNestedReference()
        {
            Plan plan = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new MergeJoinRelation()
                    {
                        Left = ReadOf("a"),
                        Right = ReadOf("a"),
                        LeftKeys = new List<FieldReference>()
                        {
                            Reference(new StructReferenceSegment()
                            {
                                Field = 0,
                                Child = new StructReferenceSegment() { Field = 2 }
                            })
                        },
                        RightKeys = new List<FieldReference>()
                        {
                            Reference(new StructReferenceSegment()
                            {
                                Field = 1,
                                Child = new MapKeyReferenceSegment() { Key = "key" }
                            })
                        },
                        Type = JoinType.Inner
                    }
                }
            };

            AssertPlanCanSerializeDeserialize(plan);
        }

        // Equality ignores the root, so it is checked directly
        [Fact]
        public void OuterReferenceWithChildRoundTrips()
        {
            var outer = new DirectFieldReference()
            {
                ReferenceSegment = new StructReferenceSegment() { Field = 0, Child = new StructReferenceSegment() { Field = 1 } },
                Root = new OuterReference() { StepsOut = 1 }
            };
            var plain = Reference(new StructReferenceSegment() { Field = 0, Child = new StructReferenceSegment() { Field = 1 } });

            var json = SubstraitSerializer.SerializeToJson(ProjectionOf(outer));
            var deserialized = SubstraitDeserializer.DeserializeFromJson(json);

            var project = Assert.IsType<ProjectRelation>(deserialized.Relations[0]);
            var reference = Assert.IsType<DirectFieldReference>(project.Expressions[0]);
            var root = Assert.IsType<OuterReference>(reference.Root);
            Assert.Equal(1u, root.StepsOut);
            Assert.Equal(outer.ReferenceSegment, reference.ReferenceSegment);
            Assert.NotEqual(json, SubstraitSerializer.SerializeToJson(ProjectionOf(plain)));
        }

        [Fact]
        public void ListElementReferenceIsRefused()
        {
            var plan = SubstraitSerializer.Serialize(ProjectionOf(Reference(new StructReferenceSegment() { Field = 0 })));
            var segment = plan.Relations[0].Rel.Project.Expressions[0].Selection.DirectReference;
            segment.StructField.Child = new Protobuf.Expression.Types.ReferenceSegment()
            {
                ListElement = new Protobuf.Expression.Types.ReferenceSegment.Types.ListElement() { Offset = 1 }
            };

            Assert.Throws<NotSupportedException>(() => new SubstraitDeserializer().Deserialize(plan));
        }

        [Fact]
        public void FixedAndVariableLengthMapKeysAreRead()
        {
            var expected = ProjectionOf(Reference(new MapKeyReferenceSegment() { Key = "key" }));
            var fixedChar = SubstraitSerializer.Serialize(expected);
            fixedChar.Relations[0].Rel.Project.Expressions[0].Selection.DirectReference.MapKey.MapKey_ = new Protobuf.Expression.Types.Literal() { FixedChar = "key" };
            var varChar = SubstraitSerializer.Serialize(expected);
            varChar.Relations[0].Rel.Project.Expressions[0].Selection.DirectReference.MapKey.MapKey_ = new Protobuf.Expression.Types.Literal()
            {
                VarChar = new Protobuf.Expression.Types.Literal.Types.VarChar() { Value = "key", Length = 3 }
            };

            Assert.Equal(expected, new SubstraitDeserializer().Deserialize(fixedChar));
            Assert.Equal(expected, new SubstraitDeserializer().Deserialize(varChar));
        }

        [Fact]
        public void MapKeyReferenceWithoutKeyIsRefused()
        {
            var plan = SubstraitSerializer.Serialize(ProjectionOf(Reference(new MapKeyReferenceSegment() { Key = "key" })));
            var segment = plan.Relations[0].Rel.Project.Expressions[0].Selection.DirectReference;
            segment.MapKey.MapKey_ = null;

            Assert.Throws<NotSupportedException>(() => new SubstraitDeserializer().Deserialize(plan));
        }

        [Fact]
        public void MapKeyReferenceWithoutStringKeyIsRefused()
        {
            var plan = SubstraitSerializer.Serialize(ProjectionOf(Reference(new MapKeyReferenceSegment() { Key = "key" })));
            var segment = plan.Relations[0].Rel.Project.Expressions[0].Selection.DirectReference;
            segment.MapKey.MapKey_ = new Protobuf.Expression.Types.Literal() { I32 = 1 };

            Assert.Throws<NotSupportedException>(() => new SubstraitDeserializer().Deserialize(plan));
        }

        private static Plan PlanOf(params Relation[] relations)
        {
            return new Plan() { Relations = relations.ToList() };
        }

        [Fact]
        public void SerializeHints()
        {
            var read = ReadOf("a", "b");
            read.Hint.Alias = "t";
            read.Hint.Optimizations.Properties["WATERMARK_OUTPUT_MODE"] = "ON_EACH_BATCH";
            var filter = new FilterRelation()
            {
                Input = read,
                Condition = Reference(new StructReferenceSegment() { Field = 1 })
            };
            filter.Hint.Optimizations.Properties["first"] = "1";
            filter.Hint.Optimizations.Properties["second"] = "2";

            AssertPlanCanSerializeDeserialize(PlanOf(filter));
        }

        [Fact]
        public void HintsAreLeftOutOfTheHashText()
        {
            var plain = ReadOf("a", "b");
            var hinted = ReadOf("a", "b");
            hinted.Hint.Alias = "t";
            hinted.Hint.Optimizations.Properties["WATERMARK_OUTPUT_MODE"] = "ON_EACH_BATCH";

            var plainJson = SubstraitSerializer.SerializeToJson(PlanOf(plain));
            Assert.Equal(plainJson, SubstraitSerializer.SerializeToJson(PlanOf(hinted), includeHints: false));
            Assert.NotEqual(plainJson, SubstraitSerializer.SerializeToJson(PlanOf(hinted)));
        }

        [Fact]
        public void SerializeAggregateWithEmit()
        {
            var aggregate = new AggregateRelation()
            {
                Input = ReadOf("a", "b"),
                Groupings = [new AggregateGrouping() { GroupingExpressions = [Reference(new StructReferenceSegment() { Field = 0 })] }],
                Measures =
                [
                    new AggregateMeasure()
                    {
                        Measure = new AggregateFunction()
                        {
                            ExtensionUrn = FunctionsArithmetic.Urn,
                            ExtensionName = FunctionsArithmetic.Sum,
                            Arguments = [Reference(new StructReferenceSegment() { Field = 1 })]
                        }
                    }
                ],
                Emit = [1, 0]
            };

            AssertPlanCanSerializeDeserialize(PlanOf(aggregate));
        }

        [Fact]
        public void SerializeAggregateWithoutGroupings()
        {
            var aggregate = new AggregateRelation()
            {
                Input = ReadOf("a", "b"),
                Measures =
                [
                    new AggregateMeasure()
                    {
                        Measure = new AggregateFunction()
                        {
                            ExtensionUrn = FunctionsArithmetic.Urn,
                            ExtensionName = FunctionsArithmetic.Sum,
                            Arguments = [Reference(new StructReferenceSegment() { Field = 1 })]
                        }
                    }
                ]
            };

            AssertPlanCanSerializeDeserialize(PlanOf(aggregate));
        }

        [Fact]
        public void SerializeDecimalAndNullTypes()
        {
            var read = ReadOf("a", "b", "c", "d", "e", "f");
            read.BaseSchema.Struct = new Struct()
            {
                Types =
                [
                    new DecimalType() { Precision = 10, Scale = 2 },
                    new DecimalType() { Nullable = false },
                    new NullType(),
                    new NullType() { Nullable = true },
                    new AnyType(),
                    new AnyType() { Nullable = false }
                ]
            };
            var plan = PlanOf(read);

            var proto = SubstraitSerializer.Serialize(plan).Relations[0].Rel.Read.BaseSchema.Struct.Types_;
            var back = ((ReadRelation)SubstraitDeserializer.DeserializeFromJson(SubstraitSerializer.SerializeToJson(plan)).Relations[0]).BaseSchema.Struct!.Types;

            Assert.Equal(10, proto[0].Decimal.Precision);
            Assert.Equal(2, proto[0].Decimal.Scale);
            var sized = Assert.IsType<DecimalType>(back[0]);
            Assert.Equal(10, sized.Precision);
            Assert.Equal(2, sized.Scale);
            var plain = Assert.IsType<DecimalType>(back[1]);
            Assert.Null(plain.Precision);
            Assert.Null(plain.Scale);
            Assert.False(Assert.IsType<NullType>(back[2]).Nullable);
            Assert.True(Assert.IsType<NullType>(back[3]).Nullable);
            Assert.True(Assert.IsType<AnyType>(back[4]).Nullable);
            Assert.False(Assert.IsType<AnyType>(back[5]).Nullable);
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Theory]
        [InlineData(10, null, 10, 0)]
        [InlineData(null, 2, null, null)]
        [InlineData(0, 0, null, null)]
        [InlineData(0, 3, null, null)]
        [InlineData(5, 5, 5, 5)]
        public void DecimalSizeIsWholeOrNotThere(int? precision, int? scale, int? expectedPrecision, int? expectedScale)
        {
            var type = new DecimalType() { Scale = scale, Precision = precision };
            var read = ReadOf("a");
            read.BaseSchema.Struct = new Struct() { Types = [type] };
            var plan = PlanOf(read);

            var back = Assert.IsType<DecimalType>(((ReadRelation)SubstraitDeserializer.DeserializeFromJson(SubstraitSerializer.SerializeToJson(plan)).Relations[0]).BaseSchema.Struct!.Types[0]);

            Assert.Equal(expectedPrecision, type.Precision);
            Assert.Equal(expectedScale, type.Scale);
            Assert.Equal(expectedPrecision, back.Precision);
            Assert.Equal(expectedScale, back.Scale);
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Theory]
        [InlineData(SetOperation.Unspecified)]
        [InlineData(SetOperation.MinusPrimary)]
        [InlineData(SetOperation.MinusPrimaryAll)]
        [InlineData(SetOperation.MinusMultiset)]
        [InlineData(SetOperation.IntersectionPrimary)]
        [InlineData(SetOperation.IntersectionMultiset)]
        [InlineData(SetOperation.IntersectionMultisetAll)]
        [InlineData(SetOperation.UnionDistinct)]
        [InlineData(SetOperation.UnionAll)]
        public void SerializeSetOperation(SetOperation operation)
        {
            var set = new SetRelation()
            {
                Inputs = [ReadOf("a"), ReadOf("a")],
                Operation = operation
            };

            AssertPlanCanSerializeDeserialize(PlanOf(set));
        }

        [Fact]
        public void SetOperationsUseTheNumbersOfTheSpecification()
        {
            foreach (var operation in Enum.GetValues<SetOperation>())
            {
                Assert.True(Enum.IsDefined((Protobuf.SetRel.Types.SetOp)(int)operation));
                Assert.Equal(operation.ToString(), ((Protobuf.SetRel.Types.SetOp)(int)operation).ToString());
            }
            Assert.Equal(Enum.GetValues<Protobuf.SetRel.Types.SetOp>().Length, Enum.GetValues<SetOperation>().Length);
        }

        [Fact]
        public void UnknownSetOperationIsRefused()
        {
            var proto = SubstraitSerializer.Serialize(PlanOf(new SetRelation()
            {
                Inputs = [ReadOf("a"), ReadOf("a")],
                Operation = SetOperation.UnionAll
            }));
            proto.Relations[0].Rel.Set.Op = (Protobuf.SetRel.Types.SetOp)99;

            Assert.Throws<SubstraitParseException>(() => new SubstraitDeserializer().Deserialize(proto));
        }

        [Fact]
        public void SerializeReferencesWithEmit()
        {
            var reference = new ReferenceRelation() { RelationId = 2, ReferenceOutputLength = 2, Emit = [1] };
            var exchangeReference = new StandardOutputExchangeReferenceRelation() { RelationId = 2, TargetId = 0, ReferenceOutputLength = 2, Emit = [1, 0] };

            AssertPlanCanSerializeDeserialize(PlanOf(reference, exchangeReference, ReadOf("a", "b")));
        }

        [Fact]
        public void ReferenceWithoutEmitKeepsItsPlainForm()
        {
            var reference = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 2 };
            reference.Hint.Optimizations.Properties["hint"] = "1";

            var proto = SubstraitSerializer.Serialize(PlanOf(reference, ReadOf("a", "b")), includeHints: false);

            Assert.Equal(Protobuf.Rel.RelTypeOneofCase.Reference, proto.Relations[0].Rel.RelTypeCase);
            AssertPlanCanSerializeDeserialize(PlanOf(reference, ReadOf("a", "b")));
        }

        // The target's own length is not known yet
        [Fact]
        public void ReferenceToARelationThatReadsALaterOne()
        {
            var set = new SetRelation()
            {
                Inputs =
                [
                    new ReferenceRelation() { RelationId = 2, ReferenceOutputLength = 1 },
                    new ReferenceRelation() { RelationId = 2, ReferenceOutputLength = 1 }
                ],
                Operation = SetOperation.UnionAll
            };
            var reference = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 1 };

            AssertPlanCanSerializeDeserialize(PlanOf(set, reference, ReadOf("a")));
        }

        [Fact]
        public void SerializeWriteRelationWithEmit()
        {
            var write = new WriteRelation()
            {
                Input = ReadOf("a", "b"),
                TableSchema = new NamedStruct() { Names = ["a"] },
                NamedObject = new NamedTable() { Names = ["out"] },
                Emit = [0]
            };

            AssertPlanCanSerializeDeserialize(PlanOf(write));
        }

        [Theory]
        [InlineData(JoinType.Unspecified)]
        [InlineData(JoinType.Inner)]
        [InlineData(JoinType.Outer)]
        [InlineData(JoinType.Left)]
        [InlineData(JoinType.Right)]
        [InlineData(JoinType.LeftMark)]
        public void SerializeJoinWithConditionAndPostJoinFilter(JoinType type)
        {
            var join = new JoinRelation()
            {
                Left = ReadOf("a"),
                Right = ReadOf("b"),
                Type = type,
                Expression = Reference(new StructReferenceSegment() { Field = 0 }),
                PostJoinFilter = Reference(new StructReferenceSegment() { Field = 1 })
            };

            AssertPlanCanSerializeDeserialize(PlanOf(join));
        }

        [Fact]
        public void SerializeTableFunctionWithInputAndNoCondition()
        {
            var tableFunction = new TableFunctionRelation()
            {
                Input = ReadOf("a"),
                Type = JoinType.Inner,
                TableFunction = new TableFunction()
                {
                    ExtensionUrn = FunctionsTableGeneric.Urn,
                    ExtensionName = FunctionsTableGeneric.Unnest,
                    Arguments = [Reference(new StructReferenceSegment() { Field = 0 })],
                    TableSchema = new NamedStruct() { Names = ["value"] }
                }
            };

            AssertPlanCanSerializeDeserialize(PlanOf(tableFunction));
        }

        [Fact]
        public void SerializeIfWithoutElseAndMoreLiterals()
        {
            var ifThen = new FlowtideDotNet.Substrait.Expressions.IfThen.IfThenExpression()
            {
                Ifs =
                [
                    new FlowtideDotNet.Substrait.Expressions.IfThen.IfClause()
                    {
                        If = new BoolLiteral() { Value = true },
                        Then = new NumericLiteral() { Value = 1.5m }
                    }
                ]
            };
            var array = new ArrayLiteral()
            {
                Expressions = [new NumericLiteral() { Value = 1 }, new StringLiteral() { Value = "a" }, new NullLiteral()]
            };

            AssertPlanCanSerializeDeserialize(ProjectionOf(ifThen, array));
        }

        [Fact]
        public void SerializeIterationWithStopConditions()
        {
            var iteration = new IterationRelation()
            {
                IterationName = "loop",
                LoopPlan = ReadOf("a"),
                Input = ReadOf("a"),
                SkipIterateCondition = Reference(new StructReferenceSegment() { Field = 0 }),
                MaxIterations = 5
            };
            var plain = new IterationRelation()
            {
                IterationName = "loop",
                LoopPlan = ReadOf("a")
            };

            AssertPlanCanSerializeDeserialize(PlanOf(iteration));
            AssertPlanCanSerializeDeserialize(PlanOf(plain));
        }

        [Fact]
        public void IterationLimitOfZeroIsKept()
        {
            var iteration = new IterationRelation()
            {
                IterationName = "loop",
                LoopPlan = ReadOf("a"),
                MaxIterations = 0
            };
            var plan = PlanOf(iteration);

            var back = SubstraitDeserializer.DeserializeFromJson(SubstraitSerializer.SerializeToJson(plan));

            Assert.Equal(0, Assert.IsType<IterationRelation>(back.Relations[0]).MaxIterations);
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Theory]
        [InlineData("0.1234567890123456789")]
        [InlineData("1234567.123456789")]
        [InlineData("0.5000000000000001")]
        [InlineData("2.0000000000000001")]
        [InlineData("0.9999999999999999")]
        [InlineData("-3.14159265358979323846")]
        [InlineData("0.0000000000001234567890123456")]
        [InlineData("3728664694.5253500")]
        [InlineData("-127.63374653514000000000000000")]
        [InlineData("7922816251426433759354395.0335")]
        [InlineData("79228162514264337593543950335")]
        [InlineData("-79228162514264337593543950335")]
        [InlineData("9223372036854775808")]
        [InlineData("-9223372036854775809")]
        public void NumericLiteralKeepsEveryDigit(string text)
        {
            var value = decimal.Parse(text, CultureInfo.InvariantCulture);
            var plan = ProjectionOf(new NumericLiteral() { Value = value });

            var proto = SubstraitSerializer.Serialize(plan).Relations[0].Rel.Project.Expressions[0].Literal;
            var back = SubstraitDeserializer.DeserializeFromJson(SubstraitSerializer.SerializeToJson(plan));
            var literal = Assert.IsType<NumericLiteral>(((ProjectRelation)back.Relations[0]).Expressions[0]);

            Assert.Equal(Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.Decimal, proto.LiteralTypeCase);
            Assert.Equal(16, proto.Decimal.Value.Length);
            Assert.Equal(value, literal.Value);
            Assert.Equal((double)value, (double)literal.Value);
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Theory]
        [InlineData("1.5", Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.Fp64)]
        [InlineData("-0.1", Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.Fp64)]
        [InlineData("123456.789012345", Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.Fp64)]
        [InlineData("0.000001", Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.Fp64)]
        [InlineData("-0.0000000000000000000000000001", Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.Fp64)]
        [InlineData("0.50", Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.Fp64)]
        [InlineData("17", Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.I64)]
        [InlineData("-9223372036854775808", Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.I64)]
        [InlineData("9223372036854775807", Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.I64)]
        public void ShortNumericLiteralKeepsItsForm(string text, Protobuf.Expression.Types.Literal.LiteralTypeOneofCase expected)
        {
            var value = decimal.Parse(text, CultureInfo.InvariantCulture);
            var plan = ProjectionOf(new NumericLiteral() { Value = value });

            var proto = SubstraitSerializer.Serialize(plan).Relations[0].Rel.Project.Expressions[0].Literal;

            Assert.Equal(expected, proto.LiteralTypeCase);
            AssertPlanCanSerializeDeserialize(plan);
        }

        [Theory]
        [InlineData(0.1, "0.1")]
        [InlineData(-12.5, "-12.5")]
        [InlineData(1E-05, "0.00001")]
        [InlineData(0.30000000000000004, "0.3")]
        [InlineData(3, "3")]
        [InlineData(7.922816251426433E+28, "79228162514264300000000000000")]
        public void DoubleLiteralIsReadWithFifteenDigits(double value, string expected)
        {
            var proto = SubstraitSerializer.Serialize(ProjectionOf(new NumericLiteral() { Value = 1.5m }));
            proto.Relations[0].Rel.Project.Expressions[0].Literal.Fp64 = value;

            var back = new SubstraitDeserializer().Deserialize(proto);

            Assert.Equal(decimal.Parse(expected, CultureInfo.InvariantCulture), Assert.IsType<NumericLiteral>(((ProjectRelation)back.Relations[0]).Expressions[0]).Value);
            AssertPlanCanSerializeDeserialize(back);
        }

        [Theory]
        [InlineData(double.NaN)]
        [InlineData(double.PositiveInfinity)]
        [InlineData(double.NegativeInfinity)]
        [InlineData(1E+300)]
        [InlineData(7.922816251426434E+28)]
        [InlineData(-7.922816251426434E+28)]
        public void DoubleLiteralWithoutADecimalFormIsRefused(double value)
        {
            var proto = SubstraitSerializer.Serialize(ProjectionOf(new NumericLiteral() { Value = 1.5m }));
            proto.Relations[0].Rel.Project.Expressions[0].Literal.Fp64 = value;
            var floatProto = SubstraitSerializer.Serialize(ProjectionOf(new NumericLiteral() { Value = 1.5m }));
            floatProto.Relations[0].Rel.Project.Expressions[0].Literal.Fp32 = (float)value;

            Assert.Throws<SubstraitParseException>(() => new SubstraitDeserializer().Deserialize(proto));
            Assert.Throws<SubstraitParseException>(() => new SubstraitDeserializer().Deserialize(floatProto));
        }

        private static Protobuf.Plan DecimalLiteralPlan(string unscaled, int scale, int length)
        {
            var number = BigInteger.Parse(unscaled, CultureInfo.InvariantCulture);
            var bytes = new byte[length];
            if (number.Sign < 0)
            {
                Array.Fill(bytes, (byte)0xFF);
            }
            Assert.True(number.TryWriteBytes(bytes, out _));
            var proto = SubstraitSerializer.Serialize(ProjectionOf(new NumericLiteral() { Value = 1.5m }));
            proto.Relations[0].Rel.Project.Expressions[0].Literal = new Protobuf.Expression.Types.Literal()
            {
                Decimal = new Protobuf.Expression.Types.Literal.Types.Decimal()
                {
                    Value = Google.Protobuf.ByteString.CopyFrom(bytes),
                    Precision = 38,
                    Scale = scale
                }
            };
            return proto;
        }

        [Theory]
        [InlineData("-1234567890123456789012", 4, "-123456789012345678.9012")]
        [InlineData("79228162514264337593543950335", 28, "7.9228162514264337593543950335")]
        [InlineData("-79228162514264337593543950335", 0, "-79228162514264337593543950335")]
        [InlineData("0", 3, "0")]
        [InlineData("-1", 0, "-1")]
        public void DecimalLiteralFromAnotherEngineIsRead(string unscaled, int scale, string expected)
        {
            var back = new SubstraitDeserializer().Deserialize(DecimalLiteralPlan(unscaled, scale, 16));

            Assert.Equal(decimal.Parse(expected, CultureInfo.InvariantCulture), Assert.IsType<NumericLiteral>(((ProjectRelation)back.Relations[0]).Expressions[0]).Value);
            AssertPlanCanSerializeDeserialize(back);
        }

        [Theory]
        [InlineData("79228162514264337593543950336", 0, 16)]
        [InlineData("-79228162514264337593543950336", 0, 16)]
        [InlineData("170141183460469231731687303715884105727", 0, 16)]
        [InlineData("-170141183460469231731687303715884105728", 0, 16)]
        [InlineData("1", 29, 16)]
        [InlineData("1", -1, 16)]
        [InlineData("1", 0, 8)]
        [InlineData("1", 0, 17)]
        public void DecimalLiteralOutsideTheRangeIsRefused(string unscaled, int scale, int length)
        {
            var proto = DecimalLiteralPlan(unscaled, scale, length);

            Assert.Throws<SubstraitParseException>(() => new SubstraitDeserializer().Deserialize(proto));
        }

        [Fact]
        public void EmptyAliasIsNoAlias()
        {
            var read = ReadOf("a", "b");
            read.Hint.Alias = "";

            Assert.Null(read.Hint.Alias);
            Assert.False(read.Hint.HasHints);
            Assert.Equal(SubstraitSerializer.SerializeToJson(PlanOf(ReadOf("a", "b"))), SubstraitSerializer.SerializeToJson(PlanOf(read)));
            AssertPlanCanSerializeDeserialize(PlanOf(read));
        }

        [Fact]
        public void ReferencesInACycleAreRefused()
        {
            var first = new ProjectRelation()
            {
                Input = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 1 },
                Expressions = [new NumericLiteral() { Value = 1 }]
            };
            var second = new ProjectRelation()
            {
                Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 1 },
                Expressions = [new NumericLiteral() { Value = 1 }]
            };
            var proto = SubstraitSerializer.Serialize(PlanOf(first, second));

            Assert.Throws<InvalidOperationException>(() => new SubstraitDeserializer().Deserialize(proto));
        }

        // Each reference is behind the one it reads
        [Fact]
        public void LongChainOfReferencesIsRead()
        {
            const int count = 400;
            var relations = new Relation[count];
            for (int i = 0; i < count - 1; i++)
            {
                relations[i] = new ReferenceRelation() { RelationId = i + 1, ReferenceOutputLength = 2 };
            }
            relations[count - 1] = ReadOf("a", "b");

            AssertPlanCanSerializeDeserialize(PlanOf(relations));
        }

        private static string TextOfAPlanWithANullType()
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(@"
                create table users (companyid any, userkey any, firstname any, doublevalue any);
                insert into output
                select companyid, userkey,
                    avg(case when userkey = 20 then firstname else doublevalue end)
                        over (partition by companyid order by userkey rows between 1 preceding and current row) as value
                from users;
            ");
            return SubstraitSerializer.SerializeToJson(builder.GetPlan(), includeHints: false);
        }

        [Fact]
        public void NullifOfNullDoesNotChangeTheTextOfOtherPlans()
        {
            var before = TextOfAPlanWithANullType();
            var other = new SqlPlanBuilder();
            other.Sql(@"
                create table t (a any);
                insert into out
                select nullif(null, a) as b from t;
            ");
            other.GetPlan();

            Assert.Contains("\"string\": \"null\"", before);
            Assert.False(NullType.Instance.Nullable);
            Assert.Equal(before, TextOfAPlanWithANullType());
        }

        private void AssertPlanCanSerializeDeserialize(Plan plan)
        {
            var json = SubstraitSerializer.SerializeToJson(plan);
            var deserializedPlan = SubstraitDeserializer.DeserializeFromJson(json);
            Assert.Equal(json, SubstraitSerializer.SerializeToJson(deserializedPlan));
            Assert.Equal(plan, deserializedPlan);
        }
    }
}

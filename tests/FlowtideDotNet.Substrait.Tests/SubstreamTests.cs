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
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using FluentAssertions;
using FluentAssertions.Execution;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.DataType;
using static SqlParser.Ast.Expression;
using static Substrait.Protobuf.FunctionSignature.Types;

namespace FlowtideDotNet.Substrait.Tests
{
    /// <summary>
    /// Tests for parsing substream code that places operations on different substreams and allows configuring exchange relations
    /// that can send data between substreams.
    /// </summary>
    public class SubstreamTests
    {
        [Fact]
        public void TestDistributedViewOnly()
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(@"
            CREATE TABLE table1 (val any);

            SUBSTREAM stream1;

            -- Create an exchange relation here that will be broadcasted
            CREATE VIEW test WITH (DISTRIBUTED = true) AS
            SELECT val FROM table1;

            SUBSTREAM stream2;

            INSERT INTO output
            SELECT * FROM test;
            ");

            var plan = builder.GetPlan();

            var expectedPlan = new Plan()
            {
                Relations = new List<Relations.Relation>()
                {
                    new SubStreamRootRelation()
                    {
                        Name = "stream1",
                        Input = new ExchangeRelation()
                        {
                            PartitionCount = default,
                            ExchangeKind = new BroadcastExchangeKind(),
                            Input = new ProjectRelation()
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
                                    NamedTable = new Type.NamedTable()
                                    {
                                        Names = new List<string>()
                                        {
                                            "table1"
                                        }
                                    },
                                    BaseSchema = new Type.NamedStruct()
                                    {
                                        Names = new List<string>()
                                        {
                                            "val"
                                        },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>()
                                            {
                                                new AnyType()
                                            }
                                        }
                                    }
                                }
                            },
                            Targets = new List<ExchangeTarget>()
                            {
                                new PullBucketExchangeTarget()
                                {
                                    ExchangeTargetId = 0,
                                    PartitionIds = new List<int>()
                                }
                            }
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "stream2",
                        Input = new WriteRelation()
                        {
                            NamedObject = new Type.NamedTable(){ Names = new List<string>() { "output" }},
                            TableSchema = new Type.NamedStruct() 
                            { 
                                Names = new List<string>() {"val" },
                                Struct = new Struct()
                                {
                                    Types = new List<SubstraitBaseType>()
                                    {
                                        new AnyType() { Nullable = true }
                                    }
                                }
                            },
                            Input = new ProjectRelation()
                            {
                                Emit = new List<int>() { 1 },
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
                                Input = new PullExchangeReferenceRelation()
                                {
                                    ExchangeTargetId = 0,
                                    SubStreamName = "stream1",
                                    ReferenceOutputLength = 1
                                }
                            }
                        }
                    }
                }
            };

            AssertionOptions.FormattingOptions.MaxDepth = 100;
            AssertionOptions.FormattingOptions.MaxLines = 1000;
            plan.Should().BeEquivalentTo(expectedPlan);
        }


        /// <summary>
        /// This test checks that when using a distributed view in the same substream, the standard output should be used instead of a pull bucket.
        /// This is useful when 
        /// </summary>
        [Fact]
        public void TestDistributedViewSelectInSameSubstream()
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(@"
            CREATE TABLE table1 (val any);

            SUBSTREAM stream1;

            -- Create an exchange relation here that will be broadcasted
            CREATE VIEW test WITH (DISTRIBUTED = true) AS
            SELECT val FROM table1;

            INSERT INTO output
            SELECT * FROM test;            
            ");

            var plan = builder.GetPlan();

            var expectedPlan = new Plan()
            {
                Relations = new List<Relations.Relation>()
                {
                    new SubStreamRootRelation()
                    {
                        Name = "stream1",
                        Input = new ExchangeRelation()
                        {
                            PartitionCount = default,
                            ExchangeKind = new BroadcastExchangeKind(),
                            Input = new ProjectRelation()
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
                                    NamedTable = new Type.NamedTable()
                                    {
                                        Names = new List<string>()
                                        {
                                            "table1"
                                        }
                                    },
                                    BaseSchema = new Type.NamedStruct()
                                    {
                                        Names = new List<string>()
                                        {
                                            "val"
                                        },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>()
                                            {
                                                new AnyType()
                                            }
                                        }
                                    }
                                }
                            },
                            Targets = new List<ExchangeTarget>()
                            {
                                new StandardOutputExchangeTarget()
                                {
                                    PartitionIds = new List<int>()
                                }
                            }
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "stream1",
                        Input = new WriteRelation()
                        {
                            NamedObject = new Type.NamedTable(){ Names = new List<string>() { "output" }},
                            TableSchema = new Type.NamedStruct()
                            {
                                Names = new List<string>() {"val" },
                                Struct = new Struct()
                                {
                                    Types = new List<SubstraitBaseType>()
                                    {
                                        new AnyType() { Nullable = true }
                                    }
                                }
                            },
                            Input = new ProjectRelation()
                            {
                                Emit = new List<int>() { 1 },
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
                                Input = new StandardOutputExchangeReferenceRelation()
                                {
                                    RelationId = 0,
                                    TargetId = 0,
                                    ReferenceOutputLength = 1
                                }
                            }
                        }
                    }
                }
            };

            AssertionOptions.FormattingOptions.MaxDepth = 100;
            AssertionOptions.FormattingOptions.MaxLines = 1000;
            plan.Should().BeEquivalentTo(expectedPlan);
        }

        [Fact]
        public void TestDistributedViewSelectInSameSubstreamAndOtherStream()
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(@"
            CREATE TABLE table1 (val any);

            SUBSTREAM stream1;

            -- Create an exchange relation here that will be broadcasted
            CREATE VIEW test WITH (DISTRIBUTED = true) AS
            SELECT val FROM table1;

            INSERT INTO output
            SELECT * FROM test;     

            SUBSTREAM stream2;

            INSERT INTO output
            SELECT * FROM test;
            ");

            var plan = builder.GetPlan();

            var expectedPlan = new Plan()
            {
                Relations = new List<Relations.Relation>()
                {
                    new SubStreamRootRelation()
                    {
                        Name = "stream1",
                        Input = new ExchangeRelation()
                        {
                            PartitionCount = default,
                            ExchangeKind = new BroadcastExchangeKind(),
                            Input = new ProjectRelation()
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
                                    NamedTable = new Type.NamedTable()
                                    {
                                        Names = new List<string>()
                                        {
                                            "table1"
                                        }
                                    },
                                    BaseSchema = new Type.NamedStruct()
                                    {
                                        Names = new List<string>()
                                        {
                                            "val"
                                        },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>()
                                            {
                                                new AnyType()
                                            }
                                        }
                                    }
                                }
                            },
                            Targets = new List<ExchangeTarget>()
                            {
                                new StandardOutputExchangeTarget()
                                {
                                    PartitionIds = new List<int>()
                                },
                                new PullBucketExchangeTarget()
                                {
                                    ExchangeTargetId = 0,
                                    PartitionIds = new List<int>()
                                }
                            }
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "stream1",
                        Input = new WriteRelation()
                        {
                            NamedObject = new Type.NamedTable(){ Names = new List<string>() { "output" }},
                            TableSchema = new Type.NamedStruct()
                            {
                                Names = new List<string>() {"val" },
                                Struct = new Struct()
                                {
                                    Types = new List<SubstraitBaseType>()
                                    {
                                        new AnyType() { Nullable = true }
                                    }
                                }
                            },
                            Input = new ProjectRelation()
                            {
                                Emit = new List<int>() { 1 },
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
                                Input = new StandardOutputExchangeReferenceRelation()
                                {
                                    RelationId = 0,
                                    TargetId = 0,
                                    ReferenceOutputLength = 1
                                }
                            }
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "stream2",
                        Input = new WriteRelation()
                        {
                            NamedObject = new Type.NamedTable(){ Names = new List<string>() { "output" }},
                            TableSchema = new Type.NamedStruct()
                            {
                                Names = new List<string>() {"val" },
                                Struct = new Struct()
                                {
                                    Types = new List<SubstraitBaseType>()
                                    {
                                        new AnyType() { Nullable = true }
                                    }
                                }
                            },
                            Input = new ProjectRelation()
                            {
                                Emit = new List<int>() { 1 },
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
                                Input = new PullExchangeReferenceRelation()
                                {
                                    ExchangeTargetId = 0,
                                    SubStreamName = "stream1",
                                    ReferenceOutputLength = 1
                                }
                            }
                        }
                    }
                }
            };

            AssertionOptions.FormattingOptions.MaxDepth = 100;
            AssertionOptions.FormattingOptions.MaxLines = 1000;
            plan.Should().BeEquivalentTo(expectedPlan);
        }

        [Fact]
        public void TestPartitionByField()
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(@"
            CREATE TABLE table1 (val any);

            SUBSTREAM stream1;

            -- Create an exchange relation here that will be broadcasted
            CREATE VIEW test WITH (DISTRIBUTED = true, SCATTER_BY = 'val', PARTITION_COUNT = 2) AS
            SELECT val FROM table1;

            INSERT INTO output
            SELECT * FROM test WITH (PARTITION_ID = 0);     

            SUBSTREAM stream2;

            INSERT INTO output
            SELECT * FROM test WITH (PARTITION_ID = 1);
            ");

            var plan = builder.GetPlan();

            var expectedPlan = new Plan()
            {
                Relations = new List<Relations.Relation>()
                {
                    new SubStreamRootRelation()
                    {
                        Name = "stream1",
                        Input = new ExchangeRelation()
                        {
                            PartitionCount = 2,
                            ExchangeKind = new ScatterExchangeKind()
                            {
                                Fields = new List<FieldReference>()
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
                            Input = new ProjectRelation()
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
                                    NamedTable = new Type.NamedTable()
                                    {
                                        Names = new List<string>()
                                        {
                                            "table1"
                                        }
                                    },
                                    BaseSchema = new Type.NamedStruct()
                                    {
                                        Names = new List<string>()
                                        {
                                            "val"
                                        },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>()
                                            {
                                                new AnyType()
                                            }
                                        }
                                    }
                                }
                            },
                            Targets = new List<ExchangeTarget>()
                            {
                                new StandardOutputExchangeTarget()
                                {
                                    PartitionIds = new List<int>()
                                    {
                                        0
                                    }
                                },
                                new PullBucketExchangeTarget()
                                {
                                    ExchangeTargetId = 0,
                                    PartitionIds = new List<int>()
                                    {
                                        1
                                    }
                                }
                            }
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "stream1",
                        Input = new WriteRelation()
                        {
                            NamedObject = new Type.NamedTable(){ Names = new List<string>() { "output" }},
                            TableSchema = new Type.NamedStruct()
                            {
                                Names = new List<string>() {"val" },
                                Struct = new Struct()
                                {
                                    Types = new List<SubstraitBaseType>()
                                    {
                                        new AnyType() { Nullable = true }
                                    }
                                }
                            },
                            Input = new ProjectRelation()
                            {
                                Emit = new List<int>() { 1 },
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
                                Input = new StandardOutputExchangeReferenceRelation()
                                {
                                    RelationId = 0,
                                    TargetId = 0,
                                    ReferenceOutputLength = 1
                                }
                            }
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "stream2",
                        Input = new WriteRelation()
                        {
                            NamedObject = new Type.NamedTable(){ Names = new List<string>() { "output" }},
                            TableSchema = new Type.NamedStruct()
                            {
                                Names = new List<string>() {"val" },
                                Struct = new Struct()
                                {
                                    Types = new List<SubstraitBaseType>()
                                    {
                                        new AnyType() { Nullable = true }
                                    }
                                }
                            },
                            Input = new ProjectRelation()
                            {
                                Emit = new List<int>() { 1 },
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
                                Input = new PullExchangeReferenceRelation()
                                {
                                    ExchangeTargetId = 0,
                                    SubStreamName = "stream1",
                                    ReferenceOutputLength = 1
                                }
                            }
                        }
                    }
                }
            };

            AssertionOptions.FormattingOptions.MaxDepth = 100;
            AssertionOptions.FormattingOptions.MaxLines = 1000;
            plan.Should().BeEquivalentTo(expectedPlan);
        }

        [Fact]
        public void ScatterByFieldNoPartitionCountWithPartitionId()
        {
            var builder = new SqlPlanBuilder();

            var ex = Assert.Throws<InvalidOperationException>(() =>
            {
                builder.Sql(@"
                CREATE TABLE table1 (val any);

                SUBSTREAM stream1;

                -- Create an exchange relation here that will be broadcasted
                CREATE VIEW test WITH (DISTRIBUTED = true, SCATTER_BY = 'val') AS
                SELECT val FROM table1;

                INSERT INTO output
                SELECT * FROM test WITH (PARTITION_ID = 0);

                SUBSTREAM stream2;

                INSERT INTO output
                SELECT * FROM test WITH (PARTITION_ID = 1);
                ");
            });
            Assert.Equal("Cannot use PARTITION_ID on a distributed view without PARTITION_COUNT hint.", ex.Message);
        }

        [Fact]
        public void ScatterByFieldNoDistributedTrueHint()
        {
            var builder = new SqlPlanBuilder();

            var ex = Assert.Throws<InvalidOperationException>(() =>
            {
                builder.Sql(@"
                CREATE TABLE table1 (val any);

                SUBSTREAM stream1;

                -- Create an exchange relation here that will be broadcasted
                CREATE VIEW test WITH (SCATTER_BY = 'val') AS
                SELECT val FROM table1;

                INSERT INTO output
                SELECT * FROM test;
                ");
            });
            Assert.Equal("SCATTER_BY can only be used on a distributed view", ex.Message);
        }

        [Fact]
        public void TestTwoParallelStreamsWithJoin()
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(@"
            CREATE TABLE table1 (val any);
            CREATE TABLE table2 (val any);

            SUBSTREAM stream1;

            CREATE VIEW read_table_1_stream1 WITH (DISTRIBUTED = true, SCATTER_BY = 'val', PARTITION_COUNT = 2) AS
            SELECT val FROM table1;

            SUBSTREAM stream2;

            CREATE VIEW read_table_2_stream2 WITH (DISTRIBUTED = true, SCATTER_BY = 'val', PARTITION_COUNT = 2) AS
            SELECT val FROM table2;

            SUBSTREAM stream1;

            INSERT INTO output
            SELECT 
              a.val 
            FROM read_table_1_stream1 a WITH (PARTITION_ID = 0)
            LEFT JOIN read_table_2_stream2 b WITH (PARTITION_ID = 0)
            ON a.val = b.val;

            SUBSTREAM stream2;

            INSERT INTO output
            SELECT 
              a.val 
            FROM read_table_1_stream1 a WITH (PARTITION_ID = 1)
            LEFT JOIN read_table_2_stream2 b WITH (PARTITION_ID = 1)
            ON a.val = b.val;
            ");

            var plan = builder.GetPlan();

            var expectedPlan = new Plan()
            {
                Relations = new List<Relations.Relation>()
                {
                    new SubStreamRootRelation()
                    {
                        Name = "stream1",
                        Input = new ExchangeRelation()
                        {
                            PartitionCount = 2,
                            ExchangeKind = new ScatterExchangeKind()
                            {
                                Fields = new List<FieldReference>()
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
                            Input = new ProjectRelation()
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
                                    NamedTable = new Type.NamedTable()
                                    {
                                        Names = new List<string>()
                                        {
                                            "table1"
                                        }
                                    },
                                    BaseSchema = new Type.NamedStruct()
                                    {
                                        Names = new List<string>()
                                        {
                                            "val"
                                        },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>()
                                            {
                                                new AnyType()
                                            }
                                        }
                                    }
                                }
                            },
                            Targets = new List<ExchangeTarget>()
                            {
                                new StandardOutputExchangeTarget()
                                {
                                    PartitionIds = new List<int>()
                                    {
                                        0
                                    }
                                },
                                new PullBucketExchangeTarget()
                                {
                                    ExchangeTargetId = 1,
                                    PartitionIds = new List<int>()
                                    {
                                        1
                                    }
                                }
                            }
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "stream2",
                        Input = new ExchangeRelation()
                        {
                            PartitionCount = 2,
                            ExchangeKind = new ScatterExchangeKind()
                            {
                                Fields = new List<FieldReference>()
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
                            Input = new ProjectRelation()
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
                                    NamedTable = new Type.NamedTable()
                                    {
                                        Names = new List<string>()
                                        {
                                            "table2"
                                        }
                                    },
                                    BaseSchema = new Type.NamedStruct()
                                    {
                                        Names = new List<string>()
                                        {
                                            "val"
                                        },
                                        Struct = new Type.Struct()
                                        {
                                            Types = new List<Type.SubstraitBaseType>()
                                            {
                                                new AnyType()
                                            }
                                        }
                                    }
                                }
                            },
                            Targets = new List<ExchangeTarget>()
                            {
                                new PullBucketExchangeTarget()
                                {
                                    PartitionIds = new List<int>()
                                    {
                                        0
                                    },
                                    ExchangeTargetId = 0
                                },
                                new StandardOutputExchangeTarget()
                                {
                                    PartitionIds = new List<int>()
                                    {
                                        1
                                    }
                                }
                            }
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "stream1",
                        Input = new WriteRelation()
                        {
                            NamedObject = new Type.NamedTable(){ Names = new List<string>() { "output" }},
                            TableSchema = new Type.NamedStruct()
                            {
                                Names = new List<string>() {"val" },
                                Struct = new Struct()
                                {
                                    Types = new List<SubstraitBaseType>()
                                    {
                                        new AnyType() { Nullable = true }
                                    }
                                }
                            },
                            Input = new ProjectRelation()
                            {
                                Emit = new List<int>() { 2 },
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
                                Input = new JoinRelation()
                                {
                                    Expression = new ScalarFunction()
                                    {
                                        Arguments = new List<Expression>()
                                        {
                                            new DirectFieldReference
                                            {
                                                ReferenceSegment = new Expressions.StructReferenceSegment
                                                {
                                                    Field = 0
                                                }
                                            },
                                            new DirectFieldReference
                                            {
                                                ReferenceSegment = new FlowtideDotNet.Substrait.Expressions.StructReferenceSegment
                                                {
                                                    Field = 1
                                                }
                                            }
                                        },
                                        ExtensionName = "equal",
                                        ExtensionUri = "/functions_comparison.yaml"
                                    },
                                    Left = new StandardOutputExchangeReferenceRelation()
                                    {
                                        ReferenceOutputLength = 1,
                                        RelationId = 0,
                                        TargetId = 0
                                    },
                                    Right = new PullExchangeReferenceRelation()
                                    {
                                        ExchangeTargetId = 0,
                                        ReferenceOutputLength = 1,
                                        SubStreamName = "stream2"
                                    },
                                    Type = JoinType.Left
                                }
                            }
                        }
                    },
                    new SubStreamRootRelation()
                    {
                        Name = "stream2",
                        Input = new WriteRelation()
                        {
                            NamedObject = new Type.NamedTable(){ Names = new List<string>() { "output" }},
                            TableSchema = new Type.NamedStruct()
                            {
                                Names = new List<string>() {"val" },
                                Struct = new Struct()
                                {
                                    Types = new List<SubstraitBaseType>()
                                    {
                                        new AnyType() { Nullable = true }
                                    }
                                }
                            },
                            Input = new ProjectRelation()
                            {
                                Emit = new List<int>() { 2 },
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
                                Input = new JoinRelation()
                                {
                                    Expression = new ScalarFunction()
                                    {
                                        Arguments = new List<Expression>()
                                        {
                                            new DirectFieldReference
                                            {
                                                ReferenceSegment = new Expressions.StructReferenceSegment
                                                {
                                                    Field = 0
                                                }
                                            },
                                            new DirectFieldReference
                                            {
                                                ReferenceSegment = new FlowtideDotNet.Substrait.Expressions.StructReferenceSegment
                                                {
                                                    Field = 1
                                                }
                                            }
                                        },
                                        ExtensionName = "equal",
                                        ExtensionUri = "/functions_comparison.yaml"
                                    },
                                    Left = new PullExchangeReferenceRelation()
                                    {
                                        ReferenceOutputLength = 1,
                                        SubStreamName = "stream1",
                                        ExchangeTargetId = 1
                                    },
                                    Right = new StandardOutputExchangeReferenceRelation()
                                    {
                                        ReferenceOutputLength = 1,
                                        RelationId = 1,
                                        TargetId = 1
                                    },
                                    Type = JoinType.Left
                                }
                            }
                        }
                    }
                }
            };

            AssertionOptions.FormattingOptions.MaxDepth = 100;
            AssertionOptions.FormattingOptions.MaxLines = 1000;
            plan.Should().BeEquivalentTo(expectedPlan);
        }
    }
}

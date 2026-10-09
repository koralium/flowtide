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

using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.LineageTests
{
    public class LineageVisitorTests
    {
        [Fact]
        public void LineageFromProjectionNoExpression()
        {
            var plan = new WriteRelation()
            {
                NamedObject = new NamedTable()
                {
                    Names = ["output"]
                },
                TableSchema = new NamedStruct()
                {
                    Names = ["c1"],
                    Struct = new Struct()
                    {
                        Types = [new Int64Type()]
                    }
                },
                Input = new ProjectRelation()
                {
                    Expressions = [],
                    Emit = [0],
                    Input = new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = ["a"],
                            Struct = new Substrait.Type.Struct()
                            {
                                Types = [new Int64Type()]
                            }
                        },
                        NamedTable = new NamedTable()
                        {
                            Names = ["table"]
                        }
                    }
                }
            };

            var visitor = new LineageVisitor([plan], new Dictionary<string, LineageInputTable>()
            {
                { "table", new LineageInputTable("namespace1", "table1") }
            });
            var result = visitor.HandleWriteRelation(plan);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace1", "table1", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])])
            }, []);

            Assert.Equal(expected, result);
        }

        [Fact]
        public void LineageFromProjectExpressionDirectField()
        {
            var plan = new WriteRelation()
            {
                NamedObject = new NamedTable()
                {
                    Names = ["output"]
                },
                TableSchema = new NamedStruct()
                {
                    Names = ["c1"],
                    Struct = new Struct()
                    {
                        Types = [new Int64Type()]
                    }
                },
                Input = new ProjectRelation()
                {
                    Expressions = [new DirectFieldReference() {
                        ReferenceSegment = new StructReferenceSegment() { Field = 0 }
                    }],
                    Emit = [1],
                    Input = new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = ["a"],
                            Struct = new Substrait.Type.Struct()
                            {
                                Types = [new Int64Type()]
                            }
                        },
                        NamedTable = new NamedTable()
                        {
                            Names = ["table"]
                        }
                    }
                }
            };

            var visitor = new LineageVisitor([plan], new Dictionary<string, LineageInputTable>()
            {
                { "table", new LineageInputTable("namespace", "table") }
            });
            var result = visitor.HandleWriteRelation(plan);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace", "table", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])])
            }, []);

            Assert.Equal(expected, result);
        }

        [Fact]
        public void LineageFromProjectExpressionTransformSingleField()
        {
            var plan = new WriteRelation()
            {
                NamedObject = new NamedTable()
                {
                    Names = ["output"]
                },
                TableSchema = new NamedStruct()
                {
                    Names = ["c1"],
                    Struct = new Struct()
                    {
                        Types = [new Int64Type()]
                    }
                },
                Input = new ProjectRelation()
                {
                    Expressions = [new ScalarFunction() {
                        ExtensionUri = FunctionsArithmetic.Uri,
                        ExtensionName = FunctionsArithmetic.Add,
                        Arguments = new List<Expression>()
                        {
                            new DirectFieldReference() {
                                ReferenceSegment = new StructReferenceSegment() { Field = 0 }
                            },
                            new NumericLiteral()
                            {
                                Value = 1
                            }
                        }
                    }],
                    Emit = [1],
                    Input = new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = ["a"],
                            Struct = new Substrait.Type.Struct()
                            {
                                Types = [new Int64Type()]
                            }
                        },
                        NamedTable = new NamedTable()
                        {
                            Names = ["table"]
                        }
                    }
                }
            };

            var visitor = new LineageVisitor([plan], new Dictionary<string, LineageInputTable>()
            {
                { "table", new LineageInputTable("namespace", "table") }
            });
            var result = visitor.HandleWriteRelation(plan);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace", "table", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Transformation)])])
            }, []);

            Assert.Equal(expected, result);
        }

        [Fact]
        public void LineageFromProjectExpressionTransformTwoFields()
        {
            var plan = new WriteRelation()
            {
                NamedObject = new NamedTable()
                {
                    Names = ["output"]
                },
                TableSchema = new NamedStruct()
                {
                    Names = ["c1"],
                    Struct = new Struct()
                    {
                        Types = [new Int64Type()]
                    }
                },
                Input = new ProjectRelation()
                {
                    Expressions = [new ScalarFunction() {
                        ExtensionUri = FunctionsArithmetic.Uri,
                        ExtensionName = FunctionsArithmetic.Add,
                        Arguments = new List<Expression>()
                        {
                            new DirectFieldReference() {
                                ReferenceSegment = new StructReferenceSegment() { Field = 0 }
                            },
                            new DirectFieldReference() {
                                ReferenceSegment = new StructReferenceSegment() { Field = 1 }
                            },
                        }
                    }],
                    Emit = [2],
                    Input = new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = ["a", "b"],
                            Struct = new Substrait.Type.Struct()
                            {
                                Types = [new Int64Type()]
                            }
                        },
                        NamedTable = new NamedTable()
                        {
                            Names = ["table"]
                        }
                    }
                }
            };

            var visitor = new LineageVisitor([plan], new Dictionary<string, LineageInputTable>()
            {
                { "table", new LineageInputTable("namespace", "table") }
            });
            var result = visitor.HandleWriteRelation(plan);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([
                        new LineageInputField("namespace", "table", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Transformation)]),
                        new LineageInputField("namespace", "table", "b", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Transformation)])
                    ])
            }, []);

            Assert.Equal(expected, result);
        }

        private void TestWithSql(string sql, Dictionary<string, LineageInputTable> inputTables, ColumnLineage expected, PlanOptimizerSettings? settings = null)
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(sql);

            var plan = sqlPlanBuilder.GetPlan();
            plan = Optimizer.PlanOptimizer.Optimize(plan, settings);

            WriteRelation? writeRel = default;

            for (int i = 0; i < plan.Relations.Count; i++)
            {
                if (plan.Relations[i] is WriteRelation w)
                {
                    writeRel = w;
                }
            }

            if (writeRel == null)
            {
                Assert.Fail("No WriteRelation found in the plan");
            }

            var visitor = new LineageVisitor(plan.Relations, inputTables);
            var result = visitor.HandleWriteRelation(writeRel);

            Assert.Equal(expected, result);
        }

        [Fact]
        public void TestProjectionWithSql()
        {
            TestWithSql(@"
                CREATE TABLE input (
                    a int
                );

                CREATE TABLE output (
                    c1 int
                );

                INSERT INTO output
                SELECT a as c1 FROM input;
            ", new Dictionary<string, LineageInputTable>()
            {
                { "input", new LineageInputTable("namespace", "input") }
            }, new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace", "input", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])])
            }, []));
        }

        [Fact]
        public void TestMergeJoinUsedLeftTable()
        {
            TestWithSql(@"
                CREATE TABLE input1 (
                    a int
                );
                CREATE TABLE input2 (
                    b int
                );
                CREATE TABLE output (
                    c1 int
                );
                INSERT INTO output
                SELECT a as c1 FROM input1 t1 JOIN input2 t2 ON t1.a = t2.b;
             ", new Dictionary<string, LineageInputTable>()
            {
                { "input1", new LineageInputTable("namespace", "input1") },
                { "input2", new LineageInputTable("namespace", "input2") }
            }
             , new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace", "input1", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])])
            }, [
                new LineageInputField("namespace", "input1", "a", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]),
                new LineageInputField("namespace", "input2", "b", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]),
                ]));
        }

        [Fact]
        public void TestMergeJoinUsedRightTable()
        {
            TestWithSql(@"
                CREATE TABLE input1 (
                    a int
                );
                CREATE TABLE input2 (
                    b int
                );
                CREATE TABLE output (
                    c1 int
                );
                INSERT INTO output
                SELECT b as c1 FROM input1 t1 JOIN input2 t2 ON t1.a = t2.b;
             ", new Dictionary<string, LineageInputTable>()
            {
                { "input1", new LineageInputTable("namespace", "input1") },
                { "input2", new LineageInputTable("namespace", "input2") }
            }, new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace", "input2", "b", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])])
            }, [
                new LineageInputField("namespace", "input1", "a", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]),
                new LineageInputField("namespace", "input2", "b", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]),
                ]));
        }

        [Fact]
        public void NestedLoopJoin()
        {
            TestWithSql(@"
                CREATE TABLE input1 (
                    a int
                );
                CREATE TABLE input2 (
                    b int
                );
                CREATE TABLE output (
                    c1 int
                );
                INSERT INTO output
                SELECT b as c1 FROM input1 t1 JOIN input2 t2 ON t1.a % t2.b;
             ", new Dictionary<string, LineageInputTable>()
            {
                { "input1", new LineageInputTable("namespace", "input1") },
                { "input2", new LineageInputTable("namespace", "input2") }
            }, new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace", "input2", "b", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])])
            }, [
                new LineageInputField("namespace", "input1", "a", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]),
                new LineageInputField("namespace", "input2", "b", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)])
                ]));
        }

        [Fact]
        public void UsageAcrossView()
        {
            TestWithSql(@"
                CREATE TABLE input1 (
                    a int
                );
                CREATE TABLE output (
                    c1 int
                );

                CREATE VIEW testview AS
                SELECT a as b FROM input1;

                INSERT INTO output
                SELECT b as c1 FROM testview;
             ", new Dictionary<string, LineageInputTable>()
            {
                { "input1", new LineageInputTable("namespace", "input1") }
            }, new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace", "input1", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])])
            }, []));
        }

        [Fact]
        public void WindowFunction()
        {
            TestWithSql(@"
                CREATE TABLE input1 (
                    a int,
                    b int,
                    c string
                );
                CREATE TABLE output (
                    c1 int,
                    c2 int
                );

                INSERT INTO output
                SELECT SUM(a) OVER (PARTITION BY b ORDER BY c) as c1, b as c2 FROM input1;
             ", new Dictionary<string, LineageInputTable>()
            {
                { "input1", new LineageInputTable("namespace", "input1") }
            }
             , new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace", "input1", "b", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.GroupBy)]),
                    new LineageInputField("namespace", "input1", "c", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Sort)]),
                    new LineageInputField("namespace", "input1", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Aggregation)])
                    ]),
                ["c2"] = new ColumnLineageField([new LineageInputField("namespace", "input1", "b", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])])
            }, []));
        }

        [Fact]
        public void Union()
        {
            TestWithSql(@"
                CREATE TABLE input1 (
                    a int
                );
                CREATE TABLE input2 (
                    a int
                );
                CREATE TABLE output (
                    c1 int
                );
                INSERT INTO output
                SELECT a as c1 FROM input1
                UNION ALL
                SELECT a as c1 FROM input2;
             ", new Dictionary<string, LineageInputTable>()
            {
                { "input1", new LineageInputTable("namespace", "input1") },
                { "input2", new LineageInputTable("namespace", "input2") }
            }, new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace", "input1", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)]),
                    new LineageInputField("namespace", "input2", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])
                    ])
            }, []));
        }

        [Fact]
        public void GroupByWithSql()
        {
            // SUM keeps the aggregate, a bare group by becomes distinct.
            TestWithSql(@"
                CREATE TABLE input1 (
                    a int,
                    b int
                );
                CREATE TABLE output (
                    c1 int,
                    c2 int
                );
                INSERT INTO output
                SELECT b as c1, SUM(a) as c2 FROM input1 GROUP BY b;
             ", new Dictionary<string, LineageInputTable>()
            {
                { "input1", new LineageInputTable("namespace", "input1") }
            }, new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([Field("namespace", "input1", "b", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)]),
                ["c2"] = new ColumnLineageField([Field("namespace", "input1", "a", LineageTransformationType.Direct, LineageTransformationSubtype.Aggregation)])
            }, [
                Field("namespace", "input1", "b", LineageTransformationType.Indirect, LineageTransformationSubtype.GroupBy)
                ]));
        }

        [Fact]
        public void SelfJoinWithSql()
        {
            TestWithSql(@"
                CREATE TABLE input1 (
                    id int,
                    parent int,
                    name string
                );
                CREATE TABLE output (
                    c1 string,
                    c2 string
                );
                INSERT INTO output
                SELECT c.name as c1, p.name as c2 FROM input1 c JOIN input1 p ON c.parent = p.id;
             ", new Dictionary<string, LineageInputTable>()
            {
                { "input1", new LineageInputTable("namespace", "input1") }
            }, new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([Field("namespace", "input1", "name", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)]),
                ["c2"] = new ColumnLineageField([Field("namespace", "input1", "name", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])
            }, [
                Field("namespace", "input1", "parent", LineageTransformationType.Indirect, LineageTransformationSubtype.Join),
                Field("namespace", "input1", "id", LineageTransformationType.Indirect, LineageTransformationSubtype.Join)
                ]));
        }

        [Fact]
        public void GetTimestampFilterWithSql()
        {
            // Timestamp read has no input table, it yields nothing.
            TestWithSql(@"
                CREATE TABLE input1 (
                    a int,
                    d timestamp
                );
                CREATE TABLE output (
                    c1 int
                );
                INSERT INTO output
                SELECT a as c1 FROM input1 WHERE d < gettimestamp();
             ", new Dictionary<string, LineageInputTable>()
            {
                { "input1", new LineageInputTable("namespace", "input1") }
            }, new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([Field("namespace", "input1", "a", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])
            }, [
                Field("namespace", "input1", "d", LineageTransformationType.Indirect, LineageTransformationSubtype.Join)
                ]));
        }

        [Fact]
        public void ParallelizedWindowFunction()
        {
            // Lanes read the window input through an exchange.
            TestWithSql(@"
                CREATE TABLE input1 (
                    a int,
                    b int,
                    c string
                );
                CREATE TABLE output (
                    c1 int,
                    c2 int
                );

                INSERT INTO output
                SELECT SUM(a) OVER (PARTITION BY b ORDER BY c) as c1, b as c2 FROM input1;
             ", new Dictionary<string, LineageInputTable>()
            {
                { "input1", new LineageInputTable("namespace", "input1") }
            }
             , new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([new LineageInputField("namespace", "input1", "b", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.GroupBy)]),
                    new LineageInputField("namespace", "input1", "c", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Sort)]),
                    new LineageInputField("namespace", "input1", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Aggregation)])
                    ]),
                ["c2"] = new ColumnLineageField([new LineageInputField("namespace", "input1", "b", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])])
            }, []), new PlanOptimizerSettings() { Parallelization = 2 });
        }

        [Fact]
        public void StandardOutputExchangeReferenceAppliesEmits()
        {
            var exchange = new ExchangeRelation()
            {
                Input = ReadT(),
                ExchangeKind = new BroadcastExchangeKind(),
                Targets = [new StandardOutputExchangeTarget() { PartitionIds = [] }]
            };
            var write = WriteOutput(new StandardOutputExchangeReferenceRelation()
            {
                RelationId = 0,
                TargetId = 0,
                ReferenceOutputLength = 2,
                Emit = [1]
            }, "c1");

            var visitor = new LineageVisitor([exchange, write], InputT());
            var result = visitor.HandleWriteRelation(write);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([Field("ns", "t", "b", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])
            }, []);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void SubstreamExchangeReferenceResolvesProducer()
        {
            var write = WriteOutput(new SubstreamExchangeReferenceRelation()
            {
                SubStreamName = "s1",
                ExchangeTargetId = 7,
                ReferenceOutputLength = 2
            }, "c1", "c2");
            List<Relation> relations = [
                new SubStreamRootRelation()
                {
                    Name = "s1",
                    Input = new ExchangeRelation()
                    {
                        Input = ReadT(),
                        ExchangeKind = new ScatterExchangeKind() { Fields = [] },
                        Targets = [new SubstreamExchangeTarget() { ExchangeTargetId = 7, SubstreamName = "s2", PartitionIds = [] }]
                    }
                },
                new SubStreamRootRelation() { Name = "s2", Input = write }
            ];

            var visitor = new LineageVisitor(relations, InputT());
            var result = visitor.HandleWriteRelation(write);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([Field("ns", "t", "a", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)]),
                ["c2"] = new ColumnLineageField([Field("ns", "t", "b", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])
            }, []);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void PullExchangeReferenceResolvesProducer()
        {
            var write = WriteOutput(new PullExchangeReferenceRelation()
            {
                SubStreamName = "s1",
                ExchangeTargetId = 3,
                ReferenceOutputLength = 2
            }, "c1", "c2");
            List<Relation> relations = [
                new SubStreamRootRelation()
                {
                    Name = "s1",
                    Input = new ExchangeRelation()
                    {
                        Input = ReadT(),
                        ExchangeKind = new ScatterExchangeKind() { Fields = [] },
                        Targets = [new PullBucketExchangeTarget() { ExchangeTargetId = 3, PartitionIds = [] }]
                    }
                },
                new SubStreamRootRelation() { Name = "s2", Input = write }
            ];

            var visitor = new LineageVisitor(relations, InputT());
            var result = visitor.HandleWriteRelation(write);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([Field("ns", "t", "a", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)]),
                ["c2"] = new ColumnLineageField([Field("ns", "t", "b", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])
            }, []);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void UnresolvedReferencesAreEmpty()
        {
            // Wrong producer name, unknown ids and out of range relations.
            var write = WriteOutput(new SetRelation()
            {
                Operation = SetOperation.UnionAll,
                Inputs = [
                    new SubstreamExchangeReferenceRelation() { SubStreamName = "s2", ExchangeTargetId = 7, ReferenceOutputLength = 2 },
                    new SubstreamExchangeReferenceRelation() { SubStreamName = "s1", ExchangeTargetId = 99, ReferenceOutputLength = 2 },
                    new PullExchangeReferenceRelation() { SubStreamName = "s1", ExchangeTargetId = 7, ReferenceOutputLength = 2 },
                    new StandardOutputExchangeReferenceRelation() { RelationId = 42, TargetId = 0, ReferenceOutputLength = 2 },
                    new StandardOutputExchangeReferenceRelation() { RelationId = 1, TargetId = 0, ReferenceOutputLength = 2 },
                    new ReferenceRelation() { RelationId = 42, ReferenceOutputLength = 2 },
                    new ReferenceRelation() { RelationId = -1, ReferenceOutputLength = 2 }
                ]
            }, "c1");
            List<Relation> relations = [
                new SubStreamRootRelation()
                {
                    Name = "s1",
                    Input = new ExchangeRelation()
                    {
                        Input = ReadT(),
                        ExchangeKind = new ScatterExchangeKind() { Fields = [] },
                        Targets = [new SubstreamExchangeTarget() { ExchangeTargetId = 7, SubstreamName = "s2", PartitionIds = [] }]
                    }
                },
                write
            ];

            var visitor = new LineageVisitor(relations, InputT());
            var result = visitor.HandleWriteRelation(write);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([])
            }, []);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void ReferenceCycleTerminates()
        {
            var write = WriteOutput(new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 1 }, "c1");
            var filter = new FilterRelation()
            {
                Input = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 1 },
                Condition = new BoolLiteral() { Value = true }
            };

            var visitor = new LineageVisitor([write, filter], InputT());
            var result = visitor.HandleWriteRelation(write);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([])
            }, []);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void ExchangeCycleTerminates()
        {
            var exchange = new ExchangeRelation()
            {
                Input = new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 0, ReferenceOutputLength = 1 },
                ExchangeKind = new BroadcastExchangeKind(),
                Targets = [new StandardOutputExchangeTarget() { PartitionIds = [] }]
            };
            var write = WriteOutput(new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 0, ReferenceOutputLength = 1 }, "c1");

            var visitor = new LineageVisitor([exchange, write], InputT());
            var result = visitor.HandleWriteRelation(write);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([])
            }, []);
            Assert.Equal(expected, result);
        }

        [Fact]
        public void DuplicateOutputColumnNamesAreMerged()
        {
            var write = WriteOutput(ReadT(), "c1", "c1");

            var visitor = new LineageVisitor([write], InputT());
            var result = visitor.HandleWriteRelation(write);

            var expected = new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["c1"] = new ColumnLineageField([
                    Field("ns", "t", "a", LineageTransformationType.Direct, LineageTransformationSubtype.Identity),
                    Field("ns", "t", "b", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)
                    ])
            }, []);
            Assert.Equal(expected, result);
        }

        private static LineageInputField Field(string @namespace, string table, string field, LineageTransformationType type, LineageTransformationSubtype subtype)
        {
            return new LineageInputField(@namespace, table, field, [new LineageTransformation(type, subtype)]);
        }

        private static ReadRelation ReadT()
        {
            return new ReadRelation()
            {
                BaseSchema = new NamedStruct()
                {
                    Names = ["a", "b"],
                    Struct = new Struct()
                    {
                        Types = [new Int64Type(), new Int64Type()]
                    }
                },
                NamedTable = new NamedTable()
                {
                    Names = ["t"]
                }
            };
        }

        private static WriteRelation WriteOutput(Relation input, params string[] columns)
        {
            return new WriteRelation()
            {
                NamedObject = new NamedTable()
                {
                    Names = ["output"]
                },
                TableSchema = new NamedStruct()
                {
                    Names = columns.ToList(),
                    Struct = new Struct()
                    {
                        Types = columns.Select(x => (SubstraitBaseType)new Int64Type()).ToList()
                    }
                },
                Input = input
            };
        }

        private static Dictionary<string, LineageInputTable> InputT()
        {
            return new Dictionary<string, LineageInputTable>()
            {
                { "t", new LineageInputTable("ns", "t") }
            };
        }
    }
}

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

            var visitor = new LineageVisitor([plan]);
            var result = visitor.HandleWriteRelation(plan);

            var expected = new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [new LineageInputField("", "table", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])]
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

            var visitor = new LineageVisitor([plan]);
            var result = visitor.HandleWriteRelation(plan);

            var expected = new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [new LineageInputField("", "table", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])]
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

            var visitor = new LineageVisitor([plan]);
            var result = visitor.HandleWriteRelation(plan);

            var expected = new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [new LineageInputField("", "table", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Transformation)])]
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

            var visitor = new LineageVisitor([plan]);
            var result = visitor.HandleWriteRelation(plan);

            var expected = new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [
                        new LineageInputField("", "table", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Transformation)]),
                        new LineageInputField("", "table", "b", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Transformation)])
                    ]
            }, []);

            Assert.Equal(expected, result);
        }

        private void TestWithSql(string sql, ColumnLineage expected)
        {
            SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(sql);

            var plan = sqlPlanBuilder.GetPlan();
            plan = Optimizer.PlanOptimizer.Optimize(plan);

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

            var visitor = new LineageVisitor(plan.Relations);
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
            ", new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [new LineageInputField("", "input", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])]
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
             ", new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [new LineageInputField("", "input1", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])]
            }, [
                new LineageInputField("", "input1", "a", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]),
                new LineageInputField("", "input2", "b", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]),
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
             ", new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [new LineageInputField("", "input2", "b", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])]
            }, [
                new LineageInputField("", "input1", "a", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]),
                new LineageInputField("", "input2", "b", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]),
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
             ", new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [new LineageInputField("", "input2", "b", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])]
            }, [
                new LineageInputField("", "input1", "a", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]),
                new LineageInputField("", "input2", "b", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)])
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
             ", new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [new LineageInputField("", "input1", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])]
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
             ", new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [
                    new LineageInputField("", "input1", "b", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.GroupBy)]),
                    new LineageInputField("", "input1", "c", [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Sort)]),
                    new LineageInputField("", "input1", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Aggregation)])
                    ],
                ["c2"] = [new LineageInputField("", "input1", "b", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])]
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
             ", new ColumnLineage(new Dictionary<string, IReadOnlyList<LineageInputField>>()
            {
                ["c1"] = [
                    new LineageInputField("", "input1", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)]),
                    new LineageInputField("", "input2", "a", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])
                    ]
            }, []));
        }
    }
}

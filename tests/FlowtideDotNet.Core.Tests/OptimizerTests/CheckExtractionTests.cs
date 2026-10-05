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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Optimizer.CheckExtraction;
using FlowtideDotNet.Core.Optimizer.CommonSubPlan;
using FlowtideDotNet.Core.Optimizer.GetTimestamp;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.OptimizerTests
{
    public class CheckExtractionTests
    {
        private static DirectFieldReference Field(int index)
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = index } };
        }

        private static StringLiteral Str(string value)
        {
            return new StringLiteral() { Value = value };
        }

        private static NumericLiteral Num(int value)
        {
            return new NumericLiteral() { Value = value };
        }

        private static ScalarFunction Function(string uri, string name, params Expression[] arguments)
        {
            return new ScalarFunction() { ExtensionUri = uri, ExtensionName = name, Arguments = arguments.ToList() };
        }

        private static ScalarFunction Lt(Expression left, Expression right)
        {
            return Function(FunctionsComparison.Uri, FunctionsComparison.LessThan, left, right);
        }

        private static ScalarFunction Gt(Expression left, Expression right)
        {
            return Function(FunctionsComparison.Uri, FunctionsComparison.GreaterThan, left, right);
        }

        private static ScalarFunction Eq(Expression left, Expression right)
        {
            return Function(FunctionsComparison.Uri, FunctionsComparison.Equal, left, right);
        }

        private static ScalarFunction And(params Expression[] arguments)
        {
            return Function(FunctionsBoolean.Uri, FunctionsBoolean.And, arguments);
        }

        private static ScalarFunction IsTrue(Expression expression)
        {
            return Function(FunctionsComparison.Uri, FunctionsComparison.IsNotDistinctFrom, expression, new BoolLiteral() { Value = true });
        }

        private static ScalarFunction IsNotNull(Expression expression)
        {
            return Function(FunctionsComparison.Uri, FunctionsComparison.IsNotNull, expression);
        }

        private static ScalarFunction CheckValue(Expression value, Expression condition, string message, params (string Key, Expression Value)[] tags)
        {
            var arguments = new List<Expression>() { value, condition, Str(message) };
            foreach (var tag in tags)
            {
                arguments.Add(Str(tag.Key));
                arguments.Add(tag.Value);
            }
            return new ScalarFunction() { ExtensionUri = FunctionsCheck.Uri, ExtensionName = FunctionsCheck.CheckValue, Arguments = arguments };
        }

        private static ScalarFunction CheckTrue(Expression condition, string message, params (string Key, Expression Value)[] tags)
        {
            var arguments = new List<Expression>() { condition, Str(message) };
            foreach (var tag in tags)
            {
                arguments.Add(Str(tag.Key));
                arguments.Add(tag.Value);
            }
            return new ScalarFunction() { ExtensionUri = FunctionsCheck.Uri, ExtensionName = FunctionsCheck.CheckTrue, Arguments = arguments };
        }

        private static CheckDefinition Check(Expression condition, string message, params CheckGuard[] guards)
        {
            return Check(condition, message, [], guards);
        }

        private static CheckDefinition Check(Expression condition, string message, (string Key, Expression Value)[] tags, params CheckGuard[] guards)
        {
            return new CheckDefinition()
            {
                Condition = condition,
                Message = message,
                Tags = tags.Select(x => new CheckTag() { Key = x.Key, Value = x.Value }).ToList(),
                Guards = guards.ToList()
            };
        }

        private static CheckGuard Guard(Expression expression, CheckGuardKind kind)
        {
            return new CheckGuard() { Expression = expression, Kind = kind };
        }

        private static ReadRelation Read(int columnCount, string table = "t")
        {
            var names = new List<string>();
            var types = new List<SubstraitBaseType>();
            for (int i = 0; i < columnCount; i++)
            {
                names.Add($"c{i}");
                types.Add(new AnyType());
            }
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { table } },
                BaseSchema = new NamedStruct() { Names = names, Struct = new Struct() { Types = types } }
            };
        }

        private static Plan PlanOf(Relation relation)
        {
            return new Plan() { Relations = new List<Relation>() { relation } };
        }

        private static Relation ExtractSingle(Relation relation)
        {
            return CheckExtractor.Extract(PlanOf(relation)).Relations[0];
        }

        private static Plan BuildSqlPlan(string sql)
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(sql);
            return builder.GetPlan();
        }

        private static List<T> FindAll<T>(Relation root) where T : Relation
        {
            var collector = new RelationCollector();
            collector.Visit(root, null!);
            return collector.Relations.OfType<T>().ToList();
        }

        private static List<T> FindAll<T>(Plan plan) where T : Relation
        {
            var result = new List<T>();
            foreach (var relation in plan.Relations)
            {
                result.AddRange(FindAll<T>(relation));
            }
            return result;
        }

        private sealed class RelationCollector : OptimizerBaseVisitor
        {
            public List<Relation> Relations { get; } = new List<Relation>();

            public override Relation Visit(Relation relation, object state)
            {
                Relations.Add(relation);
                return base.Visit(relation, state);
            }
        }

        private sealed class FieldReferenceCollector : ExpressionVisitor<object?, object?>
        {
            public HashSet<DirectFieldReference> References { get; } = new HashSet<DirectFieldReference>(ReferenceEqualityComparer.Instance);

            public List<int> Fields { get; } = new List<int>();

            public override object? VisitDirectFieldReference(DirectFieldReference directFieldReference, object? state)
            {
                References.Add(directFieldReference);
                Fields.Add(((StructReferenceSegment)directFieldReference.ReferenceSegment).Field);
                return null;
            }
        }

        private static void AssertCheckFieldsInRange(Plan plan)
        {
            foreach (var checkRelation in FindAll<CheckRelation>(plan))
            {
                var collector = new FieldReferenceCollector();
                foreach (var check in checkRelation.Checks)
                {
                    foreach (var expression in CheckFunctionMatcher.GetExpressions(check))
                    {
                        collector.Visit(expression, null);
                    }
                }
                foreach (var field in collector.Fields)
                {
                    Assert.InRange(field, 0, checkRelation.Input.OutputLength - 1);
                }
                if (checkRelation.EmitSet)
                {
                    foreach (var field in checkRelation.Emit)
                    {
                        Assert.InRange(field, 0, checkRelation.Input.OutputLength - 1);
                    }
                }
            }
        }

        [Fact]
        public void ProjectCheckValueMovesBelowProject()
        {
            var actual = ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>() { CheckValue(Field(0), Lt(Field(0), Num(900)), "too large", ("key", Field(1))) },
                Emit = new List<int>() { 2 },
                Input = Read(2)
            });

            var expected = new ProjectRelation()
            {
                Expressions = new List<Expression>() { Field(0) },
                Emit = new List<int>() { 2 },
                Input = new CheckRelation()
                {
                    Input = Read(2),
                    Checks = new List<CheckDefinition>() { Check(Lt(Field(0), Num(900)), "too large", [("key", Field(1))]) }
                }
            };
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void ProjectCheckTrueBecomesIsTrue()
        {
            var actual = ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>() { CheckTrue(Lt(Field(0), Num(900)), "m1"), CheckValue(Field(1), Lt(Field(1), Num(5)), "m2") },
                Input = Read(2)
            });

            var expected = new ProjectRelation()
            {
                Expressions = new List<Expression>() { IsTrue(Lt(Field(0), Num(900))), Field(1) },
                Input = new CheckRelation()
                {
                    Input = Read(2),
                    Checks = new List<CheckDefinition>() { Check(Lt(Field(0), Num(900)), "m1"), Check(Lt(Field(1), Num(5)), "m2") }
                }
            };
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void FilterSplitsCheckFreeConjunctsIntoLowerFilter()
        {
            var actual = ExtractSingle(new FilterRelation()
            {
                Condition = And(And(Eq(Field(0), Num(1)), CheckTrue(Lt(Field(1), Num(10)), "m")), Eq(Field(1), Num(2))),
                Emit = new List<int>() { 1 },
                Input = Read(2)
            });

            var expected = new FilterRelation()
            {
                Condition = And(IsTrue(Lt(Field(1), Num(10)))),
                Emit = new List<int>() { 1 },
                Input = new CheckRelation()
                {
                    Input = new FilterRelation()
                    {
                        Condition = And(Eq(Field(0), Num(1)), Eq(Field(1), Num(2))),
                        Input = Read(2)
                    },
                    Checks = new List<CheckDefinition>() { Check(Lt(Field(1), Num(10)), "m") }
                }
            };
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void FilterWithOnlyCheckConjunctsHasNoLowerFilter()
        {
            var single = ExtractSingle(new FilterRelation()
            {
                Condition = CheckTrue(Lt(Field(0), Num(10)), "m"),
                Input = Read(2)
            });
            Assert.Equal(new FilterRelation()
            {
                Condition = IsTrue(Lt(Field(0), Num(10))),
                Input = new CheckRelation()
                {
                    Input = Read(2),
                    Checks = new List<CheckDefinition>() { Check(Lt(Field(0), Num(10)), "m") }
                }
            }, single);

            var conjunction = ExtractSingle(new FilterRelation()
            {
                Condition = And(CheckTrue(Lt(Field(0), Num(10)), "m1"), CheckTrue(Lt(Field(1), Num(10)), "m2")),
                Input = Read(2)
            });
            Assert.Equal(new FilterRelation()
            {
                Condition = And(IsTrue(Lt(Field(0), Num(10))), IsTrue(Lt(Field(1), Num(10)))),
                Input = new CheckRelation()
                {
                    Input = Read(2),
                    Checks = new List<CheckDefinition>() { Check(Lt(Field(0), Num(10)), "m1"), Check(Lt(Field(1), Num(10)), "m2") }
                }
            }, conjunction);
        }

        [Fact]
        public void AggregateChecksInGroupingMeasureFilterAndArguments()
        {
            var actual = ExtractSingle(new AggregateRelation()
            {
                Groupings = new List<AggregateGrouping>()
                {
                    new AggregateGrouping() { GroupingExpressions = new List<Expression>() { CheckValue(Field(0), Lt(Field(0), Num(1)), "grouping") } }
                },
                Measures = new List<AggregateMeasure>()
                {
                    new AggregateMeasure()
                    {
                        Measure = new AggregateFunction()
                        {
                            ExtensionUri = FunctionsArithmetic.Uri,
                            ExtensionName = FunctionsArithmetic.Sum,
                            Arguments = new List<Expression>() { CheckValue(Field(1), Lt(Field(1), Num(2)), "argument") }
                        },
                        Filter = CheckTrue(Lt(Field(2), Num(3)), "filter")
                    }
                },
                Input = Read(3)
            });

            var aggregate = Assert.IsType<AggregateRelation>(actual);
            Assert.Equal(Field(0), aggregate.Groupings![0].GroupingExpressions[0]);
            Assert.Equal(Field(1), aggregate.Measures![0].Measure.Arguments[0]);
            Assert.Equal(IsTrue(Lt(Field(2), Num(3))), aggregate.Measures[0].Filter);

            var checkRelation = Assert.IsType<CheckRelation>(aggregate.Input);
            Assert.Equal(Read(3), checkRelation.Input);
            Assert.Equal(new List<CheckDefinition>()
            {
                Check(Lt(Field(0), Num(1)), "grouping"),
                Check(Lt(Field(2), Num(3)), "filter"),
                // Guarded by the measure filter
                Check(Lt(Field(1), Num(2)), "argument", Guard(IsTrue(Lt(Field(2), Num(3))), CheckGuardKind.IsTrue))
            }, checkRelation.Checks);
        }

        [Fact]
        public void WindowChecksInPartitionOrderAndArguments()
        {
            var actual = ExtractSingle(new ConsistentPartitionWindowRelation()
            {
                PartitionBy = new List<Expression>() { CheckValue(Field(0), Lt(Field(0), Num(1)), "partition") },
                OrderBy = new List<SortField>() { new SortField() { Expression = CheckValue(Field(1), Lt(Field(1), Num(2)), "order"), SortDirection = SortDirection.SortDirectionAscNullsFirst } },
                WindowFunctions = new List<WindowFunction>()
                {
                    new WindowFunction()
                    {
                        ExtensionUri = FunctionsArithmetic.Uri,
                        ExtensionName = FunctionsArithmetic.Sum,
                        Arguments = new List<Expression>() { CheckValue(Field(2), Lt(Field(2), Num(3)), "argument") }
                    },
                    new WindowFunction()
                    {
                        ExtensionUri = FunctionsArithmetic.Uri,
                        ExtensionName = FunctionsArithmetic.Lead,
                        Arguments = new List<Expression>() { CheckValue(Field(0), Lt(Field(0), Num(1)), "value"), Num(1), Num(0) }
                    }
                },
                Input = Read(3)
            });

            var window = Assert.IsType<ConsistentPartitionWindowRelation>(actual);
            Assert.Equal(Field(0), window.PartitionBy[0]);
            Assert.Equal(Field(1), window.OrderBy[0].Expression);
            Assert.Equal(Field(2), window.WindowFunctions[0].Arguments[0]);
            Assert.Equal(Field(0), window.WindowFunctions[1].Arguments[0]);
            var checkRelation = Assert.IsType<CheckRelation>(window.Input);
            Assert.Equal(new List<CheckDefinition>()
            {
                Check(Lt(Field(0), Num(1)), "partition"),
                Check(Lt(Field(1), Num(2)), "order"),
                Check(Lt(Field(2), Num(3)), "argument"),
                Check(Lt(Field(0), Num(1)), "value")
            }, checkRelation.Checks);
        }

        [Fact]
        public void SortAndTopNChecksInSortExpressions()
        {
            var sort = Assert.IsType<SortRelation>(ExtractSingle(new SortRelation()
            {
                Sorts = new List<SortField>() { new SortField() { Expression = CheckValue(Field(0), Lt(Field(0), Num(1)), "sort") } },
                Input = Read(1)
            }));
            Assert.Equal(Field(0), sort.Sorts[0].Expression);
            Assert.Equal(new List<CheckDefinition>() { Check(Lt(Field(0), Num(1)), "sort") }, Assert.IsType<CheckRelation>(sort.Input).Checks);

            var topN = Assert.IsType<TopNRelation>(ExtractSingle(new TopNRelation()
            {
                Sorts = new List<SortField>() { new SortField() { Expression = CheckValue(Field(0), Lt(Field(0), Num(1)), "topn") } },
                Count = 1,
                Input = Read(1)
            }));
            Assert.Equal(Field(0), topN.Sorts[0].Expression);
            Assert.Equal(new List<CheckDefinition>() { Check(Lt(Field(0), Num(1)), "topn") }, Assert.IsType<CheckRelation>(topN.Input).Checks);
        }

        private static TableFunctionRelation TableFunctionOver(Relation? input, Expression argument, Expression? joinCondition)
        {
            return new TableFunctionRelation()
            {
                TableFunction = new TableFunction()
                {
                    ExtensionUri = FunctionsTableGeneric.Uri,
                    ExtensionName = FunctionsTableGeneric.Unnest,
                    Arguments = new List<Expression>() { argument },
                    TableSchema = new NamedStruct() { Names = new List<string>() { "value" }, Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } } }
                },
                Input = input,
                JoinCondition = joinCondition,
                Type = JoinType.Left
            };
        }

        [Fact]
        public void TableFunctionArgumentAndInputSideJoinConditionGoToInput()
        {
            var actual = Assert.IsType<TableFunctionRelation>(ExtractSingle(TableFunctionOver(
                Read(2),
                CheckValue(Field(0), Lt(Field(0), Num(1)), "argument"),
                CheckTrue(Lt(Field(1), Num(2)), "join"))));

            Assert.Equal(Field(0), actual.TableFunction.Arguments[0]);
            Assert.Equal(IsTrue(Lt(Field(1), Num(2))), actual.JoinCondition);
            var checkRelation = Assert.IsType<CheckRelation>(actual.Input);
            Assert.Equal(Read(2), checkRelation.Input);
            Assert.Equal(new List<CheckDefinition>() { Check(Lt(Field(0), Num(1)), "argument"), Check(Lt(Field(1), Num(2)), "join") }, checkRelation.Checks);
        }

        private static JoinRelation Join(Expression expression, Expression? postJoinFilter = null)
        {
            return new JoinRelation()
            {
                Type = JoinType.Inner,
                Left = Read(2, "left"),
                Right = Read(2, "right"),
                Expression = expression,
                PostJoinFilter = postJoinFilter,
                Emit = new List<int>() { 0, 3 }
            };
        }

        [Fact]
        public void JoinConditionChecksGoToTheInputTheyUse()
        {
            var actual = Assert.IsType<JoinRelation>(ExtractSingle(Join(
                And(Eq(Field(0), Field(2)), CheckTrue(Lt(Field(1), Num(5)), "left"), CheckTrue(Lt(Field(3), Num(5)), "right", ("key", Field(2)))),
                CheckTrue(new BoolLiteral() { Value = true }, "constant"))));

            Assert.Equal(And(Eq(Field(0), Field(2)), IsTrue(Lt(Field(1), Num(5))), IsTrue(Lt(Field(3), Num(5)))), actual.Expression);
            Assert.Equal(IsTrue(new BoolLiteral() { Value = true }), actual.PostJoinFilter);
            Assert.Equal(new List<int>() { 0, 3 }, actual.Emit);

            var left = Assert.IsType<CheckRelation>(actual.Left);
            Assert.Equal(Read(2, "left"), left.Input);
            // No field references: left input
            Assert.Equal(new List<CheckDefinition>() { Check(Lt(Field(1), Num(5)), "left"), Check(new BoolLiteral() { Value = true }, "constant") }, left.Checks);

            var right = Assert.IsType<CheckRelation>(actual.Right);
            Assert.Equal(Read(2, "right"), right.Input);
            Assert.Equal(new List<CheckDefinition>() { Check(Lt(Field(1), Num(5)), "right", [("key", Field(0))]) }, right.Checks);
        }

        [Fact]
        public void MergeJoinPostJoinFilterRightCheckGoesToRightInput()
        {
            var actual = Assert.IsType<MergeJoinRelation>(ExtractSingle(new MergeJoinRelation()
            {
                Type = JoinType.Inner,
                Left = Read(2, "left"),
                Right = Read(2, "right"),
                LeftKeys = new List<FieldReference>() { Field(0) },
                RightKeys = new List<FieldReference>() { Field(2) },
                PostJoinFilter = CheckTrue(Lt(Field(3), Num(5)), "right")
            }));

            Assert.Equal(IsTrue(Lt(Field(3), Num(5))), actual.PostJoinFilter);
            Assert.IsType<ReadRelation>(actual.Left);
            var right = Assert.IsType<CheckRelation>(actual.Right);
            Assert.Equal(new List<CheckDefinition>() { Check(Lt(Field(1), Num(5)), "right") }, right.Checks);
        }

        [Fact]
        public void ReadFilterChecksAreLiftedAboveTheRead()
        {
            var read = Read(2);
            read.Filter = And(Eq(Field(0), Num(1)), CheckTrue(Lt(Field(1), Num(5)), "m"));
            read.Emit = new List<int>() { 1 };

            var actual = ExtractSingle(read);

            var expectedRead = Read(2);
            expectedRead.Filter = And(Eq(Field(0), Num(1)));
            var expected = new FilterRelation()
            {
                Condition = And(IsTrue(Lt(Field(1), Num(5)))),
                // Emit moves above the check
                Emit = new List<int>() { 1 },
                Input = new CheckRelation()
                {
                    Input = expectedRead,
                    Checks = new List<CheckDefinition>() { Check(Lt(Field(1), Num(5)), "m") }
                }
            };
            Assert.Equal(expected, actual);
            Assert.Equal(1, actual.OutputLength);

            var onlyCheck = Read(2);
            onlyCheck.Filter = CheckTrue(Lt(Field(1), Num(5)), "m");
            var lifted = Assert.IsType<FilterRelation>(ExtractSingle(onlyCheck));
            Assert.Null(Assert.IsType<ReadRelation>(Assert.IsType<CheckRelation>(lifted.Input).Input).Filter);
        }

        [Fact]
        public void NormalizationFilterChecksAreLiftedAboveTheNormalization()
        {
            var actual = ExtractSingle(new NormalizationRelation()
            {
                Filter = And(Eq(Field(0), Num(1)), CheckTrue(Lt(Field(1), Num(5)), "m")),
                KeyIndex = new List<int>() { 0 },
                Emit = new List<int>() { 1, 0 },
                Input = Read(2)
            });

            var expected = new FilterRelation()
            {
                Condition = And(IsTrue(Lt(Field(1), Num(5)))),
                Emit = new List<int>() { 1, 0 },
                Input = new CheckRelation()
                {
                    Input = new NormalizationRelation()
                    {
                        Filter = And(Eq(Field(0), Num(1))),
                        KeyIndex = new List<int>() { 0 },
                        Input = Read(2)
                    },
                    Checks = new List<CheckDefinition>() { Check(Lt(Field(1), Num(5)), "m") }
                }
            };
            Assert.Equal(expected, actual);
        }

        private static Plan WindowPlan(string functionName, WindowBound? lowerBound, params Expression[] arguments)
        {
            return PlanOf(new ConsistentPartitionWindowRelation()
            {
                PartitionBy = new List<Expression>(),
                OrderBy = new List<SortField>(),
                WindowFunctions = new List<WindowFunction>()
                {
                    new WindowFunction() { ExtensionUri = FunctionsArithmetic.Uri, ExtensionName = functionName, Arguments = arguments.ToList(), LowerBound = lowerBound }
                },
                Input = Read(1)
            });
        }

        private static Plan ProjectPlan(Expression expression)
        {
            return PlanOf(new ProjectRelation() { Expressions = new List<Expression>() { expression }, Input = Read(1) });
        }

        public static TheoryData<string, Func<Plan>, Type, string> RejectedPlans()
        {
            return new TheoryData<string, Func<Plan>, Type, string>()
            {
                { "window frame bound", () => WindowPlan(FunctionsArithmetic.Sum, new PreceedingRangeWindowBound() { Expression = CheckValue(Num(1), Lt(Field(0), Num(1)), "bound") }, Field(0)), typeof(NotSupportedException), "window frame bound" },
                { "lead default", () => WindowPlan(FunctionsArithmetic.Lead, null, Field(0), Num(1), CheckValue(Num(0), Lt(Field(0), Num(1)), "default")), typeof(NotSupportedException), "LEAD or LAG default argument" },
                { "lag default", () => WindowPlan(FunctionsArithmetic.Lag, null, Field(0), Num(1), CheckValue(Num(0), Lt(Field(0), Num(1)), "default")), typeof(NotSupportedException), "LEAD or LAG default argument" },
                { "upper case lead default", () => WindowPlan("LEAD", null, Field(0), Num(1), CheckValue(Num(0), Lt(Field(0), Num(1)), "default")), typeof(NotSupportedException), "LEAD or LAG default argument" },
                // Field 2 is the function output
                { "table function output", () => PlanOf(TableFunctionOver(Read(2), Field(0), CheckTrue(Lt(Field(2), Num(2)), "join"))), typeof(NotSupportedException), "table function output" },
                { "table function without input", () => PlanOf(TableFunctionOver(null, CheckValue(Num(1), new BoolLiteral() { Value = false }, "argument"), null)), typeof(NotSupportedException), "table function without an input" },
                { "join condition using both inputs", () => PlanOf(Join(And(Eq(Field(0), Field(2)), CheckTrue(Eq(Field(1), Field(3)), "both")))), typeof(NotSupportedException), "references both inputs" },
                // Guard uses the right input, check the left
                { "join guard using the other input", () => PlanOf(Join(new IfThenExpression()
                {
                    Ifs = new List<IfClause>() { new IfClause() { If = Gt(Field(3), Num(0)), Then = CheckTrue(Lt(Field(0), Num(5)), "guarded") } }
                })), typeof(NotSupportedException), "references both inputs" },
                { "values", () => PlanOf(new VirtualTableReadRelation()
                {
                    BaseSchema = new NamedStruct() { Names = new List<string>() { "c0" }, Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } } },
                    Values = new VirtualTable()
                    {
                        Expressions = new List<StructExpression>()
                        {
                            new StructExpression() { Fields = new List<Expression>() { CheckValue(Num(1), new BoolLiteral() { Value = false }, "values") } }
                        }
                    }
                }), typeof(NotSupportedException), "VALUES" },
                { "iteration skip condition", () => PlanOf(new IterationRelation() { IterationName = "loop", LoopPlan = Read(1), SkipIterateCondition = CheckTrue(Lt(Field(0), Num(1)), "skip") }), typeof(NotSupportedException), "iteration skip condition" },
                { "check in tag", () => ProjectPlan(CheckTrue(Lt(Field(0), Num(1)), "outer", ("key", CheckValue(Field(0), Lt(Field(0), Num(2)), "inner")))), typeof(NotSupportedException), "tag arguments" },
                { "too few arguments", () => ProjectPlan(Function(FunctionsCheck.Uri, FunctionsCheck.CheckValue, Field(0), Lt(Field(0), Num(1)))), typeof(InvalidOperationException), "requires at least 3 arguments" },
                { "tag key without value", () => ProjectPlan(Function(FunctionsCheck.Uri, FunctionsCheck.CheckValue, Field(0), Lt(Field(0), Num(1)), Str("m"), Str("key"))), typeof(InvalidOperationException), "invalid tag arguments" },
                { "non literal tag key", () => ProjectPlan(Function(FunctionsCheck.Uri, FunctionsCheck.CheckValue, Field(0), Lt(Field(0), Num(1)), Str("m"), Field(0), Field(0))), typeof(InvalidOperationException), "requires string literal tag keys" }
            };
        }

        [Theory]
        [MemberData(nameof(RejectedPlans))]
        public void UnsupportedChecksAreRejected(string caseName, Func<Plan> plan, Type exceptionType, string fragment)
        {
            var exception = Assert.Throws(exceptionType, () => CheckExtractor.Extract(plan()));
            Assert.True(exception.Message.Contains(fragment), $"{caseName}: {exception.Message}");
        }

        public static TheoryData<string, string> NonLiteralMessages()
        {
            var data = new TheoryData<string, string>();
            foreach (var function in new[] { FunctionsCheck.CheckValue, FunctionsCheck.CheckTrue })
            {
                foreach (var kind in new[] { "concat", "column", "null", "cast", "check" })
                {
                    data.Add(function, kind);
                }
            }
            return data;
        }

        private static Expression NonLiteralMessage(string kind)
        {
            return kind switch
            {
                "concat" => Function(FunctionsString.Uri, FunctionsString.Concat, Str("Userkey: "), Field(0)),
                "column" => Field(0),
                "null" => new NullLiteral(),
                "cast" => new CastExpression() { Expression = Str("m"), Type = new StringType() },
                "check" => CheckValue(Str("m"), Lt(Field(0), Num(2)), "inner"),
                _ => throw new ArgumentOutOfRangeException(nameof(kind))
            };
        }

        [Theory]
        [MemberData(nameof(NonLiteralMessages))]
        public void NonLiteralMessageIsRejected(string function, string kind)
        {
            var plan = function == FunctionsCheck.CheckValue
                ? ProjectPlan(Function(FunctionsCheck.Uri, FunctionsCheck.CheckValue, Field(0), Lt(Field(0), Num(1)), NonLiteralMessage(kind), Str("key"), Field(0)))
                : PlanOf(new FilterRelation() { Condition = Function(FunctionsCheck.Uri, FunctionsCheck.CheckTrue, Lt(Field(0), Num(1)), NonLiteralMessage(kind)), Input = Read(1) });
            var exception = Assert.Throws<NotSupportedException>(() => CheckExtractor.Extract(plan));
            Assert.Contains("must be a string literal", exception.Message);
            Assert.Contains("{tag}", exception.Message);
        }

        [Fact]
        public void CaseBranchesKeepGuards()
        {
            var branchCondition = Eq(Field(1), Num(1));
            var project = Assert.IsType<ProjectRelation>(ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>()
                {
                    new IfThenExpression()
                    {
                        Ifs = new List<IfClause>()
                        {
                            new IfClause() { If = branchCondition, Then = CheckValue(Field(0), Lt(Field(0), Num(1)), "then1") },
                            new IfClause() { If = CheckTrue(Lt(Field(0), Num(2)), "if2"), Then = CheckValue(Field(0), Lt(Field(0), Num(3)), "then2") }
                        },
                        Else = CheckValue(Field(0), Lt(Field(0), Num(4)), "else")
                    }
                },
                Input = Read(2)
            }));

            Assert.Equal(new IfThenExpression()
            {
                Ifs = new List<IfClause>()
                {
                    new IfClause() { If = Eq(Field(1), Num(1)), Then = Field(0) },
                    new IfClause() { If = IsTrue(Lt(Field(0), Num(2))), Then = Field(0) }
                },
                Else = Field(0)
            }, project.Expressions[0]);

            // Guards use the rewritten if conditions
            var first = Eq(Field(1), Num(1));
            var second = IsTrue(Lt(Field(0), Num(2)));
            Assert.Equal(new List<CheckDefinition>()
            {
                Check(Lt(Field(0), Num(1)), "then1", Guard(first, CheckGuardKind.IsTrue)),
                Check(Lt(Field(0), Num(2)), "if2", Guard(first, CheckGuardKind.IsNotTrue)),
                Check(Lt(Field(0), Num(3)), "then2", Guard(first, CheckGuardKind.IsNotTrue), Guard(second, CheckGuardKind.IsTrue)),
                Check(Lt(Field(0), Num(4)), "else", Guard(first, CheckGuardKind.IsNotTrue), Guard(second, CheckGuardKind.IsNotTrue))
            }, Assert.IsType<CheckRelation>(project.Input).Checks);
        }

        [Fact]
        public void CoalesceArgumentsKeepNullGuardsOutermostFirst()
        {
            var project = Assert.IsType<ProjectRelation>(ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>()
                {
                    Function(FunctionsComparison.Uri, FunctionsComparison.Coalesce,
                        Field(0),
                        CheckValue(Field(1), Lt(Field(1), Num(1)), "second"),
                        new IfThenExpression()
                        {
                            Ifs = new List<IfClause>() { new IfClause() { If = Eq(Field(0), Num(0)), Then = CheckValue(Field(2), Lt(Field(2), Num(2)), "third") } }
                        })
                },
                Input = Read(3)
            }));

            Assert.Equal(Function(FunctionsComparison.Uri, FunctionsComparison.Coalesce,
                Field(0),
                Field(1),
                new IfThenExpression() { Ifs = new List<IfClause>() { new IfClause() { If = Eq(Field(0), Num(0)), Then = Field(2) } } }), project.Expressions[0]);
            Assert.Equal(new List<CheckDefinition>()
            {
                Check(Lt(Field(1), Num(1)), "second", Guard(Field(0), CheckGuardKind.IsNull)),
                Check(Lt(Field(2), Num(2)), "third",
                    Guard(Field(0), CheckGuardKind.IsNull),
                    Guard(Field(1), CheckGuardKind.IsNull),
                    Guard(Eq(Field(0), Num(0)), CheckGuardKind.IsTrue))
            }, Assert.IsType<CheckRelation>(project.Input).Checks);
        }

        [Fact]
        public void GreatestAndConcatArgumentsGuardOnEarlierNulls()
        {
            var greatest = Assert.IsType<ProjectRelation>(ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>()
                {
                    Function(FunctionsComparison.Uri, FunctionsComparison.Greatest,
                        Field(0),
                        CheckValue(Field(1), Lt(Field(1), Num(1)), "g1"),
                        CheckValue(Field(2), Lt(Field(2), Num(2)), "g2"))
                },
                Input = Read(3)
            }));
            // No guard before the first comparison
            Assert.Equal(new List<CheckDefinition>()
            {
                Check(Lt(Field(1), Num(1)), "g1"),
                Check(Lt(Field(2), Num(2)), "g2", Guard(IsNotNull(Field(0)), CheckGuardKind.IsTrue), Guard(IsNotNull(Field(1)), CheckGuardKind.IsTrue))
            }, Assert.IsType<CheckRelation>(greatest.Input).Checks);

            var concat = Assert.IsType<ProjectRelation>(ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>() { Function(FunctionsString.Uri, FunctionsString.Concat, Field(0), CheckValue(Field(1), Lt(Field(1), Num(1)), "c1")) },
                Input = Read(2)
            }));
            Assert.Equal(new List<CheckDefinition>()
            {
                Check(Lt(Field(1), Num(1)), "c1", Guard(IsNotNull(Field(0)), CheckGuardKind.IsTrue))
            }, Assert.IsType<CheckRelation>(concat.Input).Checks);

            var ignoreNulls = Function(FunctionsString.Uri, FunctionsString.Concat, Field(0), CheckValue(Field(1), Lt(Field(1), Num(1)), "c2"));
            ignoreNulls.Options = new SortedList<string, string>() { { "null_handling", "IGNORE_NULLS" } };
            var concatIgnoreNulls = Assert.IsType<ProjectRelation>(ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>() { ignoreNulls },
                Input = Read(2)
            }));
            Assert.Equal(new List<CheckDefinition>() { Check(Lt(Field(1), Num(1)), "c2") }, Assert.IsType<CheckRelation>(concatIgnoreNulls.Input).Checks);
        }

        [Fact]
        public void EagerFunctionsDoNotAddGuards()
        {
            var project = Assert.IsType<ProjectRelation>(ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>()
                {
                    Function(FunctionsBoolean.Uri, FunctionsBoolean.Or, Eq(Field(0), Num(1)), CheckTrue(Lt(Field(1), Num(1)), "or")),
                    new SingularOrListExpression() { Value = CheckValue(Field(0), Lt(Field(0), Num(2)), "in"), Options = new List<Expression>() { Num(1), Num(2) } }
                },
                Input = Read(2)
            }));
            Assert.Equal(new List<CheckDefinition>()
            {
                Check(Lt(Field(1), Num(1)), "or"),
                Check(Lt(Field(0), Num(2)), "in")
            }, Assert.IsType<CheckRelation>(project.Input).Checks);
        }

        [Fact]
        public void NestedChecksInConditionAndValueAreExtracted()
        {
            var project = Assert.IsType<ProjectRelation>(ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>()
                {
                    CheckValue(CheckValue(Field(0), Lt(Field(0), Num(1)), "inner value"), CheckTrue(Lt(Field(1), Num(2)), "inner condition"), "outer")
                },
                Input = Read(2)
            }));

            Assert.Equal(Field(0), project.Expressions[0]);
            // Condition, then the check, then the value
            Assert.Equal(new List<CheckDefinition>()
            {
                Check(Lt(Field(1), Num(2)), "inner condition"),
                Check(IsTrue(Lt(Field(1), Num(2))), "outer"),
                Check(Lt(Field(0), Num(1)), "inner value")
            }, Assert.IsType<CheckRelation>(project.Input).Checks);
        }

        [Fact]
        public void ExtractedExpressionsAreNotSharedWithTheRelation()
        {
            var project = Assert.IsType<ProjectRelation>(ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>()
                {
                    new IfThenExpression()
                    {
                        Ifs = new List<IfClause>() { new IfClause() { If = Eq(Field(1), Num(1)), Then = CheckTrue(Lt(Field(0), Num(1)), "m", ("key", Field(1))) } }
                    }
                },
                Input = Read(2)
            }));

            var relationReferences = new FieldReferenceCollector();
            relationReferences.Visit(project.Expressions[0], null);
            var checkReferences = new FieldReferenceCollector();
            foreach (var expression in CheckFunctionMatcher.GetExpressions(Assert.IsType<CheckRelation>(project.Input).Checks[0]))
            {
                checkReferences.Visit(expression, null);
            }
            Assert.NotEmpty(checkReferences.References);
            Assert.Empty(relationReferences.References.Intersect(checkReferences.References, ReferenceEqualityComparer.Instance));
        }

        private static Plan CreateMixedPlan()
        {
            var read = Read(3, "source");
            read.Filter = And(Eq(Field(2), Num(1)), CheckTrue(Lt(Field(0), Num(5)), "read"));
            var project = new ProjectRelation()
            {
                Expressions = new List<Expression>()
                {
                    Function(FunctionsComparison.Uri, FunctionsComparison.Coalesce,
                        Field(0),
                        new IfThenExpression()
                        {
                            Ifs = new List<IfClause>() { new IfClause() { If = Gt(Field(1), Num(0)), Then = CheckValue(Field(1), Lt(Field(1), Num(9)), "case") } },
                            Else = Num(0)
                        })
                },
                Input = new FilterRelation()
                {
                    Condition = And(Eq(Field(1), Num(2)), CheckTrue(Lt(Field(2), Num(3)), "filter")),
                    Input = read
                },
                Emit = new List<int>() { 0, 3 }
            };
            var join = new JoinRelation()
            {
                Type = JoinType.Inner,
                Left = project,
                Right = Read(2, "other"),
                Expression = And(Eq(Field(0), Field(2)), CheckTrue(Lt(Field(3), Num(4)), "join"))
            };
            var aggregate = new AggregateRelation()
            {
                Groupings = new List<AggregateGrouping>() { new AggregateGrouping() { GroupingExpressions = new List<Expression>() { CheckValue(Field(0), Lt(Field(0), Num(7)), "group") } } },
                Input = join
            };
            return new Plan()
            {
                Relations = new List<Relation>()
                {
                    new WriteRelation()
                    {
                        Input = aggregate,
                        NamedObject = new NamedTable() { Names = new List<string>() { "output" } },
                        TableSchema = new NamedStruct() { Names = new List<string>() { "c0" }, Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } } }
                    }
                }
            };
        }

        [Fact]
        public void ExtractTwiceEqualsExtractOnce()
        {
            Assert.Contains(FunctionsCheck.CheckValue, SubstraitSerializer.SerializeToJson(CreateMixedPlan()));

            var once = CheckExtractor.Extract(CreateMixedPlan());
            var onceJson = SubstraitSerializer.SerializeToJson(once);
            var twice = CheckExtractor.Extract(CheckExtractor.Extract(CreateMixedPlan()));

            Assert.Equal(once, twice);
            Assert.Equal(onceJson, SubstraitSerializer.SerializeToJson(twice));
            Assert.Equal(5, FindAll<CheckRelation>(once).Sum(x => x.Checks.Count));
            Assert.DoesNotContain(FunctionsCheck.CheckValue, onceJson);
            Assert.DoesNotContain(FunctionsCheck.CheckTrue, onceJson);
            AssertCheckFieldsInRange(once);
        }

        [Fact]
        public void CommonSubPlansWithCheckRelationsAreShared()
        {
            Relation Subtree()
            {
                return new ProjectRelation()
                {
                    Expressions = new List<Expression>() { CheckValue(Field(0), Lt(Field(0), Num(1)), "shared") },
                    Input = Read(1)
                };
            }
            var plan = CheckExtractor.Extract(PlanOf(new SetRelation()
            {
                Operation = SetOperation.UnionAll,
                Inputs = new List<Relation>() { Subtree(), Subtree() }
            }));

            plan = CommonSubPlanOptimizer.Optimize(plan);

            Assert.Equal(2, plan.Relations.Count);
            var union = Assert.IsType<SetRelation>(plan.Relations[0]);
            Assert.All(union.Inputs, input => Assert.IsType<ReferenceRelation>(input));
            Assert.Single(FindAll<CheckRelation>(plan));
        }

        private const string UsersTable = "CREATE TABLE users (userkey any, companyid any, name any, extra any);";

        [Fact]
        public void SqlProjectionCheckSitsBelowTheProjectionAfterOptimization()
        {
            var plan = PlanOptimizer.Optimize(BuildSqlPlan(UsersTable + @"
                INSERT INTO output
                SELECT CHECK_VALUE(userkey, companyid < 10, 'company too large', userkey) AS userkey FROM users"));

            var checkRelation = Assert.Single(FindAll<CheckRelation>(plan));
            Assert.Equal("company too large", Assert.Single(checkRelation.Checks).Message);
            var read = Assert.IsType<ReadRelation>(checkRelation.Input);
            // Unused columns are pruned
            Assert.Equal(new List<string>() { "userkey", "companyid" }, read.BaseSchema.Names);
            AssertCheckFieldsInRange(plan);
        }

        [Fact]
        public void SqlWhereCheckFreeConjunctsArePushedIntoTheRead()
        {
            var plan = PlanOptimizer.Optimize(BuildSqlPlan(UsersTable + @"
                INSERT INTO output
                SELECT name FROM users WHERE name = 'a' AND CHECK_TRUE(userkey < 900, 'userkey too large')"));

            var checkRelation = Assert.Single(FindAll<CheckRelation>(plan));
            var read = Assert.IsType<ReadRelation>(checkRelation.Input);
            Assert.NotNull(read.Filter);
            Assert.False(CheckFunctionMatcher.ContainsCheck(read.Filter));
            var upperFilter = Assert.Single(FindAll<FilterRelation>(plan));
            Assert.Same(checkRelation, upperFilter.Input);
            AssertCheckFieldsInRange(plan);
        }

        [Fact]
        public void SqlJoinConditionCheckSitsOnTheInputItUses()
        {
            var plan = PlanOptimizer.Optimize(BuildSqlPlan(@"
                CREATE TABLE orders (orderkey any, userkey any);
                CREATE TABLE users (userkey any, name any);
                INSERT INTO output
                SELECT o.orderkey, u.name FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey AND CHECK_TRUE(u.name IS NOT NULL, 'missing name')"));

            var join = Assert.Single(FindAll<MergeJoinRelation>(plan));
            Assert.Empty(FindAll<CheckRelation>(join.Left));
            var checkRelation = Assert.Single(FindAll<CheckRelation>(join.Right));
            Assert.Equal("users", Assert.IsType<ReadRelation>(checkRelation.Input).NamedTable.DotSeperated);
            AssertCheckFieldsInRange(plan);
        }

        [Theory]
        [InlineData("CREATE TABLE orders (orderkey any, userkey any); CREATE TABLE users (userkey any, name any); INSERT INTO output SELECT o.orderkey, u.name FROM orders o INNER JOIN users u ON o.userkey = u.userkey AND CHECK_TRUE(o.orderkey > u.userkey, 'bad order')", "references both inputs")]
        [InlineData("INSERT INTO output SELECT CHECK_VALUE(1, 1 = 2, 'never true') AS v", "VALUES")]
        [InlineData(UsersTable + " INSERT INTO output SELECT CHECK_VALUE(userkey, companyid < 10, concat('Userkey: ', userkey, ' is invalid'), userkey) AS userkey FROM users", "must be a string literal")]
        [InlineData(UsersTable + " INSERT INTO output SELECT CHECK_VALUE(userkey, companyid < 10, 'Userkey: ' || userkey, userkey) AS userkey FROM users", "must be a string literal")]
        [InlineData(UsersTable + " INSERT INTO output SELECT CHECK_VALUE(userkey, companyid < 10, name, userkey) AS userkey FROM users", "must be a string literal")]
        [InlineData(UsersTable + " INSERT INTO output SELECT CHECK_VALUE(userkey, companyid < 10, NULL, userkey) AS userkey FROM users", "must be a string literal")]
        [InlineData(UsersTable + " INSERT INTO output SELECT name FROM users WHERE CHECK_TRUE(userkey < 900, concat('Userkey: ', userkey, ' is invalid'))", "must be a string literal")]
        [InlineData(UsersTable + " INSERT INTO output SELECT name FROM users WHERE CHECK_TRUE(userkey < 900, 'Userkey: ' || userkey)", "must be a string literal")]
        [InlineData(UsersTable + " INSERT INTO output SELECT name FROM users WHERE CHECK_TRUE(userkey < 900, name)", "must be a string literal")]
        [InlineData(UsersTable + " INSERT INTO output SELECT name FROM users WHERE CHECK_TRUE(userkey < 900, NULL)", "must be a string literal")]
        [InlineData(UsersTable + " INSERT INTO output SELECT CHECK_VALUE(userkey, userkey < 900, 'Userkey {innerValue} is too large', innerValue => CHECK_VALUE(userkey, userkey < 10, 'inner message')) FROM users", "tag arguments")]
        public void SqlCheckIsRejected(string sql, string fragment)
        {
            var plan = BuildSqlPlan(sql);
            var exception = Assert.Throws<NotSupportedException>(() => PlanOptimizer.Optimize(plan));
            Assert.Contains(fragment, exception.Message);
        }

        private static ScalarFunction GetTimestamp()
        {
            return Function(FunctionsDatetime.Uri, FunctionsDatetime.GetTimestamp);
        }

        private sealed class ExpressionFinder(Func<Expression, bool> predicate) : BaseRelationExpressionVisitor<object?>
        {
            private readonly Finder _finder = new Finder(predicate);

            public override ExpressionVisitor<object?, object> Visitor => _finder;

            public bool Found => _finder.Found;

            private sealed class Finder(Func<Expression, bool> match) : ExpressionVisitor<object?, object>
            {
                public bool Found { get; private set; }

                public override object? VisitScalarFunction(ScalarFunction scalarFunction, object state)
                {
                    Found |= match(scalarFunction);
                    return base.VisitScalarFunction(scalarFunction, state);
                }

                public override object? VisitSetPredicateExpression(SetPredicateExpression setPredicateExpression, object state)
                {
                    Found |= match(setPredicateExpression);
                    return null;
                }
            }
        }

        private static void AssertNoExpression(Plan plan, Func<Expression, bool> predicate)
        {
            var finder = new ExpressionFinder(predicate);
            foreach (var relation in plan.Relations)
            {
                finder.Visit(relation, null!);
            }
            Assert.False(finder.Found);
        }

        private static void AssertNoGetTimestampFunction(Plan plan)
        {
            AssertNoExpression(plan, x => x is ScalarFunction f && f.ExtensionUri == FunctionsDatetime.Uri && f.ExtensionName == FunctionsDatetime.GetTimestamp);
        }

        private static int TimestampRelationId(Plan plan)
        {
            var index = plan.Relations.FindIndex(x => x is ReadRelation read && read.NamedTable.DotSeperated == "__gettimestamp");
            Assert.True(index >= 0);
            return index;
        }

        /// <summary>
        /// A join whose right input reads the timestamp relation.
        /// </summary>
        private static bool IsTimestampJoin(Relation relation, int timestampRelationId)
        {
            var right = relation switch
            {
                JoinRelation join => join.Right,
                MergeJoinRelation mergeJoin => mergeJoin.Right,
                _ => null
            };
            return right != null && FindAll<ReferenceRelation>(right).Any(x => x.RelationId == timestampRelationId);
        }

        private static List<Relation> TimestampJoins(Relation root, int timestampRelationId)
        {
            return FindAll<Relation>(root).Where(x => IsTimestampJoin(x, timestampRelationId)).ToList();
        }

        private static bool IsCrossJoin(Relation relation)
        {
            return relation is JoinRelation join && Equals(join.Expression, new BoolLiteral() { Value = true });
        }

        [Fact]
        public void GetTimestampInCheckIsReadFromATimestampJoin()
        {
            var plan = PlanOf(new CheckRelation()
            {
                Input = Read(2),
                Checks = new List<CheckDefinition>() { Check(Lt(Field(0), GetTimestamp()), "stale", [("now", GetTimestamp())], Guard(Gt(Field(1), GetTimestamp()), CheckGuardKind.IsTrue)) }
            });

            plan = TimestampToJoin.Optimize(plan, new PlanOptimizerSettings());

            Assert.Equal(2, plan.Relations.Count);
            Assert.Equal("__gettimestamp", Assert.IsType<ReadRelation>(plan.Relations[1]).NamedTable.DotSeperated);
            var checkRelation = Assert.IsType<CheckRelation>(Assert.IsType<BufferRelation>(plan.Relations[0]).Input);
            // The timestamp column is not emitted
            Assert.Equal(new List<int>() { 0, 1 }, checkRelation.Emit);
            var join = Assert.IsType<JoinRelation>(checkRelation.Input);
            Assert.Equal(JoinType.Inner, join.Type);
            Assert.Equal(new BoolLiteral() { Value = true }, join.Expression);
            Assert.IsType<ReadRelation>(join.Left);
            Assert.True(IsTimestampJoin(join, 1));
            var check = Assert.Single(checkRelation.Checks);
            Assert.Equal(Lt(Field(0), Field(2)), check.Condition);
            Assert.Equal(Field(2), Assert.Single(check.Tags).Value);
            Assert.Equal(Gt(Field(1), Field(2)), Assert.Single(check.Guards).Expression);
            AssertNoGetTimestampFunction(plan);
            AssertCheckFieldsInRange(plan);
        }

        [Fact]
        public void GetTimestampInCheckKeepsItsEmitAndHonoursTheBufferSetting()
        {
            var plan = PlanOf(new CheckRelation()
            {
                Emit = new List<int>() { 1 },
                Input = Read(2),
                Checks = new List<CheckDefinition>() { Check(Lt(Field(0), GetTimestamp()), "stale") }
            });

            plan = TimestampToJoin.Optimize(plan, new PlanOptimizerSettings() { AddBufferBlockOnGetTimestamp = false });

            var checkRelation = Assert.IsType<CheckRelation>(plan.Relations[0]);
            Assert.Equal(new List<int>() { 1 }, checkRelation.Emit);
            Assert.True(IsTimestampJoin(Assert.IsType<JoinRelation>(checkRelation.Input), 1));
            Assert.Equal(Lt(Field(0), Field(2)), Assert.Single(checkRelation.Checks).Condition);
            AssertNoGetTimestampFunction(plan);
        }

        private const string TimestampTables = @"
            CREATE TABLE users (userkey any, companyid any, ts any);
            CREATE TABLE orders (orderkey any, userkey any);";

        [Fact]
        public void SqlWhereCheckUsingGetTimestampSitsOnATimestampJoin()
        {
            var plan = PlanOptimizer.Optimize(BuildSqlPlan(TimestampTables + @"
                INSERT INTO output
                SELECT userkey FROM users WHERE CHECK_TRUE(ts < gettimestamp(), 'stale')"));

            AssertNoGetTimestampFunction(plan);
            var timestampRelationId = TimestampRelationId(plan);
            var checkRelation = Assert.Single(FindAll<CheckRelation>(plan));
            var checkJoin = Assert.Single(TimestampJoins(checkRelation.Input, timestampRelationId));
            Assert.True(IsCrossJoin(checkJoin));
            // The rewritten WHERE filters on its own timestamp join above the check
            var whereJoin = Assert.Single(TimestampJoins(plan.Relations[0], timestampRelationId), x => FindAll<CheckRelation>(x).Count == 1);
            Assert.False(IsCrossJoin(whereJoin));
            AssertCheckFieldsInRange(plan);
        }

        [Fact]
        public void SqlWhereGetTimestampConjunctFiltersBelowTheCheck()
        {
            var plan = PlanOptimizer.Optimize(BuildSqlPlan(TimestampTables + @"
                INSERT INTO output
                SELECT userkey FROM users WHERE ts < gettimestamp() AND CHECK_TRUE(companyid < 5, 'big')"));

            AssertNoGetTimestampFunction(plan);
            var checkRelation = Assert.Single(FindAll<CheckRelation>(plan));
            // The check only sees rows passing the timestamp filter
            var timestampJoin = Assert.Single(TimestampJoins(checkRelation.Input, TimestampRelationId(plan)));
            Assert.False(IsCrossJoin(timestampJoin));
            Assert.Single(TimestampJoins(plan.Relations[0], TimestampRelationId(plan)));
            Assert.Single(FindAll<FilterRelation>(plan), x => ReferenceEquals(x.Input, checkRelation));
            AssertCheckFieldsInRange(plan);
        }

        [Fact]
        public void SqlSelectChecksUsingGetTimestampSitOnATimestampJoin()
        {
            var plan = PlanOptimizer.Optimize(BuildSqlPlan(TimestampTables + @"
                INSERT INTO output
                SELECT
                    CHECK_VALUE(userkey, ts < gettimestamp(), 'stale {userkey}', userkey) AS userkey,
                    CHECK_TRUE(ts > gettimestamp(), 'past {userkey}', userkey) AS upcoming
                FROM users"));

            AssertNoGetTimestampFunction(plan);
            var checkRelation = Assert.Single(FindAll<CheckRelation>(plan));
            Assert.Equal(2, checkRelation.Checks.Count);
            var checkJoin = Assert.IsType<JoinRelation>(Assert.Single(TimestampJoins(checkRelation.Input, TimestampRelationId(plan))));
            Assert.True(IsCrossJoin(checkJoin));
            Assert.Equal("users", Assert.IsType<ReadRelation>(checkJoin.Left).NamedTable.DotSeperated);
            AssertCheckFieldsInRange(plan);
        }

        [Fact]
        public void SqlWhereExistsAndCheckTrueSplitsTheExistsBelowTheCheck()
        {
            var plan = PlanOptimizer.Optimize(BuildSqlPlan(TimestampTables + @"
                INSERT INTO output
                SELECT u.userkey FROM users u
                WHERE EXISTS (SELECT 1 FROM orders o WHERE o.userkey = u.userkey) AND CHECK_TRUE(u.companyid < 5, 'big')"));

            var checkRelation = Assert.Single(FindAll<CheckRelation>(plan));
            // The check only sees rows passing the exists
            var semiJoin = Assert.Single(FindAll<MergeJoinRelation>(checkRelation.Input));
            Assert.Equal(JoinType.LeftMark, semiJoin.Type);
            Assert.Contains(FindAll<ReadRelation>(semiJoin.Right), x => x.NamedTable.DotSeperated == "orders");
            Assert.Single(FindAll<FilterRelation>(plan), x => ReferenceEquals(x.Input, checkRelation));
            AssertCheckFieldsInRange(plan);
        }

        [Fact]
        public void SqlWhereExistsAndCheckTrueUsingGetTimestamp()
        {
            var plan = PlanOptimizer.Optimize(BuildSqlPlan(TimestampTables + @"
                INSERT INTO output
                SELECT u.userkey FROM users u
                WHERE EXISTS (SELECT 1 FROM orders o WHERE o.userkey = u.userkey) AND CHECK_TRUE(u.ts < gettimestamp(), 'stale')"));

            AssertNoGetTimestampFunction(plan);
            var timestampRelationId = TimestampRelationId(plan);
            var checkRelation = Assert.Single(FindAll<CheckRelation>(plan));
            // The check only sees rows passing the exists
            Assert.Single(FindAll<MergeJoinRelation>(checkRelation.Input), x => x.Type == JoinType.LeftMark);
            Assert.True(IsCrossJoin(Assert.Single(TimestampJoins(checkRelation.Input, timestampRelationId))));
            var whereJoin = Assert.Single(TimestampJoins(plan.Relations[0], timestampRelationId), x => FindAll<CheckRelation>(x).Count == 1);
            Assert.False(IsCrossJoin(whereJoin));
            AssertCheckFieldsInRange(plan);
        }

        [Fact]
        public void SqlCheckInsideExistsSubqueryIsExtractedAfterDecorrelation()
        {
            var plan = PlanOptimizer.Optimize(BuildSqlPlan(TimestampTables + @"
                INSERT INTO output
                SELECT u.userkey FROM users u
                WHERE EXISTS (SELECT 1 FROM orders o WHERE o.userkey = u.userkey AND CHECK_TRUE(o.orderkey < 10, 'big order'))"));

            var json = SubstraitSerializer.SerializeToJson(plan);
            Assert.DoesNotContain(FunctionsCheck.CheckTrue, json);
            var semiJoin = Assert.Single(FindAll<MergeJoinRelation>(plan));
            var checkRelation = Assert.Single(FindAll<CheckRelation>(semiJoin.Right));
            Assert.Equal("big order", Assert.Single(checkRelation.Checks).Message);
            AssertCheckFieldsInRange(plan);
        }

        [Theory]
        [InlineData("CHECK_TRUE(EXISTS (SELECT 1 FROM orders o WHERE o.userkey = u.userkey), 'no orders')")]
        [InlineData("CHECK_TRUE(u.userkey IN (SELECT o.userkey FROM orders o), 'no orders')")]
        [InlineData("u.companyid < 5 AND CHECK_TRUE(EXISTS (SELECT 1 FROM orders o WHERE o.userkey = u.userkey), 'no orders')")]
        [InlineData("u.ts < gettimestamp() AND CHECK_TRUE(EXISTS (SELECT 1 FROM orders o WHERE o.userkey = u.userkey), 'no orders')")]
        public void SqlCheckOverSubqueryReadsOneMarkJoin(string where)
        {
            var plan = PlanOptimizer.Optimize(BuildSqlPlan(TimestampTables + @"
                INSERT INTO output
                SELECT u.userkey FROM users u
                WHERE " + where));

            AssertNoExpression(plan, x => x is SetPredicateExpression);
            AssertNoGetTimestampFunction(plan);
            var checkRelation = Assert.Single(FindAll<CheckRelation>(plan));
            // The check and the where share one mark join
            var markJoin = Assert.Single(FindAll<MergeJoinRelation>(plan), x => x.Type == JoinType.LeftMark);
            Assert.Contains(markJoin, FindAll<MergeJoinRelation>(checkRelation.Input));
            AssertCheckFieldsInRange(plan);
        }

        [Theory]
        [InlineData("SELECT userkey FROM users WHERE ts < gettimestamp()")]
        [InlineData("SELECT userkey, ts < gettimestamp() AS fresh FROM users")]
        [InlineData("SELECT companyid, list_agg(gettimestamp()) AS times FROM users GROUP BY companyid")]
        [InlineData("SELECT u.userkey FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.userkey = u.userkey)")]
        [InlineData("SELECT u.userkey FROM users u WHERE u.ts < gettimestamp() AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.userkey = u.userkey)")]
        [InlineData("SELECT u.userkey, o.orderkey FROM users u INNER JOIN orders o ON u.userkey = o.userkey WHERE u.companyid < 5")]
        public void CheckExtractionLeavesCheckFreePlansUnchanged(string query)
        {
            var plan = BuildSqlPlan(TimestampTables + "INSERT INTO output " + query);

            // The passes in the order the optimizer runs them
            AssertExtractionIsNoOp(plan);
            plan = TimestampToJoin.Optimize(plan, new PlanOptimizerSettings());
            plan = SubqueryDecorrelationVisitor.Optimize(plan);
            AssertExtractionIsNoOp(plan);
        }

        private static void AssertExtractionIsNoOp(Plan plan)
        {
            var relations = plan.Relations.ToList();
            var json = SubstraitSerializer.SerializeToJson(plan);

            var extracted = CheckExtractor.Extract(plan);

            Assert.Same(plan, extracted);
            Assert.Equal(relations.Count, extracted.Relations.Count);
            for (int i = 0; i < relations.Count; i++)
            {
                Assert.Same(relations[i], extracted.Relations[i]);
            }
            Assert.Equal(json, SubstraitSerializer.SerializeToJson(extracted));
        }

        private static IFunctionsRegister CreateFunctionsRegister()
        {
            var register = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(register);
            return register;
        }

        private static IDataValue[] ConditionValues()
        {
            return new IDataValue[]
            {
                new BoolValue(true),
                new BoolValue(false),
                NullValue.Instance,
                new Int64Value(1),
                new Int64Value(0),
                new StringValue("true"),
                new DoubleValue(1.0)
            };
        }

        [Fact]
        public void CheckTrueReplacementMatchesOldCheckTrueResults()
        {
            var project = Assert.IsType<ProjectRelation>(ExtractSingle(new ProjectRelation()
            {
                Expressions = new List<Expression>() { CheckTrue(Field(0), "m") },
                Input = Read(1)
            }));
            var compiled = ColumnProjectCompiler.CompileToValue(project.Expressions[0], CreateFunctionsRegister());

            var values = ConditionValues();
            var column = Column.Create(GlobalMemoryManager.Instance);
            foreach (var value in values)
            {
                column.Add(value);
            }
            var batch = new EventBatchData(new IColumn[] { column });

            // Old check_true: true only for Boolean true
            var expected = new bool[] { true, false, false, false, false, false, false };
            for (int i = 0; i < values.Length; i++)
            {
                var result = compiled(batch, i);
                Assert.Equal(ArrowTypeId.Boolean, result.Type);
                Assert.Equal(expected[i], result.AsBool);
            }
            batch.Dispose();
        }

        [Fact]
        public void SplitFilterMatchesTheUnsplitFilter()
        {
            var register = CreateFunctionsRegister();
            var values = ConditionValues();
            var plain = Column.Create(GlobalMemoryManager.Instance);
            var condition = Column.Create(GlobalMemoryManager.Instance);
            foreach (var a in values)
            {
                foreach (var c in values)
                {
                    plain.Add(a);
                    condition.Add(c);
                }
            }
            var batch = new EventBatchData(new IColumn[] { plain, condition });

            var unsplit = ColumnBooleanCompiler.Compile(And(Field(0), IsTrue(Field(1))), register);
            var filter = Assert.IsType<FilterRelation>(ExtractSingle(new FilterRelation()
            {
                Condition = And(Field(0), CheckTrue(Field(1), "m")),
                Input = Read(2)
            }));
            var upper = ColumnBooleanCompiler.Compile(filter.Condition, register);
            var lower = ColumnBooleanCompiler.Compile(Assert.IsType<FilterRelation>(Assert.IsType<CheckRelation>(filter.Input).Input).Condition, register);

            for (int i = 0; i < values.Length * values.Length; i++)
            {
                Assert.Equal(unsplit(batch, i), lower(batch, i) && upper(batch, i));
            }

            // Lone Int64 conjunct: bare passes, and-wrapped fails
            var bare = ColumnBooleanCompiler.Compile(Field(0), register);
            var wrapped = ColumnBooleanCompiler.Compile(And(Field(0)), register);
            var int64Row = 3 * values.Length;
            Assert.True(bare(batch, int64Row));
            Assert.False(wrapped(batch, int64Row));
            batch.Dispose();
        }
    }
}

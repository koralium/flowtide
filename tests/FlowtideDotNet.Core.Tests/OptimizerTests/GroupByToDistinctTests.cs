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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.OptimizerTests
{
    public class GroupByToDistinctTests
    {
        private const string CreateTable = "CREATE TABLE table1 (col1 any, col2 any, col3 any);";

        private static Substrait.Plan BuildPlan(string sql)
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(CreateTable + sql);
            return sqlPlanBuilder.GetPlan();
        }

        private static List<Relation> CollectRelations(Relation root)
        {
            var result = new List<Relation>();
            void Collect(Relation relation)
            {
                result.Add(relation);
                switch (relation)
                {
                    case FilterRelation r: Collect(r.Input); break;
                    case ProjectRelation r: Collect(r.Input); break;
                    case AggregateRelation r: Collect(r.Input); break;
                    case JoinRelation r: Collect(r.Left); Collect(r.Right); break;
                    case MergeJoinRelation r: Collect(r.Left); Collect(r.Right); break;
                    case SetRelation r: r.Inputs.ForEach(Collect); break;
                    case SortRelation r: Collect(r.Input); break;
                    case WriteRelation r: Collect(r.Input); break;
                    case RootRelation r: Collect(r.Input); break;
                    case TableFunctionRelation r when r.Input != null: Collect(r.Input); break;
                    case ExchangeRelation r: Collect(r.Input); break;
                }
            }
            Collect(root);
            return result;
        }

        private static List<Relation> CollectRelations(Substrait.Plan plan)
        {
            return plan.Relations.SelectMany(CollectRelations).ToList();
        }

        [Fact]
        public void GroupByWithoutMeasuresBecomesDistinct()
        {
            var plan = BuildPlan(@"
                INSERT INTO output
                SELECT col1, col2, col3 FROM table1 GROUP BY col1, col2, col3");
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings());

            var relations = CollectRelations(plan);

            Assert.Empty(relations.OfType<AggregateRelation>());
            var setRelation = Assert.Single(relations.OfType<SetRelation>());
            Assert.Equal(SetOperation.UnionDistinct, setRelation.Operation);
            Assert.Single(setRelation.Inputs);
            Assert.Equal(3, setRelation.OutputLength);
        }

        /// <summary>
        /// The rewrite gives the same input as DISTINCT.
        /// </summary>
        [Fact]
        public void RewrittenGroupByFeedsTheSameInputAsDistinct()
        {
            var groupByPlan = BuildPlan(@"
                INSERT INTO output
                SELECT col1, col2 FROM table1 GROUP BY col1, col2");
            groupByPlan = PlanOptimizer.Optimize(groupByPlan, new PlanOptimizerSettings());

            var distinctPlan = BuildPlan(@"
                INSERT INTO output
                SELECT DISTINCT col1, col2 FROM table1");
            distinctPlan = PlanOptimizer.Optimize(distinctPlan, new PlanOptimizerSettings());

            var groupBySet = Assert.Single(CollectRelations(groupByPlan).OfType<SetRelation>());
            var distinctSet = Assert.Single(CollectRelations(distinctPlan).OfType<SetRelation>());

            Assert.Equal(distinctSet.Operation, groupBySet.Operation);
            Assert.Equal(distinctSet.Inputs.Count, groupBySet.Inputs.Count);
            Assert.Equal(distinctSet.Inputs[0], groupBySet.Inputs[0]);
            Assert.Equal(distinctSet.OutputLength, groupBySet.OutputLength);
        }

        [Fact]
        public void GroupByOnExpressionBecomesDistinct()
        {
            var plan = BuildPlan(@"
                INSERT INTO output
                SELECT col1 + col2 FROM table1 GROUP BY col1 + col2");
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings());

            var relations = CollectRelations(plan);

            Assert.Empty(relations.OfType<AggregateRelation>());
            var setRelation = Assert.Single(relations.OfType<SetRelation>());
            Assert.Equal(SetOperation.UnionDistinct, setRelation.Operation);
            Assert.Equal(1, setRelation.OutputLength);

            // The addition is computed before the distinct
            var project = Assert.IsType<ProjectRelation>(setRelation.Inputs[0]);
            var expression = Assert.Single(project.Expressions);
            var function = Assert.IsType<ScalarFunction>(expression);
            Assert.Equal(FunctionsArithmetic.Add, function.ExtensionName);
            Assert.Equal(1, project.OutputLength);
        }

        /// <summary>
        /// Having filter uses the same output positions.
        /// </summary>
        [Fact]
        public void GroupByWithHavingBecomesDistinctWithFilter()
        {
            var plan = BuildPlan(@"
                INSERT INTO output
                SELECT col1 FROM table1 GROUP BY col1 HAVING col1 > 5");
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings());

            var relations = CollectRelations(plan);

            Assert.Empty(relations.OfType<AggregateRelation>());
            var setRelation = Assert.Single(relations.OfType<SetRelation>());
            Assert.Equal(SetOperation.UnionDistinct, setRelation.Operation);

            var filter = Assert.Single(relations.OfType<FilterRelation>());
            var condition = Assert.IsType<ScalarFunction>(filter.Condition);
            Assert.Equal(FunctionsComparison.GreaterThan, condition.ExtensionName);
            var field = Assert.IsType<DirectFieldReference>(condition.Arguments[0]);
            var segment = Assert.IsType<StructReferenceSegment>(field.ReferenceSegment);
            Assert.Equal(0, segment.Field);
        }

        [Fact]
        public void GroupByWithMeasureIsNotRewritten()
        {
            var plan = BuildPlan(@"
                INSERT INTO output
                SELECT col1, count(*) FROM table1 GROUP BY col1");
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings());

            var relations = CollectRelations(plan);

            Assert.Single(relations.OfType<AggregateRelation>());
            Assert.Empty(relations.OfType<SetRelation>());
        }

        [Fact]
        public void AggregateWithoutGroupingIsNotRewritten()
        {
            var plan = BuildPlan(@"
                INSERT INTO output
                SELECT count(*) FROM table1");
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings());

            var relations = CollectRelations(plan);

            Assert.Single(relations.OfType<AggregateRelation>());
            Assert.Empty(relations.OfType<SetRelation>());
        }

        /// <summary>
        /// Grouping sets add a grouping id column.
        /// </summary>
        [Fact]
        public void MultipleGroupingSetsAreNotRewritten()
        {
            var readRelation = new ReadRelation()
            {
                NamedTable = new Substrait.Type.NamedTable() { Names = new List<string>() { "table1" } },
                BaseSchema = new Substrait.Type.NamedStruct()
                {
                    Names = new List<string>() { "col1", "col2" },
                    Struct = new Substrait.Type.Struct()
                    {
                        Types = new List<Substrait.Type.SubstraitBaseType>()
                        {
                            new Substrait.Type.AnyType(),
                            new Substrait.Type.AnyType()
                        }
                    }
                }
            };
            var aggregateRelation = new AggregateRelation()
            {
                Input = readRelation,
                Groupings = new List<AggregateGrouping>()
                {
                    new AggregateGrouping()
                    {
                        GroupingExpressions = new List<Expression>() { CreateFieldReference(0) }
                    },
                    new AggregateGrouping()
                    {
                        GroupingExpressions = new List<Expression>() { CreateFieldReference(1) }
                    }
                },
                Measures = new List<AggregateMeasure>()
            };
            var plan = new Substrait.Plan()
            {
                Relations = new List<Relation>()
                {
                    new RootRelation()
                    {
                        Input = aggregateRelation,
                        Names = new List<string>() { "col1", "col2", "groupingid" }
                    }
                }
            };

            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings());

            var relations = CollectRelations(plan);
            Assert.Single(relations.OfType<AggregateRelation>());
            Assert.Empty(relations.OfType<SetRelation>());
        }

        [Fact]
        public void SettingDisablesRewrite()
        {
            var plan = BuildPlan(@"
                INSERT INTO output
                SELECT col1, col2 FROM table1 GROUP BY col1, col2");
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings() { GroupByToDistinct = false });

            var relations = CollectRelations(plan);

            Assert.Single(relations.OfType<AggregateRelation>());
            Assert.Empty(relations.OfType<SetRelation>());
        }

        /// <summary>
        /// The distinct is partitioned the same way the aggregate was.
        /// </summary>
        [Fact]
        public void ParallelizedPlanPartitionsTheDistinct()
        {
            var plan = BuildPlan(@"
                INSERT INTO output
                SELECT col1, col2 FROM table1 GROUP BY col1, col2");
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings() { Parallelization = 2 });

            var relations = CollectRelations(plan);

            Assert.Empty(relations.OfType<AggregateRelation>());

            var sets = relations.OfType<SetRelation>().ToList();
            Assert.Equal(2, sets.Count(x => x.Operation == SetOperation.UnionDistinct));
            var combine = Assert.Single(sets.Where(x => x.Operation == SetOperation.UnionAll));
            Assert.Equal(2, combine.Inputs.Count);

            // The rows are scattered on both columns, the key is the whole row
            var exchange = Assert.Single(relations.OfType<ExchangeRelation>());
            Assert.Equal(2, exchange.PartitionCount);
            var scatter = Assert.IsType<ScatterExchangeKind>(exchange.ExchangeKind);
            Assert.Equal(2, scatter.Fields.Count);
        }

        /// <summary>
        /// Every input of a multi input set operation gets its own exchange.
        /// </summary>
        [Fact]
        public void ParallelizedExceptScattersBothInputs()
        {
            var plan = BuildPlan(@"
                INSERT INTO output
                SELECT col1 FROM table1
                EXCEPT DISTINCT
                SELECT col2 FROM table1");
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings() { Parallelization = 3 });

            var relations = CollectRelations(plan);

            var sets = relations.OfType<SetRelation>().ToList();
            Assert.Equal(3, sets.Count(x => x.Operation == SetOperation.MinusPrimary));
            var combine = Assert.Single(sets.Where(x => x.Operation == SetOperation.UnionAll));
            Assert.Equal(3, combine.Inputs.Count);

            // One exchange per input, both scattered on the same single column
            var exchanges = relations.OfType<ExchangeRelation>().ToList();
            Assert.Equal(2, exchanges.Count);
            foreach (var exchange in exchanges)
            {
                Assert.Equal(3, exchange.PartitionCount);
                var scatter = Assert.IsType<ScatterExchangeKind>(exchange.ExchangeKind);
                var field = Assert.Single(scatter.Fields);
                var segment = Assert.IsType<StructReferenceSegment>(Assert.IsType<DirectFieldReference>(field).ReferenceSegment);
                Assert.Equal(0, segment.Field);
            }
        }

        private static DirectFieldReference CreateFieldReference(int field)
        {
            return new DirectFieldReference()
            {
                ReferenceSegment = new StructReferenceSegment() { Field = field }
            };
        }
    }
}

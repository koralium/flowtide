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
    public class TumblingWindowJoinKeyTests
    {
        private const string TumblingBandJoinSql = @"
            CREATE TABLE bid (auction any, bidder any, price any, channel any, url any, date_time any, extra any);

            INSERT INTO output
            SELECT B.auction, B.price, B.date_time
            FROM bid B
            JOIN (
              SELECT
                MAX(B1.price) AS maxprice,
                window_start,
                window_end
              FROM bid B1
              INNER JOIN tumbling_window(B1.date_time, 10, 'SECOND') wind
              GROUP BY window_start, window_end
            ) B1
            ON B.price = B1.maxprice AND
                B.date_time >= B1.window_start AND
                B.date_time < B1.window_end;";

        private const string HoppingBandJoinSql = @"
            CREATE TABLE bid (auction any, bidder any, price any, channel any, url any, date_time any, extra any);

            INSERT INTO output
            SELECT B.auction, B.price, B.date_time
            FROM bid B
            JOIN (
              SELECT
                MAX(B1.price) AS maxprice,
                window_start,
                window_end
              FROM bid B1
              INNER JOIN hopping_window(B1.date_time, 10, 'SECOND', 10, 'SECOND') wind
              GROUP BY window_start, window_end
            ) B1
            ON B.price = B1.maxprice AND
                B.date_time >= B1.window_start AND
                B.date_time < B1.window_end;";

        private static Substrait.Plan BuildPlan(string sql)
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(sql);
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
        public void TumblingBandBecomesLeadingBucketEquality()
        {
            var plan = BuildPlan(TumblingBandJoinSql);
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings());

            var mergeJoin = CollectRelations(plan).OfType<MergeJoinRelation>().Single();

            // The band collapsed into a second equality, no range keys remain
            Assert.Equal(2, mergeJoin.LeftKeys.Count);
            for (int i = 0; i < mergeJoin.LeftKeys.Count; i++)
            {
                Assert.Equal(JoinComparisonType.Equal, mergeJoin.GetComparisonType(i));
            }

            // The left input computes the window bucket
            var bucketProjects = CollectRelations(mergeJoin.Left)
                .OfType<ProjectRelation>()
                .SelectMany(p => p.Expressions)
                .OfType<ScalarFunction>()
                .Where(f => f.ExtensionName == FunctionsDatetime.TumblingWindowStart)
                .ToList();
            var bucket = Assert.Single(bucketProjects);
            Assert.Equal(FunctionsDatetime.Uri, bucket.ExtensionUri);

            // 10 seconds resolved to ticks at rewrite time
            var sizeLiteral = Assert.IsType<Substrait.Expressions.Literals.NumericLiteral>(bucket.Arguments[1]);
            Assert.Equal(TimeSpan.TicksPerSecond * 10, (long)sizeLiteral.Value);

            // The bucket key leads so the join trees sort window first
            var firstLeftKey = Assert.IsType<DirectFieldReference>(mergeJoin.LeftKeys[0]);
            var firstLeftSegment = Assert.IsType<StructReferenceSegment>(firstLeftKey.ReferenceSegment);
            var directLeftProject = Assert.IsType<ProjectRelation>(mergeJoin.Left);
            Assert.Equal(directLeftProject.Input.OutputLength, firstLeftSegment.Field);

            // Output width is unchanged, the bucket column is hidden
            Assert.True(mergeJoin.EmitSet);
        }

        [Fact]
        public void HoppingBandIsNotRewritten()
        {
            var plan = BuildPlan(HoppingBandJoinSql);
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings());

            var mergeJoin = CollectRelations(plan).OfType<MergeJoinRelation>().Single();

            // A hopping window band must keep its range key, a timestamp belongs
            // to more than one window so a single bucket equality would be wrong
            bool hasRangeKey = false;
            for (int i = 0; i < mergeJoin.LeftKeys.Count; i++)
            {
                if (mergeJoin.GetComparisonType(i) != JoinComparisonType.Equal)
                {
                    hasRangeKey = true;
                }
            }
            Assert.True(hasRangeKey);
        }

        [Fact]
        public void SettingDisablesRewrite()
        {
            var plan = BuildPlan(TumblingBandJoinSql);
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings() { WindowJoinKeyOptimization = false });

            var mergeJoin = CollectRelations(plan).OfType<MergeJoinRelation>().Single();

            bool hasRangeKey = false;
            for (int i = 0; i < mergeJoin.LeftKeys.Count; i++)
            {
                if (mergeJoin.GetComparisonType(i) != JoinComparisonType.Equal)
                {
                    hasRangeKey = true;
                }
            }
            Assert.True(hasRangeKey);
        }
    }
}

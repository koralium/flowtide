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
using FlowtideDotNet.Core.Optimizer.CommonSubPlan;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.OptimizerTests
{
    public class CommonSubPlanTests
    {
        private const string NexmarkQ5Sql = @"
            CREATE TABLE bid (auction any, date_time any);

            INSERT INTO output
            SELECT AuctionBids.auction, AuctionBids.num
            FROM(
               SELECT auction, count(*) AS num, window_start AS starttime, window_end AS endtime
               FROM bid
               INNER JOIN hopping_window(date_time, 2, 'SECOND', 10, 'SECOND') wind
               GROUP BY auction, window_start, window_end
            ) AS AuctionBids
            JOIN(
              SELECT max(CountBids.num) AS maxn, CountBids.starttime, CountBids.endtime
              FROM(
                SELECT count(*) AS num, window_start AS starttime, window_end AS endtime
                FROM bid
                INNER JOIN hopping_window(date_time, 2, 'SECOND', 10, 'SECOND') wind
                GROUP BY auction, window_start, window_end
              ) AS CountBids
              GROUP BY CountBids.starttime, CountBids.endtime
            ) AS MaxBids
            ON AuctionBids.starttime = MaxBids.starttime AND
                AuctionBids.endtime = MaxBids.endtime AND
                AuctionBids.num >= MaxBids.maxn;";

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
                    case SubStreamRootRelation r: Collect(r.Input); break;
                    case TableFunctionRelation r when r.Input != null: Collect(r.Input); break;
                    case ConsistentPartitionWindowRelation r: Collect(r.Input); break;
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
        public void DuplicateAggregateSubtreeIsShared()
        {
            var plan = BuildPlan(NexmarkQ5Sql);
            plan = CommonSubPlanOptimizer.Optimize(plan);

            Assert.Equal(2, plan.Relations.Count);

            // The shared subtree is the count aggregate over the hopping window
            var definition = Assert.IsType<AggregateRelation>(plan.Relations[1]);
            var tableFunction = Assert.IsType<TableFunctionRelation>(definition.Input);
            Assert.IsType<ReadRelation>(tableFunction.Input);

            var references = CollectRelations(plan.Relations[0]).OfType<ReferenceRelation>().ToList();
            Assert.Equal(2, references.Count);
            Assert.All(references, r => Assert.Equal(1, r.RelationId));
            Assert.All(references, r => Assert.Equal(definition.OutputLength, r.ReferenceOutputLength));

            // Only the shared aggregate reads the table
            Assert.Single(CollectRelations(plan).OfType<ReadRelation>());
        }

        [Fact]
        public void FullOptimizerSharesCountAggregate()
        {
            var plan = BuildPlan(NexmarkQ5Sql);
            plan = PlanOptimizer.Optimize(plan);

            var relations = CollectRelations(plan);
            Assert.Single(relations.OfType<ReadRelation>());
            Assert.Single(relations.OfType<TableFunctionRelation>());

            // One count aggregate with three group expressions and one max aggregate
            var aggregates = relations.OfType<AggregateRelation>().ToList();
            Assert.Equal(2, aggregates.Count);
            Assert.Single(aggregates.Where(a => a.Groupings![0].GroupingExpressions.Count == 3));
            Assert.Single(aggregates.Where(a => a.Groupings![0].GroupingExpressions.Count == 2));
        }

        [Fact]
        public void NoDuplicateSubtreesLeavesPlanUnchanged()
        {
            var sql = @"
                CREATE TABLE users (userkey any, name any);
                INSERT INTO output SELECT userkey, count(*) FROM users GROUP BY userkey;";
            var plan = BuildPlan(sql);
            var expected = BuildPlan(sql);

            plan = CommonSubPlanOptimizer.Optimize(plan);

            Assert.Equal(expected.Relations, plan.Relations);
        }

        [Fact]
        public void DifferentAggregatesAreNotShared()
        {
            var sql = @"
                CREATE TABLE users (userkey any, name any);
                INSERT INTO output
                SELECT a.userkey FROM (SELECT userkey, count(*) AS c FROM users GROUP BY userkey) a
                JOIN (SELECT name, count(*) AS c FROM users GROUP BY name) b ON a.c = b.c;";
            var plan = BuildPlan(sql);
            plan = CommonSubPlanOptimizer.Optimize(plan);

            Assert.Single(plan.Relations);
            Assert.Empty(CollectRelations(plan).OfType<ReferenceRelation>());
        }

        [Fact]
        public void DifferentPushedDownFiltersPreventSharing()
        {
            // The filters are pushed into each read before common sub plans run
            var sql = @"
                CREATE TABLE users (userkey any, name any);
                INSERT INTO output
                SELECT a.userkey FROM (SELECT userkey, count(*) AS c FROM users WHERE name = 'a' GROUP BY userkey) a
                JOIN (SELECT userkey, count(*) AS c FROM users WHERE name = 'b' GROUP BY userkey) b ON a.userkey = b.userkey;";
            var plan = BuildPlan(sql);
            plan = PlanOptimizer.Optimize(plan);

            Assert.Empty(CollectRelations(plan).OfType<ReferenceRelation>());
            Assert.Equal(2, CollectRelations(plan).OfType<ReadRelation>().Count());
        }

        [Fact]
        public void IdenticalFilteredSubtreesAreShared()
        {
            var sql = @"
                CREATE TABLE users (userkey any, name any);
                INSERT INTO output
                SELECT a.userkey FROM (SELECT userkey, count(*) AS c FROM users WHERE name = 'a' GROUP BY userkey) a
                JOIN (SELECT userkey, count(*) AS c FROM users WHERE name = 'a' GROUP BY userkey) b ON a.userkey = b.userkey;";
            var plan = BuildPlan(sql);
            plan = PlanOptimizer.Optimize(plan);

            Assert.Equal(2, CollectRelations(plan).OfType<ReferenceRelation>().Count());
            Assert.Single(CollectRelations(plan).OfType<ReadRelation>());
        }

        [Fact]
        public void IdenticalInsertsShareTheWholeQuery()
        {
            var sql = @"
                CREATE TABLE users (userkey any, name any);
                INSERT INTO output1 SELECT userkey, count(*) FROM users GROUP BY userkey;
                INSERT INTO output2 SELECT userkey, count(*) FROM users GROUP BY userkey;";
            var plan = BuildPlan(sql);
            plan = CommonSubPlanOptimizer.Optimize(plan);

            Assert.Equal(3, plan.Relations.Count);
            var firstWrite = Assert.IsType<WriteRelation>(plan.Relations[0]);
            var secondWrite = Assert.IsType<WriteRelation>(plan.Relations[1]);
            var firstReference = Assert.IsType<ReferenceRelation>(firstWrite.Input);
            var secondReference = Assert.IsType<ReferenceRelation>(secondWrite.Input);
            Assert.Equal(2, firstReference.RelationId);
            Assert.Equal(2, secondReference.RelationId);
        }

        [Fact]
        public void InnerSubtreeWithExtraUsageIsSharedSeparately()
        {
            // The aggregate subtree is duplicated and its table function input has a third usage
            var sql = @"
                CREATE TABLE bid (auction any, date_time any);
                INSERT INTO output
                SELECT a.auction FROM (
                    SELECT auction, count(*) AS num, window_start AS ws FROM bid
                    INNER JOIN hopping_window(date_time, 2, 'SECOND', 10, 'SECOND')
                    GROUP BY auction, window_start
                ) a
                JOIN (
                    SELECT auction, count(*) AS num, window_start AS ws FROM bid
                    INNER JOIN hopping_window(date_time, 2, 'SECOND', 10, 'SECOND')
                    GROUP BY auction, window_start
                ) b ON a.auction = b.auction
                JOIN (
                    SELECT auction, window_start AS ws FROM bid
                    INNER JOIN hopping_window(date_time, 2, 'SECOND', 10, 'SECOND')
                ) c ON a.auction = c.auction;";
            var plan = BuildPlan(sql);
            plan = CommonSubPlanOptimizer.Optimize(plan);

            // Main relation, shared aggregate and shared table function
            Assert.Equal(3, plan.Relations.Count);
            var projectDefinition = Assert.IsType<ProjectRelation>(plan.Relations[1]);
            var aggregateDefinition = Assert.IsType<AggregateRelation>(projectDefinition.Input);
            var innerReference = Assert.IsType<ReferenceRelation>(aggregateDefinition.Input);
            Assert.Equal(2, innerReference.RelationId);
            Assert.IsType<TableFunctionRelation>(plan.Relations[2]);

            var references = CollectRelations(plan.Relations[0]).OfType<ReferenceRelation>().ToList();
            Assert.Equal(2, references.Count(r => r.RelationId == 1));
            Assert.Single(references.Where(r => r.RelationId == 2));
            Assert.Single(CollectRelations(plan).OfType<ReadRelation>());
        }

        [Fact]
        public void DuplicateWindowSubtreeIsShared()
        {
            var sql = @"
                CREATE TABLE users (userkey any, companyid any, doublevalue any);
                INSERT INTO output
                SELECT a.userkey FROM (
                    SELECT userkey, SUM(doublevalue) OVER (PARTITION BY companyid ORDER BY userkey) AS s FROM users
                ) a
                JOIN (
                    SELECT userkey, SUM(doublevalue) OVER (PARTITION BY companyid ORDER BY userkey) AS s FROM users
                ) b ON a.s = b.s;";
            var plan = BuildPlan(sql);
            plan = CommonSubPlanOptimizer.Optimize(plan);

            Assert.Equal(2, plan.Relations.Count);
            var references = CollectRelations(plan.Relations[0]).OfType<ReferenceRelation>().ToList();
            Assert.Equal(2, references.Count);
            Assert.All(references, r => Assert.Equal(1, r.RelationId));
            Assert.Single(CollectRelations(plan).OfType<ConsistentPartitionWindowRelation>());
        }

        [Fact]
        public void WindowsWithDifferentBoundsAreNotShared()
        {
            var sql = @"
                CREATE TABLE users (userkey any, companyid any, doublevalue any);
                INSERT INTO output
                SELECT a.userkey FROM (
                    SELECT userkey, SUM(doublevalue) OVER (PARTITION BY companyid ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM users
                ) a
                JOIN (
                    SELECT userkey, SUM(doublevalue) OVER (PARTITION BY companyid ORDER BY userkey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM users
                ) b ON a.s = b.s;";
            var plan = BuildPlan(sql);
            plan = CommonSubPlanOptimizer.Optimize(plan);

            Assert.Single(plan.Relations);
            Assert.Empty(CollectRelations(plan).OfType<ReferenceRelation>());
            Assert.Equal(2, CollectRelations(plan).OfType<ConsistentPartitionWindowRelation>().Count());
        }

        [Fact]
        public void WindowsOverDifferentInputsAreNotShared()
        {
            var sql = @"
                CREATE TABLE users (userkey any, companyid any, doublevalue any);
                CREATE TABLE others (userkey any, companyid any, doublevalue any);
                INSERT INTO output
                SELECT a.userkey FROM (
                    SELECT userkey, SUM(doublevalue) OVER (PARTITION BY companyid ORDER BY userkey) AS s FROM users
                ) a
                JOIN (
                    SELECT userkey, SUM(doublevalue) OVER (PARTITION BY companyid ORDER BY userkey) AS s FROM others
                ) b ON a.s = b.s;";
            var plan = BuildPlan(sql);
            plan = CommonSubPlanOptimizer.Optimize(plan);

            Assert.Single(plan.Relations);
            Assert.Empty(CollectRelations(plan).OfType<ReferenceRelation>());
            Assert.Equal(2, CollectRelations(plan).OfType<ConsistentPartitionWindowRelation>().Count());
        }

        [Fact]
        public void SubstreamPlansAreSkipped()
        {
            var plan = BuildPlan(NexmarkQ5Sql);
            plan.Relations[0] = new SubStreamRootRelation()
            {
                Name = "substream_0",
                Input = plan.Relations[0]
            };
            var relationCount = plan.Relations.Count;

            plan = CommonSubPlanOptimizer.Optimize(plan);

            Assert.Equal(relationCount, plan.Relations.Count);
            Assert.Empty(CollectRelations(plan).OfType<ReferenceRelation>());
        }

        [Fact]
        public void OptimizerCanBeDisabled()
        {
            var plan = BuildPlan(NexmarkQ5Sql);
            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings()
            {
                FindCommonSubPlans = false
            });

            Assert.Empty(CollectRelations(plan).OfType<ReferenceRelation>());
            Assert.Equal(2, CollectRelations(plan).OfType<ReadRelation>().Count());
        }
    }
}

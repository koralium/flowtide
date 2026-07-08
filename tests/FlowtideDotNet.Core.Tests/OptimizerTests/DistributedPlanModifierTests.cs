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
using FlowtideDotNet.Core.Optimizer.DistributedMode;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.OptimizerTests
{
    public class DistributedPlanModifierTests
    {
        private const string DistributedViewSql = @"
            CREATE TABLE users (userkey any);

            CREATE VIEW read_users WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
            SELECT userkey FROM users;

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 0);

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 1);
            ";

        private static Substrait.Plan BuildPlan(string sql)
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(sql);
            return sqlPlanBuilder.GetPlan();
        }

        [Fact]
        public void SinkRootsAreAssignedRoundRobinAndCrossSubstreamTargetsAreConverted()
        {
            var plan = BuildPlan(DistributedViewSql);
            plan = DistributedPlanModifier.ToDistributedPlan(plan, new DistributedPlanOptions()
            {
                SubstreamCount = 2
            });

            Assert.Equal(3, plan.Relations.Count);

            // The exchange should be placed in the substream of its first consumer
            var exchangeRoot = Assert.IsType<SubStreamRootRelation>(plan.Relations[0]);
            Assert.Equal("substream_0", exchangeRoot.Name);
            var exchangeRelation = Assert.IsType<ExchangeRelation>(exchangeRoot.Input);
            Assert.Equal(2, exchangeRelation.Targets.Count);

            // First consumer is in the same substream, keeps its standard output target
            Assert.IsType<StandardOutputExchangeTarget>(exchangeRelation.Targets[0]);
            // Second consumer is in another substream, the target should be converted
            var substreamTarget = Assert.IsType<SubstreamExchangeTarget>(exchangeRelation.Targets[1]);
            Assert.Equal("substream_1", substreamTarget.SubstreamName);

            var firstSink = Assert.IsType<SubStreamRootRelation>(plan.Relations[1]);
            Assert.Equal("substream_0", firstSink.Name);
            var firstSinkReferences = CollectReferences(firstSink);
            var standardReference = Assert.Single(firstSinkReferences.OfType<StandardOutputExchangeReferenceRelation>());
            Assert.Equal(0, standardReference.RelationId);
            Assert.Equal(0, standardReference.TargetId);

            var secondSink = Assert.IsType<SubStreamRootRelation>(plan.Relations[2]);
            Assert.Equal("substream_1", secondSink.Name);
            var secondSinkReferences = CollectReferences(secondSink);
            var substreamReference = Assert.Single(secondSinkReferences.OfType<SubstreamExchangeReferenceRelation>());
            // The reference points to the substream where the exchange runs
            Assert.Equal("substream_0", substreamReference.SubStreamName);
            Assert.Equal(substreamTarget.ExchangeTargetId, substreamReference.ExchangeTargetId);
        }

        [Fact]
        public void SingleSubstreamKeepsAllStandardOutputTargets()
        {
            var plan = BuildPlan(DistributedViewSql);
            plan = DistributedPlanModifier.ToDistributedPlan(plan, new DistributedPlanOptions()
            {
                SubstreamCount = 1
            });

            foreach (var relation in plan.Relations)
            {
                var root = Assert.IsType<SubStreamRootRelation>(relation);
                Assert.Equal("substream_0", root.Name);
            }
            var exchangeRelation = Assert.IsType<ExchangeRelation>(((SubStreamRootRelation)plan.Relations[0]).Input);
            Assert.All(exchangeRelation.Targets, target => Assert.IsType<StandardOutputExchangeTarget>(target));
        }

        [Fact]
        public void AlreadyDistributedPlanThrows()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
            CREATE TABLE users (userkey any);

            SUBSTREAM sub1;

            INSERT INTO output SELECT userkey FROM users;
            ");
            var plan = sqlPlanBuilder.GetPlan();

            Assert.Throws<InvalidOperationException>(() => DistributedPlanModifier.ToDistributedPlan(plan, new DistributedPlanOptions()
            {
                SubstreamCount = 2
            }));
        }

        [Fact]
        public void InvalidSubstreamCountThrows()
        {
            var plan = BuildPlan(DistributedViewSql);
            Assert.Throws<ArgumentException>(() => DistributedPlanModifier.ToDistributedPlan(plan, new DistributedPlanOptions()
            {
                SubstreamCount = 0
            }));
        }

        /// <summary>
        /// A completely normal plan without any distribution hints should be distributed,
        /// the join is partitioned with one copy per substream.
        /// </summary>
        [Fact]
        public void NormalJoinPlanIsDistributedAcrossSubstreams()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
            CREATE TABLE users (userkey any);
            CREATE TABLE orders (orderkey any, userkey any);

            INSERT INTO output
            SELECT u.userkey FROM users u
            INNER JOIN orders o ON u.userkey = o.userkey;
            ");
            var plan = sqlPlanBuilder.GetPlan();

            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings()
            {
                DistributedPlanOptions = new DistributedPlanOptions()
                {
                    SubstreamCount = 2
                }
            });

            // Sink root, left scatter exchange, right scatter exchange and one gather exchange
            Assert.Equal(4, plan.Relations.Count);
            Assert.All(plan.Relations, relation => Assert.IsType<SubStreamRootRelation>(relation));

            var sinkRoot = (SubStreamRootRelation)plan.Relations[0];
            Assert.Equal("substream_0", sinkRoot.Name);

            // Scatter exchanges rotate between the substreams and partition on the join keys
            var leftExchangeRoot = (SubStreamRootRelation)plan.Relations[1];
            Assert.Equal("substream_0", leftExchangeRoot.Name);
            var leftExchange = Assert.IsType<ExchangeRelation>(leftExchangeRoot.Input);
            Assert.Equal(2, leftExchange.PartitionCount);
            Assert.IsType<StandardOutputExchangeTarget>(leftExchange.Targets[0]);
            var leftSubstreamTarget = Assert.IsType<SubstreamExchangeTarget>(leftExchange.Targets[1]);
            Assert.Equal("substream_1", leftSubstreamTarget.SubstreamName);

            var rightExchangeRoot = (SubStreamRootRelation)plan.Relations[2];
            Assert.Equal("substream_1", rightExchangeRoot.Name);
            var rightExchange = Assert.IsType<ExchangeRelation>(rightExchangeRoot.Input);
            var rightSubstreamTarget = Assert.IsType<SubstreamExchangeTarget>(rightExchange.Targets[0]);
            Assert.Equal("substream_0", rightSubstreamTarget.SubstreamName);
            Assert.IsType<StandardOutputExchangeTarget>(rightExchange.Targets[1]);

            // The join copy for the other substream sends its result back to the sink substream
            var gatherRoot = (SubStreamRootRelation)plan.Relations[3];
            Assert.Equal("substream_1", gatherRoot.Name);
            var gatherExchange = Assert.IsType<ExchangeRelation>(gatherRoot.Input);
            Assert.Equal(1, gatherExchange.PartitionCount);
            var gatherTarget = Assert.IsType<SubstreamExchangeTarget>(Assert.Single(gatherExchange.Targets));
            Assert.Equal("substream_0", gatherTarget.SubstreamName);

            // The sink tree has the local join copy, it reads the left side locally and the
            // right side and the gathered copy result through cross substream communication
            var sinkReferences = CollectReferences(sinkRoot);
            Assert.Single(sinkReferences.OfType<StandardOutputExchangeReferenceRelation>());
            Assert.Equal(2, sinkReferences.OfType<SubstreamExchangeReferenceRelation>().Count());

            // The hoisted join copy reads the right side locally and the left side remotely
            var gatherReferences = CollectReferences(gatherRoot);
            Assert.Single(gatherReferences.OfType<StandardOutputExchangeReferenceRelation>());
            Assert.Single(gatherReferences.OfType<SubstreamExchangeReferenceRelation>());
        }

        /// <summary>
        /// An aggregate that groups on the join key should stay in the partition lanes,
        /// every group is fully contained in one lane so no extra shuffle is needed.
        /// The pipeline is join -> aggregate inside each lane instead of
        /// join -> union -> aggregate in a single substream.
        /// </summary>
        [Fact]
        public void CoPartitionedAggregateStaysInLane()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
            CREATE TABLE users (userkey any);
            CREATE TABLE orders (orderkey any, userkey any);

            INSERT INTO output
            SELECT o.userkey, count(*) FROM orders o
            INNER JOIN users u ON o.userkey = u.userkey
            GROUP BY o.userkey;
            ");
            var plan = sqlPlanBuilder.GetPlan();

            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings()
            {
                DistributedPlanOptions = new DistributedPlanOptions()
                {
                    SubstreamCount = 2
                }
            });

            // Sink root, two join scatter exchanges and one gather exchange.
            // There must be no separate scatter exchange for the aggregate.
            Assert.Equal(4, plan.Relations.Count);

            // The hoisted lane must contain the aggregate above the join, inside the gather
            // exchange, so the aggregation runs in that substream before data is gathered.
            var gatherRoot = plan.Relations
                .OfType<SubStreamRootRelation>()
                .Single(x => x.Input is ExchangeRelation exchange && exchange.PartitionCount == 1);
            var gatherExchange = (ExchangeRelation)gatherRoot.Input;
            Assert.True(ContainsRelation<AggregateRelation>(gatherExchange.Input), "The aggregate should run inside the lane before the gather exchange");
            Assert.True(ContainsRelation<MergeJoinRelation>(gatherExchange.Input), "The join partition should run inside the lane");

            // The local lane in the sink tree should also aggregate before the union
            var sinkRoot = (SubStreamRootRelation)plan.Relations[0];
            var union = FindRelation<SetRelation>(sinkRoot);
            Assert.NotNull(union);
            Assert.Contains(union!.Inputs, input => ContainsRelation<AggregateRelation>(input));
        }

        /// <summary>
        /// An aggregate that groups on something other than the join key must not be pushed
        /// into the join lanes, its groups span multiple join partitions. It gets its own
        /// scatter exchange partitioned on the grouping columns instead.
        /// </summary>
        [Fact]
        public void NonCoPartitionedAggregateGetsItsOwnScatter()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.Sql(@"
            CREATE TABLE users (userkey any);
            CREATE TABLE orders (orderkey any, userkey any);

            INSERT INTO output
            SELECT o.orderkey, count(*) FROM orders o
            INNER JOIN users u ON o.userkey = u.userkey
            GROUP BY o.orderkey;
            ");
            var plan = sqlPlanBuilder.GetPlan();

            plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings()
            {
                DistributedPlanOptions = new DistributedPlanOptions()
                {
                    SubstreamCount = 2
                }
            });

            // Sink root, the aggregates scatter exchange and one gather exchange.
            // The join runs inside the aggregates scatter exchange input.
            Assert.Equal(3, plan.Relations.Count);

            var scatterRoot = plan.Relations
                .OfType<SubStreamRootRelation>()
                .Single(x => x.Input is ExchangeRelation exchange && exchange.PartitionCount == 2);
            var scatterExchange = (ExchangeRelation)scatterRoot.Input;
            Assert.True(ContainsRelation<MergeJoinRelation>(scatterExchange.Input), "The join should run inside the aggregate scatter exchange input");

            // The hoisted lane aggregates its partition, without a join copy
            var gatherRoot = plan.Relations
                .OfType<SubStreamRootRelation>()
                .Single(x => x.Input is ExchangeRelation exchange && exchange.PartitionCount == 1);
            var gatherExchange = (ExchangeRelation)gatherRoot.Input;
            Assert.True(ContainsRelation<AggregateRelation>(gatherExchange.Input), "The aggregate partition should run inside the lane");
            Assert.False(ContainsRelation<MergeJoinRelation>(gatherExchange.Input), "The join must not be copied into the aggregate lanes");
        }

        /// <summary>
        /// A recursive query reached only through a reference is still not split across substreams:
        /// the iteration is hoisted to a global relation and built whole in every substream.
        /// </summary>
        [Theory]
        [InlineData(@"
            CREATE TABLE users (userkey any, managerkey any);
            CREATE TABLE orders (orderkey any, userkey any);

            CREATE VIEW mgr AS
            WITH manager_cte AS (
                SELECT userkey, managerkey FROM users WHERE managerkey is null
                UNION ALL
                SELECT u.userkey, u.managerkey FROM users u INNER JOIN manager_cte c ON c.userkey = u.managerkey
            )
            SELECT userkey FROM manager_cte;

            INSERT INTO output
            SELECT m.userkey, count(*) FROM mgr m INNER JOIN orders o ON m.userkey = o.userkey GROUP BY m.userkey;
        ")]
        [InlineData(@"
            CREATE TABLE users (userkey any, managerkey any);

            CREATE VIEW mgr WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
            WITH manager_cte AS (
                SELECT userkey, managerkey FROM users WHERE managerkey is null
                UNION ALL
                SELECT u.userkey, u.managerkey FROM users u INNER JOIN manager_cte c ON c.userkey = u.managerkey
            )
            SELECT userkey, managerkey FROM manager_cte;

            INSERT INTO output SELECT userkey FROM mgr WITH (PARTITION_ID = 0);
            INSERT INTO output SELECT userkey FROM mgr WITH (PARTITION_ID = 1);
        ")]
        public void ReferencedIterationIsNotSplitAcrossSubstreams(string sql)
        {
            var plan = PlanOptimizer.Optimize(BuildPlan(sql), new PlanOptimizerSettings()
            {
                DistributedPlanOptions = new DistributedPlanOptions() { SubstreamCount = 2 }
            });

            var iterationRoot = Assert.Single(plan.Relations.Where(ContainsRelation<IterationRelation>));

            // Global (no substream wrapper), so built whole in every substream, and the loop
            // never crosses a substream boundary.
            Assert.IsNotType<SubStreamRootRelation>(iterationRoot);
            Assert.False(ContainsRelation<SubstreamExchangeReferenceRelation>(iterationRoot));
        }

        private static bool ContainsRelation<T>(Relation root) where T : Relation
        {
            return FindRelation<T>(root) != null;
        }

        private static T? FindRelation<T>(Relation root) where T : Relation
        {
            var finder = new RelationFinder<T>();
            finder.Visit(root, null!);
            return finder.Found;
        }

        private sealed class RelationFinder<T> : OptimizerBaseVisitor where T : Relation
        {
            public T? Found { get; private set; }

            public override Relation Visit(Relation relation, object state)
            {
                if (Found == null && relation is T match)
                {
                    Found = match;
                }
                return base.Visit(relation, state);
            }
        }

        [Fact]
        public void CustomSubstreamNamesAreUsed()
        {
            var plan = BuildPlan(DistributedViewSql);
            plan = DistributedPlanModifier.ToDistributedPlan(plan, new DistributedPlanOptions()
            {
                SubstreamCount = 2,
                SubstreamNameGenerator = i => $"node_{i}"
            });

            var names = plan.Relations
                .OfType<SubStreamRootRelation>()
                .Select(x => x.Name)
                .Distinct()
                .OrderBy(x => x)
                .ToList();
            Assert.Equal(new List<string>() { "node_0", "node_1" }, names);
        }

        /// <summary>
        /// Iterations run a loop that must stay inside one stream, a plan with a recursive
        /// query is not partitioned across substreams.
        /// </summary>
        [Fact]
        public void PlanWithIterationIsNotPartitioned()
        {
            var plan = BuildPlan(@"
            CREATE TABLE users (userkey any, managerkey any);

            INSERT INTO output
            WITH manager_cte AS (
                SELECT userkey, managerkey FROM users WHERE managerkey is null
                UNION ALL
                SELECT u.userkey, u.managerkey FROM users u
                INNER JOIN manager_cte c ON c.userkey = u.managerkey
            )
            SELECT userkey FROM manager_cte;
            ");
            plan = DistributedPlanModifier.ToDistributedPlan(plan, new DistributedPlanOptions()
            {
                SubstreamCount = 2
            });

            // The sink tree stays whole in a single substream, nothing is exchanged between
            // substreams.
            foreach (var relation in plan.Relations)
            {
                if (relation is SubStreamRootRelation root)
                {
                    Assert.Equal("substream_0", root.Name);
                }
                Assert.Empty(CollectReferences(relation).OfType<SubstreamExchangeReferenceRelation>());
            }
        }

        /// <summary>
        /// A recursive CTE reached through a reference and then joined with another table must not
        /// be partitioned. The loop is hoisted into its own relation and reached only through a
        /// plain reference, so the referencing join looks partitionable. Partitioning it scatters
        /// the loop's reference and leaves the loop built globally in a substream that never
        /// consumes it, an orphaned loop that commits state during checkpointing and crashes.
        /// The whole query must stay in one substream.
        /// </summary>
        [Fact]
        public void RecursiveCteReachedThroughReferenceAndJoinedIsNotPartitioned()
        {
            var plan = PlanOptimizer.Optimize(BuildPlan(@"
                CREATE TABLE users (userkey any, managerkey any);
                CREATE TABLE orders (orderkey any, userkey any);
                INSERT INTO output
                WITH manager_cte AS (
                    SELECT userkey, managerkey FROM users WHERE managerkey is null
                    UNION ALL
                    SELECT u.userkey, u.managerkey FROM users u INNER JOIN manager_cte c ON c.userkey = u.managerkey
                )
                SELECT m.userkey FROM manager_cte m INNER JOIN orders o ON m.userkey = o.userkey;
            "), new PlanOptimizerSettings()
            {
                DistributedPlanOptions = new DistributedPlanOptions() { SubstreamCount = 2 }
            });

            // Everything lands in one substream, so the loop is never built in a substream that
            // does not consume it.
            var substreamNames = plan.Relations
                .OfType<SubStreamRootRelation>()
                .Select(x => x.Name)
                .Distinct()
                .ToList();
            Assert.Equal(new List<string>() { "substream_0" }, substreamNames);

            // Exactly one loop, and nothing is scattered across substreams.
            Assert.Single(plan.Relations.Where(ContainsRelation<IterationRelation>));
            foreach (var relation in plan.Relations)
            {
                Assert.Empty(CollectReferences(relation).OfType<SubstreamExchangeReferenceRelation>());
            }
        }

        private static List<Relation> CollectReferences(Relation relation)
        {
            var collector = new ReferenceCollectingVisitor();
            collector.Visit(relation, null!);
            return collector.References;
        }

        private sealed class ReferenceCollectingVisitor : OptimizerBaseVisitor
        {
            public List<Relation> References { get; } = new List<Relation>();

            public override Relation VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, object state)
            {
                References.Add(standardOutputExchangeReferenceRelation);
                return standardOutputExchangeReferenceRelation;
            }

            public override Relation VisitSubstreamExchangeReferenceRelation(SubstreamExchangeReferenceRelation substreamExchangeReferenceRelation, object state)
            {
                References.Add(substreamExchangeReferenceRelation);
                return substreamExchangeReferenceRelation;
            }
        }
    }
}

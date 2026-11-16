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

using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Tests.Failure;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    public class MultiJoinMarkClearingTests
    {
        private const string ThreeTableJoinSql = @"
            CREATE TABLE a (k any, v any);
            CREATE TABLE b (k any, j any, v any);
            CREATE TABLE c (j any, v any);

            INSERT INTO output
            SELECT ta.v, tb.v, tc.v
            FROM a ta
            INNER JOIN b tb ON ta.k = tb.k
            INNER JOIN c tc ON tb.j = tc.j;";

        private const string FourTableJoinSql = @"
            CREATE TABLE a (k any, v any);
            CREATE TABLE b (k any, j any, v any);
            CREATE TABLE c (j any, i any, v any);
            CREATE TABLE d (i any, v any);

            INSERT INTO output
            SELECT ta.v, tb.v, tc.v, td.v
            FROM a ta
            INNER JOIN b tb ON ta.k = tb.k
            INNER JOIN c tc ON tb.j = tc.j
            INNER JOIN d td ON tc.i = td.i;";

        private static Plan ParseSql(string sql)
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(sql);
            return builder.GetPlan();
        }

        private static IConnectorManager CreateConnectorManager()
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new TestIngressFactory("*"));
            connectorManager.AddSink(new FailureEgressFactory("*", new FailureEgressOptions()));
            return connectorManager;
        }

        [Fact]
        public void AMarkedPlanOptimizedWithTheSettingOffLosesItsMarks()
        {
            var marked = PlanOptimizer.Optimize(ParseSql(FourTableJoinSql), new PlanOptimizerSettings() { MultiJoin = true });
            var json = SubstraitSerializer.SerializeToJson(marked);

            var reoptimized = PlanOptimizer.Optimize(
                SubstraitDeserializer.DeserializeFromJson(json),
                new PlanOptimizerSettings() { MultiJoin = false, Parallelization = 4 });

            Assert.Equal(0, MultiJoinMarker.Count(reoptimized));
            Assert.True(reoptimized.Relations.Count > 1);
        }

        [Fact]
        public async Task AMarkedPlanDistributedByTheOptimizerBuildsItsSubstreams()
        {
            var marked = PlanOptimizer.Optimize(ParseSql(ThreeTableJoinSql), new PlanOptimizerSettings() { MultiJoin = true });
            var json = SubstraitSerializer.SerializeToJson(marked);
            var directory = Path.Combine(Path.GetTempPath(), $"flowtide_marks_opt_{Guid.NewGuid():N}");

            try
            {
                await using var stream = new DistributedStreamBuilder("dist_opt")
                    .AddPlan(() => SubstraitDeserializer.DeserializeFromJson(json), optimize: true, new PlanOptimizerSettings() { MultiJoin = true })
                    .DistributeAutomatically(2)
                    .AddConnectorManager((_, _) => CreateConnectorManager())
                    .WithStateOptionsFactory((_, substream) => new StateManagerOptions()
                    {
                        PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                        {
                            FileProvider = new LocalDiskProvider(Path.Combine(directory, substream))
                        })
                    })
                    .Build();
            }
            finally
            {
                if (Directory.Exists(directory))
                {
                    Directory.Delete(directory, true);
                }
            }
        }

        [Fact]
        public async Task AMarkedPlanDistributedWithoutTheOptimizerBuildsItsSubstreams()
        {
            var marked = PlanOptimizer.Optimize(ParseSql(ThreeTableJoinSql), new PlanOptimizerSettings() { MultiJoin = true });
            var json = SubstraitSerializer.SerializeToJson(marked);
            var directory = Path.Combine(Path.GetTempPath(), $"flowtide_marks_preopt_{Guid.NewGuid():N}");

            try
            {
                await using var stream = new DistributedStreamBuilder("dist_preopt")
                    .AddPlan(() => SubstraitDeserializer.DeserializeFromJson(json), optimize: false)
                    .DistributeAutomatically(2)
                    .AddConnectorManager((_, _) => CreateConnectorManager())
                    .WithStateOptionsFactory((_, substream) => new StateManagerOptions()
                    {
                        PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                        {
                            FileProvider = new LocalDiskProvider(Path.Combine(directory, substream))
                        })
                    })
                    .Build();
            }
            finally
            {
                if (Directory.Exists(directory))
                {
                    Directory.Delete(directory, true);
                }
            }
        }

        [Fact]
        public void TheReportRefusesAnInputCapBelowThreeForAPlanWithoutJoins()
        {
            var plan = PlanOptimizer.Optimize(ParseSql(@"
                CREATE TABLE a (k any);
                INSERT INTO output SELECT k FROM a;"));

            Assert.Throws<ArgumentOutOfRangeException>(() => MultiJoinReport.Create(plan, 2));
        }

        [Fact]
        public void AMarkedPlanOptimizedAgainStillSharesCommonJoinSubtrees()
        {
            const string sharedJoinSql = @"
                CREATE TABLE a (k any, v any);
                CREATE TABLE b (k any, j any, v any);
                CREATE TABLE c (j any, v any);
                CREATE TABLE d (j any, v any);

                INSERT INTO out1
                SELECT ta.v, tb.v, tc.v
                FROM a ta
                INNER JOIN b tb ON ta.k = tb.k
                INNER JOIN c tc ON tb.j = tc.j;

                INSERT INTO out2
                SELECT ta.v, tb.v, td.v
                FROM a ta
                INNER JOIN b tb ON ta.k = tb.k
                INNER JOIN d td ON tb.j = td.j;";

            var unmarked = PlanOptimizer.Optimize(
                ParseSql(sharedJoinSql),
                new PlanOptimizerSettings() { MultiJoin = false, FindCommonSubPlans = false });
            var marked = PlanOptimizer.Optimize(
                ParseSql(sharedJoinSql),
                new PlanOptimizerSettings() { MultiJoin = true, FindCommonSubPlans = false });

            var fromUnmarked = PlanOptimizer.Optimize(
                SubstraitDeserializer.DeserializeFromJson(SubstraitSerializer.SerializeToJson(unmarked)),
                new PlanOptimizerSettings() { MultiJoin = false });
            var fromMarked = PlanOptimizer.Optimize(
                SubstraitDeserializer.DeserializeFromJson(SubstraitSerializer.SerializeToJson(marked)),
                new PlanOptimizerSettings() { MultiJoin = false });

            // CommonSubPlanOptimizer extracts the shared (a JOIN b) subtree into an extra relation
            Assert.Equal(3, fromUnmarked.Relations.Count);
            Assert.Equal(fromUnmarked.Relations.Count, fromMarked.Relations.Count);
        }

        [Fact]
        public void AMarkedPlanWithoutTheColumnStoreFailsTheBuild()
        {
            var marked = PlanOptimizer.Optimize(ParseSql(ThreeTableJoinSql), new PlanOptimizerSettings() { MultiJoin = true });

            var error = Assert.Throws<NotSupportedException>(() => new FlowtideBuilder("row_store_multijoin")
                .AddPlan(marked, optimize: false)
                .AddConnectorManager(CreateConnectorManager())
                .WithStateOptions(new StateManagerOptions())
                .ColumnStore(false)
                .Build());
            Assert.Contains("A multi join needs the column store", error.Message);
        }
    }
}

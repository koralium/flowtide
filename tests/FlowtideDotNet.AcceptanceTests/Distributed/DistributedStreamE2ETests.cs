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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging;
using Serilog;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Hashing;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    /// <summary>
    /// End to end tests that run multiple substreams in the same process with the
    /// in process distributed stream host. The substreams exchange data and coordinate
    /// checkpoints with each other through the local communication hub.
    /// </summary>
    public class DistributedStreamE2ETests : IAsyncLifetime
    {
        private readonly MockDatabase _db;
        private readonly DatasetGenerator _generator;
        private DistributedFlowtideStream? _stream;

        public DistributedStreamE2ETests()
        {
            _db = new MockDatabase();
            _generator = new DatasetGenerator(_db);
        }

        public Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            if (_stream != null)
            {
                await _stream.DisposeAsync();
            }
        }

        private const string SqlWithExplicitSubstreams = @"
            SUBSTREAM sub1;

            CREATE VIEW read_users WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
            SELECT userkey FROM users;

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 0);

            SUBSTREAM sub2;

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 1);
            ";

        private const string SqlWithoutSubstreams = @"
            CREATE VIEW read_users WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
            SELECT userkey FROM users;

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 0);

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 1);
            ";

        [Fact]
        public async Task TwoSubstreamsExchangeDataAndCheckpoint()
        {
            await RunTwoPartitionTest(
                "e2e_explicit_substreams",
                SqlWithExplicitSubstreams,
                autoDistribute: false,
                partition0Substream: "sub1",
                partition1Substream: "sub2");
        }

        [Fact]
        public async Task AutomaticallyDistributedPlanExchangesDataAndCheckpoints()
        {
            await RunTwoPartitionTest(
                "e2e_auto_distribute",
                SqlWithoutSubstreams,
                autoDistribute: true,
                partition0Substream: "substream_0",
                partition1Substream: "substream_1");
        }

        /// <summary>
        /// A completely normal plan without any distribution hints is distributed automatically,
        /// the join runs with one partition copy per substream and the results are gathered
        /// back to the substream that runs the sink.
        /// Also verifies that updates and deletes flow through the partition lanes.
        /// </summary>
        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        public async Task NormalJoinPlanIsDistributedAcrossSubstreams(int substreamCount)
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var testName = $"e2e_normal_join_{substreamCount}";

            _stream = new DistributedStreamBuilder(testName)
                .AddPlan(() =>
                {
                    var sqlPlanBuilder = new SqlPlanBuilder();
                    sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                    sqlPlanBuilder.Sql(@"
                    INSERT INTO output
                    SELECT u.userkey FROM users u
                    INNER JOIN orders o ON u.userkey = o.userkey;
                    ");
                    return sqlPlanBuilder.GetPlan();
                })
                .WithStateOptionsFactory(substreamName => CreateStateOptions(testName, substreamName))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, false));
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, watermark => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                })
                .DistributeAutomatically(substreamCount)
                .Build();

            // The join is partitioned across all substreams even though the plan has one sink
            Assert.Equal(
                Enumerable.Range(0, substreamCount).Select(i => $"substream_{i}").OrderBy(x => x),
                _stream.Substreams.Keys.OrderBy(x => x));

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // Add more data and verify that it flows through all join partitions
            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // Delete some rows and verify that the retractions flow through the lanes
            foreach (var order in _generator.Orders.Take(100).ToList())
            {
                _generator.DeleteOrder(order);
            }
            foreach (var user in _generator.Users.Take(20).ToList())
            {
                _generator.DeleteUser(user);
            }

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            Assert.Empty(failures);
        }

        private List<UserKeyRow> GetExpectedJoinResult()
        {
            return _generator.Orders
                .Join(_generator.Users, o => o.UserKey, u => u.UserKey, (o, u) => new UserKeyRow(u.UserKey))
                .ToList();
        }

        /// <summary>
        /// A normal aggregation plan is distributed automatically, each substream aggregates
        /// its own hash partition of the groups.
        /// </summary>
        [Fact]
        public async Task NormalAggregatePlanIsDistributedAcrossSubstreams()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = new DistributedStreamBuilder("e2e_normal_aggregate")
                .AddPlan(() =>
                {
                    var sqlPlanBuilder = new SqlPlanBuilder();
                    sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                    sqlPlanBuilder.Sql(@"
                    INSERT INTO output
                    SELECT userkey, count(*) FROM orders
                    GROUP BY userkey;
                    ");
                    return sqlPlanBuilder.GetPlan();
                })
                .WithStateOptionsFactory(substreamName => CreateStateOptions("e2e_normal_aggregate", substreamName))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, false));
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, watermark => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                })
                .DistributeAutomatically(2)
                .Build();

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedAggregateResult());

            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedAggregateResult());

            Assert.Empty(failures);
        }

        private List<UserCountRow> GetExpectedAggregateResult()
        {
            return _generator.Orders
                .GroupBy(x => x.UserKey)
                .Select(x => new UserCountRow(x.Key, x.Count()))
                .ToList();
        }

        private record UserCountRow(long UserKey, long Count);

        /// <summary>
        /// An aggregate that groups on the join key stays inside the partition lanes,
        /// the pipeline join -> aggregate runs in every substream and only the aggregated
        /// results are gathered to the sink substream.
        /// </summary>
        [Fact]
        public async Task CoPartitionedAggregateOverJoinIsDistributed()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = new DistributedStreamBuilder("e2e_agg_over_join")
                .AddPlan(() =>
                {
                    var sqlPlanBuilder = new SqlPlanBuilder();
                    sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                    sqlPlanBuilder.Sql(@"
                    INSERT INTO output
                    SELECT o.userkey, count(*) FROM orders o
                    INNER JOIN users u ON o.userkey = u.userkey
                    GROUP BY o.userkey;
                    ");
                    return sqlPlanBuilder.GetPlan();
                })
                .WithStateOptionsFactory(substreamName => CreateStateOptions("e2e_agg_over_join", substreamName))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, false));
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, watermark => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                })
                .DistributeAutomatically(2)
                .Build();

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinAggregateResult());

            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinAggregateResult());

            // Delete some orders and verify that the counts are retracted through the lanes
            foreach (var order in _generator.Orders.Take(100).ToList())
            {
                _generator.DeleteOrder(order);
            }

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinAggregateResult());

            Assert.Empty(failures);
        }

        private List<UserCountRow> GetExpectedJoinAggregateResult()
        {
            return _generator.Orders
                .Join(_generator.Users, o => o.UserKey, u => u.UserKey, (o, u) => o.UserKey)
                .GroupBy(x => x)
                .Select(x => new UserCountRow(x.Key, x.Count()))
                .ToList();
        }

        /// <summary>
        /// A having filter above a co partitioned aggregate is also pushed into the lanes,
        /// the pipeline join -> aggregate -> filter runs in every substream.
        /// </summary>
        [Fact]
        public async Task HavingFilterOverCoPartitionedAggregateIsDistributed()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = new DistributedStreamBuilder("e2e_having_over_join")
                .AddPlan(() =>
                {
                    var sqlPlanBuilder = new SqlPlanBuilder();
                    sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                    sqlPlanBuilder.Sql(@"
                    INSERT INTO output
                    SELECT o.userkey, count(*) FROM orders o
                    INNER JOIN users u ON o.userkey = u.userkey
                    GROUP BY o.userkey
                    HAVING count(*) > 1;
                    ");
                    return sqlPlanBuilder.GetPlan();
                })
                .WithStateOptionsFactory(substreamName => CreateStateOptions("e2e_having_over_join", substreamName))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, false));
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, watermark => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                })
                .DistributeAutomatically(2)
                .Build();

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedHavingResult());

            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedHavingResult());

            Assert.Empty(failures);
        }

        private List<UserCountRow> GetExpectedHavingResult()
        {
            return GetExpectedJoinAggregateResult()
                .Where(x => x.Count > 1)
                .ToList();
        }

        /// <summary>
        /// An aggregate that groups on something other than the join key cannot stay in the
        /// join lanes, it gets its own scatter exchange. Verifies the fallback produces
        /// correct results.
        /// </summary>
        [Fact]
        public async Task NonCoPartitionedAggregateOverJoinIsDistributed()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = new DistributedStreamBuilder("e2e_noncopart_agg")
                .AddPlan(() =>
                {
                    var sqlPlanBuilder = new SqlPlanBuilder();
                    sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                    sqlPlanBuilder.Sql(@"
                    INSERT INTO output
                    SELECT o.orderkey, count(*) FROM orders o
                    INNER JOIN users u ON o.userkey = u.userkey
                    GROUP BY o.orderkey;
                    ");
                    return sqlPlanBuilder.GetPlan();
                })
                .WithStateOptionsFactory(substreamName => CreateStateOptions("e2e_noncopart_agg", substreamName))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, false));
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, watermark => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                })
                .DistributeAutomatically(2)
                .Build();

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedOrderCountResult());

            // Delete some orders and verify the retractions
            foreach (var order in _generator.Orders.Take(100).ToList())
            {
                _generator.DeleteOrder(order);
            }

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedOrderCountResult());

            Assert.Empty(failures);
        }

        private List<OrderCountRow> GetExpectedOrderCountResult()
        {
            return _generator.Orders
                .Join(_generator.Users, o => o.UserKey, u => u.UserKey, (o, u) => o.OrderKey)
                .GroupBy(x => x)
                .Select(x => new OrderCountRow(x.Key, x.Count()))
                .ToList();
        }

        private record OrderCountRow(int OrderKey, long Count);

        /// <summary>
        /// When a substream crashes, the failure propagates to the other substream and both
        /// recover to a common checkpoint, after which the data becomes correct again.
        /// </summary>
        [Fact]
        public async Task SubstreamCrashRecoversAndProducesCorrectData()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = new DistributedStreamBuilder("e2e_crash_recovery")
                .AddPlan(() =>
                {
                    var sqlPlanBuilder = new SqlPlanBuilder();
                    sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                    sqlPlanBuilder.Sql(@"
                    INSERT INTO output
                    SELECT u.userkey FROM users u
                    INNER JOIN orders o ON u.userkey = o.userkey;
                    ");
                    return sqlPlanBuilder.GetPlan();
                })
                .WithStateOptionsFactory(substreamName => CreateStateOptions("e2e_crash_recovery", substreamName))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, false));
                    // The sink in the sink substream crashes on its first checkpoint,
                    // the failure must propagate to the other substream and both recover.
                    var crashCount = substreamName == "substream_0" ? 1 : 0;
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, crashCount, watermark => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                })
                .DistributeAutomatically(2)
                .Build();

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            // The crash must actually have happened
            Assert.NotEmpty(failures);

            // Data added after the recovery must still flow through both substreams
            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// Reproduces the mutual recovery storm seen when a node is lost: four substreams,
        /// a crash in the sink substream propagates fail and recover to all of them, they
        /// restart together and their checkpoint cycles collide while replay data is in
        /// flight. The sink crashes on its first three checkpoints so every run goes through
        /// several storms. Hunts the deadlock where one exchange never receives a checkpoint
        /// barrier and every substream then waits forever on each others acknowledgements.
        /// </summary>
        [Fact]
        public async Task FourSubstreamMutualRecoveryUnderLoadConverges()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var ringLogger = new RingBufferLoggerProvider();
            var loggerFactory = LoggerFactory.Create(logging =>
            {
                logging.SetMinimumLevel(LogLevel.Debug);
                logging.AddProvider(ringLogger);
            });

            _stream = new DistributedStreamBuilder("e2e_mutual_recovery")
                .AddPlan(() =>
                {
                    var sqlPlanBuilder = new SqlPlanBuilder();
                    sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                    sqlPlanBuilder.Sql(@"
                    INSERT INTO output
                    SELECT u.userkey FROM users u
                    INNER JOIN orders o ON u.userkey = o.userkey;
                    ");
                    return sqlPlanBuilder.GetPlan();
                })
                .WithStateOptionsFactory(substreamName => CreateStateOptions("e2e_mutual_recovery", substreamName))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, false));
                    // Several crashes so each run goes through several mutual recoveries.
                    var crashCount = substreamName == "substream_0" ? 3 : 0;
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, crashCount, watermark => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                    substreamBuilder.WithLoggerFactory(loggerFactory);
                })
                .DistributeAutomatically(4)
                .Build();

            await _stream.StartAsync();

            try
            {
                // Fresh data in every wave so the colliding recovery cycles always have
                // batches in flight between the substreams.
                for (int wave = 0; wave < 5; wave++)
                {
                    _generator.Generate(500);
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
                }
            }
            catch
            {
                var dumpPath = Path.GetFullPath($"./mutual_recovery_failure_{DateTime.UtcNow:HHmmss}.log");
                ringLogger.WriteToFile(dumpPath);
                throw new Exception($"Mutual recovery test failed, log dump: {dumpPath}");
            }

            // The crashes must actually have happened
            Assert.NotEmpty(failures);
        }

        private async Task RunTwoPartitionTest(
            string testName,
            string sql,
            bool autoDistribute,
            string partition0Substream,
            string partition1Substream)
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            var builder = new DistributedStreamBuilder(testName)
                .AddPlan(() =>
                {
                    // The plan is rebuilt for every substream since building a stream
                    // modifies the plan in place.
                    var sqlPlanBuilder = new SqlPlanBuilder();
                    sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                    sqlPlanBuilder.Sql(sql);
                    return sqlPlanBuilder.GetPlan();
                })
                .WithStateOptionsFactory(substreamName => CreateStateOptions(testName, substreamName))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, false));
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, watermark => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                });

            if (autoDistribute)
            {
                builder.DistributeAutomatically(2);
            }

            _stream = builder.Build();

            Assert.Equal(
                new[] { partition0Substream, partition1Substream }.OrderBy(x => x),
                _stream.Substreams.Keys.OrderBy(x => x));

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, partition0Substream, GetExpectedPartition(0));
            await WaitForSinkData(latestData, failures, partition1Substream, GetExpectedPartition(1));

            // Add more data and verify that it flows through both substreams
            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, partition0Substream, GetExpectedPartition(0));
            await WaitForSinkData(latestData, failures, partition1Substream, GetExpectedPartition(1));

            Assert.Empty(failures);

            // Verify that a coordinated stop completes, the final checkpoints require
            // communication between the substreams.
            var stopTask = _stream.StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping the distributed stream timed out");
            await stopTask;
        }

        private const string NormalJoinSql = @"
            INSERT INTO output
            SELECT u.userkey FROM users u
            INNER JOIN orders o ON u.userkey = o.userkey;
            ";

        /// <summary>
        /// Builds a distributed host with the standard mock connectors for the new tests.
        /// </summary>
        private DistributedFlowtideStream BuildHost(
            string testName,
            string sql,
            ConcurrentDictionary<string, EventBatchData> latestData,
            ConcurrentBag<(string Substream, Exception? Exception)> failures,
            int substreamCount = 2,
            Func<string, (int CrashCount, int CheckpointsBeforeCrash)>? crashConfig = null,
            Func<string, Storage.StateManager.StateManagerOptions>? stateOptions = null,
            bool debugLog = false,
            Func<string, Microsoft.Extensions.Logging.ILoggerFactory>? loggerFactory = null,
            TimeSpan? stopDrainTimeout = null,
            Action<string, Watermark>? onWatermark = null)
        {
            return new DistributedStreamBuilder(testName)
                .AddPlan(() =>
                {
                    var sqlPlanBuilder = new SqlPlanBuilder();
                    sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                    sqlPlanBuilder.Sql(sql);
                    return sqlPlanBuilder.GetPlan();
                })
                .WithStateOptionsFactory(substreamName => stateOptions != null ? stateOptions(substreamName) : CreateStateOptions(testName, substreamName))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, false));
                    var crash = crashConfig?.Invoke(substreamName) ?? (0, 0);
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, crash.CrashCount, watermark => onWatermark?.Invoke(substreamName, watermark), crash.CheckpointsBeforeCrash));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                    if (stopDrainTimeout.HasValue)
                    {
                        substreamBuilder.SetStopDrainTimeout(stopDrainTimeout.Value);
                    }
                    if (loggerFactory != null)
                    {
                        substreamBuilder.WithLoggerFactory(loggerFactory(substreamName));
                    }
                    else if (debugLog)
                    {
                        var serilogLogger = new Serilog.LoggerConfiguration()
                            .MinimumLevel.Debug()
                            .WriteTo.File($"./debugwrite/{testName}_{substreamName}.log")
                            .CreateLogger();
                        substreamBuilder.WithLoggerFactory(Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddSerilog(serilogLogger)));
                    }
                })
                .DistributeAutomatically(substreamCount)
                .Build();
        }

        /// <summary>
        /// A crash after checkpoints have already completed rolls both substreams back to the
        /// last common checkpoint instead of the beginning, and the stream recovers with
        /// correct data including changes made after the recovery.
        /// </summary>
        [Fact]
        public async Task MidRunCrashRecoversFromCheckpoint()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            // The sink completes its first checkpoint and crashes on the second
            _stream = BuildHost("e2e_midrun_crash", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 1) : (0, 0));

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            // New data triggers the next checkpoint where the sink crashes,
            // both substreams must recover to the first checkpoint and catch up
            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            Assert.NotEmpty(failures);

            // Deletes after the recovery must also flow correctly
            foreach (var order in _generator.Orders.Take(100).ToList())
            {
                _generator.DeleteOrder(order);
            }

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// A distributed stream can be stopped and started again with a new host from the
        /// persisted checkpoints, the substreams handshake on their restored versions and
        /// continue processing new data.
        /// </summary>
        [Fact]
        public async Task DistributedStreamRestartsFromCheckpoint()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, MemoryFileProvider>();
            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

            Storage.StateManager.StateManagerOptions CreateOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100_000,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        // The file providers are shared between the hosts so the second host
                        // restores from the state the first host persisted
                        FileProvider = fileProviders.GetOrAdd(substreamName, _ => new MemoryFileProvider())
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_restart/{substreamName}/tmp"
                    }
                };
            }

            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug);
                    b.AddProvider(provider);
                });
            }

            void DumpLogBuffers(string phase)
            {
                foreach (var buffer in logBuffers)
                {
                    buffer.Value.WriteToFile($"./debugwrite/e2e_restart_{phase}_{buffer.Key}.log");
                }
            }

            _stream = BuildHost("e2e_restart", NormalJoinSql, latestData, failures, stateOptions: CreateOptions, loggerFactory: CreateBufferedLoggerFactory);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            var stopTask = _stream.StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            if (finished != stopTask)
            {
                DumpLogBuffers("firststop");
            }
            Assert.True(finished == stopTask, "Stopping the distributed stream timed out");
            await stopTask;
            await _stream.DisposeAsync();

            // Start a new host from the persisted state and verify new data flows
            _stream = BuildHost("e2e_restart", NormalJoinSql, latestData, failures, stateOptions: CreateOptions);
            await _stream.StartAsync();

            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// Stopping while data is still flowing between the substreams must not lose the data
        /// in flight. The stop drains, each substream keeps running stop cycles until it has
        /// consumed the other substreams stop barrier, so everything sent before the stop is
        /// part of the final checkpoints and a restarted host produces the complete result.
        /// </summary>
        [Fact]
        public async Task StopUnderLoadKeepsDataCompleteAcrossRestart()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, MemoryFileProvider>();

            Storage.StateManager.StateManagerOptions CreateOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100_000,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = fileProviders.GetOrAdd(substreamName, _ => new MemoryFileProvider())
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_stop_load/{substreamName}/tmp"
                    }
                };
            }

            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();
            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug);
                    b.AddProvider(provider);
                });
            }

            _stream = BuildHost("e2e_stop_load", NormalJoinSql, latestData, failures, stateOptions: CreateOptions, loggerFactory: CreateBufferedLoggerFactory);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // Generate new data and stop immediately while it is still flowing between the
            // substreams, the stop must drain the in flight data.
            _generator.Generate(500);

            var stopTask = _stream.StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            if (finished != stopTask)
            {
                foreach (var buffer in logBuffers)
                {
                    buffer.Value.WriteToFile($"./debugwrite/e2e_stop_load_{buffer.Key}.log");
                }
            }
            Assert.True(finished == stopTask, "Stopping the distributed stream under load timed out");
            await stopTask;
            await _stream.DisposeAsync();

            // A new host started from the persisted state must produce the complete result.
            _stream = BuildHost("e2e_stop_load", NormalJoinSql, latestData, failures, stateOptions: CreateOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// Watermarks must propagate across the substreams: the sinks combined watermark
        /// must contain the watermark names of all sources, including sources whose lanes
        /// run in the other substream, with values covering all emitted data, and it must
        /// advance when new data arrives. Watermarks travel through the exchange queues and
        /// are min combined across the lanes, a lost name or a stale value here would make
        /// downstream consumers think data is missing or complete too early.
        /// </summary>
        [Fact]
        public async Task WatermarksReachTheSinkAcrossSubstreams()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var latestWatermarks = new ConcurrentDictionary<string, Watermark>();

            _stream = BuildHost("e2e_watermarks", NormalJoinSql, latestData, failures,
                onWatermark: (substreamName, watermark) => latestWatermarks[substreamName] = watermark);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            await WaitForWatermark(latestWatermarks, "substream_0", _generator.Users.Count, _generator.Orders.Count);

            // New data must advance the watermark across the substreams.
            _generator.Generate(500);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
            await WaitForWatermark(latestWatermarks, "substream_0", _generator.Users.Count, _generator.Orders.Count);
        }

        private async Task WaitForWatermark(ConcurrentDictionary<string, Watermark> latestWatermarks, string substreamName, long expectedUsersOffset, long expectedOrdersOffset)
        {
            Debug.Assert(_stream != null);
            var stopwatch = Stopwatch.StartNew();
            while (true)
            {
                if (latestWatermarks.TryGetValue(substreamName, out var watermark) &&
                    watermark.Watermarks.TryGetValue("users", out var usersValue) && usersValue is LongWatermarkValue usersLong && usersLong.Value >= expectedUsersOffset &&
                    watermark.Watermarks.TryGetValue("orders", out var ordersValue) && ordersValue is LongWatermarkValue ordersLong && ordersLong.Value >= expectedOrdersOffset)
                {
                    return;
                }

                if (stopwatch.Elapsed >= TimeSpan.FromSeconds(60))
                {
                    var current = latestWatermarks.TryGetValue(substreamName, out var last)
                        ? string.Join(",", last.Watermarks.Select(kv => $"{kv.Key}={(kv.Value as LongWatermarkValue)?.Value}"))
                        : "none";
                    Assert.Fail($"The sink watermark did not reach users>={expectedUsersOffset}, orders>={expectedOrdersOffset}, last seen: [{current}]");
                }
                await Task.Delay(10);
            }
        }

        /// <summary>
        /// Rows deleted after a crash recovery must retract through the recovered
        /// substreams, the rollback and replay must not lose the ability to process
        /// retractions or double apply them.
        /// </summary>
        [Fact]
        public async Task RetractionsFlowCorrectlyAfterCrashRecovery()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_retract_recovery", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 0) : (0, 0));
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
            Assert.NotEmpty(failures);

            // Delete a slice of the orders after the recovery, the join result must shrink.
            var ordersToDelete = _generator.Orders.Take(100).ToList();
            foreach (var order in ordersToDelete)
            {
                _generator.DeleteOrder(order);
            }

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            // And new data must still flow after the retractions.
            _generator.Generate(200);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// Stopping must be idempotent: concurrent stop calls and a stop after the stream
        /// already stopped must all complete without hanging or failing.
        /// </summary>
        [Fact]
        public async Task StopIsIdempotent()
        {
            _generator.Generate(200);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_double_stop", NormalJoinSql, latestData, failures);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            var firstStop = _stream.StopAsync();
            var secondStop = _stream.StopAsync();
            var both = Task.WhenAll(firstStop, secondStop);
            var finished = await Task.WhenAny(both, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == both, "Concurrent stops did not both complete");
            await both;

            // A stop after the stream already stopped must also complete.
            var thirdStop = _stream.StopAsync();
            finished = await Task.WhenAny(thirdStop, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.True(finished == thirdStop, "Stopping an already stopped stream did not complete");
            await thirdStop;
        }

        /// <summary>
        /// A crash in a lane substream, not the sink substream, must also recover both
        /// substreams. All other crash tests fail the sink side, so the fail and recover
        /// propagation in the other direction, from a lane substream towards the sink
        /// substream, is only covered here.
        /// </summary>
        [Fact]
        public async Task CrashInLaneSubstreamRecoversAndProducesCorrectData()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_lane_crash", NormalJoinSql, latestData, failures);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // Crash the sources of the lane substream, the recovery must roll back the sink
            // substream as well.
            await _stream.Substreams["substream_1"].CallTrigger("crash", null);

            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
            Assert.NotEmpty(failures);
        }

        /// <summary>
        /// Several crashes spaced over different checkpoints, every recovery starts from a
        /// different persisted state, the data must be complete after each one.
        /// </summary>
        [Fact]
        public async Task RepeatedCrashesAcrossCheckpointsRecoverEveryTime()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
                    b.AddProvider(provider);
                });
            }

            _stream = BuildHost("e2e_repeated_crash", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (3, 2) : (0, 0),
                loggerFactory: CreateBufferedLoggerFactory);
            await _stream.StartAsync();

            try
            {
                for (int wave = 0; wave < 4; wave++)
                {
                    _generator.Generate(200);
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
                }
            }
            catch
            {
                foreach (var buffer in logBuffers)
                {
                    buffer.Value.WriteToFile($"./debugwrite/e2e_repeated_crash_{buffer.Key}.log");
                }
                throw;
            }
            Assert.NotEmpty(failures);
        }

        /// <summary>
        /// A distributed window function holds ordered state per partition, a crash and
        /// rollback must restore it correctly and produce correct window results for data
        /// added after the recovery.
        /// </summary>
        [Fact]
        public async Task WindowFunctionRecoversAfterCrash()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_window_crash", @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users;
            ", latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 1) : (0, 0));

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedWindowResult(), allowFailures: true);

            // New data triggers the next checkpoint where the sink crashes, the window state
            // must recover and stay correct.
            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedWindowResult(), allowFailures: true);
            Assert.NotEmpty(failures);
        }

        /// <summary>
        /// A co partitioned aggregate holds per group state in every lane, a crash and
        /// rollback must restore the counts correctly, including for retractions applied
        /// after the recovery.
        /// </summary>
        [Fact]
        public async Task CoPartitionedAggregateRecoversAfterCrash()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_agg_crash", @"
            INSERT INTO output
            SELECT o.userkey, count(*) FROM orders o
            INNER JOIN users u ON o.userkey = u.userkey
            GROUP BY o.userkey;
            ", latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 1) : (0, 0));

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinAggregateResult(), allowFailures: true);

            // New data triggers the checkpoint where the sink crashes, the aggregate state
            // must recover, including for retractions applied after the recovery.
            foreach (var order in _generator.Orders.Take(100).ToList())
            {
                _generator.DeleteOrder(order);
            }
            _generator.Generate(200);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinAggregateResult(), allowFailures: true);
            Assert.NotEmpty(failures);
        }

        /// <summary>
        /// After a crash recovery the sinks watermark must advance past the values it had
        /// before the crash once new data flows, a rollback must not leave the watermark
        /// stuck at a stale value.
        /// </summary>
        [Fact]
        public async Task WatermarkAdvancesPastPreCrashValuesAfterRecovery()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var latestWatermarks = new ConcurrentDictionary<string, Watermark>();

            _stream = BuildHost("e2e_watermark_crash", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 1) : (0, 0),
                onWatermark: (substreamName, watermark) => latestWatermarks[substreamName] = watermark);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            // New data triggers the checkpoint where the sink crashes, the watermark must
            // still advance past its pre crash values after the recovery.
            _generator.Generate(500);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
            await WaitForWatermark(latestWatermarks, "substream_0", _generator.Users.Count, _generator.Orders.Count);
            Assert.NotEmpty(failures);
        }

        /// <summary>
        /// Stopping under load while the exchange queues have spilled pages to temporary
        /// storage, the drain must deliver the spilled events before the stop finishes and a
        /// restarted host must produce the complete result.
        /// </summary>
        [Fact]
        public async Task StopUnderLoadWithSpillingQueuesKeepsDataComplete()
        {
            _generator.Generate(1000);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, MemoryFileProvider>();

            Storage.StateManager.StateManagerOptions CreateSmallCacheOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    // A small page cache so the exchange queues spill under the load.
                    CachePageCount = 64,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = fileProviders.GetOrAdd(substreamName, _ => new MemoryFileProvider())
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_spill_stop/{substreamName}/tmp"
                    }
                };
            }

            _stream = BuildHost("e2e_spill_stop", NormalJoinSql, latestData, failures, stateOptions: CreateSmallCacheOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // New data and an immediate stop, the in flight events must survive the drain
            // even when queue pages sit in temporary storage.
            _generator.Generate(1000);

            var stopTask = _stream.StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping with spilled queues timed out");
            await stopTask;
            await _stream.DisposeAsync();

            _stream = BuildHost("e2e_spill_stop", NormalJoinSql, latestData, failures, stateOptions: CreateSmallCacheOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// Deleting a stopped distributed stream must remove the state of all substreams and
        /// complete, and building a fresh host afterwards must work like a first start.
        /// </summary>
        [Fact]
        public async Task DeleteRemovesStateAndStreamRestartsFresh()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, MemoryFileProvider>();

            Storage.StateManager.StateManagerOptions CreateOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100_000,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = fileProviders.GetOrAdd(substreamName, _ => new MemoryFileProvider())
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_delete/{substreamName}/tmp"
                    }
                };
            }

            _stream = BuildHost("e2e_delete", NormalJoinSql, latestData, failures, stateOptions: CreateOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            var stopTask = _stream.StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping before the delete timed out");
            await stopTask;

            var deleteTask = _stream.DeleteAsync();
            finished = await Task.WhenAny(deleteTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == deleteTask, "Deleting the distributed stream timed out");
            await deleteTask;
            await _stream.DisposeAsync();

            // A fresh host over the same storage must start from scratch and produce the
            // complete result again.
            _generator.Generate(200);
            _stream = BuildHost("e2e_delete", NormalJoinSql, latestData, failures, stateOptions: CreateOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// Rapid stop and start cycles while data is in flight. The first stop happens right
        /// after start, so it hits streams that are still starting up, later stops hit
        /// checkpoints in flight. This targets the races between stopping, checkpoint
        /// completion and the checkpoint acknowledgements from the other substream, which
        /// take the stream context and checkpoint locks and have deadlocked before when a
        /// transition ran under the wrong lock. Every stop must finish, and the final host
        /// must produce the complete result.
        /// </summary>
        [Fact]
        public async Task RepeatedStopStartUnderLoadKeepsDataComplete()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, MemoryFileProvider>();
            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

            Storage.StateManager.StateManagerOptions CreateOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100_000,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = fileProviders.GetOrAdd(substreamName, _ => new MemoryFileProvider())
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_stop_cycles/{substreamName}/tmp"
                    }
                };
            }

            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug);
                    b.AddProvider(provider);
                });
            }

            for (int cycle = 0; cycle < 5; cycle++)
            {
                _stream = BuildHost("e2e_stop_cycles", NormalJoinSql, latestData, failures, stateOptions: CreateOptions, loggerFactory: CreateBufferedLoggerFactory);
                await _stream.StartAsync();

                // Fresh data every cycle so the stop always has data in flight, the stop is
                // requested without waiting so it can hit startup and running checkpoints at
                // different points every cycle.
                _generator.Generate(200);
                await Task.Delay(cycle * 20);

                var stopTask = _stream.StopAsync();
                var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
                if (finished != stopTask)
                {
                    foreach (var buffer in logBuffers)
                    {
                        buffer.Value.WriteToFile($"./debugwrite/e2e_stop_cycles_{cycle}_{buffer.Key}.log");
                    }
                }
                Assert.True(finished == stopTask, $"Stopping the distributed stream timed out in cycle {cycle}");
                await stopTask;
                await _stream.DisposeAsync();
                _stream = null;
            }

            // The final host must produce the complete result for all generated data.
            _stream = BuildHost("e2e_stop_cycles", NormalJoinSql, latestData, failures, stateOptions: CreateOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// Stopping a single substream while the other substream keeps running must not hang.
        /// The drain waits for the other substreams stop barrier which never comes, after the
        /// configured drain timeout the stop finishes anyway.
        /// </summary>
        [Fact]
        public async Task LoneSubstreamStopFinishesAfterDrainTimeout()
        {
            _generator.Generate(200);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_lone_stop", NormalJoinSql, latestData, failures,
                stopDrainTimeout: TimeSpan.FromSeconds(2));

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // Stop only one substream, the other keeps running and never sends a stop barrier
            var stopTask = _stream.Substreams["substream_0"].StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.True(finished == stopTask, "Stopping a lone substream did not finish within the drain timeout");
            await stopTask;
        }

        /// <summary>
        /// The exchange queues can spill their pages to temporary storage when the page cache
        /// is small, the exchanged events must survive the serialize and reload round trip.
        /// </summary>
        [Fact]
        public async Task ExchangeSpillsPagesUnderMemoryPressure()
        {
            _generator.Generate(1000);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            Storage.StateManager.StateManagerOptions CreateSmallCacheOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    // A very small page cache forces pages, including the exchange queue
                    // pages, to be evicted to temporary storage and reloaded.
                    CachePageCount = 64,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = new MemoryFileProvider()
                    }),
                    DefaultBPlusTreePageSize = 128,
                    DefaultBPlusTreePageSizeBytes = 8 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_spill/{substreamName}/tmp"
                    }
                };
            }

            _stream = BuildHost("e2e_spill", NormalJoinSql, latestData, failures, stateOptions: CreateSmallCacheOptions);

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // More data and deletes must flow correctly through the spilled queues
            _generator.Generate(500);
            foreach (var order in _generator.Orders.Take(100).ToList())
            {
                _generator.DeleteOrder(order);
            }

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            Assert.Empty(failures);

            var stopTask = _stream.StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping the distributed stream timed out");
            await stopTask;
        }

        /// <summary>
        /// A window function partitioned by a column is distributed automatically, each
        /// substream computes the windows for its hash partition.
        /// </summary>
        [Fact]
        public async Task WindowFunctionIsDistributedAcrossSubstreams()
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_window", @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users;
            ", latestData, failures);

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedWindowResult());

            _generator.Generate(500);

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedWindowResult());

            Assert.Empty(failures);
        }

        private record WindowSumRow(string? CompanyId, int UserKey, long Value);

        private List<WindowSumRow> GetExpectedWindowResult()
        {
            return _generator.Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var sum = 0.0;
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    Queue<double> values = new Queue<double>();
                    List<WindowSumRow> output = new List<WindowSumRow>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        while (values.Count > 4)
                        {
                            var dequeued = values.Dequeue();
                            sum -= dequeued;
                        }
                        values.Enqueue(orderedByKey[i].DoubleValue);
                        sum += orderedByKey[i].DoubleValue;
                        output.Add(new WindowSumRow(orderedByKey[i].CompanyId, orderedByKey[i].UserKey, (long)sum));
                    }
                    return output;
                }).ToList();
        }

        /// <summary>
        /// The sink crashes on the stop checkpoint itself. The failure interrupts the stop,
        /// the stream recovers, and the pending stop wish must still be honored after the
        /// recovery so the stop call completes. No other test fails a checkpoint that was
        /// started by a stop.
        /// </summary>
        [Fact]
        public async Task CrashOnStopCheckpointStillStops()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            // One clean checkpoint covers the initial data, the next checkpoint crashes.
            // No data is generated after the first wait, so the next checkpoint is the one
            // started by the stop.
            _stream = BuildHost("e2e_crash_on_stop", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 1) : (0, 0));
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            var stopTask = _stream.StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stop did not complete after the sink crashed on the stop checkpoint");
            await stopTask;
        }

        /// <summary>
        /// A stop is requested immediately after a lane substream crashed, while the
        /// substreams are still failing over and rolling back. The stop must complete even
        /// though it races the whole recovery path.
        /// </summary>
        [Fact]
        public async Task StopDuringRecoveryCompletes()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug);
                    b.AddProvider(provider);
                });
            }

            _stream = BuildHost("e2e_stop_during_recovery", NormalJoinSql, latestData, failures,
                loggerFactory: CreateBufferedLoggerFactory);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // Crash the lane substream and stop right away, the stop lands during the
            // failure handling and restart of both substreams.
            await _stream.Substreams["substream_1"].CallTrigger("crash", null);
            var stopTask = _stream.StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            if (finished != stopTask)
            {
                foreach (var buffer in logBuffers)
                {
                    buffer.Value.WriteToFile($"./debugwrite/e2e_stop_during_recovery_{buffer.Key}.log");
                }
            }
            Assert.True(finished == stopTask, "Stop did not complete when requested during a crash recovery");
            await stopTask;
        }

        /// <summary>
        /// Crashes fire in BOTH substreams interleaved over several data waves, the sink
        /// substream crashes on checkpoints while the lane substream crashes through its
        /// source trigger between waves. Every recovery races the other sides recovery, the
        /// data must still converge after each wave.
        /// </summary>
        [Fact]
        public async Task InterleavedCrashesInBothSubstreamsConverge()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_interleaved_crash", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (2, 1) : (0, 0));
            await _stream.StartAsync();

            for (int wave = 0; wave < 4; wave++)
            {
                _generator.Generate(200);
                if (wave == 1 || wave == 3)
                {
                    // Crash the lane substream while the sink substreams crash counter may
                    // also still be armed, the recoveries interleave.
                    await _stream.Substreams["substream_1"].CallTrigger("crash", null);
                }
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
            }
            Assert.NotEmpty(failures);
        }

        private record LeftJoinRow(long UserKey, long? OrderKey);

        private List<LeftJoinRow> GetExpectedLeftJoinResult()
        {
            return _generator.Users
                .GroupJoin(_generator.Orders, u => u.UserKey, o => o.UserKey, (u, orders) => (u, orders))
                .SelectMany(x => x.orders.Any()
                    ? x.orders.Select(o => new LeftJoinRow(x.u.UserKey, o.OrderKey))
                    : new[] { new LeftJoinRow(x.u.UserKey, null) })
                .ToList();
        }

        /// <summary>
        /// A LEFT JOIN produces null padded rows for users without orders. Deleting all
        /// orders of a user after a crash recovery must retract the matched rows and emit
        /// the null padded row again, the transition in both directions flows through the
        /// partition lanes and the recovered join state. No other distributed test covers
        /// outer join null padding.
        /// </summary>
        [Fact]
        public async Task LeftJoinNullPaddingRecoversAfterCrash()
        {
            const string leftJoinSql = @"
            INSERT INTO output
            SELECT u.userkey, o.orderkey FROM users u
            LEFT JOIN orders o ON u.userkey = o.userkey;
            ";

            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_left_join_crash", leftJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 1) : (0, 0));
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedLeftJoinResult(), allowFailures: true);

            // New data triggers the checkpoint where the sink crashes
            _generator.Generate(200);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedLeftJoinResult(), allowFailures: true);
            Assert.NotEmpty(failures);

            // Delete every order of the first users, their rows must fall back to null
            // padded rows through the recovered join state.
            var usersToClear = _generator.Users.Take(20).Select(x => x.UserKey).ToHashSet();
            foreach (var order in _generator.Orders.Where(o => usersToClear.Contains(o.UserKey)).ToList())
            {
                _generator.DeleteOrder(order);
            }
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedLeftJoinResult(), allowFailures: true);

            // Deleting users entirely must remove their null padded rows as well
            foreach (var user in _generator.Users.Take(10).ToList())
            {
                _generator.DeleteUser(user);
            }
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedLeftJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// A crash recovery with a very small page cache, the exchange queues and operator
        /// trees spill pages to temporary storage before the crash and the restore must work
        /// against spilled state. The existing spill test only covers stop, not recovery.
        /// </summary>
        [Fact]
        public async Task MidRunCrashWithSpillingQueuesRecovers()
        {
            Storage.StateManager.StateManagerOptions CreateSpillOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 64,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = new MemoryFileProvider()
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_spill_crash/{substreamName}/tmp"
                    }
                };
            }

            _generator.Generate(1000);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_spill_crash", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 1) : (0, 0),
                stateOptions: CreateSpillOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            // The next checkpoint crashes, recovery restores from state that contains
            // spilled pages
            _generator.Generate(1000);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
            Assert.NotEmpty(failures);

            foreach (var order in _generator.Orders.Take(200).ToList())
            {
                _generator.DeleteOrder(order);
            }
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// Both substreams crash AT THE SAME INSTANT through their source triggers. Both
        /// sides then initiate fail and recover towards each other simultaneously, the
        /// initialize handshakes race in both directions and must converge on a common
        /// version. Other tests crash the sides at different times.
        /// </summary>
        [Fact]
        public async Task SimultaneousCrashInBothSubstreamsConverges()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_simultaneous_crash", NormalJoinSql, latestData, failures);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            for (int round = 0; round < 3; round++)
            {
                // Fire the crash trigger in both substreams at the same time
                await Task.WhenAll(
                    _stream.Substreams["substream_0"].CallTrigger("crash", null),
                    _stream.Substreams["substream_1"].CallTrigger("crash", null));

                _generator.Generate(200);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
            }
            Assert.NotEmpty(failures);
        }

        /// <summary>
        /// A stream with a crash history is stopped, a brand new host restores from the
        /// persisted storage and then crashes again in its second life. The storage left
        /// behind by rollbacks must be fully consistent for a cold restore, and a recovery
        /// in the second life must roll back across state written by both hosts.
        /// </summary>
        [Fact]
        public async Task CrashHistorySurvivesColdRestartAndCrashesAgain()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, MemoryFileProvider>();

            Storage.StateManager.StateManagerOptions CreateOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100_000,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = fileProviders.GetOrAdd(substreamName, _ => new MemoryFileProvider())
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_crash_cold_restart/{substreamName}/tmp"
                    }
                };
            }

            // First life: two crashes roll versions back and rewrite them, leaving a
            // storage history of overwritten checkpoint versions.
            _stream = BuildHost("e2e_crash_cold_restart", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (2, 1) : (0, 0),
                stateOptions: CreateOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
            _generator.Generate(200);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
            Assert.NotEmpty(failures);

            var stopTask = _stream.StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == stopTask, "Stopping the first host timed out");
            await stopTask;
            await _stream.DisposeAsync();

            var firstLifeFailureCount = failures.Count;

            // Second life: a fresh host restores from the storage the crashes left behind,
            // processes new data and then crashes again.
            _stream = BuildHost("e2e_crash_cold_restart", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 0) : (0, 0),
                stateOptions: CreateOptions);
            await _stream.StartAsync();

            // The first checkpoint of the second life crashes immediately, the recovery
            // rolls back across state written by both hosts.
            _generator.Generate(200);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            _generator.Generate(200);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            Assert.True(failures.Count > firstLifeFailureCount, "The crash in the second life never fired");

            foreach (var order in _generator.Orders.Take(100).ToList())
            {
                _generator.DeleteOrder(order);
            }
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// A plain distributed aggregate, no join, uses a different exchange topology than
        /// the join tests, the groups are hash scattered across the substreams. A crash must
        /// recover the partial aggregates in every substream. No other crash test covers the
        /// aggregate only topology.
        /// </summary>
        [Fact]
        public async Task DistributedAggregateRecoversAfterCrash()
        {
            const string aggregateSql = @"
            INSERT INTO output
            SELECT userkey, count(*) FROM orders
            GROUP BY userkey;
            ";

            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
                    b.AddProvider(provider);
                });
            }

            _stream = BuildHost("e2e_aggregate_crash", aggregateSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 1) : (0, 0),
                loggerFactory: CreateBufferedLoggerFactory);
            await _stream.StartAsync();

            try
            {
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedAggregateResult(), allowFailures: true);

                // The next checkpoint crashes, the partial aggregates in both substreams must
                // roll back together.
                _generator.Generate(500);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedAggregateResult(), allowFailures: true);
                Assert.NotEmpty(failures);

                // Deletes decrement the counts through the recovered aggregate state
                foreach (var order in _generator.Orders.Take(200).ToList())
                {
                    _generator.DeleteOrder(order);
                }
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedAggregateResult(), allowFailures: true);
            }
            catch
            {
                foreach (var buffer in logBuffers)
                {
                    buffer.Value.WriteToFile($"./debugwrite/e2e_aggregate_crash_{buffer.Key}.log");
                }
                throw;
            }
        }

        /// <summary>
        /// A stop that lands during a crash recovery skips the final checkpoint, the stream
        /// stops at the last committed state. A fresh host restoring from that storage must
        /// produce complete data, verifying that the stop during failure path leaves fully
        /// consistent storage behind.
        /// </summary>
        [Fact]
        public async Task StopDuringRecoveryThenColdRestartHasCompleteData()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, MemoryFileProvider>();
            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

            Storage.StateManager.StateManagerOptions CreateOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100_000,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = fileProviders.GetOrAdd(substreamName, _ => new MemoryFileProvider())
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_stoprec_restart/{substreamName}/tmp"
                    }
                };
            }

            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug);
                    b.AddProvider(provider);
                });
            }

            _stream = BuildHost("e2e_stoprec_restart", NormalJoinSql, latestData, failures,
                stateOptions: CreateOptions, loggerFactory: CreateBufferedLoggerFactory);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // Crash the lane substream and stop while the recovery runs
            await _stream.Substreams["substream_1"].CallTrigger("crash", null);
            var stopTask = _stream.StopAsync();
            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
            if (finished != stopTask)
            {
                foreach (var buffer in logBuffers)
                {
                    buffer.Value.WriteToFile($"./debugwrite/e2e_stoprec_restart_{buffer.Key}.log");
                }
            }
            Assert.True(finished == stopTask, "Stop during recovery did not complete");
            await stopTask;
            await _stream.DisposeAsync();

            // New data while stopped, a fresh host restores from whatever the interrupted
            // stop left behind and must catch up to the complete result.
            _generator.Generate(200);

            _stream = BuildHost("e2e_stoprec_restart", NormalJoinSql, latestData, failures, stateOptions: CreateOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// A randomized loop of data waves, crashes in either or both substreams and full
        /// stop and restart cycles. Every iteration must converge to the complete join
        /// result. The seed is logged on failure so a failing sequence can be replayed.
        /// </summary>
        [Fact]
        public async Task ChaosCrashStopLoopConverges()
        {
            int seed = Environment.TickCount;
            var random = new Random(seed);

            _generator.Generate(200);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, MemoryFileProvider>();
            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

            Storage.StateManager.StateManagerOptions CreateOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100_000,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = fileProviders.GetOrAdd(substreamName, _ => new MemoryFileProvider())
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_chaos/{substreamName}/tmp"
                    }
                };
            }

            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug);
                    b.AddProvider(provider);
                });
            }

            _stream = BuildHost("e2e_chaos", NormalJoinSql, latestData, failures,
                stateOptions: CreateOptions, loggerFactory: CreateBufferedLoggerFactory);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            try
            {
                for (int iteration = 0; iteration < 8; iteration++)
                {
                    var action = random.Next(5);
                    switch (action)
                    {
                        case 0:
                            _generator.Generate(100);
                            break;
                        case 1:
                        case 2:
                            // Crash one substream, the call itself may fail when the stream
                            // is already failing over, that interleaving is part of the chaos.
                            try
                            {
                                await _stream.Substreams[$"substream_{action - 1}"].CallTrigger("crash", null);
                            }
                            catch
                            {
                            }
                            break;
                        case 3:
                            try
                            {
                                await Task.WhenAll(
                                    _stream.Substreams["substream_0"].CallTrigger("crash", null),
                                    _stream.Substreams["substream_1"].CallTrigger("crash", null));
                            }
                            catch
                            {
                            }
                            break;
                        case 4:
                            var stopTask = _stream.StopAsync();
                            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
                            if (finished != stopTask)
                            {
                                throw new TimeoutException($"Chaos stop hung in iteration {iteration}");
                            }
                            await stopTask;
                            await _stream.DisposeAsync();
                            _stream = BuildHost("e2e_chaos", NormalJoinSql, latestData, failures,
                                stateOptions: CreateOptions, loggerFactory: CreateBufferedLoggerFactory);
                            await _stream.StartAsync();
                            break;
                    }

                    _generator.Generate(50);
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
                }
            }
            catch (Exception e)
            {
                foreach (var buffer in logBuffers)
                {
                    buffer.Value.WriteToFile($"./debugwrite/e2e_chaos_seed{seed}_{buffer.Key}.log");
                }
                throw new Exception($"Chaos loop failed with seed {seed}", e);
            }
        }

        /// <summary>
        /// A delete requested while the substreams are failing over. The delete must
        /// complete even though blocks are being faulted and disposed by the failure
        /// handling, and a fresh host on the same storage must start like a first start.
        /// The stop equivalents of this interleaving exposed several hangs, delete goes
        /// through a different state and is only covered here.
        /// </summary>
        [Fact]
        public async Task DeleteDuringRecoveryRemovesStateAndRestartsFresh()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, MemoryFileProvider>();

            Storage.StateManager.StateManagerOptions CreateOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100_000,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = fileProviders.GetOrAdd(substreamName, _ => new MemoryFileProvider())
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_delete_recovery/{substreamName}/tmp"
                    }
                };
            }

            _stream = BuildHost("e2e_delete_recovery", NormalJoinSql, latestData, failures, stateOptions: CreateOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // Crash the lane substream and delete while the recovery runs
            await _stream.Substreams["substream_1"].CallTrigger("crash", null);
            var deleteTask = _stream.DeleteAsync();
            var finished = await Task.WhenAny(deleteTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == deleteTask, "Delete during recovery did not complete");
            await deleteTask;
            await _stream.DisposeAsync();

            // A fresh host must start from nothing and produce the full result
            _generator.Generate(200);
            _stream = BuildHost("e2e_delete_recovery", NormalJoinSql, latestData, failures, stateOptions: CreateOptions);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// A delete requested immediately after start, while the substreams are still
        /// initializing and handshaking. The delete must complete without waiting out the
        /// startup.
        /// </summary>
        [Fact]
        public async Task DeleteDuringStartCompletes()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_delete_during_start", NormalJoinSql, latestData, failures);
            await _stream.StartAsync();

            // Delete right away, the substreams are typically still starting up
            var deleteTask = _stream.DeleteAsync();
            var finished = await Task.WhenAny(deleteTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == deleteTask, "Delete during start did not complete");
            await deleteTask;
        }

        /// <summary>
        /// Repeated crash recoveries on an IDLE stream, no new data between the crashes, so
        /// every recovery replays nothing, the sources resume at their committed offsets
        /// with empty initial sends and the substreams must still align their checkpoints.
        /// After the idle recoveries new data must flow normally.
        /// </summary>
        [Fact]
        public async Task IdleCrashRecoveryLoopThenDataFlows()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_idle_crash", NormalJoinSql, latestData, failures);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            for (int i = 0; i < 3; i++)
            {
                try
                {
                    await _stream.Substreams["substream_1"].CallTrigger("crash", null);
                }
                catch
                {
                    // The stream may still be recovering from the previous crash
                }
                await Task.Delay(500);
            }
            Assert.NotEmpty(failures);

            // The stream must process new data after the idle recoveries
            _generator.Generate(200);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            foreach (var order in _generator.Orders.Take(50).ToList())
            {
                _generator.DeleteOrder(order);
            }
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// Several stop calls race each other while a crash recovery is running, every call
        /// must complete and observe the same stop.
        /// </summary>
        [Fact]
        public async Task ConcurrentStopsDuringRecoveryAllComplete()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_concurrent_stops", NormalJoinSql, latestData, failures);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            await _stream.Substreams["substream_1"].CallTrigger("crash", null);

            var stops = new[] { _stream.StopAsync(), _stream.StopAsync(), _stream.StopAsync() };
            var all = Task.WhenAll(stops);
            var finished = await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == all, "Concurrent stops during recovery did not all complete");
            await all;
        }

        /// <summary>
        /// Orders are reassigned to different users after a crash recovery, an update on the
        /// join key retracts the row from the old user and emits it for the new one, both
        /// sides flow through different partition lanes when the keys hash differently. No
        /// other test updates the join key.
        /// </summary>
        [Fact]
        public async Task JoinKeyUpdatesRecoverAfterCrash()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_joinkey_update", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (1, 1) : (0, 0));
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            // New data triggers the crash checkpoint
            _generator.Generate(200);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
            Assert.NotEmpty(failures);

            // Move the first orders to other users, the join key changes so the row is
            // retracted from one lane and inserted through another
            var userKeys = _generator.Users.Select(x => x.UserKey).ToList();
            int target = 0;
            foreach (var order in _generator.Orders.Take(100).ToList())
            {
                order.UserKey = userKeys[target % userKeys.Count];
                target += 7;
                _generator.AddOrUpdateOrder(order);
            }

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
        }

        /// <summary>
        /// Repeated crashes landing while INSERT AND DELETE traffic is crossing the exchange
        /// at the same time, on the aggregate topology. Counts move up and down between
        /// every crash, a lost retraction or a double applied update after any recovery
        /// leaves a stale (user, count) row in the sink. This is the targeted amplifier for
        /// a once-observed off-by-one group row after a recovery.
        /// </summary>
        [Fact]
        public async Task RepeatedCrashesWithDeletesAggregateConverges()
        {
            const string aggregateSql = @"
            INSERT INTO output
            SELECT userkey, count(*) FROM orders
            GROUP BY userkey;
            ";

            _generator.Generate(400);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
                    b.AddProvider(provider);
                });
            }

            _stream = BuildHost("e2e_agg_crash_deletes", aggregateSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (3, 1) : (0, 0),
                loggerFactory: CreateBufferedLoggerFactory);
            await _stream.StartAsync();

            try
            {
                for (int wave = 0; wave < 5; wave++)
                {
                    // Inserts and deletes in the same wave, the crash checkpoints land while
                    // both cross the exchange
                    _generator.Generate(150);
                    foreach (var order in _generator.Orders.Take(50).ToList())
                    {
                        _generator.DeleteOrder(order);
                    }
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedAggregateResult(), allowFailures: true);
                }
                Assert.NotEmpty(failures);
            }
            catch
            {
                foreach (var buffer in logBuffers)
                {
                    buffer.Value.WriteToFile($"./debugwrite/e2e_agg_crash_deletes_{buffer.Key}.log");
                }
                throw;
            }
        }

        /// <summary>
        /// The same crash cadence over mixed insert and delete traffic for the join
        /// topology, retractions of joined rows cross the exchange while the crash
        /// checkpoints land.
        /// </summary>
        [Fact]
        public async Task RepeatedCrashesWithDeletesJoinConverges()
        {
            _generator.Generate(400);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
                    b.AddProvider(provider);
                });
            }

            _stream = BuildHost("e2e_join_crash_deletes", NormalJoinSql, latestData, failures,
                crashConfig: substreamName => substreamName == "substream_0" ? (3, 1) : (0, 0),
                loggerFactory: CreateBufferedLoggerFactory);
            await _stream.StartAsync();

            try
            {
                for (int wave = 0; wave < 5; wave++)
                {
                    _generator.Generate(150);
                    foreach (var order in _generator.Orders.Take(50).ToList())
                    {
                        _generator.DeleteOrder(order);
                    }
                    foreach (var user in _generator.Users.Take(5).ToList())
                    {
                        _generator.DeleteUser(user);
                    }
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
                }
                Assert.NotEmpty(failures);
            }
            catch
            {
                foreach (var buffer in logBuffers)
                {
                    buffer.Value.WriteToFile($"./debugwrite/e2e_join_crash_deletes_{buffer.Key}.log");
                }
                throw;
            }
        }

        /// <summary>
        /// The chaos loop over THREE substreams, crashes hit random substreams and the
        /// version convergence after every failure is a three way handshake instead of a
        /// pair. Stop and rebuild cycles are included. The seed is logged on failure.
        /// </summary>
        [Fact]
        public async Task ThreeSubstreamChaosConverges()
        {
            int seed = Environment.TickCount;
            var random = new Random(seed);

            _generator.Generate(200);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, MemoryFileProvider>();
            var logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

            Storage.StateManager.StateManagerOptions CreateOptions(string substreamName)
            {
                return new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100_000,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = fileProviders.GetOrAdd(substreamName, _ => new MemoryFileProvider())
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/e2e_chaos3/{substreamName}/tmp"
                    }
                };
            }

            Microsoft.Extensions.Logging.ILoggerFactory CreateBufferedLoggerFactory(string substreamName)
            {
                var provider = logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
                return Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
                {
                    b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug);
                    b.AddProvider(provider);
                });
            }

            _stream = BuildHost("e2e_chaos3", NormalJoinSql, latestData, failures,
                substreamCount: 3, stateOptions: CreateOptions, loggerFactory: CreateBufferedLoggerFactory);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            try
            {
                for (int iteration = 0; iteration < 8; iteration++)
                {
                    var action = random.Next(6);
                    switch (action)
                    {
                        case 0:
                            _generator.Generate(100);
                            break;
                        case 1:
                        case 2:
                        case 3:
                            try
                            {
                                await _stream.Substreams[$"substream_{action - 1}"].CallTrigger("crash", null);
                            }
                            catch
                            {
                            }
                            break;
                        case 4:
                            // Two random substreams crash at the same time
                            var first = random.Next(3);
                            var second = (first + 1 + random.Next(2)) % 3;
                            try
                            {
                                await Task.WhenAll(
                                    _stream.Substreams[$"substream_{first}"].CallTrigger("crash", null),
                                    _stream.Substreams[$"substream_{second}"].CallTrigger("crash", null));
                            }
                            catch
                            {
                            }
                            break;
                        case 5:
                            var stopTask = _stream.StopAsync();
                            var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(60)));
                            if (finished != stopTask)
                            {
                                throw new TimeoutException($"Chaos stop hung in iteration {iteration}");
                            }
                            await stopTask;
                            await _stream.DisposeAsync();
                            _stream = BuildHost("e2e_chaos3", NormalJoinSql, latestData, failures,
                                substreamCount: 3, stateOptions: CreateOptions, loggerFactory: CreateBufferedLoggerFactory);
                            await _stream.StartAsync();
                            break;
                    }

                    _generator.Generate(50);
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
                }
            }
            catch (Exception e)
            {
                foreach (var buffer in logBuffers)
                {
                    buffer.Value.WriteToFile($"./debugwrite/e2e_chaos3_seed{seed}_{buffer.Key}.log");
                }
                throw new Exception($"Three substream chaos failed with seed {seed}", e);
            }
        }

        /// <summary>
        /// Deletes fired right after generating data, so the delete frequently lands while a
        /// checkpoint is being written. The delete must wait for the checkpoint instead of
        /// disposing the state manager under it, and a concurrent stop must also complete
        /// since the delete implies the stop.
        /// </summary>
        [Fact]
        public async Task DeleteDuringActiveCheckpointCompletes()
        {
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_delete_active_checkpoint", NormalJoinSql, latestData, failures);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // New data makes checkpoints run, the delete lands in the middle of them
            _generator.Generate(300);
            var deleteTask = _stream.DeleteAsync();
            var stopTask = _stream.StopAsync();

            var all = Task.WhenAll(deleteTask, stopTask);
            var finished = await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == all, "Delete and stop during an active checkpoint did not complete");
            await all;
        }

        /// <summary>
        /// A join with an equality key AND an inequality condition in the join. The
        /// distributed lane copies must keep the comparison types and the partitioning must
        /// only use the equality key, otherwise the inequality is evaluated as equality and
        /// matching rows land in different partitions, silently producing wrong results.
        /// </summary>
        [Fact]
        public async Task InequalityJoinIsDistributedCorrectly()
        {
            const string inequalityJoinSql = @"
            INSERT INTO output
            SELECT u.userkey FROM users u
            INNER JOIN orders o ON u.userkey = o.userkey AND o.orderkey >= u.userkey;
            ";

            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_inequality_join", inequalityJoinSql, latestData, failures);
            await _stream.StartAsync();

            List<UserKeyRow> Expected() => _generator.Orders
                .Join(_generator.Users, o => o.UserKey, u => u.UserKey, (o, u) => (o, u))
                .Where(x => x.o.OrderKey >= x.u.UserKey)
                .Select(x => new UserKeyRow(x.u.UserKey))
                .ToList();

            await WaitForSinkData(latestData, failures, "substream_0", Expected());

            _generator.Generate(500);
            await WaitForSinkData(latestData, failures, "substream_0", Expected());

            foreach (var order in _generator.Orders.Take(100).ToList())
            {
                _generator.DeleteOrder(order);
            }
            await WaitForSinkData(latestData, failures, "substream_0", Expected());

            Assert.Empty(failures);
        }

        /// <summary>
        /// A distributed view without SCATTER_BY consumed from another substream would be a
        /// broadcast across substreams, which the executor does not support, the data would
        /// silently be dropped and the checkpoints would hang. The plan build must fail with
        /// an actionable error instead.
        /// </summary>
        [Fact]
        public void BroadcastAcrossSubstreamsFailsAtPlanBuild()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));

            var exception = Assert.Throws<InvalidOperationException>(() => sqlPlanBuilder.Sql(@"
            SUBSTREAM sub1;

            CREATE VIEW broadcast_view WITH (DISTRIBUTED = true) AS
            SELECT userkey FROM users;

            INSERT INTO output1 SELECT userkey FROM broadcast_view;

            SUBSTREAM sub2;

            INSERT INTO output2 SELECT userkey FROM broadcast_view;
            "));
            Assert.Contains("SCATTER_BY", exception.Message);
        }

        /// <summary>
        /// A plan that uses substreams but leaves an insert at top level would run that
        /// insert in every substream and silently duplicate its output. The plan build must
        /// reject the mix.
        /// </summary>
        [Fact]
        public void TopLevelInsertMixedWithSubstreamsFailsAtPlanBuild()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));

            var exception = Assert.Throws<InvalidOperationException>(() => sqlPlanBuilder.Sql(@"
            INSERT INTO output1 SELECT userkey FROM users;

            SUBSTREAM sub1;

            INSERT INTO output2 SELECT userkey FROM users;
            "));
            Assert.Contains("substream", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// A stop after a completed delete must complete, the stop task is created before
        /// the deleted state is consulted and has to be finished by it.
        /// </summary>
        [Fact]
        public async Task StopAfterDeleteCompletes()
        {
            _generator.Generate(200);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = BuildHost("e2e_stop_after_delete", NormalJoinSql, latestData, failures);
            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            var deleteTask = _stream.DeleteAsync();
            var finished = await Task.WhenAny(deleteTask, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == deleteTask, "Delete did not complete");
            await deleteTask;

            // Stops after the delete must complete, repeatedly
            for (int i = 0; i < 2; i++)
            {
                var stopTask = _stream.StopAsync();
                finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.True(finished == stopTask, $"Stop {i} after delete did not complete");
                await stopTask;
            }
        }

        private static Storage.StateManager.StateManagerOptions CreateStateOptions(string testName, string substreamName)
        {
            return new Storage.StateManager.StateManagerOptions()
            {
                CachePageCount = 100_000,
                PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = new MemoryFileProvider()
                }),
                DefaultBPlusTreePageSize = 1024,
                DefaultBPlusTreePageSizeBytes = 32 * 1024,
                TemporaryStorageOptions = new Storage.FileCacheOptions()
                {
                    DirectoryPath = $"./data/tempFiles/{testName}/{substreamName}/tmp"
                }
            };
        }

        private List<UserKeyRow> GetExpectedPartition(int partitionId)
        {
            return _generator.Users.Where(x =>
            {
                byte[] bytes = new byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(bytes, x.UserKey);
                var hash = BinaryPrimitives.ReadUInt32BigEndian(XxHash32.Hash(bytes));
                return (int)(hash % 2) == partitionId;
            })
            .Select(x => new UserKeyRow(x.UserKey))
            .ToList();
        }

        private record UserKeyRow(long UserKey);

        private async Task WaitForSinkData<T>(
            ConcurrentDictionary<string, EventBatchData> latestData,
            ConcurrentBag<(string Substream, Exception? Exception)> failures,
            string substreamName,
            List<T> expected,
            bool allowFailures = false)
        {
            Debug.Assert(_stream != null);
            var expectedBatch = BatchConverter.ConvertToBatchSorted(expected, GlobalMemoryManager.Instance);

            var stopwatch = Stopwatch.StartNew();
            while (true)
            {
                if (!allowFailures)
                {
                    var failure = failures.FirstOrDefault(x => x.Exception != null);
                    if (failure.Exception != null)
                    {
                        throw new Exception($"Substream {failure.Substream} failed", failure.Exception);
                    }
                }

                if (latestData.TryGetValue(substreamName, out var actual))
                {
                    try
                    {
                        EventBatchAssertion.Equal(expectedBatch, actual);
                        return;
                    }
                    catch when (stopwatch.Elapsed < TimeSpan.FromSeconds(60))
                    {
                        // Data has not caught up yet, keep waiting
                    }
                }

                if (stopwatch.Elapsed >= TimeSpan.FromSeconds(60))
                {
                    if (!latestData.TryGetValue(substreamName, out var lastSeen))
                    {
                        Assert.Fail($"Substream {substreamName} never produced any output data.");
                    }
                    // Produces the assertion failure message with the last seen data
                    EventBatchAssertion.Equal(expectedBatch, lastSeen);
                    return;
                }

                await Task.Delay(10);
            }
        }
    }
}

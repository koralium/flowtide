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
            TimeSpan? stopDrainTimeout = null)
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
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, crash.CrashCount, watermark => { }, crash.CheckpointsBeforeCrash));
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
                foreach (var substream in _stream.Substreams.Values)
                {
                    if (substream.Scheduler is DefaultStreamScheduler scheduler)
                    {
                        await scheduler.Tick();
                    }
                }

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

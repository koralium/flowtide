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

using BenchmarkDotNet.Attributes;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;
using System.Collections.Concurrent;
using System.Diagnostics;
using EngineDataflowStream = FlowtideDotNet.Base.Engine.DataflowStream;

namespace FlowtideDotNet.Benchmarks.Stream
{
    /// <summary>
    /// A heavier pipeline where distributing over two substreams is expected to win: two
    /// joins and an aggregate that all share the same key, so after the initial scatter on
    /// userkey the lane pushdown runs join, join and aggregate for each hash partition in
    /// parallel without any further shuffles. The per row work of the pipeline dominates the
    /// constant exchange and checkpoint coordination overhead that makes distribution lose on
    /// the small single join benchmark.
    ///
    /// Completion is detected with the watermark: an aggregates group counts refine over
    /// several checkpoints, so row counts are not a safe completion signal, but the sources
    /// are immutable and emit one watermark after their data, and the sink only receives it
    /// after everything before it, including the lanes from the other substream, has been
    /// processed and emitted.
    /// </summary>
    [Config(typeof(BenchmarkConfiguration))]
    public class DistributedJoinAggregateBenchmark
    {
        private const int RowCount = 300_000;
        private const string AggregateJoinSql = @"
            INSERT INTO output
            SELECT u.userkey, count(*) FROM users u
            INNER JOIN orders o ON u.userkey = o.userkey
            INNER JOIN projectmembers pm ON u.userkey = pm.userkey
            GROUP BY u.userkey;
            ";

        private MockDatabase? _db;
        private DatasetGenerator? _generator;
        private int _expectedGroupCount;
        private int _iteration;
        private readonly ConcurrentDictionary<string, EventBatchData> _latestData = new ConcurrentDictionary<string, EventBatchData>();
        private volatile bool _watermarkSeen;
        private volatile bool _complete;
        private EngineDataflowStream? _singleStream;
        private DistributedFlowtideStream? _distributedStream;

        [GlobalSetup]
        public void GlobalSetup()
        {
            // The sources are immutable, the same generated dataset is reused by every
            // iteration, only the streams and their state storage are created per iteration.
            _db = new MockDatabase();
            _generator = new DatasetGenerator(_db);
            _generator.Generate(RowCount);

            var userKeysWithOrders = _generator.Orders.Select(x => x.UserKey).ToHashSet();
            var userKeysWithMembers = _generator.ProjectMembers.Select(x => x.UserKey).ToHashSet();
            _expectedGroupCount = _generator.Users.Count(u => userKeysWithOrders.Contains(u.UserKey) && userKeysWithMembers.Contains(u.UserKey));
        }

        [IterationSetup]
        public void IterationSetup()
        {
            _latestData.Clear();
            _watermarkSeen = false;
            _complete = false;
            _iteration++;
        }

        [IterationCleanup(Target = nameof(JoinAggregateSingleStream))]
        public void AfterSingleStream()
        {
            StreamGraphMetadata.SaveGraphData(nameof(JoinAggregateSingleStream), _singleStream!.GetDiagnosticsGraph());
            DisposeStreams();
        }

        [IterationCleanup(Target = nameof(JoinAggregateDistributedTwoSubstreams))]
        public void AfterDistributed()
        {
            StreamGraphMetadata.SaveGraphData(nameof(JoinAggregateDistributedTwoSubstreams), _distributedStream!.Substreams["substream_0"].GetDiagnosticsGraph());
            foreach (var substream in _distributedStream!.Substreams)
            {
                StreamGraphMetadata.SaveGraphData($"{nameof(JoinAggregateDistributedTwoSubstreams)}_{substream.Key}", substream.Value.GetDiagnosticsGraph());
            }
            DisposeStreams();
        }

        private void DisposeStreams()
        {
            if (_singleStream != null)
            {
                _singleStream.DisposeAsync().AsTask().GetAwaiter().GetResult();
                _singleStream = null;
            }
            if (_distributedStream != null)
            {
                _distributedStream.DisposeAsync().AsTask().GetAwaiter().GetResult();
                _distributedStream = null;
            }
        }

        [Benchmark(Baseline = true)]
        public async Task JoinAggregateSingleStream()
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new MockSourceFactory("*", _db!, true));
            connectorManager.AddSink(new MockSinkFactory("*", data => OnSinkData("single", data), 0, watermark => _watermarkSeen = true));

            var builder = new FlowtideBuilder($"benchagg_single_{_iteration}");
            builder.AddPlan(BuildPlan());
            builder.AddConnectorManager(connectorManager);
            builder.WithStateOptions(CreateStateOptions($"single_{_iteration}"));
            _singleStream = builder.Build();

            await _singleStream.StartAsync();
            await WaitForCompletion("single", TickSingle);
        }

        /// <summary>
        /// The same pipeline in one stream but with the plan optimizers parallelization set
        /// to two, the joins and the aggregate are partitioned into two copies inside the
        /// stream without any substream exchanges, the intra machine counterpart to the
        /// distributed variant.
        /// </summary>
        [Benchmark]
        public async Task JoinAggregateSingleStreamParallel2()
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new MockSourceFactory("*", _db!, true));
            connectorManager.AddSink(new MockSinkFactory("*", data => OnSinkData("single", data), 0, watermark => _watermarkSeen = true));

            var builder = new FlowtideBuilder($"benchagg_par_{_iteration}");
            builder.AddPlan(BuildPlan(), true, new Core.Optimizer.PlanOptimizerSettings()
            {
                Parallelization = 2
            });
            builder.AddConnectorManager(connectorManager);
            builder.WithStateOptions(CreateStateOptions($"par_{_iteration}"));
            _singleStream = builder.Build();

            await _singleStream.StartAsync();
            await WaitForCompletion("single", TickSingle);
        }

        [IterationCleanup(Target = nameof(JoinAggregateSingleStreamParallel2))]
        public void AfterSingleStreamParallel()
        {
            StreamGraphMetadata.SaveGraphData(nameof(JoinAggregateSingleStreamParallel2), _singleStream!.GetDiagnosticsGraph());
            DisposeStreams();
        }

        [Benchmark]
        public async Task JoinAggregateDistributedTwoSubstreams()
        {
            _distributedStream = new DistributedStreamBuilder($"benchagg_dist_{_iteration}")
                .AddPlan(BuildPlan)
                .WithStateOptionsFactory((streamName, substreamName) => CreateStateOptions($"dist_{_iteration}_{substreamName}"))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db!, true));
                    connectorManager.AddSink(new MockSinkFactory("*", data => OnSinkData(substreamName, data), 0, watermark => _watermarkSeen = true));
                    substreamBuilder.AddConnectorManager(connectorManager);
                })
                .DistributeAutomatically(2)
                .Build();

            await _distributedStream.StartAsync();
            await WaitForCompletion("substream_0", TickDistributed);
        }

        private void OnSinkData(string key, EventBatchData data)
        {
            _latestData[key] = data;
            if (_watermarkSeen)
            {
                // The watermark only reaches the sink after everything the sources emitted
                // before it has been processed, the data published at this checkpoint is the
                // final result.
                _complete = true;
            }
        }

        private Plan BuildPlan()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db!));
            sqlPlanBuilder.Sql(AggregateJoinSql);
            return sqlPlanBuilder.GetPlan();
        }

        private static StateManagerOptions CreateStateOptions(string name)
        {
            return new StateManagerOptions()
            {
                CachePageCount = 100_000,
                PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = new MemoryFileProvider()
                }),
                TemporaryStorageOptions = new FileCacheOptions()
                {
                    DirectoryPath = $"./data/tempFiles/bench_distributed_join_agg/{name}/tmp"
                }
            };
        }

        private async Task TickSingle()
        {
            if (_singleStream!.Scheduler is DefaultStreamScheduler scheduler)
            {
                await scheduler.Tick();
            }
        }

        private async Task TickDistributed()
        {
            foreach (var substream in _distributedStream!.Substreams.Values)
            {
                if (substream.Scheduler is DefaultStreamScheduler scheduler)
                {
                    await scheduler.Tick();
                }
            }
        }

        private async Task WaitForCompletion(string sinkKey, Func<Task> tick)
        {
            var stopwatch = Stopwatch.StartNew();
            while (!_complete)
            {
                await tick();
                if (stopwatch.Elapsed > TimeSpan.FromMinutes(5))
                {
                    throw new TimeoutException($"The aggregate result was not complete in time, watermark seen: {_watermarkSeen}.");
                }
                await Task.Delay(10);
            }
            if (!_latestData.TryGetValue(sinkKey, out var batch) || batch.Count != _expectedGroupCount)
            {
                throw new InvalidOperationException($"The result is incomplete, expected {_expectedGroupCount} groups, got {(_latestData.TryGetValue(sinkKey, out var last) ? last.Count : 0)}.");
            }
        }
    }
}

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
    /// Compares the same inner join running as one normal stream against running distributed
    /// over two substreams in the same process. Measures the time from stream start until the
    /// sink has produced the complete join result, so it includes the substream exchange
    /// overhead, the extra checkpoint coordination and the win from joining the two hash
    /// partitions in parallel.
    /// </summary>
    [Config(typeof(BenchmarkConfiguration))]
    public class DistributedJoinBenchmark
    {
        private const int RowCount = 100_000;
        private const string JoinSql = @"
            INSERT INTO output
            SELECT u.userkey FROM users u
            INNER JOIN orders o ON u.userkey = o.userkey;
            ";

        private MockDatabase? _db;
        private DatasetGenerator? _generator;
        private int _expectedCount;
        private int _iteration;
        private readonly ConcurrentDictionary<string, EventBatchData> _latestData = new ConcurrentDictionary<string, EventBatchData>();
        private EngineDataflowStream? _singleStream;
        private DistributedFlowtideStream? _distributedStream;

        [IterationSetup]
        public void IterationSetup()
        {
            _db = new MockDatabase();
            _generator = new DatasetGenerator(_db);
            _generator.Generate(RowCount);
            _expectedCount = _generator.Orders
                .Join(_generator.Users, o => o.UserKey, u => u.UserKey, (o, u) => u.UserKey)
                .Count();
            _latestData.Clear();
            _iteration++;
        }

        [IterationCleanup(Target = nameof(JoinSingleStream))]
        public void AfterSingleStream()
        {
            StreamGraphMetadata.SaveGraphData(nameof(JoinSingleStream), _singleStream!.GetDiagnosticsGraph());
            DisposeStreams();
        }

        [IterationCleanup(Target = nameof(JoinDistributedTwoSubstreams))]
        public void AfterDistributed()
        {
            StreamGraphMetadata.SaveGraphData(nameof(JoinDistributedTwoSubstreams), _distributedStream!.Substreams["substream_0"].GetDiagnosticsGraph());
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
        public async Task JoinSingleStream()
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new MockSourceFactory("*", _db!, true));
            connectorManager.AddSink(new MockSinkFactory("*", data => _latestData["single"] = data, 0, watermark => { }));

            var builder = new FlowtideBuilder($"bench_single_{_iteration}");
            builder.AddPlan(BuildPlan());
            builder.AddConnectorManager(connectorManager);
            builder.WithStateOptions(CreateStateOptions($"single_{_iteration}"));
            _singleStream = builder.Build();

            await _singleStream.StartAsync();
            await WaitForCompleteResult("single", TickSingle);
        }

        [Benchmark]
        public async Task JoinDistributedTwoSubstreams()
        {
            _distributedStream = new DistributedStreamBuilder($"bench_dist_{_iteration}")
                .AddPlan(BuildPlan)
                .WithStateOptionsFactory((streamName, substreamName) => CreateStateOptions($"dist_{_iteration}_{substreamName}"))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db!, true));
                    connectorManager.AddSink(new MockSinkFactory("*", data => _latestData[substreamName] = data, 0, watermark => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                })
                .DistributeAutomatically(2)
                .Build();

            await _distributedStream.StartAsync();
            // The sink lives in the first substream, the other substream sends its join
            // partition there through the exchange.
            await WaitForCompleteResult("substream_0", TickDistributed);
        }

        private Plan BuildPlan()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db!));
            sqlPlanBuilder.Sql(JoinSql);
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
                    DirectoryPath = $"./data/tempFiles/bench_distributed_join/{name}/tmp"
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

        private async Task WaitForCompleteResult(string sinkKey, Func<Task> tick)
        {
            var stopwatch = Stopwatch.StartNew();
            while (true)
            {
                await tick();
                if (_latestData.TryGetValue(sinkKey, out var batch) && batch.Count == _expectedCount)
                {
                    return;
                }
                if (stopwatch.Elapsed > TimeSpan.FromMinutes(5))
                {
                    throw new TimeoutException($"The join result did not reach {_expectedCount} rows in time, last seen {(_latestData.TryGetValue(sinkKey, out var last) ? last.Count : 0)}.");
                }
                await Task.Delay(10);
            }
        }
    }
}

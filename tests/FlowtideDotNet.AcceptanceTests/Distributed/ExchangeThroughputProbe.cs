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
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    /// <summary>
    /// Manual throughput probe for the substream exchange, run explicitly with a filter in
    /// Release. Measures the same join as DistributedJoinBenchmark (whose BenchmarkDotNet
    /// report column crashes) single stream versus distributed, wall clock from start until
    /// the sink holds the complete result. Results are written to exchange_probe_results.txt
    /// in the working directory.
    /// </summary>
    [Trait("Category", "Probe")]
    public class ExchangeThroughputProbe
    {
        private const int RowCount = 200_000;
        private const string JoinSql = @"
            INSERT INTO output
            SELECT u.userkey FROM users u
            INNER JOIN orders o ON u.userkey = o.userkey;
            ";

        private readonly ITestOutputHelper _output;

        public ExchangeThroughputProbe(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task MeasureSingleVsDistributed()
        {
            if (Environment.GetEnvironmentVariable("FLOWTIDE_RUN_PROBES") != "1")
            {
                // Opt-in only: a manual throughput measurement, nine full stream runs over
                // 200k-row datasets is far too slow for the normal suite.
                return;
            }
            var results = new List<string>();
            foreach (var run in new[] { "warmup", "run1", "run2" })
            {
                foreach (var substreams in new[] { 0, 2, 4 })
                {
                    var elapsed = await RunOnce(substreams, $"{run}_{substreams}");
                    var label = substreams == 0 ? "single" : $"dist_{substreams}";
                    var line = $"{run} {label}: {elapsed.TotalMilliseconds:F0} ms ({RowCount * 2 / elapsed.TotalSeconds:F0} rows/s ingested)";
                    results.Add(line);
                    _output.WriteLine(line);
                }
            }
            File.WriteAllLines("exchange_probe_results.txt", results);
        }

        private async Task<TimeSpan> RunOnce(int substreamCount, string name)
        {
            var db = new MockDatabase();
            var generator = new DatasetGenerator(db);
            generator.Generate(RowCount);
            var expectedCount = generator.Orders
                .Join(generator.Users, o => o.UserKey, u => u.UserKey, (o, u) => u.UserKey)
                .Count();
            var latestData = new ConcurrentDictionary<string, EventBatchData>();

            Plan BuildPlan()
            {
                var sqlPlanBuilder = new SqlPlanBuilder();
                sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(db));
                sqlPlanBuilder.Sql(JoinSql);
                return sqlPlanBuilder.GetPlan();
            }

            StateManagerOptions CreateStateOptions(string storageName)
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
                        DirectoryPath = $"./data/tempFiles/exchange_probe/{storageName}/tmp"
                    }
                };
            }

            var stopwatch = Stopwatch.StartNew();
            if (substreamCount == 0)
            {
                var connectorManager = new ConnectorManager();
                connectorManager.AddSource(new MockSourceFactory("*", db, true));
                connectorManager.AddSink(new MockSinkFactory("*", data => latestData["single"] = data, 0, _ => { }));
                var builder = new FlowtideBuilder($"probe_{name}");
                builder.AddPlan(BuildPlan());
                builder.AddConnectorManager(connectorManager);
                builder.WithStateOptions(CreateStateOptions(name));
                var stream = builder.Build();
                await stream.StartAsync();
                try
                {
                    await WaitForCount(latestData, "single", expectedCount, async () =>
                    {
                        if (stream.Scheduler is DefaultStreamScheduler scheduler)
                        {
                            await scheduler.Tick();
                        }
                    });
                    stopwatch.Stop();
                }
                finally
                {
                    await stream.DisposeAsync();
                }
            }
            else
            {
                var stream = new DistributedStreamBuilder($"probe_{name}")
                    .AddPlan(BuildPlan)
                    .WithStateOptionsFactory((_, substreamName) => CreateStateOptions($"{name}_{substreamName}"))
                    .ConfigureSubstream((substreamName, substreamBuilder) =>
                    {
                        var connectorManager = new ConnectorManager();
                        connectorManager.AddSource(new MockSourceFactory("*", db, true));
                        connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, _ => { }));
                        substreamBuilder.AddConnectorManager(connectorManager);
                    })
                    .DistributeAutomatically(substreamCount)
                    .Build();
                await stream.StartAsync();
                try
                {
                    await WaitForCount(latestData, "substream_0", expectedCount, () => Task.CompletedTask);
                    stopwatch.Stop();
                }
                finally
                {
                    await stream.DisposeAsync();
                }
            }
            return stopwatch.Elapsed;
        }

        private static async Task WaitForCount(ConcurrentDictionary<string, EventBatchData> latestData, string key, int expectedCount, Func<Task> tick)
        {
            var deadline = Stopwatch.StartNew();
            while (true)
            {
                await tick();
                if (latestData.TryGetValue(key, out var batch) && batch.Count == expectedCount)
                {
                    return;
                }
                if (deadline.Elapsed > TimeSpan.FromMinutes(5))
                {
                    throw new TimeoutException($"Result did not reach {expectedCount} rows, last {(latestData.TryGetValue(key, out var last) ? last.Count : 0)}.");
                }
                await Task.Delay(5);
            }
        }
    }
}

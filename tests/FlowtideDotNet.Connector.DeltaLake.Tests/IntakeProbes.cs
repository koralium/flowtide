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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using Stowage;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Xunit.Abstractions;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection("Probes")]
    public class IntakeProbes
    {
        private const int Rows = 200_000;
        private const int Seed = 123;
        private static readonly int[] s_batchSizes = { 100, 1_000, 10_000 };
        private static readonly string[] s_variants = { "per row", "bulk" };
        private readonly ITestOutputHelper _output;

        public IntakeProbes(ITestOutputHelper output)
        {
            _output = output;
        }

        // Ordered keys are report-only, the gate uses random keys
        [Theory]
        [InlineData("flat", false)]
        [InlineData("flat", true)]
        [InlineData("nested", false)]
        [InlineData("nested", true)]
        [InlineData("ordered", false)]
        [InlineData("ordered", true)]
        public async Task B1bIntake(string shape, bool spilled)
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var kit = new ProbeKit(_output, $"B1b intake, {shape}, {Cache(spilled)}, {Rows} rows, seed {Seed}");
            foreach (var batchSize in s_batchSizes)
            {
                var batches = Batches(shape, batchSize);
                try
                {
                    var samples = await Measure((variant, _) => Time(variant, batches, spilled));
                    kit.Write($"batch {batchSize} | {Format(samples)}");
                }
                finally
                {
                    foreach (var (batch, weights) in batches)
                    {
                        batch.Dispose();
                        weights.Dispose();
                    }
                }
            }
        }

        // Join and union pass 30 s beyond 60k users
        [Theory]
        [InlineData("filter", false, 200_000)]
        [InlineData("filter", true, 200_000)]
        [InlineData("join", false, 60_000)]
        [InlineData("join", true, 60_000)]
        [InlineData("union", false, 60_000)]
        [InlineData("union", true, 60_000)]
        public async Task B1bPipelines(string pipeline, bool spilled, int users)
        {
            if (!ProbeKit.Enabled)
            {
                return;
            }
            var kit = new ProbeKit(_output, $"B1b pipeline, {pipeline}, {Cache(spilled)}, {users} fixed users");
            foreach (var batchSize in s_batchSizes)
            {
                long wrapperRows = 0;
                long rows = 0;
                var samples = await Measure(async (variant, last) =>
                {
                    var sample = await TimePipeline(pipeline, variant, batchSize, spilled, users);
                    if (last && sample != null)
                    {
                        wrapperRows = sample.Value.WrapperRows;
                        rows = sample.Value.Rows;
                    }
                    return sample?.Sample;
                });
                kit.Write($"source batch {batchSize} | {Format(samples)} | wrapper rows {wrapperRows} of {rows}");
            }
        }

        private static string Cache(bool spilled) => spilled ? "spilled (64 pages)" : "in memory";

        // Warm-up, five alternating runs, late variants stop alone
        private static async Task<(List<double> Ms, List<double> PeakMiB, bool Late)[]> Measure(Func<int, bool, Task<(double Ms, long Peak)?>> time)
        {
            var samples = s_variants.Select(_ => (Ms: new List<double>(), PeakMiB: new List<double>(), Late: false)).ToArray();
            for (int run = 0; run <= ProbeKit.Runs; run++)
            {
                for (int k = 0; k < s_variants.Length; k++)
                {
                    var v = (run + k) % s_variants.Length;
                    if (samples[v].Late)
                    {
                        continue;
                    }
                    var sample = await time(v, run == ProbeKit.Runs);
                    if (sample == null)
                    {
                        samples[v].Late = true;
                    }
                    else if (run > 0)
                    {
                        samples[v].Ms.Add(sample.Value.Ms);
                        samples[v].PeakMiB.Add(sample.Value.Peak / 1048576.0);
                    }
                }
            }
            return samples;
        }

        private static string Format((List<double> Ms, List<double> PeakMiB, bool Late)[] samples)
        {
            var perRow = samples[0].Late ? double.NaN : ProbeKit.Median(samples[0].Ms);
            return string.Join(" | ", s_variants.Select((name, v) => samples[v].Late
                ? $"{name} did not finish within {ProbeKit.DidNotFinish.TotalSeconds:0} s"
                : $"{name} {ProbeKit.Spread(samples[v].Ms)} ms ({(double.IsNaN(perRow) ? "per row did not finish" : $"{perRow / ProbeKit.Median(samples[v].Ms):0.00}x")}), peak working set +{ProbeKit.Spread(samples[v].PeakMiB)} MiB"));
        }

        private static DeleteLimits Limits(int variant)
        {
            return new DeleteLimits() { BulkIntake = variant != 0 };
        }

        // Intake time on a new tree, null if late
        private static async Task<(double Ms, long Peak)?> Time(int variant, List<(EventBatchData Batch, PrimitiveList<int> Weights)> batches, bool spilled)
        {
            using var storage = new FileCachePersistentStorage(new FileCacheOptions());
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = spilled ? 64 : 1_000_000,
                MinCachePageCount = spilled ? 64 : 1000,
                PersistentStorage = storage
            }, NullLoggerFactory.Instance, new Meter("b1b"), "b1b", GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            var width = batches[0].Batch.Columns.Count;
            var tree = await BulkIntakeTests.Tree(stateManager.GetOrCreateClient("sink"), "temporary", width);
            var intake = new TemporaryTreeIntake(tree, width, Limits(variant));
            var limit = Stopwatch.GetTimestamp() + (long)(ProbeKit.DidNotFinish.TotalSeconds * Stopwatch.Frequency);
            await using var peak = new PeakWorkingSet();
            foreach (var (batch, weights) in batches)
            {
                await intake.Apply(batch, weights);
                if (Stopwatch.GetTimestamp() > limit)
                {
                    return null;
                }
            }
            await peak.DisposeAsync();
            return (intake.IntakeTicks * 1000.0 / Stopwatch.Frequency, peak.Peak);
        }

        // B1's rows: 10 % retractions, 5 % repeats inside the batch
        private static List<(EventBatchData, PrimitiveList<int>)> Batches(string shape, int batchSize)
        {
            var random = new Random(Seed);
            var written = new List<IDataValue[]>();
            var header = StructHeader.Create("a", "b");
            var batches = new List<(EventBatchData, PrimitiveList<int>)>();
            for (int offset = 0; offset < Rows; offset += batchSize)
            {
                var rows = new List<IDataValue[]>();
                var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
                var batchStart = written.Count;
                for (int i = 0; i < batchSize; i++)
                {
                    var kind = random.Next(100);
                    if (kind < 10 && written.Count > 0)
                    {
                        rows.Add(written[random.Next(written.Count)]);
                        weights.Add(-1);
                        continue;
                    }
                    if (kind < 15 && written.Count > batchStart)
                    {
                        rows.Add(written[random.Next(batchStart, written.Count)]);
                        weights.Add(1);
                        continue;
                    }
                    // Few first values, so the nested columns compare
                    var row = shape == "nested"
                        ? new IDataValue[]
                        {
                            new Int64Value(random.Next(50)),
                            new StructValue(header, new Int64Value(random.Next(1000)), new StringValue($"s{random.Next(1000)}")),
                            new ListValue(new Int64Value(random.Next(10)), new Int64Value(random.Next(10))),
                            new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("k"), new Int64Value(random.Next(1_000_000))))
                        }
                        : new IDataValue[]
                        {
                            shape == "ordered" ? new Int64Value(offset + i) : new Int64Value(random.NextInt64()),
                            new StringValue($"s{random.Next(1_000_000)}"),
                            new DoubleValue(random.NextDouble()),
                            new TimestampTzValue(DateTime.UnixEpoch.AddSeconds(random.Next(1_000_000_000)))
                        };
                    written.Add(row);
                    rows.Add(row);
                    weights.Add(1);
                }
                var columns = new IColumn[rows[0].Length];
                for (int c = 0; c < columns.Length; c++)
                {
                    var column = Column.Create(GlobalMemoryManager.Instance);
                    foreach (var row in rows)
                    {
                        column.Add(row[c]);
                    }
                    columns[c] = column;
                }
                batches.Add((new EventBatchData(columns), weights));
            }
            return batches;
        }

        // Sink intake over load and retraction, null if late
        private static async Task<((double Ms, long Peak) Sample, long WrapperRows, long Rows)?> TimePipeline(string pipeline, int variant, int batchSize, bool spilled, int userCount)
        {
            var testName = $"b1b_{pipeline}_{variant}_{ScratchOwners.ShortId()}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.DeleteLimitsOverride = Limits(variant));
            stream.SourceBatchSize = batchSize;
            if (spilled)
            {
                stream.CachePageCount = 64;
                stream.MinCachePageCount = 64;
            }
            // Fixed rows, every hundredth user is wide and fans out
            var users = Enumerable.Range(1, userCount).Select(x => new User() { UserKey = x, FirstName = x % 100 == 1 ? new string('w', 500) : $"First{x}", Visits = x % 4 }).ToList();
            stream.AddUsers(users);
            foreach (var user in users.Where(x => x.UserKey % 2 == 1))
            {
                for (int i = 0; i < (user.UserKey % 100 == 1 ? 20 : 2); i++)
                {
                    stream.AddOrder(new Order() { OrderKey = user.UserKey * 100 + i, UserKey = user.UserKey });
                }
            }
            await stream.StartStream(pipeline switch
            {
                "filter" => "CREATE TABLE t (userkey INT, name STRING); INSERT INTO t SELECT userKey AS userkey, firstName AS name FROM users WHERE visits > 1",
                "join" => "CREATE TABLE t (userkey INT, name STRING, orderkey INT); INSERT INTO t SELECT u.userKey AS userkey, u.firstName AS name, o.orderKey AS orderkey FROM users u LEFT JOIN orders o ON u.userKey = o.userKey",
                _ => "CREATE TABLE t (userkey INT, name STRING); INSERT INTO t SELECT userKey AS userkey, firstName AS name FROM users WHERE visits > 1 UNION ALL SELECT userKey AS userkey, firstName AS name FROM users"
            });
            await using var peak = new PeakWorkingSet();
            // One limit for the load and the retraction together
            var deadline = DateTime.UtcNow + ProbeKit.DidNotFinish;
            try
            {
                await WaitForVersion(storage, "t", stream, 0, deadline - DateTime.UtcNow);
                stream.DeleteUsers(users.Where(x => x.UserKey % 10 == 0).ToList());
                await WaitForVersion(storage, "t", stream, 1, deadline - DateTime.UtcNow);
            }
            catch (TimeoutException)
            {
                return null;
            }
            await peak.DisposeAsync();
            var intakes = stream.Sinks.Select(x => x.Intake!).ToList();
            var ms = intakes.Sum(x => x.IntakeTicks) * 1000.0 / Stopwatch.Frequency;
            return ((ms, peak.Peak), intakes.Sum(x => x.WrapperRows), intakes.Sum(x => x.BulkRows + x.PerRowRows));
        }
    }
}

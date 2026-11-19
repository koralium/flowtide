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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;
using Xunit.Abstractions;
using Host = FlowtideDotNet.Core.Tests.MultiJoin.MultiJoinAllocationTests.Host;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinPairConditionPerfTests
    {
        private readonly ITestOutputHelper _output;

        public MultiJoinPairConditionPerfTests(ITestOutputHelper output)
        {
            _output = output;
        }

        private const int RowsPerMessage = 256;

        // FLOWTIDE_MULTIJOIN_PERF_SCALE: times the keys and the changed rows, so a phase runs long enough to time
        private static int Scale => int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_SCALE"), out int scale) ? scale : 1;
        private const int PeriodLength = 10;

        private static ReadRelation Read(string name, int columns)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { name } },
                BaseSchema = new NamedStruct()
                {
                    Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList(),
                    Struct = new Struct() { Types = Enumerable.Range(0, columns).Select(x => (SubstraitBaseType)new AnyType()).ToList() }
                }
            };
        }

        private static FieldReference Field(int index) => new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = index } };

        // t1(k, date, v) left join t2(k, start, end) on the key and start <= date < end, left join t3(k, v) on the key of t1
        private static (MergeJoinRelation periods, MergeJoinRelation root) Plan()
        {
            var periods = new MergeJoinRelation()
            {
                Type = JoinType.Left,
                Left = Read("t1", 3),
                Right = Read("t2", 3),
                LeftKeys = new List<FieldReference>() { Field(0), Field(1) },
                RightKeys = new List<FieldReference>() { Field(3), Field(4) },
                ComparisonTypes = new List<JoinComparisonType>() { JoinComparisonType.Equal, JoinComparisonType.GreaterThanOrEqual },
                PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.LessThan,
                    Arguments = new List<Expression>() { Field(1), Field(5) }
                }
            };
            var root = new MergeJoinRelation()
            {
                Type = JoinType.Left,
                Left = periods,
                Right = Read("t3", 2),
                LeftKeys = new List<FieldReference>() { Field(0) },
                RightKeys = new List<FieldReference>() { Field(6) }
            };
            return (periods, root);
        }

        private static List<StreamMessage<StreamEventBatch>> Messages(List<(long[] row, int weight)> rows, int columnCount)
        {
            var allocator = GlobalMemoryManager.Instance;
            var messages = new List<StreamMessage<StreamEventBatch>>();
            for (int at = 0; at < rows.Count; at += RowsPerMessage)
            {
                int count = Math.Min(RowsPerMessage, rows.Count - at);
                var columns = new IColumn[columnCount];
                for (int c = 0; c < columnCount; c++)
                {
                    var column = Column.Create(allocator);
                    for (int r = 0; r < count; r++)
                    {
                        column.Add(new Int64Value(rows[at + r].row[c]));
                    }
                    columns[c] = column;
                }
                var weights = new PrimitiveList<int>(allocator);
                var iterations = new PrimitiveList<uint>(allocator);
                for (int r = 0; r < count; r++)
                {
                    weights.Add(rows[at + r].weight);
                    iterations.Add(0);
                }
                var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
                batch.Data.Rent(1);
                messages.Add(new StreamMessage<StreamEventBatch>(batch, 1));
            }
            return messages;
        }

        private sealed class Phase
        {
            public required string Name { get; init; }
            public required int Target { get; init; }
            public required List<(long[] row, int weight)> Rows { get; init; }
            public required int Columns { get; init; }
        }

        // The same rows for both runs: every key has the periods [p * 10, p * 10 + 8) and one t1 row per period, nine of ten inside it
        private static List<Phase> Phases(int keys, int periods)
        {
            var random = new Random(7);
            var t1 = new List<(long[], int)>();
            var t2 = new List<(long[], int)>();
            var t3 = new List<(long[], int)>();
            for (int k = 0; k < keys; k++)
            {
                t3.Add((new long[] { k, k }, 1));
                for (int p = 0; p < periods; p++)
                {
                    long start = p * PeriodLength;
                    t2.Add((new long[] { k, start, start + 8 }, 1));
                    t1.Add((new long[] { k, start + (p % 10 == 9 ? 9 : 3), 0 }, 1));
                }
            }
            // A period ends before the row in it, and comes back
            var shorter = new List<(long[], int)>();
            var back = new List<(long[], int)>();
            // A row gets another value, its date stays
            var values = new List<(long[], int)>();
            int changes = Math.Min(1024 * Scale, keys * periods / 4);
            // Every period at most once
            var picked = new HashSet<(int, long)>();
            while (picked.Count < changes)
            {
                picked.Add((random.Next(keys), random.Next(periods) * PeriodLength));
            }
            foreach (var (k, start) in picked)
            {
                shorter.Add((new long[] { k, start, start + 8 }, -1));
                shorter.Add((new long[] { k, start, start + 2 }, 1));
                back.Add((new long[] { k, start, start + 2 }, -1));
                back.Add((new long[] { k, start, start + 8 }, 1));
                long date = start + (start / PeriodLength % 10 == 9 ? 9 : 3);
                values.Add((new long[] { k, date, 0 }, -1));
                values.Add((new long[] { k, date, 1 }, 1));
            }
            return new List<Phase>()
            {
                new Phase() { Name = "load t3", Target = 2, Rows = t3, Columns = 2 },
                new Phase() { Name = "load t2", Target = 1, Rows = t2, Columns = 3 },
                new Phase() { Name = "load t1", Target = 0, Rows = t1, Columns = 3 },
                new Phase() { Name = "periods end earlier", Target = 1, Rows = shorter, Columns = 3 },
                new Phase() { Name = "periods come back", Target = 1, Rows = back, Columns = 3 },
                new Phase() { Name = "t1 values change", Target = 0, Rows = values, Columns = 3 },
            };
        }

        private static async Task<List<(string name, int rows, long outputRows, double seconds, long managed)>> Run(ITargetBlock<IStreamEvent>[] targets, BufferBlock<IStreamEvent> output, List<Phase> phases)
        {
            var results = new List<(string, int, long, double, long)>();
            long time = 1;
            foreach (var phase in phases)
            {
                var messages = Messages(phase.Rows, phase.Columns);
                GC.Collect();
                long managedBefore = GC.GetTotalAllocatedBytes(true);
                long scannedBefore = Interlocked.Read(ref MultiJoinTreeLookup.ScannedForTests);
                long rangesBefore = Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests);
                var watch = System.Diagnostics.Stopwatch.StartNew();
                foreach (var message in messages)
                {
                    await targets[phase.Target].SendAsync(message);
                }
                foreach (var target in targets)
                {
                    await target.SendAsync(new Checkpoint(time, time + 1, 1));
                }
                time++;
                long outputRows = await MultiJoinAllocationTests.Drain(output);
                watch.Stop();
                Counts.Add($"{phase.Name}: {Probe?.Invoke()} rows met by counting lookups {Interlocked.Read(ref MultiJoinTreeLookup.ScannedForTests) - scannedBefore}, lookups with a range {Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests) - rangesBefore}");
                results.Add((phase.Name, phase.Rows.Count, outputRows, watch.Elapsed.TotalSeconds, GC.GetTotalAllocatedBytes(true) - managedBefore));
            }
            return results;
        }

        /// <summary>
        /// Prints rows per second for the range join as ordinary joins and as a multi join, it asserts only that both give the same number of rows.
        /// </summary>
        [Theory]
        [InlineData(20000, 1)]
        [InlineData(2000, 10)]
        [InlineData(200, 100)]
        [InlineData(20, 1000)]
        public async Task PrintRangeJoinThroughput(int keys, int periods)
        {
            if (Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF") == null)
            {
                return;
            }
            var options = new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
            // FLOWTIDE_MULTIJOIN_PERF_ROUNDS=0: messages one by one, so the run does not depend on what happened to queue up
            // FLOWTIDE_MULTIJOIN_PERF_RANGE=0: no lookup takes a range into its key
            bool roundsBefore = MultiJoinOperator.Rounds;
            bool rangeBefore = MultiJoinPlanBuilder.RangeKeys;
            MultiJoinOperator.Rounds = Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_ROUNDS") != "0";
            MultiJoinPlanBuilder.RangeKeys = Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_RANGE") != "0";
            try
            {
                await Compare(keys * Scale, periods, options);
            }
            finally
            {
                MultiJoinOperator.Rounds = roundsBefore;
                MultiJoinPlanBuilder.RangeKeys = rangeBefore;
            }
        }

        /// <summary>
        /// Prints rows per second for an inner join with a comparison beside its key, a join that is a multi join without the setting.
        /// </summary>
        [Theory]
        [InlineData(2000, 100, 10, 2)]
        [InlineData(2000, 100, 50, 2)]
        [InlineData(2000, 100, 100, 2)]
        [InlineData(20000, 4, 50, 2)]
        [InlineData(2000, 100, 10, 12)]
        [InlineData(2000, 100, 50, 12)]
        public async Task PrintInnerComparisonThroughput(int keys, int rowsPerKey, int passPercent, int columns)
        {
            if (Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF") == null)
            {
                return;
            }
            keys *= Scale;
            var options = new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
            bool roundsBefore = MultiJoinOperator.Rounds;
            MultiJoinOperator.Rounds = Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_PERF_ROUNDS") != "0";
            try
            {
                // a(k, v) join b(k, v) on a.k = b.k and a.v < b.v, join c(k, x) on a.k = c.k
                var ab = new MergeJoinRelation()
                {
                    Type = JoinType.Inner,
                    Left = Read("a", 2),
                    Right = Read("b", columns),
                    LeftKeys = new List<FieldReference>() { Field(0) },
                    RightKeys = new List<FieldReference>() { Field(2) },
                    PostJoinFilter = new ScalarFunction()
                    {
                        ExtensionUrn = FunctionsComparison.Urn,
                        ExtensionName = FunctionsComparison.LessThan,
                        Arguments = new List<Expression>() { Field(1), Field(3) }
                    }
                };
                var root = new MergeJoinRelation()
                {
                    Type = JoinType.Inner,
                    Left = ab,
                    Right = Read("c", 2),
                    LeftKeys = new List<FieldReference>() { Field(0) },
                    RightKeys = new List<FieldReference>() { Field(2 + columns) }
                };
                var a = new List<(long[], int)>();
                var b = new List<(long[], int)>();
                var c = new List<(long[], int)>();
                // A row of a passes the comparison with the given share of the rows of its key in b
                long value = rowsPerKey - rowsPerKey * passPercent / 100 - 1;
                for (int k = 0; k < keys; k++)
                {
                    c.Add((new long[] { k, k }, 1));
                    a.Add((new long[] { k, value }, 1));
                    for (int r = 0; r < rowsPerKey; r++)
                    {
                        var row = new long[columns];
                        row[0] = k;
                        row[1] = r;
                        b.Add((row, 1));
                    }
                }
                var phases = new List<Phase>()
                {
                    new Phase() { Name = "load c", Target = 2, Rows = c, Columns = 2 },
                    new Phase() { Name = "load b", Target = 1, Rows = b, Columns = columns },
                    new Phase() { Name = "load a", Target = 0, Rows = a, Columns = 2 },
                };
                using var host = new Host();
                var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: false);
                Assert.NotNull(plan);
                PrintLayout(plan);
                {
                    using var firstHost = new Host();
                    using var secondHost = new Host();
                    var chainOutput = new BufferBlock<IStreamEvent>();
                    var first = new ColumnBulkMergeJoin(ab, firstHost.FunctionsRegister, options);
                    var second = new ColumnBulkMergeJoin(root, secondHost.FunctionsRegister, options);
                    second.LinkTo(chainOutput);
                    await secondHost.InitializeOperator(second);
                    first.LinkTo(second.Targets[0]);
                    await firstHost.InitializeOperator(first);
                    try
                    {
                        foreach (var m in await Run(new ITargetBlock<IStreamEvent>[] { first.Targets[0], first.Targets[1], second.Targets[1] }, chainOutput, phases))
                        {
                            _output.WriteLine($"PERF innerchain, {keys} keys x {rowsPerKey} rows of {columns} columns, {passPercent}% pass, {m.name} ({m.rows} rows): {m.rows / m.seconds:F0} rows/s; output rows {m.outputRows}");
                        }
                    }
                    finally
                    {
                        await first.DisposeAsync();
                        await second.DisposeAsync();
                    }
                }
                var output = new BufferBlock<IStreamEvent>();
                var op = new MultiJoinOperator(plan, host.FunctionsRegister, options);
                op.LinkTo(output);
                await host.InitializeOperator(op);
                try
                {
                    var results = await Run(op.Targets, output, phases);
                    _output.WriteLine($"PERF innerinfo, filtered step runs {op.FilteredRunsForTests}");
                    foreach (var m in results)
                    {
                        _output.WriteLine($"PERF inner, {keys} keys x {rowsPerKey} rows of {columns} columns, {passPercent}% pass, {m.name} ({m.rows} rows): {m.rows / m.seconds:F0} rows/s, {m.seconds * 1000:F0} ms, rows of b met per second {(m.name == "load a" ? (long)m.rows * rowsPerKey / m.seconds : 0):F0}; managed bytes per row {m.managed / (double)m.rows:F0}; output rows {m.outputRows}");
                    }
                }
                finally
                {
                    await op.DisposeAsync();
                }
            }
            finally
            {
                MultiJoinOperator.Rounds = roundsBefore;
            }
        }

        // Per phase of the last run what the lookups counted
        private static readonly List<string> Counts = new List<string>();
        private static Func<string>? Probe;

        private void PrintLayout(MultiJoinPlan plan)
        {
            for (int i = 0; i < plan.Inputs.Count; i++)
            {
                var input = plan.Inputs[i];
                _output.WriteLine($"PERF layout, range keys {plan.RangeKeys}, input {i}: primary [{string.Join(",", input.PrimaryOrder)}], key lists {string.Join(" ", input.KeyLists.Select(x => "[" + string.Join(",", x) + "]"))}, second indexes {string.Join(" ", input.SecondaryLayouts.Select(x => "[" + string.Join(",", x) + "]"))}");
            }
        }

        private async Task Compare(int keys, int periods, ExecutionDataflowBlockOptions options)
        {
            List<(string name, int rows, long outputRows, double seconds, long managed)> chain;
            {
                using var firstHost = new Host();
                using var secondHost = new Host();
                var (periodsJoin, root) = Plan();
                var output = new BufferBlock<IStreamEvent>();
                var first = new ColumnBulkMergeJoin(periodsJoin, firstHost.FunctionsRegister, options);
                var second = new ColumnBulkMergeJoin(root, secondHost.FunctionsRegister, options);
                second.LinkTo(output);
                await secondHost.InitializeOperator(second);
                first.LinkTo(second.Targets[0]);
                await firstHost.InitializeOperator(first);
                try
                {
                    chain = await Run(new ITargetBlock<IStreamEvent>[] { first.Targets[0], first.Targets[1], second.Targets[1] }, output, Phases(keys, periods));
                }
                finally
                {
                    await first.DisposeAsync();
                    await second.DisposeAsync();
                }
            }
            List<(string name, int rows, long outputRows, double seconds, long managed)> multi;
            {
                using var host = new Host();
                var (_, root) = Plan();
                var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true);
                Assert.NotNull(plan);
                PrintLayout(plan);
                var output = new BufferBlock<IStreamEvent>();
                var op = new MultiJoinOperator(plan, host.FunctionsRegister, options);
                op.LinkTo(output);
                await host.InitializeOperator(op);
                try
                {
                    Counts.Clear();
                    Probe = () => string.Join(" ", op.PadTables.Select((x, i) => $"table {i} kept input {x.KeptInput}: keys added {x.KeysAdded}, leaves drained {x.LeavesDrained};"));
                    multi = await Run(op.Targets, output, Phases(keys, periods));
                    Probe = null;
                    foreach (var line in Counts)
                    {
                        _output.WriteLine($"PERF counts, {keys} keys x {periods} periods, filtered step runs so far {op.FilteredRunsForTests}, {line}");
                    }
                }
                finally
                {
                    await op.DisposeAsync();
                }
            }
            for (int i = 0; i < chain.Count; i++)
            {
                var c = chain[i];
                var m = multi[i];
                _output.WriteLine($"PERF range, rounds {MultiJoinOperator.Rounds}, range {(MultiJoinPlanBuilder.RangeKeys ? "on" : "off")}, {keys} keys x {periods} periods, {c.name} ({c.rows} rows): chain {c.rows / c.seconds:F0} rows/s, multi join {m.rows / m.seconds:F0} rows/s, ratio {c.seconds / m.seconds:F2}; managed bytes per row: chain {c.managed / (double)c.rows:F0}, multi join {m.managed / (double)m.rows:F0}; output rows {c.outputRows} and {m.outputRows}");
            }
            for (int i = 0; i < chain.Count; i++)
            {
                Assert.Equal(chain[i].outputRows, multi[i].outputRows);
            }
        }
    }
}

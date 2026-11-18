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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinChainTests
    {
        public enum Values
        {
            Integers,
            Strings,
            Doubles,
            Decimals,
            Timestamps,
            Mixed
        }

        private static readonly string[] Plans = new[]
        {
            "chain", "star", "nestedKeys",
            "left:chain", "left:star", "left:joinedRight", "left:nested", "right:chain", "right:joinedRight", "outer:chain", "outer:star", "outer:nested",
            "markRoot", "markBelowInner", "markUnderLeft", "markUnderOuter", "markInMark", "mixedNested", "outerOverInner",
            "checkChain", "checkEqualOtherInput", "checkUnderLeft", "checkOverOuter", "leftOnly", "leftOnlyJoinedRight", "rightOnly", "rightOnlyJoinedLeft",
            "pushedIntoJoinedRight", "pushedIntoEmittedRight", "pushedIntoJoinedLeftOfRight"
        };

        public static IEnumerable<object[]> Cases()
        {
            int seeds = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_MULTIJOIN_SEEDS"), out var count) ? count : 2;
            foreach (var plan in Plans)
            {
                // Conditions compare by order, which only numbers of one kind keep
                var kinds = plan.StartsWith("check") || plan.Contains("Only") || plan.StartsWith("pushed")
                    ? new[] { Values.Integers }
                    : Enum.GetValues<Values>();
                foreach (var kind in kinds)
                {
                    for (int seed = 1; seed <= seeds; seed++)
                    {
                        yield return new object[] { plan, kind, seed };
                    }
                }
            }
        }

        // The same value written in more than one way where the type has several: both signs of zero, two scales
        private static IDataValue Encode(long? logical, Values kind, Random random)
        {
            if (logical == null)
            {
                return NullValue.Instance;
            }
            long value = logical.Value;
            switch (kind)
            {
                case Values.Strings:
                    return new StringValue("s" + value);
                case Values.Doubles:
                    if (value == 0)
                    {
                        return new DoubleValue(random.Next(2) == 0 ? 0.0 : -0.0);
                    }
                    return new DoubleValue(value == 1 ? double.NaN : value * 0.5);
                case Values.Decimals:
                    return new DecimalValue(random.Next(2) == 0 ? new decimal((int)value * 10 + 5, 0, 0, false, 1) : new decimal((int)value * 100 + 50, 0, 0, false, 2));
                case Values.Timestamps:
                    return new TimestampTzValue(new DateTime(2026, 1, 1).AddHours(value));
                case Values.Mixed:
                    return (value % 3) switch
                    {
                        0 => new Int64Value(value),
                        1 => new StringValue("s" + value),
                        _ => new DoubleValue(value * 0.5)
                    };
                default:
                    return new Int64Value(value);
            }
        }

        // By value: rows that compare equal are stored as one row, which spelling of it comes out depends on what arrived first
        private static string Render(IDataValue value)
        {
            if (value.IsNull)
            {
                return "NULL";
            }
            return value.Type switch
            {
                ArrowTypeId.Int64 => "i" + value.AsLong,
                ArrowTypeId.String => value.AsString.ToString(),
                ArrowTypeId.Double => "d" + (value.AsDouble == 0 ? 0.0 : value.AsDouble).ToString("R", System.Globalization.CultureInfo.InvariantCulture),
                ArrowTypeId.Decimal128 => "m" + (value.AsDecimal / 1.0000000000000000000000000000m).ToString(System.Globalization.CultureInfo.InvariantCulture),
                ArrowTypeId.Boolean => value.AsBool.ToString(),
                ArrowTypeId.Timestamp => "t" + value.AsTimestamp.ticks,
                _ => throw new InvalidOperationException($"No rendering for {value.Type}")
            };
        }

        private static StreamMessage<StreamEventBatch> CreateMessage(List<(IDataValue[] row, int weight)> rows, int columnCount)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                var column = Column.Create(allocator);
                foreach (var (row, _) in rows)
                {
                    column.Add(row[c]);
                }
                columns[c] = column;
            }
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            foreach (var (_, weight) in rows)
            {
                weights.Add(weight);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        // One ordinary join per join of the plan, each with storage of its own. The leaves come out left to right.
        private static async Task<ColumnBulkMergeJoin> BuildChain(MergeJoinRelation join, ITargetBlock<IStreamEvent> output, List<ITargetBlock<IStreamEvent>> leaves, List<MultiJoinAllocationTests.Host> hosts, List<int> columnCounts, List<IAsyncDisposable> operators)
        {
            var host = new MultiJoinAllocationTests.Host();
            hosts.Add(host);
            var op = new ColumnBulkMergeJoin(join, host.FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            operators.Add(op);
            op.LinkTo(output);
            await host.InitializeOperator(op);
            var sides = new[] { join.Left, join.Right };
            for (int side = 0; side < 2; side++)
            {
                if (sides[side] is MergeJoinRelation child)
                {
                    await BuildChain(child, op.Targets[side], leaves, hosts, columnCounts, operators);
                }
                else
                {
                    leaves.Add(op.Targets[side]);
                    columnCounts.Add(sides[side].OutputLength);
                }
            }
            return op;
        }

        private static async Task Drain(BufferBlock<IStreamEvent> output, Task completion, Dictionary<string, long> rows, string what)
        {
            while (true)
            {
                var receive = output.ReceiveAsync(TimeSpan.FromSeconds(30));
                if (await Task.WhenAny(receive, completion) != receive)
                {
                    await completion;
                    throw new InvalidOperationException($"The {what} completed before its checkpoint came out");
                }
                var received = await receive;
                if (received is ICheckpointEvent)
                {
                    return;
                }
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    var data = message.Data.Data;
                    var columns = data.EventBatchData.Columns;
                    for (int i = 0; i < data.Count; i++)
                    {
                        var key = string.Join("|", Enumerable.Range(0, columns.Count).Select(c => Render(columns[c].GetValueAt(i, default))));
                        rows.TryGetValue(key, out var sum);
                        sum += data.Weights.Get(i);
                        if (sum == 0)
                        {
                            rows.Remove(key);
                        }
                        else
                        {
                            rows[key] = sum;
                        }
                    }
                    data.Return();
                }
            }
        }

        [Theory]
        [MemberData(nameof(Cases))]
        public async Task SameRowsAsAChainOfOrdinaryJoins(string planName, Values kind, int seed)
        {
            var hosts = new List<MultiJoinAllocationTests.Host>();
            var operators = new List<IAsyncDisposable>();
            // Every other seed stores a batch while it is being joined, whatever its size
            int minRowsBefore = MultiJoinOperator.ParallelInsertMinRows;
            MultiJoinOperator.ParallelInsertMinRows = seed % 2 == 0 ? 1 : minRowsBefore;
            // The other seeds look every later join up beside the one before it, as when a lookup waits for a page
            bool lookupsBefore = MultiJoinOperator.ParallelLookupsAlways;
            MultiJoinOperator.ParallelLookupsAlways = seed % 2 == 1;
            try
            {
                var chainOutput = new BufferBlock<IStreamEvent>();
                var chainLeaves = new List<ITargetBlock<IStreamEvent>>();
                var columnCounts = new List<int>();
                var chainRoot = await BuildChain(MultiJoinSignedTests.NamedPlan(planName), chainOutput, chainLeaves, hosts, columnCounts, operators);

                var plan = MultiJoinPlanBuilder.Discover(MultiJoinSignedTests.NamedPlan(planName), 9);
                Assert.NotNull(plan);
                Assert.Equal(chainLeaves.Count, plan.Inputs.Count);
                // Only c is filtered, and only in the plans with a pushed condition
                var filtered = plan.Inputs.Where(x => x.Relation is FilterRelation).ToList();
                Assert.Equal(planName.StartsWith("pushed") ? 1 : 0, filtered.Count);
                Assert.All(filtered, x => Assert.Equal("c", Assert.IsType<ReadRelation>(((FilterRelation)x.Relation).Input).NamedTable.Names[0]));
                var multiHost = new MultiJoinAllocationTests.Host();
                hosts.Add(multiHost);
                var multiOutput = new BufferBlock<IStreamEvent>();
                var multi = new MultiJoinOperator(plan, multiHost.FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
                operators.Add(multi);
                multi.LinkTo(multiOutput);
                await multiHost.InitializeOperator(multi);
                // A condition the plan turned into a filter on an input, as the filter operator in front of it would run it
                var filters = plan.Inputs.Select(x => x.Relation is FilterRelation filter ? ColumnBooleanCompiler.Compile(filter.Condition, multiHost.FunctionsRegister) : null).ToArray();

                var random = new Random(seed * 31 + planName.Length);
                var chainRows = new Dictionary<string, long>();
                var multiRows = new Dictionary<string, long>();
                var sent = columnCounts.Select(x => new List<IDataValue[]>()).ToArray();
                int keyDomain = planName.EndsWith("star") ? 2 : 4;
                bool any = false;
                for (int round = 1; round <= 10; round++)
                {
                    int batches = random.Next(1, 5);
                    for (int b = 0; b < batches; b++)
                    {
                        int input = random.Next(columnCounts.Count);
                        var rows = new List<(IDataValue[] row, int weight)>();
                        int rowCount = random.Next(1, 25);
                        for (int r = 0; r < rowCount; r++)
                        {
                            if (sent[input].Count > 0 && random.Next(3) == 0)
                            {
                                // A row that exists: retracted, repeated or sent without weight
                                rows.Add((sent[input][random.Next(sent[input].Count)], random.Next(-2, 2)));
                                continue;
                            }
                            var row = new IDataValue[columnCounts[input]];
                            for (int c = 0; c < row.Length; c++)
                            {
                                row[c] = Encode(random.Next(12) == 0 ? null : random.Next(keyDomain), kind, random);
                            }
                            sent[input].Add(row);
                            rows.Add((row, random.Next(10) == 0 ? -1 : random.Next(1, 4)));
                        }
                        await chainLeaves[input].SendAsync(CreateMessage(rows, columnCounts[input]));
                        if (filters[input] != null)
                        {
                            var unfiltered = CreateMessage(rows, columnCounts[input]);
                            var data = unfiltered.Data.Data.EventBatchData;
                            rows = rows.Where((_, index) => filters[input]!(data, index)).ToList();
                            unfiltered.Data.Data.Return();
                            if (rows.Count == 0)
                            {
                                continue;
                            }
                        }
                        await multi.Targets[input].SendAsync(CreateMessage(rows, columnCounts[input]));
                    }
                    foreach (var leaf in chainLeaves)
                    {
                        await leaf.SendAsync(new Checkpoint(round, round + 1, 1));
                    }
                    foreach (var target in multi.Targets)
                    {
                        await target.SendAsync(new Checkpoint(round, round + 1, 1));
                    }
                    await Drain(chainOutput, chainRoot.Completion, chainRows, "chain");
                    await Drain(multiOutput, multi.Completion, multiRows, "multi join");

                    var differences = new List<string>();
                    foreach (var key in chainRows.Keys.Union(multiRows.Keys))
                    {
                        chainRows.TryGetValue(key, out var expected);
                        multiRows.TryGetValue(key, out var actual);
                        if (expected != actual)
                        {
                            differences.Add($"{key}: chain {expected}, multi join {actual}");
                        }
                    }
                    Assert.True(differences.Count == 0, $"Round {round}: {string.Join("; ", differences.Take(10))}");
                    any |= chainRows.Count > 0;
                }
                // Without rows on every input an inner join has nothing to give
                Assert.True(any || sent.Any(x => x.Count == 0), "The joins gave no rows, the comparison says nothing");
            }
            finally
            {
                MultiJoinOperator.ParallelInsertMinRows = minRowsBefore;
                MultiJoinOperator.ParallelLookupsAlways = lookupsBefore;
                // The operators let go of their cursors and scratch before their storage goes
                foreach (var op in operators)
                {
                    await op.DisposeAsync();
                }
                foreach (var host in hosts)
                {
                    ((IDisposable)host).Dispose();
                }
            }
        }
    }
}

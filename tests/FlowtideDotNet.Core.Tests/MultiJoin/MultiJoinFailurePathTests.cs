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
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinFailurePathTests : IAsyncLifetime
    {
        private readonly string _directory = $"./data/multiJoinFailurePathTests/{Guid.NewGuid():N}";
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
        private readonly List<IAsyncDisposable> _asyncDisposables = new List<IAsyncDisposable>();

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            foreach (var item in _asyncDisposables)
            {
                await item.DisposeAsync();
            }
            foreach (var item in _disposables)
            {
                item.Dispose();
            }
            try
            {
                if (Directory.Exists(_directory))
                {
                    Directory.Delete(_directory, true);
                }
            }
            catch (IOException)
            {
            }
        }

        private static NamedStruct Schema(int columns)
        {
            return new NamedStruct()
            {
                Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList(),
                Struct = new Struct() { Types = Enumerable.Range(0, columns).Select(_ => (SubstraitBaseType)new AnyType()).ToList() }
            };
        }

        private static VirtualTableReadRelation VirtualTable(int columns)
        {
            return new VirtualTableReadRelation()
            {
                BaseSchema = Schema(columns),
                Values = new VirtualTable() { Expressions = new List<StructExpression>() }
            };
        }

        private static ReadRelation Read(string name, int columns)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { name } },
                BaseSchema = Schema(columns)
            };
        }

        private static FieldReference Field(int index)
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = index } };
        }

        private static MergeJoinRelation Join(JoinType type, Relation left, Relation right, params (int left, int right)[] keys)
        {
            return new MergeJoinRelation()
            {
                Type = type,
                Left = left,
                Right = right,
                LeftKeys = keys.Select(x => Field(x.left)).ToList(),
                RightKeys = keys.Select(x => Field(x.right)).ToList()
            };
        }

        private static StreamEventBatch Batch((long c0, long c1)[] rows, int[] weights)
        {
            var col0 = Column.Create(GlobalMemoryManager.Instance);
            var col1 = Column.Create(GlobalMemoryManager.Instance);
            var weightList = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            for (int i = 0; i < rows.Length; i++)
            {
                col0.Add(new Int64Value(rows[i].c0));
                col1.Add(new Int64Value(rows[i].c1));
                weightList.Add(weights[i]);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weightList, iterations, new EventBatchData(new IColumn[] { col0, col1 })));
            batch.Data.Rent(1);
            return batch;
        }

        private async Task<(StateManagerSync<StreamState> stateManager, StreamMemoryManager streamMemory)> CreateStateManager(string folder, bool reservoir = false)
        {
            FlowtideDotNet.Storage.Persistence.IPersistentStorage storage = reservoir
                ? new FlowtideDotNet.Storage.Persistence.Reservoir.Internal.ReservoirPersistentStorage(new FlowtideDotNet.Storage.Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = new FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk.MemoryFileProvider() })
                : new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/{folder}/persistent" });
            _disposables.Add(storage);
            var meter = new Meter("statemanager");
            _disposables.Add(meter);
            var stateManager = new StateManagerSync<StreamState>(
                new StateManagerOptions()
                {
                    PersistentStorage = storage,
                    TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"{_directory}/{folder}/tmp" }
                },
                NullLoggerFactory.Instance,
                meter,
                "stream",
                GlobalMemoryManager.Instance);
            _disposables.Add(stateManager);
            await stateManager.InitializeAsync();
            var streamMemory = new StreamMemoryManager("stream");
            _disposables.Add(streamMemory);
            return (stateManager, streamMemory);
        }

        private MultiJoinOperator CreateOperator(MultiJoinPlan plan)
        {
            var functions = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functions);
            var op = new MultiJoinOperator(plan, functions, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            _asyncDisposables.Add(op);
            op.Setup("stream", "1");
            op.CreateBlock();
            op.Link();
            return op;
        }

        private static Task InitOperator(MultiJoinOperator op, StateManagerSync<StreamState> stateManager, StreamMemoryManager streamMemory, long restoreTime)
        {
            var handler = new VertexHandler(
                "stream",
                "1",
                (_, _) => { },
                (_, _, _) => Task.CompletedTask,
                new StreamMetrics("stream").GetOrCreateVertexMeter("1", () => ""),
                stateManager.GetOrCreateClient("1"),
                NullLoggerFactory.Instance,
                streamMemory.CreateOperatorMemoryManager("1"),
                (_, _) => Task.CompletedTask);
            return op.Initialize("1", restoreTime, restoreTime, handler, null);
        }

        [Fact]
        public async Task CutShortSpilledPadTableClearsTreeOnAbandon()
        {
            int budgetBefore = MultiJoinOperator.RowBudget;
            int memoryKeysBefore = MultiJoinPadTable.MemoryKeys;
            MultiJoinOperator.RowBudget = 1;
            MultiJoinPadTable.MemoryKeys = 0;
            try
            {
                var root = Join(JoinType.Left, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                var (stateManager, streamMemory) = await CreateStateManager("pad_abandon");
                var op = CreateOperator(plan);
                await InitOperator(op, stateManager, streamMemory, 0);

                var batchA = Batch(new[] { (1L, 10L), (2L, 20L) }, new[] { 1, 1 });
                try
                {
                    await foreach (var item in op.OnRecieve(0, batchA, 0))
                    {
                        item.Data.Rent(1);
                        item.Data.Return();
                    }
                }
                finally
                {
                    batchA.Data.Return();
                }

                var batchB = Batch(new[] { (1L, 100L), (2L, 200L) }, new[] { 1, 1 });
                try
                {
                    await foreach (var item in op.OnRecieve(1, batchB, 0))
                    {
                        item.Data.Rent(1);
                        item.Data.Return();
                    }
                }
                finally
                {
                    batchB.Data.Return();
                }

                // Cut the right-side message short after spilling keys to the pad tree and yielding one batch
                var batchC = Batch(new[] { (1L, 99L), (2L, 99L) }, new[] { 1, 1 });
                try
                {
                    await using var enumerator = op.OnRecieve(2, batchC, 0).GetAsyncEnumerator();
                    Assert.True(await enumerator.MoveNextAsync());
                    enumerator.Current.Data.Rent(1);
                    enumerator.Current.Data.Return();
                    Assert.True(await enumerator.MoveNextAsync());
                    enumerator.Current.Data.Rent(1);
                    enumerator.Current.Data.Return();
                    Assert.True(await enumerator.MoveNextAsync());
                    enumerator.Current.Data.Rent(1);
                    enumerator.Current.Data.Return();
                }
                finally
                {
                    batchC.Data.Return();
                }

                Assert.False(op.PadTables[0].HasRows);
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
                MultiJoinPadTable.MemoryKeys = memoryKeysBefore;
            }
        }

        [Fact]
        public async Task CutShortSpilledEntriesOfAConditionOnBothSidesAreClearedAndTheTableWorksAgain()
        {
            int budgetBefore = MultiJoinOperator.RowBudget;
            int memoryKeysBefore = MultiJoinPadTable.MemoryKeys;
            MultiJoinOperator.RowBudget = 1;
            MultiJoinPadTable.MemoryKeys = 0;
            try
            {
                // (a join b) left join c on a.k = c.k and a.v >= c.v
                var root = Join(JoinType.Left, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                root.PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.GreaterThanOrEqual,
                    Arguments = new List<Expression>() { Field(1), Field(5) }
                };
                var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true)!;
                async Task<MultiJoinOperator> Loaded(string folder)
                {
                    var (stateManager, streamMemory) = await CreateStateManager(folder);
                    var loaded = CreateOperator(plan);
                    await InitOperator(loaded, stateManager, streamMemory, 0);
                    await Send(loaded, 0, Batch(new[] { (1L, 10L), (2L, 20L), (3L, 30L) }, new[] { 1, 1, 1 }));
                    await Send(loaded, 1, Batch(new[] { (1L, 100L), (2L, 200L), (3L, 300L) }, new[] { 1, 1, 1 }));
                    return loaded;
                }

                // The rows a message gives, with the sum of their weights
                async Task<Dictionary<string, int>> Send(MultiJoinOperator to, int input, StreamEventBatch batch, int take = int.MaxValue)
                {
                    var rows = new Dictionary<string, int>();
                    try
                    {
                        await using var enumerator = to.OnRecieve(input, batch, 0).GetAsyncEnumerator();
                        for (int i = 0; i < take && await enumerator.MoveNextAsync(); i++)
                        {
                            var part = enumerator.Current.Data;
                            part.Rent(1);
                            for (int r = 0; r < part.Weights.Count; r++)
                            {
                                var text = string.Join(",", part.EventBatchData.Columns.Select(x => x.GetValueAt(r, default).IsNull ? "null" : x.GetValueAt(r, default).AsLong.ToString()));
                                rows[text] = rows.GetValueOrDefault(text) + part.Weights[r];
                            }
                            part.Return();
                        }
                    }
                    finally
                    {
                        batch.Data.Return();
                    }
                    return rows;
                }

                var op = await Loaded("pair_abandon");
                // Every key gets its first match: three joined rows, then the three padded rows leave. Cut short among the padded rows
                await Send(op, 2, Batch(new[] { (1L, 5L), (2L, 5L), (3L, 5L) }, new[] { 1, 1, 1 }), take: 4);
                Assert.False(op.PadTables[0].HasRows);

                // A message that is cut short is not stored. The next one gives what it gives an operator that never saw the first.
                // It takes a row of the cut message away again: an entry left behind would cancel it and no row would turn
                var after = await Send(op, 2, Batch(new[] { (1L, 5L), (2L, 99L) }, new[] { -1, 1 }));
                var untouched = await Send(await Loaded("pair_abandon_reference"), 2, Batch(new[] { (1L, 5L), (2L, 99L) }, new[] { -1, 1 }));
                Assert.NotEmpty(untouched);
                Assert.Equal(untouched.OrderBy(x => x.Key).Select(x => $"{x.Key} x{x.Value}"), after.OrderBy(x => x.Key).Select(x => $"{x.Key} x{x.Value}"));
                Assert.False(op.PadTables[0].HasRows);
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
                MultiJoinPadTable.MemoryKeys = memoryKeysBefore;
            }
        }

        [Fact]
        public async Task ALookupThatTakesTheRangeReadsOnlyTheRowsInsideIt()
        {
            bool rangeKeysBefore = MultiJoinPlanBuilder.RangeKeys;
            try
            {
                // 300 rows of one key in b, a row of a with the value 50 matches the 50 rows whose value is from 1 to 50
                async Task<(long scanned, int rows)> Run(bool range, string folder)
                {
                    MultiJoinPlanBuilder.RangeKeys = range;
                    // (a left join b on a.k = b.k and a.v >= b.v and a.k <= b.v) left join c on a.k = c.k
                    var periods = Join(JoinType.Left, Read("a", 2), Read("b", 2), (0, 2));
                    periods.PostJoinFilter = new ScalarFunction()
                    {
                        ExtensionUrn = FunctionsBoolean.Urn,
                        ExtensionName = FunctionsBoolean.And,
                        Arguments = new List<Expression>()
                        {
                            new ScalarFunction()
                            {
                                ExtensionUrn = FunctionsComparison.Urn,
                                ExtensionName = FunctionsComparison.GreaterThanOrEqual,
                                Arguments = new List<Expression>() { Field(1), Field(3) }
                            },
                            new ScalarFunction()
                            {
                                ExtensionUrn = FunctionsComparison.Urn,
                                ExtensionName = FunctionsComparison.LessThanOrEqual,
                                Arguments = new List<Expression>() { Field(0), Field(3) }
                            }
                        }
                    };
                    var root = Join(JoinType.Left, periods, Read("c", 2), (0, 4));
                    var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true)!;
                    Assert.Equal(range, plan.RangeKeys);
                    var (stateManager, streamMemory) = await CreateStateManager(folder);
                    var op = CreateOperator(plan);
                    await InitOperator(op, stateManager, streamMemory, 0);

                    async Task<int> Send(int input, StreamEventBatch batch)
                    {
                        int rows = 0;
                        try
                        {
                            await foreach (var part in op.OnRecieve(input, batch, 0))
                            {
                                part.Data.Rent(1);
                                rows += part.Data.Weights.Count;
                                part.Data.Return();
                            }
                        }
                        finally
                        {
                            batch.Data.Return();
                        }
                        return rows;
                    }

                    await Send(1, Batch(Enumerable.Range(0, 300).Select(x => (1L, (long)x)).ToArray(), Enumerable.Repeat(1, 300).ToArray()));
                    long before = Interlocked.Read(ref MultiJoinTreeLookup.ScannedForTests);
                    int rows = await Send(0, Batch(new[] { (1L, 50L) }, new[] { 1 }));
                    return (Interlocked.Read(ref MultiJoinTreeLookup.ScannedForTests) - before, rows);
                }

                var equal = await Run(false, "pair_range_off");
                var range = await Run(true, "pair_range_on");

                Assert.Equal(50, equal.rows);
                Assert.Equal(50, range.rows);
                // Without the range every row of the key is met, with it only the ones up to the bound
                Assert.Equal(300, equal.scanned);
                Assert.Equal(51, range.scanned);
            }
            finally
            {
                MultiJoinPlanBuilder.RangeKeys = rangeKeysBefore;
            }
        }

        [Fact]
        public async Task ALookupWithARangeReadsTheRestoredTreesAfterTheOperatorIsInitializedAgain()
        {
            bool rangeKeysBefore = MultiJoinPlanBuilder.RangeKeys;
            try
            {
                MultiJoinPlanBuilder.RangeKeys = true;
                // (a left join b on a.k = b.k and a.v >= b.v) left join c on a.k = c.k
                var periods = Join(JoinType.Left, Read("a", 2), Read("b", 2), (0, 2));
                periods.PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.GreaterThanOrEqual,
                    Arguments = new List<Expression>() { Field(1), Field(3) }
                };
                var root = Join(JoinType.Left, periods, Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true)!;
                var (stateManager, streamMemory) = await CreateStateManager("pair_range_restore", reservoir: true);
                var op = CreateOperator(plan);
                await InitOperator(op, stateManager, streamMemory, 0);

                async Task<int> Send(int input, StreamEventBatch batch)
                {
                    int rows = 0;
                    try
                    {
                        await foreach (var part in op.OnRecieve(input, batch, 0))
                        {
                            part.Data.Rent(1);
                            rows += part.Data.Weights.Count;
                            part.Data.Return();
                        }
                    }
                    finally
                    {
                        batch.Data.Return();
                    }
                    return rows;
                }

                await Send(1, Batch(Enumerable.Range(0, 300).Select(x => (1L, (long)x)).ToArray(), Enumerable.Repeat(1, 300).ToArray()));
                long rangesBefore = Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests);
                Assert.Equal(51, await Send(0, Batch(new[] { (1L, 50L) }, new[] { 1 })));
                Assert.True(Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests) > rangesBefore);

                await op.OnCheckpoint();
                stateManager.Metadata = new StreamState(1, "hash");
                await stateManager.CheckpointAsync(false);
                // Stored after the checkpoint, gone when the state is set back to it
                await Send(1, Batch(Enumerable.Range(0, 20).Select(x => (1L, -100L + x)).ToArray(), Enumerable.Repeat(1, 20).ToArray()));
                await stateManager.InitializeAsync();
                await InitOperator(op, stateManager, streamMemory, 1);

                // A row of b that came after the state was set back is found by the next lookup with a range
                await Send(1, Batch(new[] { (1L, -5L) }, new[] { 1 }));
                rangesBefore = Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests);
                Assert.Equal(41, await Send(0, Batch(new[] { (1L, 39L) }, new[] { 1 })));
                Assert.True(Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests) > rangesBefore);
            }
            finally
            {
                MultiJoinPlanBuilder.RangeKeys = rangeKeysBefore;
            }
        }

        // b is looked up by k with a range on v, and by v alone through its second index
        [Fact]
        public async Task ARangeOnAnInputWithASecondIndexGivesTheRowsOfTheJoins()
        {
            bool rangeKeysBefore = MultiJoinPlanBuilder.RangeKeys;
            int budgetBefore = MultiJoinOperator.RowBudget;
            try
            {
                MultiJoinOperator.RowBudget = 7;
                // (a(k, v) left join b(k, v) on a.k = b.k and a.v >= b.v) left join c(x, y) on b.v = c.x
                var periods = Join(JoinType.Left, Read("a", 2), Read("b", 2), (0, 2));
                periods.PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.GreaterThanOrEqual,
                    Arguments = new List<Expression>() { Field(1), Field(3) }
                };
                var root = Join(JoinType.Left, periods, Read("c", 2), (3, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true)!;
                var b = plan.Inputs[1];
                Assert.True(plan.RangeKeys);
                Assert.NotEmpty(b.SecondaryLayouts);
                int rangeList = b.FindKeyList(new[] { 0, 1 });
                Assert.True(rangeList >= 0 && b.SecondaryOf[rangeList] < 0, $"key lists {string.Join(" ", b.KeyLists.Select(x => "[" + string.Join(",", x) + "]"))}");

                var (stateManager, streamMemory) = await CreateStateManager("pair_range_second");
                var op = CreateOperator(plan);
                await InitOperator(op, stateManager, streamMemory, 0);

                var inputs = new[] { new List<(long, long)>(), new List<(long, long)>(), new List<(long, long)>() };
                var actual = new Dictionary<string, int>();
                async Task Send(int input, (long c0, long c1)[] rows, int[] weights)
                {
                    for (int i = 0; i < rows.Length; i++)
                    {
                        if (weights[i] > 0)
                        {
                            inputs[input].Add(rows[i]);
                        }
                        else
                        {
                            Assert.True(inputs[input].Remove(rows[i]));
                        }
                    }
                    var batch = Batch(rows, weights);
                    try
                    {
                        await foreach (var part in op.OnRecieve(input, batch, 0))
                        {
                            part.Data.Rent(1);
                            for (int r = 0; r < part.Data.Weights.Count; r++)
                            {
                                var text = string.Join(",", part.Data.EventBatchData.Columns.Select(x => x.GetValueAt(r, default).IsNull ? "null" : x.GetValueAt(r, default).AsLong.ToString()));
                                actual[text] = actual.GetValueOrDefault(text) + part.Data.Weights[r];
                                if (actual[text] == 0)
                                {
                                    actual.Remove(text);
                                }
                            }
                            part.Data.Return();
                        }
                    }
                    finally
                    {
                        batch.Data.Return();
                    }

                    var expected = new Dictionary<string, int>();
                    foreach (var ra in inputs[0])
                    {
                        var bs = inputs[1].Where(rb => rb.Item1 == ra.Item1 && ra.Item2 >= rb.Item2).ToList();
                        if (bs.Count == 0)
                        {
                            var text = $"{ra.Item1},{ra.Item2},null,null,null,null";
                            expected[text] = expected.GetValueOrDefault(text) + 1;
                            continue;
                        }
                        foreach (var rb in bs)
                        {
                            var cs = inputs[2].Where(rc => rc.Item1 == rb.Item2).ToList();
                            var texts = cs.Count == 0 ? new List<string>() { $"{ra.Item1},{ra.Item2},{rb.Item1},{rb.Item2},null,null" } : cs.Select(rc => $"{ra.Item1},{ra.Item2},{rb.Item1},{rb.Item2},{rc.Item1},{rc.Item2}").ToList();
                            foreach (var text in texts)
                            {
                                expected[text] = expected.GetValueOrDefault(text) + 1;
                            }
                        }
                    }
                    Assert.Equal(expected.OrderBy(x => x.Key).Select(x => $"{x.Key} x{x.Value}"), actual.OrderBy(x => x.Key).Select(x => $"{x.Key} x{x.Value}"));
                }

                long rangesBefore = Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests);
                // 60 rows per key in b, c for every third value
                await Send(1, Enumerable.Range(0, 120).Select(x => ((long)(x % 2), (long)(x / 2))).ToArray(), Enumerable.Repeat(1, 120).ToArray());
                await Send(2, Enumerable.Range(0, 20).Select(x => ((long)(x * 3), (long)(x * 100))).ToArray(), Enumerable.Repeat(1, 20).ToArray());
                await Send(0, new[] { (0L, 10L), (1L, 25L), (0L, -1L), (2L, 5L), (1L, 59L) }, new[] { 1, 1, 1, 1, 1 });
                // A c row whose value b has, found through the second index of b
                await Send(2, new[] { (10L, 7L), (0L, 0L) }, new[] { 1, -1 });
                // b rows inside and outside the bounds of the a rows
                await Send(1, new[] { (0L, 4L), (0L, 11L), (1L, 30L), (2L, 5L), (2L, 6L) }, new[] { -1, 1, -1, 1, 1 });
                // a rows move their bound
                await Send(0, new[] { (0L, 10L), (0L, 3L), (1L, 25L), (1L, 2L), (0L, -1L), (0L, 100L) }, new[] { -1, 1, -1, 1, -1, 1 });
                await Send(2, new[] { (3L, 100L), (3L, 101L), (11L, 1L) }, new[] { -1, 1, 1 });
                Assert.True(Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests) > rangesBefore);
            }
            finally
            {
                MultiJoinPlanBuilder.RangeKeys = rangeKeysBefore;
                MultiJoinOperator.RowBudget = budgetBefore;
            }
        }

        private static StreamEventBatch NullableBatch((long? c0, long? c1)[] rows, int[] weights)
        {
            var col0 = Column.Create(GlobalMemoryManager.Instance);
            var col1 = Column.Create(GlobalMemoryManager.Instance);
            var weightList = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            for (int i = 0; i < rows.Length; i++)
            {
                if (rows[i].c0 == null) { col0.Add(NullValue.Instance); } else { col0.Add(new Int64Value(rows[i].c0!.Value)); }
                if (rows[i].c1 == null) { col1.Add(NullValue.Instance); } else { col1.Add(new Int64Value(rows[i].c1!.Value)); }
                weightList.Add(weights[i]);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weightList, iterations, new EventBatchData(new IColumn[] { col0, col1 })));
            batch.Data.Rent(1);
            return batch;
        }

        // One row per key, keys without rows, nulls in the bound and in the key, every comparison type: the range gives what the chain gives
        [Theory]
        [InlineData(FunctionsComparison.GreaterThanOrEqual, true)]
        [InlineData(FunctionsComparison.GreaterThan, true)]
        [InlineData(FunctionsComparison.LessThanOrEqual, true)]
        [InlineData(FunctionsComparison.LessThan, true)]
        [InlineData(FunctionsComparison.GreaterThanOrEqual, false)]
        [InlineData(FunctionsComparison.LessThan, false)]
        public async Task ARangeAtOneRowPerKeyEmptyKeysAndNullBoundsGivesTheRowsOfTheJoins(string op, bool range)
        {
            bool rangeKeysBefore = MultiJoinPlanBuilder.RangeKeys;
            try
            {
                MultiJoinPlanBuilder.RangeKeys = range;
                Func<long, long, bool> holds = op switch
                {
                    FunctionsComparison.GreaterThanOrEqual => (x, y) => x >= y,
                    FunctionsComparison.GreaterThan => (x, y) => x > y,
                    FunctionsComparison.LessThanOrEqual => (x, y) => x <= y,
                    _ => (x, y) => x < y
                };
                // (a(k, v) left join b(k, v) on a.k = b.k and a.v OP b.v) left join c(k, x) on a.k = c.k
                var periods = Join(JoinType.Left, Read("a", 2), Read("b", 2), (0, 2));
                periods.PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = op,
                    Arguments = new List<Expression>() { Field(1), Field(3) }
                };
                var root = Join(JoinType.Left, periods, Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true)!;
                Assert.Equal(range, plan.RangeKeys);
                var (stateManager, streamMemory) = await CreateStateManager($"pair_range_edges_{op}_{range}");
                var multi = CreateOperator(plan);
                await InitOperator(multi, stateManager, streamMemory, 0);

                var inputs = new[] { new List<(long?, long?)>(), new List<(long?, long?)>(), new List<(long?, long?)>() };
                var actual = new Dictionary<string, int>();
                static string Text(long? v) => v == null ? "null" : v.Value.ToString();
                async Task Send(int input, (long? c0, long? c1)[] rows, int[] weights)
                {
                    for (int i = 0; i < rows.Length; i++)
                    {
                        if (weights[i] > 0)
                        {
                            inputs[input].Add(rows[i]);
                        }
                        else
                        {
                            Assert.True(inputs[input].Remove(rows[i]));
                        }
                    }
                    var batch = NullableBatch(rows, weights);
                    try
                    {
                        await foreach (var part in multi.OnRecieve(input, batch, 0))
                        {
                            part.Data.Rent(1);
                            for (int r = 0; r < part.Data.Weights.Count; r++)
                            {
                                var text = string.Join(",", part.Data.EventBatchData.Columns.Select(x => x.GetValueAt(r, default).IsNull ? "null" : x.GetValueAt(r, default).AsLong.ToString()));
                                actual[text] = actual.GetValueOrDefault(text) + part.Data.Weights[r];
                                if (actual[text] == 0)
                                {
                                    actual.Remove(text);
                                }
                            }
                            part.Data.Return();
                        }
                    }
                    finally
                    {
                        batch.Data.Return();
                    }

                    var expected = new Dictionary<string, int>();
                    foreach (var ra in inputs[0])
                    {
                        var bs = inputs[1].Where(rb => ra.Item1 != null && rb.Item1 == ra.Item1 && ra.Item2 != null && rb.Item2 != null && holds(ra.Item2.Value, rb.Item2.Value)).ToList();
                        var cs = inputs[2].Where(rc => ra.Item1 != null && rc.Item1 == ra.Item1).ToList();
                        var bTexts = bs.Count == 0 ? new List<string>() { "null,null" } : bs.Select(rb => $"{Text(rb.Item1)},{Text(rb.Item2)}").ToList();
                        var cTexts = cs.Count == 0 ? new List<string>() { "null,null" } : cs.Select(rc => $"{Text(rc.Item1)},{Text(rc.Item2)}").ToList();
                        foreach (var bt in bTexts)
                        {
                            foreach (var ct in cTexts)
                            {
                                var text = $"{Text(ra.Item1)},{Text(ra.Item2)},{bt},{ct}";
                                expected[text] = expected.GetValueOrDefault(text) + 1;
                            }
                        }
                    }
                    Assert.Equal(expected.OrderBy(x => x.Key).Select(x => $"{x.Key} x{x.Value}"), actual.OrderBy(x => x.Key).Select(x => $"{x.Key} x{x.Value}"));
                }

                long rangesBefore = Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests);
                // One row per key, a key with a null bound only, a row with a null key
                await Send(1, new (long?, long?)[] { (1, 5), (2, null), (3, 5), (null, 5), (5, 5), (5, null) }, new[] { 1, 1, 1, 1, 1, 1 });
                await Send(2, new (long?, long?)[] { (1, 0), (4, 0) }, new[] { 1, 1 });
                // Below, at and above the bound, a null bound, keys without rows in b, a null key
                await Send(0, new (long?, long?)[] { (1, 4), (1, 5), (1, 6), (2, 1), (3, null), (4, 5), (6, 5), (null, 5), (5, 5), (5, 4) }, new[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 });
                // b rows move across the bound, a key gets its first and loses its last row
                await Send(1, new (long?, long?)[] { (1, 5), (1, 6), (3, 5), (4, 4), (2, null), (2, 1) }, new[] { -1, 1, -1, 1, -1, 1 });
                // a rows move their bound, a bound becomes null
                await Send(0, new (long?, long?)[] { (1, 4), (1, 7), (4, 5), (4, null), (5, 4), (5, 6) }, new[] { -1, 1, -1, 1, -1, 1 });
                // A row whose bound is null in b next to rows that match
                await Send(1, new (long?, long?)[] { (1, null), (4, null), (4, 4) }, new[] { 1, 1, -1 });
                if (range)
                {
                    Assert.True(Interlocked.Read(ref MultiJoinCursor.RangeLookupsForTests) > rangesBefore);
                }
            }
            finally
            {
                MultiJoinPlanBuilder.RangeKeys = rangeKeysBefore;
            }
        }

        [Fact]
        public async Task SpilledEntriesOfAConditionOnBothSidesAreSortedByTheirKeyAndNumberOnly()
        {
            int memoryKeysBefore = MultiJoinPadTable.MemoryKeys;
            MultiJoinPadTable.MemoryKeys = 0;
            try
            {
                // (a left join b on a.k = b.k and a.v >= b.v) left join c on a.k = c.k
                var periods = Join(JoinType.Left, Read("a", 2), Read("b", 2), (0, 2));
                periods.PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.GreaterThanOrEqual,
                    Arguments = new List<Expression>() { Field(1), Field(3) }
                };
                var root = Join(JoinType.Left, periods, Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true)!;
                var (stateManager, streamMemory) = await CreateStateManager("pair_entry_sort");
                var op = CreateOperator(plan);
                await InitOperator(op, stateManager, streamMemory, 0);

                async Task Send(int input, StreamEventBatch batch)
                {
                    try
                    {
                        await foreach (var part in op.OnRecieve(input, batch, 0))
                        {
                            part.Data.Rent(1);
                            part.Data.Return();
                        }
                    }
                    finally
                    {
                        batch.Data.Return();
                    }
                }

                await Send(0, Batch(new[] { (1L, 5L), (2L, 5L) }, new[] { 1, 1 }));
                await Send(1, Batch(new[] { (1L, 1L), (1L, 2L), (2L, 3L) }, new[] { 1, 1, 1 }));

                var writer = (MultiJoinTreeWriter?)typeof(MultiJoinPadTable)
                    .GetField("_entryWriter", BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(op.PadTables[0]);
                Assert.NotNull(writer);
                // The key column and the number of the entry: the column the condition reads is not sorted by
                Assert.Equal(2, writer!.SortCount);
                Assert.True(writer.Comparer.ColumnOrder.Count > 2);
            }
            finally
            {
                MultiJoinPadTable.MemoryKeys = memoryKeysBefore;
            }
        }

        [Fact]
        public async Task AGroupWithoutAConditionOnBothSidesOfAKeptSideComparesNothingBeforeTheCopy()
        {
            // (a join b on a.k = b.k and a.v < b.v) join c on a.k = c.k
            var ab = Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2));
            ab.PostJoinFilter = new ScalarFunction()
            {
                ExtensionUrn = FunctionsComparison.Urn,
                ExtensionName = FunctionsComparison.LessThan,
                Arguments = new List<Expression>() { Field(1), Field(3) }
            };
            var root = Join(JoinType.Inner, ab, Read("c", 2), (0, 4));
            var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
            Assert.False(plan.PairConditions);
            var (stateManager, streamMemory) = await CreateStateManager("inner_no_filter");
            var op = CreateOperator(plan);
            await InitOperator(op, stateManager, streamMemory, 0);

            async Task<int> Send(int input, StreamEventBatch batch)
            {
                int rows = 0;
                try
                {
                    await foreach (var part in op.OnRecieve(input, batch, 0))
                    {
                        part.Data.Rent(1);
                        rows += part.Data.Weights.Count;
                        part.Data.Return();
                    }
                }
                finally
                {
                    batch.Data.Return();
                }
                return rows;
            }

            await Send(1, Batch(Enumerable.Range(0, 10).Select(x => (1L, (long)x)).ToArray(), Enumerable.Repeat(1, 10).ToArray()));
            await Send(2, Batch(new[] { (1L, 1L) }, new[] { 1 }));
            // The four rows of b above 5
            Assert.Equal(4, await Send(0, Batch(new[] { (1L, 5L) }, new[] { 1 })));
            Assert.Equal(0, op.FilteredRunsForTests);
        }

        // Rows stored before a checkpoint are there after the state is set back to it, rows stored after it are not
        [Fact]
        public async Task RowsOfACheckpointAreFoundAfterTheStateIsSetBackToIt()
        {
            // (a join b on a.k = b.k) join c on a.k = c.k
            var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
            var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
            // The file cache storage does not bring a checkpoint back
            var (stateManager, streamMemory) = await CreateStateManager("rollback", reservoir: true);
            var op = CreateOperator(plan);
            await InitOperator(op, stateManager, streamMemory, 0);

            async Task<int> Send(int input, StreamEventBatch batch)
            {
                int rows = 0;
                try
                {
                    await foreach (var part in op.OnRecieve(input, batch, 0))
                    {
                        part.Data.Rent(1);
                        rows += part.Data.Weights.Count;
                        part.Data.Return();
                    }
                }
                finally
                {
                    batch.Data.Return();
                }
                return rows;
            }

            await Send(1, Batch(Enumerable.Range(0, 300).Select(x => (1L, (long)x)).ToArray(), Enumerable.Repeat(1, 300).ToArray()));
            await Send(2, Batch(new[] { (1L, 1L) }, new[] { 1 }));
            Assert.Equal(300, await Send(0, Batch(new[] { (1L, 50L) }, new[] { 1 })));

            await op.OnCheckpoint();
            stateManager.Metadata = new StreamState(1, "hash");
            await stateManager.CheckpointAsync(false);

            // Stored after the checkpoint
            await Send(1, Batch(Enumerable.Range(0, 20).Select(x => (1L, 1000L + x)).ToArray(), Enumerable.Repeat(1, 20).ToArray()));
            Assert.Equal(320, await Send(0, Batch(new[] { (1L, 51L) }, new[] { 1 })));

            await stateManager.InitializeAsync();
            await InitOperator(op, stateManager, streamMemory, 1);

            // The row of a from before the checkpoint joins a new row of b, and a new row of a joins the 300 rows of b
            Assert.Equal(1, await Send(1, Batch(new[] { (1L, 2000L) }, new[] { 1 })));
            Assert.Equal(301, await Send(0, Batch(new[] { (1L, 60L) }, new[] { 1 })));
        }

        // Runs of steps that compared before the copy, over messages of one row of a each. b holds ten rows of the key
        private async Task<long[]> FilteredRuns(string folder, bool lists, params (int count, Func<int, long> value)[] phases)
        {
            bool rangeKeysBefore = MultiJoinPlanBuilder.RangeKeys;
            try
            {
                MultiJoinPlanBuilder.RangeKeys = false;
                // (a left join b on a.k = b.k and a.v >= b.v) left join c on a.k = c.k
                var periods = Join(JoinType.Left, Read("a", 2), Read("b", 2), (0, 2));
                periods.PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.GreaterThanOrEqual,
                    Arguments = new List<Expression>() { Field(1), Field(3) }
                };
                var root = Join(JoinType.Left, periods, Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true)!;
                var (stateManager, streamMemory) = await CreateStateManager(folder);
                var op = CreateOperator(plan);
                await InitOperator(op, stateManager, streamMemory, 0);

                StreamEventBatch Rows((long c0, long c1)[] rows)
                {
                    if (!lists)
                    {
                        return Batch(rows, Enumerable.Repeat(1, rows.Length).ToArray());
                    }
                    var col0 = Column.Create(GlobalMemoryManager.Instance);
                    var col1 = Column.Create(GlobalMemoryManager.Instance);
                    var weightList = new PrimitiveList<int>(GlobalMemoryManager.Instance);
                    var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
                    for (int i = 0; i < rows.Length; i++)
                    {
                        col0.Add(new Int64Value(rows[i].c0));
                        col1.Add(new ListValue(new Int64Value(rows[i].c1)));
                        weightList.Add(1);
                        iterations.Add(0);
                    }
                    var batch = new StreamEventBatch(new EventBatchWeighted(weightList, iterations, new EventBatchData(new IColumn[] { col0, col1 })));
                    batch.Data.Rent(1);
                    return batch;
                }

                async Task Send(int input, StreamEventBatch batch)
                {
                    try
                    {
                        await foreach (var part in op.OnRecieve(input, batch, 0))
                        {
                            part.Data.Rent(1);
                            part.Data.Return();
                        }
                    }
                    finally
                    {
                        batch.Data.Return();
                    }
                }

                await Send(1, Rows(Enumerable.Range(0, 10).Select(x => (1L, (long)x)).ToArray()));
                var runs = new long[phases.Length];
                for (int p = 0; p < phases.Length; p++)
                {
                    long before = op.FilteredRunsForTests;
                    for (int i = 0; i < phases[p].count; i++)
                    {
                        await Send(0, Rows(new[] { (1L, phases[p].value(i)) }));
                    }
                    runs[p] = op.FilteredRunsForTests - before;
                }
                return runs;
            }
            finally
            {
                MultiJoinPlanBuilder.RangeKeys = rangeKeysBefore;
            }
        }

        [Fact]
        public async Task ComparingBeforeTheCopyStopsWhileNearlyEveryRowPassesAndComesBackWhenRowsFail()
        {
            // Every row of b passes, passes again, then none does
            var runs = await FilteredRuns("pair_filter_guard", false, (40, i => 100 + i), (40, i => 200 + i), (200, i => -1 - i));

            Assert.True(runs[0] > 0, "The comparison was never made before the copy");
            Assert.True(runs[1] <= 2, $"Still compared before the copy on {runs[1]} of 40 runs where every row passes");
            Assert.True(runs[2] > 100, $"Compared before the copy on {runs[2]} of 200 runs where no row passes");
        }

        [Fact]
        public async Task ComparingBeforeTheCopyStopsForValuesItCannotCompare()
        {
            // Lists are left to the compiled condition
            var runs = await FilteredRuns("pair_filter_lists", true, (40, i => -1 - i), (40, i => -100 - i));

            Assert.True(runs[0] <= 2, $"Counted {runs[0]} of 40 runs for values the comparison is not made on");
            Assert.True(runs[1] <= 2, $"Counted {runs[1]} of 40 runs for values the comparison is not made on");
        }

        // The condition holds for a kept row whose column is null: such a row turns like any other
        [Theory]
        [InlineData(1024)]
        [InlineData(0)]
        public async Task AKeptRowWithANullInTheConditionLosesItsPaddedRowWhenItGetsAMatch(int memoryKeys)
        {
            int memoryKeysBefore = MultiJoinPadTable.MemoryKeys;
            MultiJoinPadTable.MemoryKeys = memoryKeys;
            try
            {
                // (a left join b on a.k = b.k and (a.v is null or a.v < b.v)) left join c on a.k = c.k
                var periods = Join(JoinType.Left, Read("a", 2), Read("b", 2), (0, 2));
                periods.PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsBoolean.Urn,
                    ExtensionName = FunctionsBoolean.Or,
                    Arguments = new List<Expression>()
                    {
                        new ScalarFunction() { ExtensionUrn = FunctionsComparison.Urn, ExtensionName = FunctionsComparison.IsNull, Arguments = new List<Expression>() { Field(1) } },
                        new ScalarFunction() { ExtensionUrn = FunctionsComparison.Urn, ExtensionName = FunctionsComparison.LessThan, Arguments = new List<Expression>() { Field(1), Field(3) } }
                    }
                };
                var root = Join(JoinType.Left, periods, Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5, null, pairConditions: true)!;
                var (stateManager, streamMemory) = await CreateStateManager("pair_null_" + memoryKeys);
                var op = CreateOperator(plan);
                await InitOperator(op, stateManager, streamMemory, 0);

                var rows = new Dictionary<string, int>();
                async Task Send(int input, long key, long? value, int weight)
                {
                    var keys = Column.Create(GlobalMemoryManager.Instance);
                    var values = Column.Create(GlobalMemoryManager.Instance);
                    keys.Add(new Int64Value(key));
                    if (value == null)
                    {
                        values.Add(NullValue.Instance);
                    }
                    else
                    {
                        values.Add(new Int64Value(value.Value));
                    }
                    var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
                    var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
                    weights.Add(weight);
                    iterations.Add(0);
                    var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(new IColumn[] { keys, values })));
                    batch.Data.Rent(1);
                    try
                    {
                        await foreach (var part in op.OnRecieve(input, batch, 0))
                        {
                            part.Data.Rent(1);
                            for (int r = 0; r < part.Data.Weights.Count; r++)
                            {
                                var text = string.Join(",", part.Data.EventBatchData.Columns.Select(x => x.GetValueAt(r, default).IsNull ? "null" : x.GetValueAt(r, default).AsLong.ToString()));
                                rows[text] = rows.GetValueOrDefault(text) + part.Data.Weights[r];
                            }
                            part.Data.Return();
                        }
                    }
                    finally
                    {
                        batch.Data.Return();
                    }
                }
                string Current() => string.Join("; ", rows.Where(x => x.Value != 0).OrderBy(x => x.Key).Select(x => $"{x.Key} x{x.Value}"));

                await Send(0, 1, null, 1);
                await Send(0, 2, 9, 1);
                Assert.Equal("1,null,null,null,null,null x1; 2,9,null,null,null,null x1", Current());

                // The first match of the row with the null, and one that fails for the other row
                await Send(1, 1, 5, 1);
                await Send(1, 2, 5, 1);
                Assert.Equal("1,null,1,5,null,null x1; 2,9,null,null,null,null x1", Current());

                // The match goes again
                await Send(1, 1, 5, -1);
                Assert.Equal("1,null,null,null,null,null x1; 2,9,null,null,null,null x1", Current());
            }
            finally
            {
                MultiJoinPadTable.MemoryKeys = memoryKeysBefore;
            }
        }

        private sealed class GatedReviewInserter : IBPlusTreeBulkInserter<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer>
        {
            private readonly IBPlusTreeBulkInserter<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer> _inner;
            public readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly TaskCompletionSource Finished = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public Exception? ErrorAfterRelease;

            public GatedReviewInserter(IBPlusTreeBulkInserter<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer> inner) => _inner = inner;
            public int LeafHitCount => _inner.LeafHitCount;
            public int[] SortAndGetIndices(ColumnRowReference[] keys, int keyLength) => _inner.SortAndGetIndices(keys, keyLength);

            public ValueTask ApplyBatch<TMutator>(ColumnRowReference[] keys, MultiJoinWeight[] values, int keyLength, TMutator mutator, int totalBatchByteSize)
                where TMutator : IRowMutator<ColumnRowReference, MultiJoinWeight> =>
                _inner.ApplyBatch(keys, values, keyLength, mutator, totalBatchByteSize);

            public async ValueTask ApplyBatch<TMutator>(ColumnRowReference[] keys, MultiJoinWeight[] values, int keyLength, int[] sortedIndices, int[] duplicateTags, TMutator mutator, int totalBatchByteSize)
                where TMutator : IRowMutator<ColumnRowReference, MultiJoinWeight>
            {
                Entered.TrySetResult();
                try
                {
                    await Release.Task.ConfigureAwait(false);
                    if (ErrorAfterRelease != null)
                    {
                        throw ErrorAfterRelease;
                    }
                    await _inner.ApplyBatch(keys, values, keyLength, sortedIndices, duplicateTags, mutator, totalBatchByteSize);
                }
                finally
                {
                    Finished.TrySetResult();
                }
            }
        }

        private sealed class RejectSecondReviewScheduler : TaskScheduler
        {
            private int _queued;
            protected override IEnumerable<Task>? GetScheduledTasks() => Array.Empty<Task>();
            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;
            protected override void QueueTask(Task task)
            {
                if (Interlocked.Increment(ref _queued) == 2)
                {
                    throw new InvalidOperationException("Rejected the second secondary task");
                }
                TryExecuteTask(task);
            }
        }

        private static GatedReviewInserter GateWriter(MultiJoinTreeWriter writer)
        {
            var gate = new GatedReviewInserter(writer.Inserter);
            typeof(MultiJoinTreeWriter).GetProperty(nameof(MultiJoinTreeWriter.Inserter))!.SetValue(writer, gate);
            return gate;
        }

        [Fact]
        public async Task SchedulerRejectionWaitsForTheSecondaryAlreadyStarted()
        {
            var root = Join(JoinType.Inner,
                Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 3), Read("b", 1), (0, 3)), Read("c", 1), (1, 4)),
                Read("d", 1), (2, 5));
            var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
            Assert.Equal(2, plan.Inputs[0].SecondaryLayouts.Count);
            var (stateManager, _) = await CreateStateManager("secondary_schedule_rejection");
            var input = new MultiJoinInputState(plan.Inputs[0], 0);
            await input.Initialize(stateManager.GetOrCreateClient("1"), GlobalMemoryManager.Instance);
            var secondaries = (MultiJoinSecondaryIndex[])typeof(MultiJoinInputState)
                .GetField("_secondaries", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(input)!;
            var gate = GateWriter(secondaries[0].Writer);
            using var c0 = Column.Create(GlobalMemoryManager.Instance);
            using var c1 = Column.Create(GlobalMemoryManager.Instance);
            using var c2 = Column.Create(GlobalMemoryManager.Instance);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            c0.Add(new Int64Value(1));
            c1.Add(new Int64Value(2));
            c2.Add(new Int64Value(3));
            weights.Add(1);
            Task insert = input.Insert(new EventBatchData(new IColumn[] { c0, c1, c2 }), weights, 1,
                GlobalMemoryManager.Instance, new RejectSecondReviewScheduler()).AsTask();
            try
            {
                await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
                Assert.False(insert.IsCompleted, "Insert must retain the batch until its already started secondary has drained, even when scheduling another secondary fails.");
            }
            finally
            {
                gate.Release.TrySetResult();
                await gate.Finished.Task.WaitAsync(TimeSpan.FromSeconds(30));
                var tasks = (Task<int>[])typeof(MultiJoinInputState)
                    .GetField("_secondaryTasks", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(input)!;
                if (tasks[0] != null)
                {
                    await tasks[0].WaitAsync(TimeSpan.FromSeconds(30));
                }
                await Assert.ThrowsAsync<TaskSchedulerException>(() => insert);
                input.ReleaseScratch();
            }
        }

        [Fact]
        public async Task SecondaryFailureWaitsForItsBlockedSibling()
        {
            var root = Join(JoinType.Inner,
                Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 3), Read("b", 1), (0, 3)), Read("c", 1), (1, 4)),
                Read("d", 1), (2, 5));
            var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
            Assert.Equal(2, plan.Inputs[0].SecondaryLayouts.Count);
            var (stateManager, _) = await CreateStateManager("secondary_failure_waits_for_sibling");
            var input = new MultiJoinInputState(plan.Inputs[0], 0);
            await input.Initialize(stateManager.GetOrCreateClient("1"), GlobalMemoryManager.Instance);
            var secondaries = (MultiJoinSecondaryIndex[])typeof(MultiJoinInputState)
                .GetField("_secondaries", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(input)!;
            var failed = GateWriter(secondaries[0].Writer);
            var sibling = GateWriter(secondaries[1].Writer);
            var error = new IOException("The first secondary failed");
            failed.ErrorAfterRelease = error;
            using var c0 = Column.Create(GlobalMemoryManager.Instance);
            using var c1 = Column.Create(GlobalMemoryManager.Instance);
            using var c2 = Column.Create(GlobalMemoryManager.Instance);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            c0.Add(new Int64Value(1));
            c1.Add(new Int64Value(2));
            c2.Add(new Int64Value(3));
            weights.Add(1);
            Task insert = input.Insert(new EventBatchData(new IColumn[] { c0, c1, c2 }), weights, 1,
                GlobalMemoryManager.Instance, TaskScheduler.Default).AsTask();
            try
            {
                await Task.WhenAll(failed.Entered.Task, sibling.Entered.Task).WaitAsync(TimeSpan.FromSeconds(30));
                var tasks = (Task<int>[])typeof(MultiJoinInputState)
                    .GetField("_secondaryTasks", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(input)!;
                failed.Release.TrySetResult();
                Assert.Same(error, await Assert.ThrowsAsync<IOException>(() => tasks[0]));
                Assert.False(insert.IsCompleted, "A failed secondary must not release the message while its sibling still reads it.");
            }
            finally
            {
                failed.Release.TrySetResult();
                sibling.Release.TrySetResult();
                await Task.WhenAll(failed.Finished.Task, sibling.Finished.Task).WaitAsync(TimeSpan.FromSeconds(30));
                Assert.Same(error, await Assert.ThrowsAsync<IOException>(() => insert));
                input.ReleaseScratch();
            }
        }

        [Fact]
        public async Task LookupFailureWaitsForBlockedInsertAndPreservesItsOriginalError()
        {
            int minRowsBefore = MultiJoinOperator.ParallelInsertMinRows;
            bool parallelBefore = MultiJoinOperator.ParallelInserts;
            var hookBefore = MultiJoinOperator.JoinHookForTests;
            try
            {
                MultiJoinOperator.ParallelInsertMinRows = 1;
                MultiJoinOperator.ParallelInserts = true;
                var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                var (stateManager, streamMemory) = await CreateStateManager("lookup_waits_for_insert");
                var op = CreateOperator(plan);
                await InitOperator(op, stateManager, streamMemory, 0);
                var inputs = (MultiJoinInputState[])typeof(MultiJoinOperator)
                    .GetField("_inputs", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(op)!;
                var writer = (MultiJoinTreeWriter)typeof(MultiJoinInputState)
                    .GetField("_primary", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(inputs[0])!;
                var gate = GateWriter(writer);
                gate.ErrorAfterRelease = new IOException("The insert also failed");
                var lookupError = new InvalidOperationException("The lookup failed first");
                MultiJoinOperator.JoinHookForTests = () => throw lookupError;
                var batch = Batch(new[] { (1L, 2L) }, new[] { 1 });
                await using var enumerator = op.OnRecieve(0, batch, 0).GetAsyncEnumerator();
                Task<bool> receive = enumerator.MoveNextAsync().AsTask();
                try
                {
                    await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
                    Assert.False(receive.IsCompleted, "The failed lookup must not release its message while the insert is still running.");
                }
                finally
                {
                    gate.Release.TrySetResult();
                    await gate.Finished.Task.WaitAsync(TimeSpan.FromSeconds(30));
                    var actual = await Assert.ThrowsAsync<InvalidOperationException>(() => receive);
                    Assert.Same(lookupError, actual);
                    batch.Data.Return();
                }
            }
            finally
            {
                MultiJoinOperator.ParallelInsertMinRows = minRowsBefore;
                MultiJoinOperator.ParallelInserts = parallelBefore;
                MultiJoinOperator.JoinHookForTests = hookBefore;
            }
        }

        private sealed class PartialFailInserter : IBPlusTreeBulkInserter<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer>
        {
            private readonly IBPlusTreeBulkInserter<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer> _inner;
            public bool FailAfterFirstKey = true;

            public PartialFailInserter(IBPlusTreeBulkInserter<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer> inner)
            {
                _inner = inner;
            }

            public int LeafHitCount => _inner.LeafHitCount;

            public int[] SortAndGetIndices(ColumnRowReference[] keys, int keyLength) => _inner.SortAndGetIndices(keys, keyLength);

            public ValueTask ApplyBatch<TMutator>(ColumnRowReference[] keys, MultiJoinWeight[] values, int keyLength, TMutator mutator, int totalBatchByteSize)
                where TMutator : IRowMutator<ColumnRowReference, MultiJoinWeight> =>
                _inner.ApplyBatch(keys, values, keyLength, mutator, totalBatchByteSize);

            public async ValueTask ApplyBatch<TMutator>(
                ColumnRowReference[] keys,
                MultiJoinWeight[] values,
                int keyLength,
                int[] sortedIndices,
                int[] duplicateTags,
                TMutator mutator,
                int totalBatchByteSize)
                where TMutator : IRowMutator<ColumnRowReference, MultiJoinWeight>
            {
                if (FailAfterFirstKey && keyLength > 0)
                {
                    FailAfterFirstKey = false;
                    await _inner.ApplyBatch(keys, values, 1, new[] { sortedIndices[0] }, new[] { 0 }, mutator, sizeof(int));
                    throw new IOException("Injected failure after writing first leaf key");
                }
                await _inner.ApplyBatch(keys, values, keyLength, sortedIndices, duplicateTags, mutator, totalBatchByteSize);
            }
        }

        [Fact]
        public async Task FailedFirstSpillInPadTableIsClearedBeforeNextSpilledDrain()
        {
            int memoryKeysBefore = MultiJoinPadTable.MemoryKeys;
            MultiJoinPadTable.MemoryKeys = 0;
            try
            {
                var root = Join(JoinType.Left, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                var (stateManager, streamMemory) = await CreateStateManager("pad_partial_spill");
                var op = CreateOperator(plan);
                await InitOperator(op, stateManager, streamMemory, 0);

                var batchA = Batch(new[] { (1L, 10L), (2L, 20L) }, new[] { 1, 1 });
                try
                {
                    await foreach (var item in op.OnRecieve(0, batchA, 0))
                    {
                        item.Data.Rent(1);
                        item.Data.Return();
                    }
                }
                finally
                {
                    batchA.Data.Return();
                }

                var batchB = Batch(new[] { (1L, 100L), (2L, 200L) }, new[] { 1, 1 });
                try
                {
                    await foreach (var item in op.OnRecieve(1, batchB, 0))
                    {
                        item.Data.Rent(1);
                        item.Data.Return();
                    }
                }
                finally
                {
                    batchB.Data.Return();
                }

                var table = op.PadTables[0];
                var writer = (MultiJoinTreeWriter)typeof(MultiJoinPadTable)
                    .GetField("_writer", BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(table)!;
                var wrapper = new PartialFailInserter(writer.Inserter);
                typeof(MultiJoinTreeWriter)
                    .GetProperty(nameof(MultiJoinTreeWriter.Inserter))!
                    .SetValue(writer, wrapper);

                // First spill writes key 1 to the pad tree and faults before MoveToTree finishes
                using var failedKeys = Column.Create(GlobalMemoryManager.Instance);
                failedKeys.Add(new Int64Value(1));
                failedKeys.Add(new Int64Value(2));
                await Assert.ThrowsAsync<IOException>(() =>
                    table.Add(new IColumn[] { failedKeys }, 2, new[] { true, true }, new[] { 1, 1 }).AsTask());
                table.Abandon();

                // Second message touches only key 2; stale key 1 from the aborted spill must not be drained
                using var cleanKeys = Column.Create(GlobalMemoryManager.Instance);
                cleanKeys.Add(new Int64Value(2));
                await table.Add(new IColumn[] { cleanKeys }, 1, new[] { true }, new[] { 1 });

                int drainedRows = 0;
                await foreach (var batch in table.Drain())
                {
                    drainedRows += batch.Data.Count;
                    batch.Data.Rent(1);
                    batch.Data.Return();
                }
                Assert.Equal(1, drainedRows);
            }
            finally
            {
                MultiJoinPadTable.MemoryKeys = memoryKeysBefore;
            }
        }

        [Fact]
        public async Task CutShortSpilledPadTableReleasesStalePagesOnNextSmallDrain()
        {
            int memoryKeysBefore = MultiJoinPadTable.MemoryKeys;
            try
            {
                var root = Join(JoinType.Left, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                var (stateManager, streamMemory) = await CreateStateManager("pad_stale_pages");
                var op = CreateOperator(plan);
                await InitOperator(op, stateManager, streamMemory, 0);
                var table = op.PadTables[0];

                using var oneKey = Column.Create(GlobalMemoryManager.Instance);
                oneKey.Add(new FlowtideDotNet.Core.ColumnStore.StringValue(new string('a', 64)));
                await table.Add(new IColumn[] { oneKey }, 1, new[] { true }, new[] { 1 });
                await foreach (var batch in table.Drain())
                {
                    batch.Data.Rent(1);
                    batch.Data.Return();
                }
                long baselineBytes = streamMemory.GetAllocatedMemory();

                // Spill 1,500 wide keys into the ephemeral pad tree and abandon the message
                MultiJoinPadTable.MemoryKeys = 0;
                const int SpilledKeys = 1500;
                using var manyKeys = Column.Create(GlobalMemoryManager.Instance);
                for (int i = 0; i < SpilledKeys; i++)
                {
                    manyKeys.Add(new FlowtideDotNet.Core.ColumnStore.StringValue(i.ToString().PadLeft(64, 'x')));
                }
                await table.Add(
                    new IColumn[] { manyKeys },
                    SpilledKeys,
                    Enumerable.Repeat(true, SpilledKeys).ToArray(),
                    Enumerable.Repeat(1, SpilledKeys).ToArray());
                table.Abandon();

                // Subsequent small messages stay in memory; draining must not retain the abandoned spilled tree
                MultiJoinPadTable.MemoryKeys = 1024;
                await table.Add(new IColumn[] { oneKey }, 1, new[] { true }, new[] { 1 });
                await foreach (var batch in table.Drain())
                {
                    batch.Data.Rent(1);
                    batch.Data.Return();
                }

                long afterSmallDrainBytes = streamMemory.GetAllocatedMemory();
                Assert.True(
                    afterSmallDrainBytes <= baselineBytes + 8192,
                    $"Abandoned spilled pad tree retained {afterSmallDrainBytes - baselineBytes} bytes after subsequent small drain");
            }
            finally
            {
                MultiJoinPadTable.MemoryKeys = memoryKeysBefore;
            }
        }

        [Fact]
        public async Task KeysOfACutShortMessageAreNotDrainedWithTheNextOne()
        {
            int budgetBefore = MultiJoinOperator.RowBudget;
            int memoryKeysBefore = MultiJoinPadTable.MemoryKeys;
            MultiJoinOperator.RowBudget = 1;
            MultiJoinPadTable.MemoryKeys = 0;
            try
            {
                var root = Join(JoinType.Left, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                var (stateManager, streamMemory) = await CreateStateManager("pad_stale");
                var op = CreateOperator(plan);
                await InitOperator(op, stateManager, streamMemory, 0);

                await RowsPerKey(op, 0, Batch(new[] { (1L, 10L), (2L, 20L) }, new[] { 1, 1 }));
                await RowsPerKey(op, 1, Batch(new[] { (1L, 100L), (2L, 200L) }, new[] { 1, 1 }));

                // Keys 1 and 2 go to the tree, the message is dropped while its corrections are drained
                var cut = Batch(new[] { (1L, 99L), (2L, 99L) }, new[] { 1, 1 });
                try
                {
                    await using var enumerator = op.OnRecieve(2, cut, 0).GetAsyncEnumerator();
                    for (int i = 0; i < 3; i++)
                    {
                        Assert.True(await enumerator.MoveNextAsync());
                        enumerator.Current.Data.Rent(1);
                        enumerator.Current.Data.Return();
                    }
                }
                finally
                {
                    cut.Data.Return();
                }

                // Only key 1 arrives now: its joined row and the removal of its padded row, nothing for key 2
                var rows = await RowsPerKey(op, 2, Batch(new[] { (1L, 99L) }, new[] { 1 }));
                Assert.False(rows.ContainsKey(2), $"{rows.GetValueOrDefault(2)} rows of key 2 came out");
                Assert.Equal(2, rows.GetValueOrDefault(1));
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
                MultiJoinPadTable.MemoryKeys = memoryKeysBefore;
            }
        }

        // Rows that come out per value of the first output column
        private static async Task<Dictionary<long, int>> RowsPerKey(MultiJoinOperator op, int target, StreamEventBatch batch)
        {
            var rows = new Dictionary<long, int>();
            try
            {
                await foreach (var item in op.OnRecieve(target, batch, 0))
                {
                    item.Data.Rent(1);
                    var column = item.Data.EventBatchData.Columns[0];
                    for (int i = 0; i < item.Data.Weights.Count; i++)
                    {
                        long key = column.GetValueAt(i, default).AsLong;
                        rows[key] = rows.GetValueOrDefault(key) + 1;
                    }
                    item.Data.Return();
                }
            }
            finally
            {
                batch.Data.Return();
            }
            return rows;
        }

        [Fact]
        public async Task StateWrittenForDifferentInputColumnCountIsRefused()
        {
            var (stateManager, streamMemory) = await CreateStateManager("col_count");

            var root2Cols = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (2, 4));
            var plan2Cols = MultiJoinPlanBuilder.Discover(root2Cols, 5)!;
            var first = CreateOperator(plan2Cols);
            await InitOperator(first, stateManager, streamMemory, 0);

            var batch = Batch(new[] { (1L, 10L) }, new[] { 1 });
            try
            {
                await foreach (var item in first.OnRecieve(0, batch, 0))
                {
                    item.Data.Rent(1);
                    item.Data.Return();
                }
            }
            finally
            {
                batch.Data.Return();
            }

            await first.OnCheckpoint();
            stateManager.Metadata = new StreamState(1, "hash");
            await stateManager.CheckpointAsync(false);

            // Input 0 now has 3 columns instead of 2, while PrimaryOrder ([0]) and SecondaryLayouts ([]) are unchanged
            var root3Cols = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 3), Read("b", 2), (0, 3)), Read("c", 2), (3, 5));
            var plan3Cols = MultiJoinPlanBuilder.Discover(root3Cols, 5)!;
            var second = CreateOperator(plan3Cols);
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => InitOperator(second, stateManager, streamMemory, 1));
            Assert.Contains("new version", error.Message);
        }

        [Fact]
        public void LoopBodyReferencedThroughInnerLoopIsNotMarked()
        {
            var outerRead = new IterationReferenceReadRelation() { IterationName = "outer", ReferenceOutputLength = 2 };
            var innerRead = new IterationReferenceReadRelation() { IterationName = "inner", ReferenceOutputLength = 2 };
            var innerStep = Join(JoinType.Inner, innerRead, new ReferenceRelation() { RelationId = 2, ReferenceOutputLength = 2 }, (0, 2));
            innerStep.Emit = new List<int>() { 0, 1 };
            var innerLoop = new IterationRelation()
            {
                IterationName = "inner",
                Input = VirtualTable(2),
                LoopPlan = innerStep
            };

            var lower = Join(JoinType.Inner, VirtualTable(2), VirtualTable(2), (0, 2));
            var rootInOuter = Join(JoinType.Inner, lower, innerLoop, (0, 4));
            rootInOuter.Emit = new List<int>() { 0, 1 };

            var outerLoop = new IterationRelation()
            {
                IterationName = "outer",
                Input = VirtualTable(2),
                LoopPlan = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = 2 }
            };

            var plan = new Plan()
            {
                Relations = new List<Relation>() { outerLoop, rootInOuter, outerRead }
            };

            Assert.Equal(0, MultiJoinMarker.Mark(plan, 5));
        }
    }
}

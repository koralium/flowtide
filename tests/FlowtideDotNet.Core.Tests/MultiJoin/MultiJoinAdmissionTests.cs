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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
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
    public class MultiJoinAdmissionTests : IAsyncLifetime
    {
        private readonly string _directory = $"./data/multiJoinAdmissionTests/{Guid.NewGuid():N}";
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

        private static ReadRelation Read(string name, int columns)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { name } },
                BaseSchema = new NamedStruct()
                {
                    Names = Enumerable.Range(0, columns).Select(x => "c" + x).ToList(),
                    Struct = new Struct() { Types = Enumerable.Range(0, columns).Select(_ => (SubstraitBaseType)new AnyType()).ToList() }
                }
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

        private static StreamEventBatch Batch(long[][] rows)
        {
            int columnCount = rows[0].Length;
            var columns = new IColumn[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                columns[c] = Column.Create(GlobalMemoryManager.Instance);
            }
            var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            for (int r = 0; r < rows.Length; r++)
            {
                for (int c = 0; c < columnCount; c++)
                {
                    columns[c].Add(new Int64Value(rows[r][c]));
                }
                weights.Add(1);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return batch;
        }

        private static async Task SendToOperator(MultiJoinOperator op, int targetIndex, StreamEventBatch batch)
        {
            try
            {
                await foreach (var output in op.OnRecieve(targetIndex, batch, 0))
                {
                    output.Data.Rent(1);
                    output.Data.Return();
                }
            }
            finally
            {
                batch.Data.Return();
            }
        }

        [Theory]
        [InlineData(MultiJoinOperator.DefaultRowBudget)]
        [InlineData(1)]
        public async Task PreservedSideEndingInMarkJoinDoesNotRememberKeysFailingKeptOnlyCondition(int rowBudget)
        {
            int budgetBefore = MultiJoinOperator.RowBudget;
            MultiJoinOperator.RowBudget = rowBudget;
            try
            {
                // (a(k, m, flag) LEFT MARK JOIN b(m, v) ON a.m = b.m) LEFT JOIN c(k, v) ON a.k = c.k AND a.flag > a.m
                var abMark = Join(JoinType.LeftMark, Read("a", 3), Read("b", 2), (1, 3));
                var root = Join(JoinType.Left, abMark, Read("c", 2), (0, 4));
                root.PostJoinFilter = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsComparison.Urn,
                    ExtensionName = FunctionsComparison.GreaterThan,
                    Arguments = new List<Expression>() { Field(2), Field(1) }
                };

                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                Assert.NotNull(plan);

                var storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/f20_{rowBudget}/persistent" });
                _disposables.Add(storage);
                var meter = new Meter("statemanager");
                _disposables.Add(meter);
                var stateManager = new StateManagerSync<StreamState>(
                    new StateManagerOptions()
                    {
                        PersistentStorage = storage,
                        TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"{_directory}/f20_{rowBudget}/tmp" }
                    },
                    NullLoggerFactory.Instance,
                    meter,
                    "stream",
                    GlobalMemoryManager.Instance);
                _disposables.Add(stateManager);
                await stateManager.InitializeAsync();
                var streamMemory = new StreamMemoryManager("stream");
                _disposables.Add(streamMemory);

                var functions = new FunctionsRegister();
                BuiltinFunctions.RegisterFunctions(functions);
                var op = new MultiJoinOperator(plan, functions, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
                _asyncDisposables.Add(op);
                op.Setup("stream", "1");
                op.CreateBlock();
                op.Link();
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
                await op.Initialize("1", 0, 0, handler, null);

                // Every row in a has m = 1, flag = 1, so KeptOnlyCheck (a.flag > a.m) is false for every key 1..1025.
                var rowsA = Enumerable.Range(1, 1025).Select(k => new[] { (long)k, 1L, 1L }).ToArray();
                var rowsB = new[] { new[] { 1L, 0L } };
                var rowsC = Enumerable.Range(1, 1025).Select(k => new[] { (long)k, 0L }).ToArray();

                await SendToOperator(op, 0, Batch(rowsA));
                await SendToOperator(op, 1, Batch(rowsB));
                await SendToOperator(op, 2, Batch(rowsC));

                // PadTables[0] belongs to abMark; PadTables[1] belongs to the outer LeftJoin whose KeptOnlyCheck failed on every row.
                Assert.Equal(2, op.PadTables.Count);
                Assert.Equal(0, op.PadTables[1].KeysAdded);
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
            }
        }

        private static long MeasurePadGatherAllocations(IColumn seed, IColumn incoming, int count)
        {
            var join = new MultiJoinNode
            {
                RightKey = new MultiJoinKeySide { Input = 0, Columns = new[] { 0 } }
            };
            var table = new MultiJoinPadTable(join, true, 0);
            typeof(MultiJoinPadTable).GetField("_allocator", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(table, GlobalMemoryManager.Instance);
            var seedColumns = new[] { seed };
            var incomingColumns = new[] { incoming };
            var found = Enumerable.Repeat(true, count).ToArray();
            var weights = Enumerable.Repeat(1, count).ToArray();
            int previousLimit = MultiJoinPadTable.MemoryKeys;
            MultiJoinPadTable.MemoryKeys = int.MaxValue;
            try
            {
                for (int i = 0; i < 4; i++)
                {
                    Assert.True(table.Add(seedColumns, 1, found, weights).IsCompletedSuccessfully);
                    Assert.True(table.Add(incomingColumns, count, found, weights).IsCompletedSuccessfully);
                    table.Abandon();
                }
                Assert.True(table.Add(seedColumns, 1, found, weights).IsCompletedSuccessfully);
                long before = GC.GetAllocatedBytesForCurrentThread();
                var copy = table.Add(incomingColumns, count, found, weights);
                long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                Assert.True(copy.IsCompletedSuccessfully);
                return allocated;
            }
            finally
            {
                table.Abandon();
                MultiJoinPadTable.MemoryKeys = previousLimit;
            }
        }

        [Fact]
        public void PadKeysWithMismatchedStructHeaderOrListColumnsDoNotAllocateAnObjectPerRow()
        {
            var headerA = StructHeader.Create("a");
            var headerB = StructHeader.Create("b");
            using var seedInt = Column.Create(GlobalMemoryManager.Instance);
            using var incomingInt = Column.Create(GlobalMemoryManager.Instance);
            using var seedStruct = Column.Create(GlobalMemoryManager.Instance);
            using var incomingStruct = Column.Create(GlobalMemoryManager.Instance);
            using var seedList = Column.Create(GlobalMemoryManager.Instance);
            using var incomingList = Column.Create(GlobalMemoryManager.Instance);

            seedInt.Add(new Int64Value(0));
            seedStruct.Add(new StructValue(headerA, new Int64Value(0)));
            seedList.Add(new ListValue(new Int64Value(0)));

            for (int i = 0; i < 512; i++)
            {
                incomingInt.Add(new Int64Value(i + 1));
                incomingStruct.Add(new StructValue(headerB, new Int64Value(i + 1)));
                incomingList.Add(new ListValue(new Int64Value(i + 1)));
            }

            long intSmall = MeasurePadGatherAllocations(seedInt, incomingInt, 64);
            long intLarge = MeasurePadGatherAllocations(seedInt, incomingInt, 512);

            long structSmall = MeasurePadGatherAllocations(seedStruct, incomingStruct, 64);
            long structLarge = MeasurePadGatherAllocations(seedStruct, incomingStruct, 512);
            long structExcessGrowth = (structLarge - intLarge) - (structSmall - intSmall);

            long listSmall = MeasurePadGatherAllocations(seedList, incomingList, 64);
            long listLarge = MeasurePadGatherAllocations(seedList, incomingList, 512);
            long listExcessGrowth = (listLarge - intLarge) - (listSmall - intSmall);

            Assert.True(structExcessGrowth <= 4096 && listExcessGrowth <= 4096,
                $"Container pad-key gather allocates per row: int 64/512={intSmall}/{intLarge}, " +
                $"mismatched-struct 64/512={structSmall}/{structLarge} (excess={structExcessGrowth}), " +
                $"list 64/512={listSmall}/{listLarge} (excess={listExcessGrowth}).");
        }
    }
}

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

        private async Task<(StateManagerSync<StreamState> stateManager, StreamMemoryManager streamMemory)> CreateStateManager(string folder)
        {
            var storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/{folder}/persistent" });
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

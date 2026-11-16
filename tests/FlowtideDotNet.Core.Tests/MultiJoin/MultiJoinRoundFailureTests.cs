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
using FlowtideDotNet.Base.Metrics.Internal;
using FlowtideDotNet.Base.Vertices;
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
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinRoundFailureTests : IAsyncLifetime
    {
        private readonly string _directory = $"./data/multiJoinRoundReviewTests/{Guid.NewGuid():N}";
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

        private static StreamEventBatch PlainBatch(long[][] rows, int[] weights)
        {
            var allocator = GlobalMemoryManager.Instance;
            int columnCount = rows[0].Length;
            var columns = new IColumn[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                var column = Column.Create(allocator);
                for (int r = 0; r < rows.Length; r++)
                {
                    column.Add(new Int64Value(rows[r][c]));
                }
                columns[c] = column;
            }
            var weightList = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            for (int r = 0; r < rows.Length; r++)
            {
                weightList.Add(weights[r]);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weightList, iterations, new EventBatchData(columns)));
            batch.Rent(1);
            return batch;
        }

        // Non-contiguous offset views as emitted by an upstream join at a group boundary
        private static StreamEventBatch ReversedViewBatch(int rowCount, int columnCount)
        {
            var allocator = GlobalMemoryManager.Instance;
            var offsets = new PrimitiveList<int>(allocator, rowCount);
            for (int r = 0; r < rowCount; r++)
            {
                offsets.Add(rowCount - 1 - r);
            }
            var columns = new IColumn[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                var inner = Column.Create(allocator);
                for (int r = 0; r < rowCount; r++)
                {
                    inner.Add(new Int64Value((r / 2) + 100 + c));
                }
                columns[c] = new ColumnWithOffset(inner, offsets);
            }
            var weights = new PrimitiveList<int>(allocator, rowCount);
            var iterations = new PrimitiveList<uint>(allocator, rowCount);
            for (int r = 0; r < rowCount; r++)
            {
                weights.Add(r % 2 == 0 ? 1 : -1);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Rent(1);
            return batch;
        }

        private async Task<(MultiJoinOperator op, StreamMemoryManager streamMemory, string streamName)> CreateInitializedOperator(MultiJoinPlan plan, string folder, TaskScheduler? scheduler = null)
        {
            string streamName = $"stream_{Guid.NewGuid():N}";
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
                streamName,
                GlobalMemoryManager.Instance);
            _disposables.Add(stateManager);
            await stateManager.InitializeAsync();

            var streamMemory = new StreamMemoryManager(streamName);
            _disposables.Add(streamMemory);
            var metrics = new StreamMetrics(streamName);
            _disposables.Add(metrics);

            var functions = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functions);
            var options = new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = 100,
                MaxDegreeOfParallelism = 1
            };
            if (scheduler != null)
            {
                options.TaskScheduler = scheduler;
            }
            var op = new MultiJoinOperator(plan, functions, options);
            _asyncDisposables.Add(op);
            op.LinkTo(new BufferBlock<IStreamEvent>());
            op.Setup(streamName, "1");
            op.CreateBlock();
            op.Link();

            var handler = new VertexHandler(
                streamName,
                "1",
                (_, _) => { },
                (_, _, _) => Task.CompletedTask,
                metrics.GetOrCreateVertexMeter("1", () => ""),
                stateManager.GetOrCreateClient("1"),
                NullLoggerFactory.Instance,
                streamMemory.CreateOperatorMemoryManager("1"),
                (_, _) => Task.CompletedTask);
            await op.Initialize("1", 0, 0, handler, null);
            return (op, streamMemory, streamName);
        }

        private static Task Drain(IAsyncEnumerable<StreamEventBatch> output)
        {
            // A round that never ends fails the test, it does not hang it
            return DrainAll(output).WaitAsync(TimeSpan.FromSeconds(60));
        }

        private static async Task DrainAll(IAsyncEnumerable<StreamEventBatch> output)
        {
            await foreach (var part in output)
            {
                part.Rent(1);
                part.Return();
            }
        }

        [Fact]
        public async Task MergingOffsetViewBatchesInARoundDoesNotBoxPerRow()
        {
            bool parallelBefore = MultiJoinOperator.ParallelInserts;
            MultiJoinOperator.ParallelInserts = false;
            try
            {
                var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 4), Read("b", 2), (0, 4)), Read("c", 2), (4, 6));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                var (op, _, _) = await CreateInitializedOperator(plan, "merge_view_alloc");

                async Task<long> RunMergedViews(int rowsPerBatch)
                {
                    var first = ReversedViewBatch(rowsPerBatch, 4);
                    var second = ReversedViewBatch(rowsPerBatch, 4);
                    var round = new[]
                    {
                        new RoundMessage<StreamEventBatch>(0, first),
                        new RoundMessage<StreamEventBatch>(0, second)
                    };
                    try
                    {
                        long before = GC.GetAllocatedBytesForCurrentThread();
                        await Drain(op.OnRecieveRound(round, 0));
                        return GC.GetAllocatedBytesForCurrentThread() - before;
                    }
                    finally
                    {
                        first.Return();
                        second.Return();
                    }
                }

                for (int i = 0; i < 4; i++)
                {
                    await RunMergedViews(64);
                    await RunMergedViews(512);
                }

                long smallBytes = await RunMergedViews(64);
                long largeBytes = await RunMergedViews(512);
                long extraGrowth = largeBytes - smallBytes;

                // 896 added rows across 4 Int64 view columns box 86,016 B when Merge calls Column.InsertRangeFrom directly
                Assert.True(
                    extraGrowth <= 8192,
                    $"Round Merge allocated {extraGrowth} extra managed bytes across 896 added view rows (small={smallBytes}, large={largeBytes})");
            }
            finally
            {
                MultiJoinOperator.ParallelInserts = parallelBefore;
            }
        }

        [Fact]
        public async Task EarlyDisposalOfARoundReleasesTheYieldedOutputBatch()
        {
            int budgetBefore = MultiJoinOperator.RowBudget;
            int partsBefore = MultiJoinOperator.RoundPartBudgets;
            MultiJoinOperator.RowBudget = 1;
            MultiJoinOperator.RoundPartBudgets = 1;
            try
            {
                var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                var (op, streamMemory, _) = await CreateInitializedOperator(plan, "early_dispose_round");

                var seedB = PlainBatch(new[] { new[] { 1L, 10L } }, new[] { 1 });
                var seedC = PlainBatch(new[] { new[] { 1L, 20L } }, new[] { 1 });
                try
                {
                    await Drain(op.OnRecieve(1, seedB, 0));
                    await Drain(op.OnRecieve(2, seedC, 0));
                }
                finally
                {
                    seedB.Return();
                    seedC.Return();
                }

                async Task RunOneRound(bool stopAfterFirstBatch)
                {
                    var batchA = PlainBatch(new[] { new[] { 1L, 100L }, new[] { 1L, 100L } }, new[] { 1, -1 });
                    var batchB = PlainBatch(new[] { new[] { 2L, 200L }, new[] { 2L, 200L } }, new[] { 1, -1 });
                    var round = new[]
                    {
                        new RoundMessage<StreamEventBatch>(0, batchA),
                        new RoundMessage<StreamEventBatch>(1, batchB)
                    };
                    try
                    {
                        await using var enumerator = op.OnRecieveRound(round, 0).GetAsyncEnumerator();
                        while (await enumerator.MoveNextAsync())
                        {
                            enumerator.Current.Rent(1);
                            enumerator.Current.Return();
                            if (stopAfterFirstBatch)
                            {
                                break;
                            }
                        }
                    }
                    finally
                    {
                        batchA.Return();
                        batchB.Return();
                    }
                }

                await RunOneRound(stopAfterFirstBatch: false);
                await RunOneRound(stopAfterFirstBatch: false);
                long baseline = streamMemory.GetAllocatedMemory();

                for (int i = 0; i < 32; i++)
                {
                    await RunOneRound(stopAfterFirstBatch: true);
                }

                long afterChurn = streamMemory.GetAllocatedMemory();
                Assert.True(
                    afterChurn <= baseline + 4096,
                    $"Stopping a round at yield return leaked {afterChurn - baseline} native bytes across 32 rounds");
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
                MultiJoinOperator.RoundPartBudgets = partsBefore;
            }
        }

        private sealed class FailSecondOwnerQueueScheduler : TaskScheduler, IDisposable
        {
            private readonly ManualResetEventSlim _firstParked = new ManualResetEventSlim(false);
            private int _ownerThreadId = -1;
            private int _ownerQueues;

            public void ArmOnCurrentThread()
            {
                _firstParked.Reset();
                _ownerQueues = 0;
                Volatile.Write(ref _ownerThreadId, Environment.CurrentManagedThreadId);
            }

            protected override void QueueTask(Task task)
            {
                if (Environment.CurrentManagedThreadId == Volatile.Read(ref _ownerThreadId))
                {
                    int call = Interlocked.Increment(ref _ownerQueues);
                    if (call == 1)
                    {
                        ThreadPool.QueueUserWorkItem(_ =>
                        {
                            try
                            {
                                TryExecuteTask(task);
                            }
                            finally
                            {
                                _firstParked.Set();
                            }
                        });
                        return;
                    }
                    if (call == 2)
                    {
                        _firstParked.Wait(TimeSpan.FromSeconds(5));
                        throw new InvalidOperationException("Rejected round worker scheduling");
                    }
                }
                ThreadPool.QueueUserWorkItem(_ => TryExecuteTask(task));
            }

            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

            protected override IEnumerable<Task> GetScheduledTasks() => Enumerable.Empty<Task>();

            public void Dispose() => _firstParked.Dispose();
        }

        [Fact]
        public async Task RoundSchedulingFailureAwaitsStartedWorkersAndReleasesOutputParts()
        {
            int budgetBefore = MultiJoinOperator.RowBudget;
            int partsBefore = MultiJoinOperator.RoundPartBudgets;
            MultiJoinOperator.RowBudget = 1;
            MultiJoinOperator.RoundPartBudgets = 1;
            try
            {
                using var scheduler = new FailSecondOwnerQueueScheduler();
                var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
                var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
                var (op, streamMemory, _) = await CreateInitializedOperator(plan, "round_sched_fail", scheduler);

                // Six matches exceed BoundedChannel(participants * 2 = 4) so ReadInRound(0) parks on WriteAsync unless drained
                var seedB = PlainBatch(
                    Enumerable.Range(10, 6).Select(v => new[] { 1L, (long)v }).ToArray(),
                    Enumerable.Repeat(1, 6).ToArray());
                var seedC = PlainBatch(new[] { new[] { 1L, 20L } }, new[] { 1 });
                try
                {
                    await Drain(op.OnRecieve(1, seedB, 0));
                    await Drain(op.OnRecieve(2, seedC, 0));
                }
                finally
                {
                    seedB.Return();
                    seedC.Return();
                }

                long baseline = streamMemory.GetAllocatedMemory();
                var b0First = PlainBatch(new[] { new[] { 1L, 100L } }, new[] { 1 });
                var b0Second = PlainBatch(new[] { new[] { 1L, 100L } }, new[] { -1 });
                var b1 = PlainBatch(new[] { new[] { 2L, 200L } }, new[] { 1 });
                try
                {
                    scheduler.ArmOnCurrentThread();
                    await Assert.ThrowsAnyAsync<Exception>(() => Drain(op.OnRecieveRound(new[]
                    {
                        new RoundMessage<StreamEventBatch>(0, b0First),
                        new RoundMessage<StreamEventBatch>(0, b0Second),
                        new RoundMessage<StreamEventBatch>(1, b1)
                    }, 0)));
                }
                finally
                {
                    b0First.Return();
                    b0Second.Return();
                    b1.Return();
                }

                long afterFailure = streamMemory.GetAllocatedMemory();
                Assert.True(
                    afterFailure <= baseline + 4096,
                    $"Failed round scheduling left {afterFailure - baseline} native bytes outstanding");
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
                MultiJoinOperator.RoundPartBudgets = partsBefore;
            }
        }

        [Fact]
        public async Task ParallelLookupsInAMultiInputRoundAreRecordedInMetric()
        {
            bool alwaysBefore = MultiJoinOperator.ParallelLookupsAlways;
            try
            {
                MultiJoinOperator.ParallelLookupsAlways = true;
                var ab = Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2));
                var abc = Join(JoinType.Inner, ab, Read("c", 2), (0, 4));
                var abcd = Join(JoinType.Inner, abc, Read("d", 2), (0, 6));
                var plan = MultiJoinPlanBuilder.Discover(abcd, 5)!;
                var (op, _, streamName) = await CreateInitializedOperator(plan, "round_parallel_metric");

                long parallelLookups = 0;
                using var listener = new MeterListener();
                listener.InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Name == "flowtide_parallel_lookups" && instrument.Meter.Name.Contains(streamName, StringComparison.Ordinal))
                    {
                        l.EnableMeasurementEvents(instrument);
                    }
                };
                listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref parallelLookups, value));
                listener.Start();

                for (int input = 0; input < 4; input++)
                {
                    var seed = PlainBatch(new[] { new[] { 1L, 10L + input } }, new[] { 1 });
                    try
                    {
                        await Drain(op.OnRecieve(input, seed, 0));
                    }
                    finally
                    {
                        seed.Return();
                    }
                }
                // The counter reports its total when it is asked: what the seeds counted is taken off
                listener.RecordObservableInstruments();
                long seeded = Interlocked.Exchange(ref parallelLookups, 0);

                var b0 = PlainBatch(new[] { new[] { 1L, 99L } }, new[] { 1 });
                var b1 = PlainBatch(new[] { new[] { 1L, 88L } }, new[] { 1 });
                try
                {
                    await Drain(op.OnRecieveRound(new[]
                    {
                        new RoundMessage<StreamEventBatch>(0, b0),
                        new RoundMessage<StreamEventBatch>(1, b1)
                    }, 0));
                }
                finally
                {
                    b0.Return();
                    b1.Return();
                }

                // The counter is an observable one, it reports when it is asked
                listener.RecordObservableInstruments();
                Assert.True(Interlocked.Read(ref parallelLookups) > seeded, "Parallel lookups run inside ReadInRound were not added to flowtide_parallel_lookups");
            }
            finally
            {
                MultiJoinOperator.ParallelLookupsAlways = alwaysBefore;
            }
        }

        private sealed class HoldReaderRejectStoreScheduler : TaskScheduler
        {
            private int _queued;
            private Task? _reader;
            public readonly InvalidOperationException Rejection = new InvalidOperationException("Rejected round store scheduling");
            public bool ReaderHeld => _reader != null;

            protected override IEnumerable<Task>? GetScheduledTasks() => Array.Empty<Task>();
            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

            protected override void QueueTask(Task task)
            {
                if (Interlocked.Increment(ref _queued) == 1)
                {
                    _reader = task;
                    return;
                }
                throw Rejection;
            }

            public void RunReader()
            {
                var reader = Interlocked.Exchange(ref _reader, null);
                if (reader != null)
                {
                    TryExecuteTask(reader);
                }
            }
        }

        [Fact]
        public async Task SchedulingAndReaderFailuresAreBothReportedAndMergedBatchIsReturned()
        {
            var hookBefore = MultiJoinOperator.RoundReadHookForTests;
            var scheduler = new HoldReaderRejectStoreScheduler();
            var root = Join(JoinType.Inner, Join(JoinType.Inner, Read("a", 2), Read("b", 2), (0, 2)), Read("c", 2), (0, 4));
            var plan = MultiJoinPlanBuilder.Discover(root, 5)!;
            var (op, streamMemory, _) = await CreateInitializedOperator(plan, "round_two_failures");
            typeof(MultiJoinOperator).GetField("_scheduler", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.SetValue(op, scheduler);
            var first = PlainBatch(new[] { new[] { 1L, 100L } }, new[] { 1 });
            var second = PlainBatch(new[] { new[] { 1L, 100L } }, new[] { -1 });
            var other = PlainBatch(new[] { new[] { 2L, 200L } }, new[] { 1 });
            var workerFailure = new IOException("The already started reader also failed");
            long baseline = streamMemory.GetAllocatedMemory();
            Task? running = null;
            try
            {
                MultiJoinOperator.RoundReadHookForTests = input =>
                {
                    Assert.Equal(0, input);
                    throw workerFailure;
                };
                running = Drain(op.OnRecieveRound(new[]
                {
                    new RoundMessage<StreamEventBatch>(0, first),
                    new RoundMessage<StreamEventBatch>(0, second),
                    new RoundMessage<StreamEventBatch>(1, other)
                }, 0));
                Assert.True(scheduler.ReaderHeld);
                Assert.False(running.IsCompleted);
                Assert.True(streamMemory.GetAllocatedMemory() > baseline, "The merged input must stay alive until its reader ends.");
                scheduler.RunReader();

                var failures = await Assert.ThrowsAsync<AggregateException>(() => running);
                Assert.Equal(2, failures.InnerExceptions.Count);
                var schedulingFailure = Assert.IsType<TaskSchedulerException>(failures.InnerExceptions[0]);
                Assert.Same(scheduler.Rejection, schedulingFailure.InnerException);
                Assert.Same(workerFailure, failures.InnerExceptions[1]);
                Assert.Equal(baseline, streamMemory.GetAllocatedMemory());
            }
            finally
            {
                scheduler.RunReader();
                if (running != null)
                {
                    try { await running; } catch { }
                }
                MultiJoinOperator.RoundReadHookForTests = hookBefore;
                first.Return();
                second.Return();
                other.Return();
            }
        }

        private sealed class StubVertexHandler : IVertexHandler
        {
            private readonly string _streamName = $"stream_{Guid.NewGuid():N}";
            public string OperatorId => "op";
            public string StreamName => _streamName;
            public IMeter Metrics { get; } = new FlowtideMeter(new Meter("test"), new TagList(), () => "test");
            public IStateManagerClient StateClient => null!;
            public ILoggerFactory LoggerFactory => NullLoggerFactory.Instance;
            public IOperatorMemoryManager MemoryManager => null!;
            public void ScheduleCheckpoint(TimeSpan time, long? checkpointVersion) { }
            public Task RegisterTrigger(string name, TimeSpan? scheduledInterval = null) => Task.CompletedTask;
            public Task FailAndRollback(Exception? exception, long? restoreVersion = default) => Task.CompletedTask;
        }

        private sealed class RentableItem : IRentable
        {
            public RentableItem(string id) => Id = id;
            public string Id { get; }
            public int Rented;
            public int Returned;
            public void Rent(int count) => Interlocked.Add(ref Rented, count);
            public void Return() => Interlocked.Increment(ref Returned);
        }

        private sealed class TestRoundVertex : MultipleInputRoundVertex<RentableItem>
        {
            public readonly List<string> Rounds = new List<string>();
            public readonly TaskCompletionSource FirstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly TaskCompletionSource Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public bool ThrowInRound;

            public TestRoundVertex(int capacity = 100) : base(2, new ExecutionDataflowBlockOptions { BoundedCapacity = capacity })
            {
            }

            public override string DisplayName => "test";
            public int Queued => QueuedEvents;
            protected override int MaxRoundMessages => 16;

            public override async IAsyncEnumerable<RentableItem> OnRecieve(int targetId, RentableItem msg, long time)
            {
                if (!FirstEntered.Task.IsCompleted)
                {
                    FirstEntered.TrySetResult();
                    await Gate.Task;
                }
                yield return new RentableItem($"{msg.Id}@{time}");
            }

            public override async IAsyncEnumerable<RentableItem> OnRecieveRound(IReadOnlyList<RoundMessage<RentableItem>> messages, long time)
            {
                lock (Rounds)
                {
                    Rounds.Add($"{time}:" + string.Join(",", messages.Select(m => m.Message.Id)));
                }
                if (ThrowInRound)
                {
                    throw new InvalidOperationException("Round failed");
                }
                foreach (var message in messages)
                {
                    yield return new RentableItem($"{message.Message.Id}@{time}");
                }
                await Task.CompletedTask;
            }

            public override Task OnCheckpoint() => Task.CompletedTask;
            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        [Fact]
        public async Task AFaultedRoundReturnsItsInputMessages()
        {
            await using var vertex = new TestRoundVertex(capacity: 2) { ThrowInRound = true };
            var collector = new BufferBlock<IStreamEvent>();
            var handler = new StubVertexHandler();
            vertex.Setup(handler.StreamName, "op");
            vertex.LinkTo(collector, new DataflowLinkOptions());
            vertex.CreateBlock();
            vertex.Link();
            await vertex.Initialize("op", 0, 0, handler, null);

            var first = new StreamMessage<RentableItem>(new RentableItem("a"), 1);
            var b = new StreamMessage<RentableItem>(new RentableItem("b"), 1);
            var c = new StreamMessage<RentableItem>(new RentableItem("c"), 1);

            await vertex.Targets[0].SendAsync(first);
            await vertex.FirstEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await vertex.Targets[0].SendAsync(b);
            await vertex.Targets[1].SendAsync(c);
            var watch = Stopwatch.StartNew();
            while (vertex.Queued < 2)
            {
                Assert.True(watch.Elapsed < TimeSpan.FromSeconds(10));
                await Task.Delay(1);
            }
            vertex.Gate.SetResult();

            await Assert.ThrowsAnyAsync<Exception>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal(1, b.Data.Returned);
            Assert.Equal(1, c.Data.Returned);
        }

        [Fact]
        public async Task MessagesWithDifferentLogicalTimesAreNotMergedIntoOneRound()
        {
            await using var vertex = new TestRoundVertex();
            var collector = new BufferBlock<IStreamEvent>();
            var handler = new StubVertexHandler();
            vertex.Setup(handler.StreamName, "op");
            vertex.LinkTo(collector, new DataflowLinkOptions());
            vertex.CreateBlock();
            vertex.Link();
            await vertex.Initialize("op", 0, 0, handler, null);

            await vertex.Targets[0].SendAsync(new StreamMessage<RentableItem>(new RentableItem("a"), 1));
            await vertex.FirstEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await vertex.Targets[0].SendAsync(new StreamMessage<RentableItem>(new RentableItem("b"), 1));
            await vertex.Targets[0].SendAsync(new StreamMessage<RentableItem>(new RentableItem("c"), 1));
            await vertex.Targets[0].SendAsync(new StreamMessage<RentableItem>(new RentableItem("d"), 2));
            await vertex.Targets[0].SendAsync(new StreamMessage<RentableItem>(new RentableItem("e"), 2));
            var watch = Stopwatch.StartNew();
            while (vertex.Queued < 4)
            {
                Assert.True(watch.Elapsed < TimeSpan.FromSeconds(10));
                await Task.Delay(1);
            }
            vertex.Gate.SetResult();

            var emitted = new List<(string Id, long Time)>();
            for (int i = 0; i < 5; i++)
            {
                var msg = Assert.IsType<StreamMessage<RentableItem>>(await collector.ReceiveAsync(TimeSpan.FromSeconds(10)));
                emitted.Add((msg.Data.Id, msg.Time));
            }

            Assert.Equal(new[] { "1:b,c", "2:d,e" }, vertex.Rounds);
            Assert.Equal(new[] { ("a@1", 1L), ("b@1", 1L), ("c@1", 1L), ("d@2", 2L), ("e@2", 2L) }, emitted);
        }
    }
}

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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using System.Reflection;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Base.Vertices;
using Host = FlowtideDotNet.Core.Tests.MultiJoin.MultiJoinAllocationTests.Host;
using System.Buffers;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinLookupSchedulingTests
    {
        private sealed class CountingAllocator : IMemoryAllocator
        {
            public long OutstandingBytes;
            public IMemoryOwner<byte> Allocate(int size, int alignment) => GlobalMemoryManager.Instance.Allocate(size, alignment);
            public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment) => GlobalMemoryManager.Instance.Realloc(memory, size, alignment);
            public void RegisterAllocationToMetrics(int size) => OutstandingBytes += size;
            public void RegisterFreeToMetrics(int size) => OutstandingBytes -= size;
        }

        [Fact]
        public async Task RoundParallelLookupsAreReportedInTheCounter()
        {
            bool forcedBefore = MultiJoinOperator.ParallelLookupsAlways;
            using var host = new Host();
            var plan = MultiJoinPlanBuilder.Discover(MultiJoinSignedTests.NamedPlan("star"), 9)!;
            await using var op = new MultiJoinOperator(plan, host.FunctionsRegister,
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            long reported = 0;
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Name.EndsWith("parallel_lookups", StringComparison.Ordinal))
                    l.EnableMeasurementEvents(instrument);
            };
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) => Interlocked.Add(ref reported, value));
            listener.Start();
            var messages = new List<RoundMessage<StreamEventBatch>>();
            try
            {
                MultiJoinOperator.ParallelLookupsAlways = true;
                op.LinkTo(new BufferBlock<IStreamEvent>());
                await host.InitializeOperator(op);
                for (int i = 0; i < plan.Inputs.Count; i++)
                    messages.Add(new RoundMessage<StreamEventBatch>(i, MultiJoinAllocationTests.CreateMessage(i, 1, 1, 0, plan.Inputs[i].ColumnCount).Data));
                // Operators of earlier tests may still report, only what the round adds counts
                listener.RecordObservableInstruments();
                long before = Interlocked.Exchange(ref reported, 0);
                await foreach (var part in op.OnRecieveRound(messages, 0))
                {
                    part.Rent(1);
                    part.Return();
                }
                listener.RecordObservableInstruments();
                Interlocked.Add(ref reported, -before);
                var cursors = (MultiJoinCursor[])typeof(MultiJoinOperator).GetField("_roundCursors", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(op)!;
                long unreported = cursors.Sum(c => c.TakeLookupsRunBeside());
                Assert.True(reported + unreported > 0, "The test must execute a beside lookup.");
                Assert.True(reported > 0, $"The round ran {unreported} beside lookups but its metric reported none.");
            }
            finally
            {
                MultiJoinOperator.ParallelLookupsAlways = forcedBefore;
                foreach (var message in messages) message.Message.Return();
            }
        }

        private sealed class RejectScheduler : TaskScheduler
        {
            protected override IEnumerable<Task>? GetScheduledTasks() => Array.Empty<Task>();
            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;
            protected override void QueueTask(Task task) => throw new InvalidOperationException("Rejected beside lookup");
        }

        private sealed class GatedSearch : IBplusTreeBulkSearch<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer, MultiJoinSearchComparer>
        {
            private readonly IBplusTreeBulkSearch<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer, MultiJoinSearchComparer> _inner;
            private readonly bool _gateStart;
            public readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public Task? Pending;
            public GatedSearch(IBplusTreeBulkSearch<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer, MultiJoinSearchComparer> inner, bool gateStart)
            {
                _inner = inner;
                _gateStart = gateStart;
            }
            public LeafNode<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer> CurrentLeaf => _inner.CurrentLeaf;
            public IReadOnlyList<BulkSearchKeyResult> CurrentResults => _inner.CurrentResults;
            public void Dispose() => _inner.Dispose();
            public ValueTask Start(ColumnRowReference[] keys, int keyLength) => _inner.Start(keys, keyLength);
            public ValueTask Start(ColumnRowReference[] keys, int keyLength, int[] sortedIndices)
            {
                if (!_gateStart) return _inner.Start(keys, keyLength, sortedIndices);
                Pending = StartGated(keys, keyLength, sortedIndices);
                return new ValueTask(Pending);
            }
            private async Task StartGated(ColumnRowReference[] keys, int keyLength, int[] sortedIndices)
            {
                Entered.TrySetResult();
                await Release.Task;
                await _inner.Start(keys, keyLength, sortedIndices);
            }
            public ValueTask<bool> MoveNextLeaf()
            {
                if (_gateStart) return _inner.MoveNextLeaf();
                var pending = MoveGated();
                Pending = pending;
                return new ValueTask<bool>(pending);
            }
            private async Task<bool> MoveGated()
            {
                Entered.TrySetResult();
                await Release.Task;
                return await _inner.MoveNextLeaf();
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task RejectedBesideLookupWaitsForTheForegroundLookup(bool gateStart)
        {
            using var host = new Host();
            var plan = MultiJoinPlanBuilder.Discover(MultiJoinSignedTests.NamedPlan("star"), 9)!;
            await using var op = new MultiJoinOperator(plan, host.FunctionsRegister,
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            op.LinkTo(new BufferBlock<IStreamEvent>());
            await host.InitializeOperator(op);
            var inputs = (MultiJoinInputState[])typeof(MultiJoinOperator).GetField("_inputs", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(op)!;
            var program = MultiJoinProgram.ForChange(plan, 0);
            Assert.True(program.Steps.Length >= 2);
            Assert.Equal(0, program.Steps[1].KeyFrame);
            var lookup = inputs[program.Steps[0].Input].GetLookup(program.Steps[0].KeyList);
            var primary = (MultiJoinTreeLookup)typeof(MultiJoinInputLookup).GetField("_primary", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(lookup)!;
            var searchField = typeof(MultiJoinTreeLookup).GetField("_searcher", BindingFlags.NonPublic | BindingFlags.Instance)!;
            var search = (IBplusTreeBulkSearch<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer, MultiJoinSearchComparer>)searchField.GetValue(primary)!;
            var gate = new GatedSearch(search, gateStart);
            searchField.SetValue(primary, gate);
            var cursor = new MultiJoinCursor(plan, program, inputs, Array.Empty<MultiJoinPadTable>(), GlobalMemoryManager.Instance, 1024, new RejectScheduler());
            var batch = MultiJoinAllocationTests.CreateMessage(0, 1, 1, 0, plan.Inputs[0].ColumnCount).Data;
            Assert.True(cursor.Start(batch.Data));
            Task<bool> move = cursor.MoveNext().AsTask();
            try
            {
                await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
                Assert.False(move.IsCompleted, "A scheduling failure must drain the already pending foreground lookup before reporting failure.");
            }
            finally
            {
                gate.Release.TrySetResult();
                await gate.Pending!.WaitAsync(TimeSpan.FromSeconds(30));
                await Assert.ThrowsAsync<TaskSchedulerException>(() => move);
                cursor.Dispose();
                batch.Return();
            }
        }

        [Fact]
        public async Task RejectedBesideLookupReleasesTheForegroundPartItWaitedFor()
        {
            using var host = new Host();
            var plan = MultiJoinPlanBuilder.Discover(MultiJoinSignedTests.NamedPlan("star"), 9)!;
            await using var op = new MultiJoinOperator(plan, host.FunctionsRegister,
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            op.LinkTo(new BufferBlock<IStreamEvent>());
            await host.InitializeOperator(op);
            var program = MultiJoinProgram.ForChange(plan, 0);
            int seededInput = program.Steps[0].Input;
            var seed = MultiJoinAllocationTests.CreateMessage(seededInput, 1, 1, 0, plan.Inputs[seededInput].ColumnCount).Data;
            try
            {
                await foreach (var part in op.OnRecieve(seededInput, seed, 0))
                {
                    part.Rent(1);
                    part.Return();
                }
            }
            finally
            {
                seed.Return();
            }
            var inputs = (MultiJoinInputState[])typeof(MultiJoinOperator).GetField("_inputs", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(op)!;
            var lookup = inputs[seededInput].GetLookup(program.Steps[0].KeyList);
            var primary = (MultiJoinTreeLookup)typeof(MultiJoinInputLookup).GetField("_primary", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(lookup)!;
            var searchField = typeof(MultiJoinTreeLookup).GetField("_searcher", BindingFlags.NonPublic | BindingFlags.Instance)!;
            var search = (IBplusTreeBulkSearch<ColumnRowReference, MultiJoinWeight, ColumnKeyStorageContainer, MultiJoinWeightContainer, MultiJoinSearchComparer>)searchField.GetValue(primary)!;
            var gate = new GatedSearch(search, false);
            searchField.SetValue(primary, gate);
            var allocator = new CountingAllocator();
            var cursor = new MultiJoinCursor(plan, program, inputs, Array.Empty<MultiJoinPadTable>(), allocator, 1024, new RejectScheduler());
            var batch = MultiJoinAllocationTests.CreateMessage(0, 1, 1, 0, plan.Inputs[0].ColumnCount).Data;
            Assert.True(cursor.Start(batch.Data));
            var frames = (MultiJoinFrame[])typeof(MultiJoinCursor).GetField("_frames", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(cursor)!;
            Task<bool> move = cursor.MoveNext().AsTask();
            try
            {
                await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
                Assert.False(move.IsCompleted);
                gate.Release.TrySetResult();
                await Assert.ThrowsAsync<TaskSchedulerException>(() => move);
                Assert.Equal(1, frames[1].Count);
                Assert.NotEmpty(frames[1].Columns);
                cursor.Dispose();
                Assert.Equal(0, allocator.OutstandingBytes);
            }
            finally
            {
                gate.Release.TrySetResult();
                try { await move; } catch (TaskSchedulerException) { }
                cursor.Dispose();
                // Clean up columns that a failing ownership assertion leaves behind.
                foreach (var frame in frames)
                {
                    foreach (var column in frame.Columns) column?.Dispose();
                }
                batch.Return();
            }
        }
    }
}

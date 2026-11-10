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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Metrics.Internal;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Tests
{
    public class VertexCancellationFaultGuardTests
    {
        private sealed class StubVertexHandler : IVertexHandler
        {
            public string OperatorId => "op";
            public string StreamName => "stream";
            public IMeter Metrics { get; } = new FlowtideMeter(new Meter("test"), new TagList(), () => "test");
            public IStateManagerClient StateClient => null!;
            public ILoggerFactory LoggerFactory => NullLoggerFactory.Instance;
            public IOperatorMemoryManager MemoryManager => null!;
            public void ScheduleCheckpoint(TimeSpan time, long? checkpointVersion) { }
            public Task RegisterTrigger(string name, TimeSpan? scheduledInterval = null) => Task.CompletedTask;
            public Task FailAndRollback(Exception? exception = null, long? restoreVersion = default) => Task.CompletedTask;
        }

        // Held callbacks throw a cancellation once released
        private sealed class Gate
        {
            public TaskCompletionSource Entered { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource Release { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public async Task HoldThenCancel()
            {
                Entered.TrySetResult();
                await Release.Task;
                throw new TaskCanceledException("http timeout");
            }
        }

        private sealed class SyncCancellingEgressVertex() : EgressVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public override string DisplayName => "test";
            protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
            protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
            public override Task OnTrigger(string name, object? state) => throw new TaskCanceledException("http timeout");
            protected override Task OnWatermark(Watermark watermark) => throw new TaskCanceledException("http timeout");
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class HeldEgressVertex() : EgressVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public Gate Gate { get; } = new Gate();
            public override string DisplayName => "test";
            protected override Task OnCheckpoint(long checkpointTime) => Gate.HoldThenCancel();
            protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class StopCommitEgressVertex() : EgressVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public override string DisplayName => "test";

            protected override async Task OnCheckpoint(long checkpointTime)
            {
                await Task.Yield();
                throw new FlowtideCommitStoppedException("The commit was given up on a stop request.");
            }

            protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class OutputThenCancelUnaryVertex() : UnaryVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public Gate Gate { get; } = new Gate();
            public override string DisplayName => "test";

            public override async IAsyncEnumerable<string> OnRecieve(string msg, long time)
            {
                yield return msg;
                await Gate.HoldThenCancel();
            }

            public override Task OnCheckpoint() => Task.CompletedTask;
            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class SyncCancellingUnaryVertex() : UnaryVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public override string DisplayName => "test";
            public override IAsyncEnumerable<string> OnRecieve(string msg, long time) => throw new TaskCanceledException("http timeout");
            public override Task OnCheckpoint() => Task.CompletedTask;
            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class HeldUnaryVertex() : UnaryVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public Gate Gate { get; } = new Gate();
            public override string DisplayName => "test";
            public override IAsyncEnumerable<string> OnRecieve(string msg, long time) => EmptyAsyncEnumerable<string>.Instance;
            public override Task OnCheckpoint() => Gate.HoldThenCancel();
            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class StopCommitUnaryVertex() : UnaryVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public override string DisplayName => "test";
            public override IAsyncEnumerable<string> OnRecieve(string msg, long time) => EmptyAsyncEnumerable<string>.Instance;

            public override async Task OnCheckpoint()
            {
                await Task.Yield();
                throw new FlowtideCommitStoppedException("The commit was given up on a stop request.");
            }

            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class HeldMultiVertex() : MultipleInputVertex<string>(2, new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public Gate Gate { get; } = new Gate();
            public override string DisplayName => "test";

            public override async IAsyncEnumerable<string> OnRecieve(int targetId, string msg, long time)
            {
                await Gate.HoldThenCancel();
                yield return msg;
            }

            public override Task OnCheckpoint() => Task.CompletedTask;
            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class HeldPartitionVertex() : PartitionVertex<string>(2, new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public Gate Gate { get; } = new Gate();
            public override string DisplayName => "test";

            protected override IAsyncEnumerable<KeyValuePair<int, StreamMessage<string>>> PartitionData(string data, long time) => EmptyAsyncEnumerable<KeyValuePair<int, StreamMessage<string>>>.Instance;

            protected override Task OnLockingEvent(ILockingEvent lockingEvent) => Gate.HoldThenCancel();

            protected override Task InitializeOrRestore(long restoreVersion, IStateManagerClient stateManagerClient) => Task.CompletedTask;
        }

        private sealed class HeldFixedPointVertex() : FixedPointVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public Gate Gate { get; } = new Gate();
            public override string DisplayName => "test";

            public override bool NoReadSourceInLoop() => true;

            protected override async IAsyncEnumerable<KeyValuePair<int, StreamMessage<string>>> OnIngressRecieve(string data, long time)
            {
                await Gate.HoldThenCancel();
                yield return new KeyValuePair<int, StreamMessage<string>>(0, new StreamMessage<string>(data, time));
            }

            protected override IAsyncEnumerable<KeyValuePair<int, StreamMessage<string>>> OnFeedbackRecieve(string data, long time) => EmptyAsyncEnumerable<KeyValuePair<int, StreamMessage<string>>>.Instance;

            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
        }

        private static async Task Start(IStreamVertex vertex)
        {
            vertex.Setup("stream", "op");
            vertex.CreateBlock();
            vertex.Link();
            await vertex.Initialize("op", 0, 0, new StubVertexHandler(), null);
        }

        private static async Task AssertCancellationFault(IStreamVertex vertex)
        {
            var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal("Operator op (test) was cancelled, but the stream did not request it", fault.Message);
            var cancellation = Assert.IsType<TaskCanceledException>(fault.InnerException);
            Assert.Equal("http timeout", cancellation.Message);
        }

        private static async Task FaultWhileHeld(IStreamVertex vertex, Gate gate, Exception stop)
        {
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            vertex.Fault(stop);
            gate.Release.TrySetResult();
            await Assert.ThrowsAnyAsync<Exception>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));
        }

        private static async Task FaultFirstRun(IStreamVertex vertex, Gate gate)
        {
            vertex.Fault(new InvalidOperationException("first run"));
            await Assert.ThrowsAnyAsync<Exception>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));
            gate.Release.TrySetResult();
        }

        [Fact]
        public async Task EgressVertexFaultsOnSyncStrayCancelInTrigger()
        {
            var vertex = new SyncCancellingEgressVertex();
            await Start(vertex);

            await vertex.QueueTrigger(new TriggerEvent("t", null));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task EgressVertexFaultsOnSyncStrayCancelInWatermark()
        {
            var vertex = new SyncCancellingEgressVertex();
            await Start(vertex);

            await vertex.SendAsync(new Watermark("w", LongWatermarkValue.Create(1), DateTimeOffset.UtcNow));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task EgressVertexPassesACommitGivenUpOnAStop()
        {
            var vertex = new StopCommitEgressVertex();
            ((IStreamEgressVertex)vertex).SetCheckpointDoneFunction((_, _) => { }, (_, _) => { });
            await Start(vertex);

            await vertex.SendAsync(new Checkpoint(0, 1, 1));
            vertex.Complete();

            await vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10));
        }

        [Fact]
        public async Task EgressVertexTokenIsFreshAfterRecreate()
        {
            var vertex = new HeldEgressVertex();
            ((IStreamEgressVertex)vertex).SetCheckpointDoneFunction((_, _) => { }, (_, _) => { });
            await Start(vertex);
            await FaultFirstRun(vertex, vertex.Gate);

            await Start(vertex);
            await vertex.SendAsync(new Checkpoint(0, 1, 1));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task UnaryVertexFaultsOnStrayCancelAfterOutput()
        {
            var vertex = new OutputThenCancelUnaryVertex();
            var output = new BufferBlock<IStreamEvent>();
            vertex.Setup("stream", "op");
            vertex.CreateBlock();
            vertex.LinkTo(output);
            vertex.Link();
            await vertex.Initialize("op", 0, 0, new StubVertexHandler(), null);

            await vertex.SendAsync(new StreamMessage<string>("a", 1));
            var first = await output.ReceiveAsync(TimeSpan.FromSeconds(10));
            Assert.Equal("a", Assert.IsType<StreamMessage<string>>(first).Data);
            vertex.Gate.Release.TrySetResult();

            await AssertCancellationFault(vertex);
            Assert.False(await vertex.SendAsync(new Checkpoint(0, 1, 1)));
        }

        [Fact]
        public async Task UnaryVertexFaultsOnSyncStrayCancelInRecieve()
        {
            var vertex = new SyncCancellingUnaryVertex();
            await Start(vertex);

            await vertex.SendAsync(new StreamMessage<string>("a", 1));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task UnaryVertexPassesACommitGivenUpOnAStop()
        {
            var vertex = new StopCommitUnaryVertex();
            await Start(vertex);

            await vertex.SendAsync(new Checkpoint(0, 1, 1));
            vertex.Complete();

            await vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10));
        }

        [Fact]
        public async Task UnaryVertexTokenIsFreshAfterRecreate()
        {
            var vertex = new HeldUnaryVertex();
            await Start(vertex);
            await FaultFirstRun(vertex, vertex.Gate);

            await Start(vertex);
            await vertex.SendAsync(new Checkpoint(0, 1, 1));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task MultipleInputVertexEndsCancelledOnItsOwnCancel()
        {
            var vertex = new HeldMultiVertex();
            await Start(vertex);
            await vertex.Targets[0].SendAsync(new StreamMessage<string>("a", 1));

            await FaultWhileHeld(vertex, vertex.Gate, new InvalidOperationException("stop"));

            Assert.True(vertex.Completion.IsCanceled);
        }

        [Fact]
        public async Task PartitionVertexPassesItsOwnCancelThroughOnFault()
        {
            var vertex = new HeldPartitionVertex();
            await Start(vertex);
            await vertex.SendAsync(new Checkpoint(0, 1, 1));

            var stop = new InvalidOperationException("stop");
            await FaultWhileHeld(vertex, vertex.Gate, stop);

            Assert.Same(stop, Assert.Single(vertex.Completion.Exception!.InnerExceptions));
        }

        [Fact]
        public async Task PartitionVertexTokenIsFreshAfterRecreate()
        {
            var vertex = new HeldPartitionVertex();
            await Start(vertex);
            await FaultFirstRun(vertex, vertex.Gate);

            await Start(vertex);
            await vertex.SendAsync(new Checkpoint(0, 1, 1));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task FixedPointVertexPassesItsOwnCancelThroughOnFault()
        {
            var vertex = new HeldFixedPointVertex();
            await Start(vertex);
            await vertex.IngressTarget.SendAsync(new StreamMessage<string>("a", 1));

            var stop = new InvalidOperationException("stop");
            await FaultWhileHeld(vertex, vertex.Gate, stop);

            Assert.Same(stop, Assert.Single(vertex.Completion.Exception!.InnerExceptions));
        }

        [Fact]
        public async Task FixedPointVertexTokenIsFreshAfterRecreate()
        {
            var vertex = new HeldFixedPointVertex();
            await Start(vertex);
            await FaultFirstRun(vertex, vertex.Gate);

            await Start(vertex);
            await vertex.IngressTarget.SendAsync(new StreamMessage<string>("a", 1));

            await AssertCancellationFault(vertex);
        }
    }
}

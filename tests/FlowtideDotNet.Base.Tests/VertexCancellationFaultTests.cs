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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Tests
{
    public class VertexCancellationFaultTests
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

        private sealed class CancellingEgressVertex() : EgressVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public TaskCompletionSource Entered { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public override string DisplayName => "test";

            protected override async Task OnCheckpoint(long checkpointTime)
            {
                await Task.Yield();
                throw new TaskCanceledException("http timeout");
            }

            protected override async Task OnRecieve(string msg, long time)
            {
                Entered.TrySetResult();
                await Task.Delay(Timeout.Infinite, CancellationToken);
            }

            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class CancellingUnaryVertex() : UnaryVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public TaskCompletionSource Entered { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public override string DisplayName => "test";

            public override async IAsyncEnumerable<string> OnRecieve(string msg, long time)
            {
                if (msg == "wait")
                {
                    Entered.TrySetResult();
                    await Task.Delay(Timeout.Infinite, CancellationToken);
                }
                await Task.Yield();
                if (msg != null)
                {
                    throw new TaskCanceledException("http timeout");
                }
                yield return msg!;
            }

            public override async Task OnCheckpoint()
            {
                await Task.Yield();
                throw new TaskCanceledException("http timeout");
            }

            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class CancellingMultiVertex() : MultipleInputVertex<string>(2, new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public override string DisplayName => "test";

            public override IAsyncEnumerable<string> OnRecieve(int targetId, string msg, long time) => EmptyAsyncEnumerable<string>.Instance;

            public override async Task OnCheckpoint()
            {
                await Task.Yield();
                throw new TaskCanceledException("http timeout");
            }

            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class CancellingPartitionVertex() : PartitionVertex<string>(2, new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public override string DisplayName => "test";

            protected override IAsyncEnumerable<KeyValuePair<int, StreamMessage<string>>> PartitionData(string data, long time) => EmptyAsyncEnumerable<KeyValuePair<int, StreamMessage<string>>>.Instance;

            protected override async Task OnLockingEvent(ILockingEvent lockingEvent)
            {
                await Task.Yield();
                throw new TaskCanceledException("http timeout");
            }

            protected override Task InitializeOrRestore(long restoreVersion, IStateManagerClient stateManagerClient) => Task.CompletedTask;
        }

        private sealed class CancellingFixedPointVertex() : FixedPointVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public override string DisplayName => "test";

            public override bool NoReadSourceInLoop() => true;

            protected override async IAsyncEnumerable<KeyValuePair<int, StreamMessage<string>>> OnIngressRecieve(string data, long time)
            {
                await Task.Yield();
                if (data != null)
                {
                    throw new TaskCanceledException("http timeout");
                }
                yield return new KeyValuePair<int, StreamMessage<string>>(0, new StreamMessage<string>(data!, time));
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

        private static async Task AssertOnlyStopFault(IStreamVertex vertex, Exception stop)
        {
            await Assert.ThrowsAnyAsync<Exception>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Same(stop, Assert.Single(vertex.Completion.Exception!.InnerExceptions));
        }

        [Fact]
        public async Task EgressVertexFaultsOnStrayCancelInCheckpoint()
        {
            var vertex = new CancellingEgressVertex();
            ((IStreamEgressVertex)vertex).SetCheckpointDoneFunction((_, _) => { }, (_, _) => { });
            await Start(vertex);

            await vertex.SendAsync(new Checkpoint(0, 1, 1));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task EgressVertexPassesItsOwnCancelThrough()
        {
            var vertex = new CancellingEgressVertex();
            await Start(vertex);
            await vertex.SendAsync(new StreamMessage<string>("wait", 1));
            await vertex.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

            var stop = new InvalidOperationException("stop");
            vertex.Fault(stop);

            await AssertOnlyStopFault(vertex, stop);
        }

        [Fact]
        public async Task UnaryVertexFaultsOnStrayCancelInCheckpoint()
        {
            var vertex = new CancellingUnaryVertex();
            await Start(vertex);

            await vertex.SendAsync(new Checkpoint(0, 1, 1));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task UnaryVertexFaultsOnStrayCancelInRecieve()
        {
            var vertex = new CancellingUnaryVertex();
            await Start(vertex);

            await vertex.SendAsync(new StreamMessage<string>("a", 1));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task UnaryVertexPassesItsOwnCancelThrough()
        {
            var vertex = new CancellingUnaryVertex();
            await Start(vertex);
            await vertex.SendAsync(new StreamMessage<string>("wait", 1));
            await vertex.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

            var stop = new InvalidOperationException("stop");
            vertex.Fault(stop);

            await AssertOnlyStopFault(vertex, stop);
        }

        [Fact]
        public async Task MultipleInputVertexFaultsOnStrayCancelInCheckpoint()
        {
            var vertex = new CancellingMultiVertex();
            await Start(vertex);

            await vertex.Targets[0].SendAsync(new Checkpoint(0, 1, 1));
            await vertex.Targets[1].SendAsync(new Checkpoint(0, 1, 1));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task PartitionVertexFaultsOnStrayCancelInCheckpoint()
        {
            var vertex = new CancellingPartitionVertex();
            await Start(vertex);

            await vertex.SendAsync(new Checkpoint(0, 1, 1));

            await AssertCancellationFault(vertex);
        }

        [Fact]
        public async Task PartitionVertexPassesCancelAfterCancellationRequest()
        {
            var vertex = new CancellingPartitionVertex();
            await Start(vertex);
            await ((IStreamVertexCancellation)vertex).CancelPendingOperations();

            await vertex.SendAsync(new Checkpoint(0, 1, 1));
            vertex.Complete();

            await vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10));
        }

        [Fact]
        public async Task FixedPointVertexFaultsOnStrayCancelInRecieve()
        {
            var vertex = new CancellingFixedPointVertex();
            await Start(vertex);

            await vertex.IngressTarget.SendAsync(new StreamMessage<string>("a", 1));

            await AssertCancellationFault(vertex);
        }
    }
}

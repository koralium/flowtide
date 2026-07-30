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
    public class VertexCheckpointFlushTests
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
            public Task FailAndRollback(Exception? exception, long? restoreVersion = default) => Task.CompletedTask;
        }

        private sealed class BufferingUnaryVertex : UnaryVertex<string>
        {
            public readonly List<string> Buffered = new List<string>();
            public int FlushCalls;
            public int PendingAtCheckpoint = -1;

            // Bounded capacity matches how the engine always runs vertices
            public BufferingUnaryVertex() : base(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
            {
            }

            public override string DisplayName => "test";

            public override IAsyncEnumerable<string> OnRecieve(string msg, long time)
            {
                Buffered.Add(msg);
                return EmptyAsyncEnumerable<string>.Instance;
            }

            protected override IAsyncEnumerable<string> OnCheckpointFlush()
            {
                return FlushBuffered();
            }

            private async IAsyncEnumerable<string> FlushBuffered()
            {
                FlushCalls++;
                foreach (var item in Buffered)
                {
                    yield return item;
                }
                Buffered.Clear();
                await Task.CompletedTask;
            }

            public override Task OnCheckpoint()
            {
                PendingAtCheckpoint = Buffered.Count;
                return Task.CompletedTask;
            }

            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class BufferingMultiVertex : MultipleInputVertex<string>
        {
            public readonly List<string> Buffered = new List<string>();
            public int FlushCalls;
            public int PrepareFlushCalls;
            public int PendingAtCheckpoint = -1;

            // Bounded capacity matches how the engine always runs vertices
            public BufferingMultiVertex() : base(2, new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
            {
            }

            public override string DisplayName => "test";

            public override IAsyncEnumerable<string> OnRecieve(int targetId, string msg, long time)
            {
                Buffered.Add(msg);
                return EmptyAsyncEnumerable<string>.Instance;
            }

            protected override IAsyncEnumerable<string> OnCheckpointFlush()
            {
                return FlushBuffered(false);
            }

            protected override IAsyncEnumerable<string> OnLockingEventPrepare()
            {
                return FlushBuffered(true);
            }

            private async IAsyncEnumerable<string> FlushBuffered(bool prepare)
            {
                if (prepare)
                {
                    PrepareFlushCalls++;
                }
                else
                {
                    FlushCalls++;
                }
                foreach (var item in Buffered)
                {
                    yield return item;
                }
                Buffered.Clear();
                await Task.CompletedTask;
            }

            public override Task OnCheckpoint()
            {
                PendingAtCheckpoint = Buffered.Count;
                return Task.CompletedTask;
            }

            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private static async Task<BufferBlock<IStreamEvent>> SetupVertex<TVertex>(TVertex vertex)
            where TVertex : IStreamVertex, ISourceBlock<IStreamEvent>
        {
            var collector = new BufferBlock<IStreamEvent>();
            vertex.Setup("stream", "op");
            vertex.LinkTo(collector, new DataflowLinkOptions());
            vertex.CreateBlock();
            vertex.Link();
            await vertex.Initialize("op", 0, 0, new StubVertexHandler(), null);
            return collector;
        }

        private static async Task<IStreamEvent> ReceiveWithTimeout(BufferBlock<IStreamEvent> collector, IStreamVertex vertex)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var receive = collector.ReceiveAsync(cts.Token);
            var finished = await Task.WhenAny(receive, vertex.Completion);
            if (finished == vertex.Completion)
            {
                // Surfaces the fault when the transform threw instead of a timeout
                await vertex.Completion;
                throw new InvalidOperationException("Vertex completed without emitting");
            }
            return await receive;
        }

        [Fact]
        public async Task UnaryVertexFlushesBufferedDataBeforeCheckpointEvent()
        {
            var vertex = new BufferingUnaryVertex();
            var collector = await SetupVertex(vertex);

            await vertex.SendAsync(new StreamMessage<string>("a", 1));
            await vertex.SendAsync(new Checkpoint(0, 1));

            var first = await ReceiveWithTimeout(collector, vertex);
            var flushed = Assert.IsType<StreamMessage<string>>(first);
            Assert.Equal("a", flushed.Data);

            var second = await ReceiveWithTimeout(collector, vertex);
            Assert.IsAssignableFrom<ICheckpointEvent>(second);

            Assert.Equal(1, vertex.FlushCalls);
            Assert.Equal(0, vertex.PendingAtCheckpoint);
        }

        [Fact]
        public async Task UnaryVertexDoesNotFlushForInitWatermarksEvent()
        {
            var vertex = new BufferingUnaryVertex();
            var collector = await SetupVertex(vertex);

            await vertex.SendAsync(new StreamMessage<string>("a", 1));
            await vertex.SendAsync(new InitWatermarksEvent());

            var first = await ReceiveWithTimeout(collector, vertex);
            Assert.IsType<InitWatermarksEvent>(first);

            Assert.Equal(0, vertex.FlushCalls);
            Assert.Single(vertex.Buffered);
        }

        [Fact]
        public async Task MultipleInputVertexFlushesAfterBarrierAlignment()
        {
            var vertex = new BufferingMultiVertex();
            var collector = await SetupVertex(vertex);

            await vertex.Targets[0].SendAsync(new StreamMessage<string>("a", 1));
            await vertex.Targets[1].SendAsync(new StreamMessage<string>("b", 1));

            // The barrier gates its input, this send waits for alignment
            await vertex.Targets[0].SendAsync(new Checkpoint(0, 1));
            var gatedSend = vertex.Targets[0].SendAsync(new StreamMessage<string>("gated", 1));
            Assert.False(gatedSend.IsCompleted, "The barrier did not gate its input");

            // Second barrier completes alignment, flush must precede the checkpoint event
            await vertex.Targets[1].SendAsync(new Checkpoint(0, 1));

            var first = await ReceiveWithTimeout(collector, vertex);
            var flushedA = Assert.IsType<StreamMessage<string>>(first);
            Assert.Equal("a", flushedA.Data);

            var second = await ReceiveWithTimeout(collector, vertex);
            var flushedB = Assert.IsType<StreamMessage<string>>(second);
            Assert.Equal("b", flushedB.Data);

            var third = await ReceiveWithTimeout(collector, vertex);
            Assert.IsAssignableFrom<ICheckpointEvent>(third);

            // A flush per arriving barrier instead of per alignment would be two
            Assert.Equal(1, vertex.FlushCalls);
            Assert.Equal(0, vertex.PendingAtCheckpoint);

            // The gate opens once the checkpoint is sent
            Assert.True(await gatedSend);
        }

        [Fact]
        public async Task MultipleInputVertexFlushesBeforeForwardingLockingEventPrepare()
        {
            var vertex = new BufferingMultiVertex();
            var collector = await SetupVertex(vertex);

            await vertex.Targets[0].SendAsync(new StreamMessage<string>("a", 1));
            var prepare = new LockingEventPrepare(new Checkpoint(0, 1), isInitEvent: true);
            await vertex.Targets[0].SendAsync(prepare);

            var first = await ReceiveWithTimeout(collector, vertex);
            var flushed = Assert.IsType<StreamMessage<string>>(first);
            Assert.Equal("a", flushed.Data);

            var second = await ReceiveWithTimeout(collector, vertex);
            Assert.Same(prepare, second);

            Assert.Equal(1, vertex.PrepareFlushCalls);
            Assert.Equal(0, vertex.FlushCalls);
        }
    }
}

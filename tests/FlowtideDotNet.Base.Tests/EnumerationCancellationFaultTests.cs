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
    public class EnumerationCancellationFaultTests
    {
        public enum ThrowAt
        {
            GetAsyncEnumerator,
            Current,
            DisposeAsync,
            DisposeAsyncAfterAwait,
            MoveNextAsync,
            MoveNextAsyncAfterAwait
        }

        private sealed class StubVertexHandler : IVertexHandler
        {
            public string OperatorId => "op";
            public string StreamName => "stream";
            public IMeter Metrics { get; } = new FlowtideMeter(new Meter("enumeration-cancel-test"), new TagList(), () => "test");
            public IStateManagerClient StateClient => null!;
            public ILoggerFactory LoggerFactory => NullLoggerFactory.Instance;
            public IOperatorMemoryManager MemoryManager => null!;
            public void ScheduleCheckpoint(TimeSpan time, long? checkpointVersion) { }
            public Task RegisterTrigger(string name, TimeSpan? scheduledInterval = null) => Task.CompletedTask;
            public Task FailAndRollback(Exception? exception = null, long? restoreVersion = default) => Task.CompletedTask;
        }

        private sealed class CancellingEnumerable(ThrowAt stage, OperationCanceledException failure) : IAsyncEnumerable<string>, IAsyncEnumerator<string>
        {
            private bool _moved;

            public string Current => stage == ThrowAt.Current ? throw failure : "row";

            public IAsyncEnumerator<string> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            {
                if (stage == ThrowAt.GetAsyncEnumerator)
                {
                    throw failure;
                }
                return this;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                if (stage == ThrowAt.MoveNextAsync)
                {
                    throw failure;
                }
                if (stage == ThrowAt.MoveNextAsyncAfterAwait)
                {
                    return MoveNextSlow();
                }
                if (stage == ThrowAt.DisposeAsync || stage == ThrowAt.DisposeAsyncAfterAwait)
                {
                    return ValueTask.FromResult(false);
                }
                var result = !_moved;
                _moved = true;
                return ValueTask.FromResult(result);
            }

            private async ValueTask<bool> MoveNextSlow()
            {
                await Task.Yield();
                throw failure;
            }

            public ValueTask DisposeAsync()
            {
                if (stage == ThrowAt.DisposeAsync)
                {
                    throw failure;
                }
                if (stage == ThrowAt.DisposeAsyncAfterAwait)
                {
                    return DisposeSlow();
                }
                return ValueTask.CompletedTask;
            }

            private async ValueTask DisposeSlow()
            {
                await Task.Yield();
                throw failure;
            }
        }

        private sealed class TestVertex(ThrowAt stage, OperationCanceledException failure) : UnaryVertex<string>(new ExecutionDataflowBlockOptions { BoundedCapacity = 100 })
        {
            public override string DisplayName => "enumeration test";
            public override IAsyncEnumerable<string> OnRecieve(string msg, long time) => new CancellingEnumerable(stage, failure);
            public override Task OnCheckpoint() => Task.CompletedTask;
            protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private static async Task<TestVertex> Start(ThrowAt stage, OperationCanceledException failure)
        {
            var vertex = new TestVertex(stage, failure);
            vertex.Setup("stream", "op");
            vertex.CreateBlock();
            vertex.LinkTo(DataflowBlock.NullTarget<IStreamEvent>(), new DataflowLinkOptions { PropagateCompletion = true });
            vertex.Link();
            await vertex.Initialize("op", 0, 0, new StubVertexHandler(), null);
            return vertex;
        }

        [Theory]
        [InlineData(ThrowAt.GetAsyncEnumerator)]
        [InlineData(ThrowAt.Current)]
        [InlineData(ThrowAt.DisposeAsync)]
        [InlineData(ThrowAt.DisposeAsyncAfterAwait)]
        [InlineData(ThrowAt.MoveNextAsync)]
        [InlineData(ThrowAt.MoveNextAsyncAfterAwait)]
        public async Task StrayCancellationAtEveryEnumerationStepFaultsTheVertex(ThrowAt stage)
        {
            var cancellation = new TaskCanceledException("http timeout");
            var vertex = await Start(stage, cancellation);

            Assert.True(await vertex.SendAsync(new StreamMessage<string>("input", 1)));
            vertex.Complete();

            var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal("Operator op (enumeration test) was cancelled, but the stream did not request it", fault.Message);
            Assert.Same(cancellation, fault.InnerException);
        }

        [Theory]
        [InlineData(ThrowAt.GetAsyncEnumerator)]
        [InlineData(ThrowAt.Current)]
        [InlineData(ThrowAt.DisposeAsync)]
        [InlineData(ThrowAt.DisposeAsyncAfterAwait)]
        [InlineData(ThrowAt.MoveNextAsync)]
        [InlineData(ThrowAt.MoveNextAsyncAfterAwait)]
        public async Task OwnCancellationAtEveryEnumerationStepPassesThrough(ThrowAt stage)
        {
            var vertex = await Start(stage, new TaskCanceledException("own cancel"));
            await ((IStreamVertexCancellation)vertex).CancelPendingOperations();

            Assert.True(await vertex.SendAsync(new StreamMessage<string>("input", 1)));
            vertex.Complete();

            await vertex.Completion.WaitAsync(TimeSpan.FromSeconds(10));
        }
    }
}

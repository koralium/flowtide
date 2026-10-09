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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Tests
{
    public class StreamOwnedResourceTests
    {
        private sealed class CountingResource(bool fail = false) : IAsyncDisposable
        {
            private int _disposeCount;

            public int DisposeCount => Volatile.Read(ref _disposeCount);

            public ValueTask DisposeAsync()
            {
                Interlocked.Increment(ref _disposeCount);
                if (fail)
                {
                    throw new InvalidOperationException("dispose failed");
                }
                return ValueTask.CompletedTask;
            }
        }

        [Fact]
        public async Task StreamDisposeDisposesOwnedResourcesOnce()
        {
            var failing = new CountingResource(fail: true);
            var second = new CountingResource();
            using var storage = CreateStorage();
            var stream = new DataflowStreamBuilder("owned-" + Guid.NewGuid().ToString("N"))
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .AddOwnedResource(failing)
                .AddOwnedResource(second)
                .Build();
            Assert.Equal(0, failing.DisposeCount);

            await stream.DisposeAsync();
            await stream.DisposeAsync();

            // A failing resource does not skip the next.
            Assert.Equal(1, failing.DisposeCount);
            Assert.Equal(1, second.DisposeCount);
        }

        [Fact]
        public async Task EachBuildOwnsOnlyItsResources()
        {
            var resource = new CountingResource();
            using var storage = CreateStorage();
            var builder = new DataflowStreamBuilder("owned-" + Guid.NewGuid().ToString("N"))
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .AddOwnedResource(resource);
            var first = builder.Build();
            var second = builder.Build();

            await second.DisposeAsync();
            Assert.Equal(0, resource.DisposeCount);
            await first.DisposeAsync();
            Assert.Equal(1, resource.DisposeCount);
        }

        private sealed class OrderLog : IStreamStateChangeListener, IAsyncDisposable
        {
            public ConcurrentQueue<string> Entries { get; } = new ConcurrentQueue<string>();

            public void OnStreamStateChange(StreamStateChangeNotification notification)
            {
                Entries.Enqueue(notification.State.ToString());
            }

            public ValueTask DisposeAsync()
            {
                Entries.Enqueue("Disposed");
                return ValueTask.CompletedTask;
            }
        }

        [Fact]
        public async Task OwnedResourcesDisposeAfterAStopsTeardown()
        {
            var log = new OrderLog();
            using var storage = CreateStorage();
            var source = new Source();
            var sink = new Sink();
            source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
            var stream = new DataflowStreamBuilder("owned-" + Guid.NewGuid().ToString("N"))
                .AddIngressBlock("source", source)
                .AddEgressBlock("sink", sink)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .AddStateChangeListener(log)
                .AddOwnedResource(log)
                .Build();
            await stream.StartAsync();
            using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            {
                while (stream.State != StreamStateValue.Running) await Task.Delay(10, timeout.Token);
            }

            var stop = stream.StopAsync();
            // The stop holds the teardown gate.
            await sink.DisposeEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var dispose = stream.DisposeAsync().AsTask();
            await Task.Delay(100);
            Assert.False(dispose.IsCompleted);
            sink.ReleaseDispose.TrySetResult();
            await dispose.WaitAsync(TimeSpan.FromSeconds(10));
            await stop.WaitAsync(TimeSpan.FromSeconds(10));

            // The stop's NotStarted is queued before it.
            Assert.Equal(["Starting", "Running", "Stopping", "NotStarted", "Disposed"], log.Entries.SkipWhile(x => x == "NotStarted"));
        }

        private sealed class Source() : IngressVertex<string>(new DataflowBlockOptions())
        {
            public override string DisplayName => "owned source";
            protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
            protected override Task SendInitial(IngressOutput<string> output) => Task.CompletedTask;
            protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task OnTrigger(string triggerName, object? state) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class Sink() : EgressVertex<string>(new ExecutionDataflowBlockOptions())
        {
            public TaskCompletionSource DisposeEntered { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public TaskCompletionSource ReleaseDispose { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public override string DisplayName => "owned sink";
            public override async ValueTask DisposeAsync()
            {
                DisposeEntered.TrySetResult();
                await ReleaseDispose.Task;
                await base.DisposeAsync();
            }
            protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
            protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private static ReservoirPersistentStorage CreateStorage()
        {
            return new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        }
    }
}

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
    public class StreamCancellationFaultTests
    {
        private sealed class FailureLog : IFailureListener, ICheckpointListener
        {
            public ConcurrentQueue<Exception?> Failures { get; } = new ConcurrentQueue<Exception?>();

            public TaskCompletionSource Failed { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public TaskCompletionSource CheckpointAfterFailure { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public void OnFailure(StreamFailureNotification notification)
            {
                Failures.Enqueue(notification.Exception);
                Failed.TrySetResult();
            }

            public void OnCheckpointComplete(StreamCheckpointNotification notification)
            {
                if (Failed.Task.IsCompleted)
                {
                    CheckpointAfterFailure.TrySetResult();
                }
            }
        }

        private sealed class Source() : IngressVertex<string>(new DataflowBlockOptions())
        {
            public override string DisplayName => "cancel source";
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
            private int _thrown;

            public volatile bool Armed;

            public TaskCompletionSource Recovered { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public override string DisplayName => "cancel sink";

            protected override async Task OnCheckpoint(long checkpointTime)
            {
                await Task.Yield();
                if (Volatile.Read(ref _thrown) == 1)
                {
                    Recovered.TrySetResult();
                    return;
                }
                if (Armed && Interlocked.Exchange(ref _thrown, 1) == 0)
                {
                    throw new TaskCanceledException("http timeout");
                }
            }

            protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        [Fact]
        public async Task StrayCancelInSinkCheckpointIsReportedAndRecovers()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var failures = new FailureLog();
            var source = new Source();
            var sink = new Sink();
            source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
            var stream = new DataflowStreamBuilder("cancel-" + Guid.NewGuid().ToString("N"))
                .AddIngressBlock("source", source)
                .AddEgressBlock("sink", sink)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .AddFailureListener(failures)
                .AddCheckpointListener(failures)
                .Build();
            try
            {
                await stream.StartAsync();
                using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                {
                    while (stream.State != StreamStateValue.Running) await Task.Delay(10, timeout.Token);
                }

                sink.Armed = true;
                await stream.TriggerCheckpoint();
                await failures.Failed.Task.WaitAsync(TimeSpan.FromSeconds(10));

                Assert.True(failures.Failures.TryPeek(out var first));
                var aggregate = Assert.IsType<AggregateException>(first);
                var fault = Assert.IsType<InvalidOperationException>(Assert.Single(aggregate.InnerExceptions));
                Assert.Equal("Operator sink (cancel sink) was cancelled, but the stream did not request it", fault.Message);
                Assert.IsType<TaskCanceledException>(fault.InnerException);

                using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                {
                    while (!failures.CheckpointAfterFailure.Task.IsCompleted)
                    {
                        if (stream.State == StreamStateValue.Running)
                        {
                            await stream.TriggerCheckpoint();
                        }
                        await Task.Delay(50, timeout.Token);
                    }
                }
                Assert.True(sink.Recovered.Task.IsCompleted);
                Assert.Single(failures.Failures);
                Assert.Equal(StreamStateValue.Running, stream.State);
            }
            finally
            {
                await stream.DisposeAsync();
            }
        }
    }
}

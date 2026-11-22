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
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Engine
{
    public class StopCheckpointGuardTests
    {
        [Theory]
        [InlineData("failure")]
        [InlineData("dispose")]
        public async Task AnInterruptedStopHookSkipsLaterHooksAndTheSave(string interruption)
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var source = new HoldingSource();
            var sink = new CountingSink();
            source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
            var stream = new DataflowStreamBuilder("stop-guard-" + Guid.NewGuid().ToString("N"))
                .AddIngressBlock("source", source)
                .AddEgressBlock("sink", sink)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build();
            Task? disposal = null;
            try
            {
                await stream.StartAsync();
                await WaitUntil(() => stream.State == StreamStateValue.Running && stream.CheckpointSchedulingIdleForTests);

                source.Armed = true;
                var stop = stream.StopAsync();
                await source.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
                Assert.Equal(StreamStateValue.Stopping, stream.State);
                var version = storage.CurrentVersion;
                var hooks = sink.Hooks;

                if (interruption == "dispose") disposal = stream.DisposeAsync().AsTask();
                else await stream.InjectFailureForTests(new InvalidOperationException("Another operator failed."));
                source.Release.TrySetResult();

                if (disposal != null)
                {
                    await disposal.WaitAsync(TimeSpan.FromSeconds(10));
                    try { await stop.WaitAsync(TimeSpan.FromSeconds(10)); }
                    catch (ObjectDisposedException) { }
                }
                else
                {
                    await stop.WaitAsync(TimeSpan.FromSeconds(10));
                }

                Assert.Equal(hooks, sink.Hooks);
                Assert.Equal(version, storage.CurrentVersion);
            }
            finally
            {
                source.Release.TrySetResult();
                if (disposal != null) await disposal.WaitAsync(TimeSpan.FromSeconds(10));
                else await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            }
        }

        [Fact]
        public async Task AThrowingStopHookSkipsTheSaveAndTheStreamStartsAgain()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var source = new HoldingSource { Throws = true };
            var sink = new CountingSink();
            source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
            var stream = new DataflowStreamBuilder("stop-throw-" + Guid.NewGuid().ToString("N"))
                .AddIngressBlock("source", source)
                .AddEgressBlock("sink", sink)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build();
            try
            {
                await stream.StartAsync();
                await WaitUntil(() => stream.State == StreamStateValue.Running && stream.CheckpointSchedulingIdleForTests);
                var version = storage.CurrentVersion;
                var hooks = sink.Hooks;

                source.Armed = true;
                var stop = stream.StopAsync();
                await stop.WaitAsync(TimeSpan.FromSeconds(10));
                Assert.Equal(StreamStateValue.NotStarted, stream.State);
                Assert.Equal(hooks, sink.Hooks);
                Assert.Equal(version, storage.CurrentVersion);

                source.Armed = false;
                await stream.StartAsync();
                await WaitUntil(() => stream.State == StreamStateValue.Running);
            }
            finally
            {
                await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            }
        }

        [Fact]
        public async Task ADuplicateAckStartsNoSecondSave()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var source = new HoldingSource();
            var sink = new CountingSink();
            source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
            var stream = new DataflowStreamBuilder("stop-duplicate-" + Guid.NewGuid().ToString("N"))
                .AddIngressBlock("source", source)
                .AddEgressBlock("sink", sink)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build();
            try
            {
                await stream.StartAsync();
                await WaitUntil(() => stream.State == StreamStateValue.Running && stream.CheckpointSchedulingIdleForTests);
                var version = storage.CurrentVersion;
                var hooks = sink.Hooks;

                source.Armed = true;
                var stop = stream.StopAsync();
                await source.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
                stream.InjectEgressCheckpointDoneForTests("sink", null);
                stream.InjectEgressCheckpointDoneForTests("sink", null);
                source.Release.TrySetResult();
                await stop.WaitAsync(TimeSpan.FromSeconds(10));

                Assert.Equal(1, source.Calls);
                Assert.Equal(hooks + 1, sink.Hooks);
                Assert.Equal(version + 1, storage.CurrentVersion);
            }
            finally
            {
                source.Release.TrySetResult();
                await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            }
        }

        [Theory]
        [InlineData("failure")]
        [InlineData("dispose")]
        public async Task AnAckAfterTeardownBeganStartsNoSave(string interruption)
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var source = new HoldingSource();
            var sink = new CountingSink();
            source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
            var streamName = "stop-late-ack-" + Guid.NewGuid().ToString("N");
            var stream = new DataflowStreamBuilder(streamName)
                .AddIngressBlock("source", source)
                .AddEgressBlock("sink", sink)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build();
            Task? disposal = null;
            int saveTasks = 0;
            try
            {
                await stream.StartAsync();
                await WaitUntil(() => stream.State == StreamStateValue.Running && stream.CheckpointSchedulingIdleForTests);
                StreamContext.CheckpointCommitScheduledHookForTests = (name) =>
                {
                    if (name == streamName)
                    {
                        Interlocked.Increment(ref saveTasks);
                    }
                    return Task.CompletedTask;
                };

                sink.HoldCheckpoint = true;
                var stop = stream.StopAsync();
                await sink.CheckpointEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
                if (interruption == "dispose") disposal = stream.DisposeAsync().AsTask();
                else await stream.InjectFailureForTests(new InvalidOperationException("Another operator failed."));
                stream.InjectEgressCheckpointDoneForTests("sink", null);
                await WaitUntil(() => Volatile.Read(ref GetContext(stream)._stateManagerWriteCount) == 0);
                sink.ReleaseCheckpoint.TrySetResult();
                if (disposal != null)
                {
                    await disposal.WaitAsync(TimeSpan.FromSeconds(10));
                    try { await stop.WaitAsync(TimeSpan.FromSeconds(10)); }
                    catch (ObjectDisposedException) { }
                }
                else
                {
                    await stop.WaitAsync(TimeSpan.FromSeconds(10));
                }

                Assert.Equal(0, Volatile.Read(ref saveTasks));
            }
            finally
            {
                StreamContext.CheckpointCommitScheduledHookForTests = null;
                sink.ReleaseCheckpoint.TrySetResult();
                if (disposal != null) await disposal.WaitAsync(TimeSpan.FromSeconds(10));
                else await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            }
        }

        private static StreamContext GetContext(Base.Engine.DataflowStream stream) =>
            (StreamContext)typeof(Base.Engine.DataflowStream).GetField("streamContext", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(stream)!;

        private static async Task WaitUntil(Func<bool> condition)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while (!condition()) await Task.Delay(10, timeout.Token);
        }

        private sealed class HoldingSource() : IngressVertex<string>(new DataflowBlockOptions())
        {
            public volatile bool Armed;
            public bool Throws;
            private int _calls;
            public int Calls => Volatile.Read(ref _calls);
            public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public override string DisplayName => "holding source";

            public override async Task BeforeSaveCheckpoint()
            {
                if (!Armed) return;
                Interlocked.Increment(ref _calls);
                if (Throws) throw new InvalidOperationException("Hook failed.");
                Entered.TrySetResult();
                await Release.Task;
            }

            protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
            protected override Task SendInitial(IngressOutput<string> output) => Task.CompletedTask;
            protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task OnTrigger(string triggerName, object? state) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class CountingSink() : EgressVertex<string>(new ExecutionDataflowBlockOptions())
        {
            private int _hooks;
            public int Hooks => Volatile.Read(ref _hooks);
            public override string DisplayName => "counting sink";

            public override Task BeforeSaveCheckpoint()
            {
                Interlocked.Increment(ref _hooks);
                return Task.CompletedTask;
            }

            public volatile bool HoldCheckpoint;
            public TaskCompletionSource CheckpointEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource ReleaseCheckpoint { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

            protected override async Task OnCheckpoint(long checkpointTime)
            {
                if (HoldCheckpoint)
                {
                    CheckpointEntered.TrySetResult();
                    await ReleaseCheckpoint.Task;
                }
            }

            protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }
    }
}

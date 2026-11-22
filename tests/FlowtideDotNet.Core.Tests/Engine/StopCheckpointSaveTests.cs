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
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Engine
{
    public class StopCheckpointSaveTests
    {
        [Fact]
        public async Task AStopSavesWhatBlocksCommitBeforeTheSave()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var streamName = "stop-save-" + Guid.NewGuid().ToString("N");

            var source = new Source();
            var sink = new Sink();
            await using (var stream = Build(streamName, storage, source, sink))
            {
                await stream.StartAsync();
                await WaitForRunning(stream);
                var barriers = source.Barriers;
                var sourceHooks = source.Hooks;
                var sinkHooks = sink.Hooks;
                await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));

                Assert.Equal(barriers + 1, source.Barriers);
                Assert.Equal(sourceHooks + 1, source.Hooks);
                Assert.Equal(sinkHooks + 1, sink.Hooks);
                Assert.Equal(source.Barriers, source.SavedBarriers);
                Assert.Equal(sink.Barriers, sink.SavedBarriers);
                Assert.Equal(source.LastCheckpointTime, stream.GetLatestState()?.Time);
            }

            var restartedSource = new Source();
            await using var restarted = Build(streamName, storage, restartedSource, new Sink());
            await restarted.StartAsync();
            await WaitForRunning(restarted);

            Assert.Equal(source.Barriers, restartedSource.RestoredBarriers);
            Assert.Equal(source.LastCheckpointTime, restarted.GetLatestState()?.Time);
        }

        [Fact]
        public async Task AStopSendsOneBarrierAndSavesItOnce()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var source = new Source();
            var sink = new Sink();
            await using var stream = Build("stop-once-" + Guid.NewGuid().ToString("N"), storage, source, sink);
            await stream.StartAsync();
            await WaitForRunning(stream);
            var barriers = source.Barriers;
            var hooks = source.Hooks;
            var version = storage.CurrentVersion;

            source.Ready = false;
            var stop = stream.StopAsync();
            try
            {
                using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                {
                    while (storage.CurrentVersion == version || GetContext(stream).checkpointTask != null)
                    {
                        await Task.Delay(10, timeout.Token);
                    }
                }
                Assert.False(stop.IsCompleted);

                await stream.TriggerCheckpoint().WaitAsync(TimeSpan.FromSeconds(10));
                await Task.Delay(100);

                Assert.Equal(barriers + 1, source.Barriers);
                Assert.Equal(barriers + 1, sink.Barriers);
            }
            finally
            {
                source.Ready = true;
            }
            await stop.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(barriers + 1, source.Barriers);
            Assert.Equal(hooks + 1, source.Hooks);
            Assert.Equal(version + 1, storage.CurrentVersion);
        }

        [Fact]
        public async Task AStaleCheckpointAckDuringStopDoesNotRunBeforeSaveEarlyOrTwice()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var streamName = "stop-stale-ack-" + Guid.NewGuid().ToString("N");

            var source = new GatedBeforeSaveSource();
            var sink = new GatedStopSink();
            source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });

            await using var stream = new DataflowStreamBuilder(streamName)
                .AddIngressBlock("source", source)
                .AddEgressBlock("sink", sink)
                .WaitForCheckpointAfterInitialData(false)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build();

            try
            {
                await stream.StartAsync();
                await WaitForRunning(stream);

                sink.HoldStopCheckpoint = true;
                source.HoldFirstBeforeSave = true;

                var stop = stream.StopAsync();
                await sink.StopCheckpointEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));

                stream.InjectEgressCheckpointDoneForTests("sink", new Checkpoint(1, 2, 1));

                bool startedWhileSinkInOnCheckpoint = GetContext(stream)._stateManagerWriteCount > 0;
                if (startedWhileSinkInOnCheckpoint)
                {
                    await source.FirstBeforeSaveEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
                    source.ReleaseFirstBeforeSave.TrySetResult();
                    sink.ReleaseStopCheckpoint.TrySetResult();
                }
                else
                {
                    source.HoldFirstBeforeSave = false;
                    source.ReleaseFirstBeforeSave.TrySetResult();
                    sink.ReleaseStopCheckpoint.TrySetResult();
                }

                await stop.WaitAsync(TimeSpan.FromSeconds(10));

                Assert.False(startedWhileSinkInOnCheckpoint, "BeforeSaveCheckpoint started while sink was still inside OnCheckpoint for the stop barrier.");
                Assert.Equal(1, source.BeforeSaveCalls);
                Assert.Equal(1, sink.BeforeSaveCalls);
            }
            finally
            {
                sink.ReleaseStopCheckpoint.TrySetResult();
                source.ReleaseFirstBeforeSave.TrySetResult();
            }
        }

        [Fact]
        public async Task AFailedBeforeSaveDuringStopKeepsLastPersistedStreamState()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
            var streamName = "stop-failed-before-save-" + Guid.NewGuid().ToString("N");

            var source = new FailingBeforeSaveSource();
            var sink = new Sink();
            source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
            await using (var stream = new DataflowStreamBuilder(streamName)
                .AddIngressBlock("source", source)
                .AddEgressBlock("sink", sink)
                .WaitForCheckpointAfterInitialData(false)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build())
            {
                await stream.StartAsync();
                await WaitForRunning(stream);

                await stream.TriggerCheckpoint();
                using (var checkpointTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
                {
                    while (GetContext(stream)._stateManager.LastCompletedCheckpointVersion < 1 || GetContext(stream)._stateManagerWriteCount != 0)
                    {
                        await Task.Delay(10, checkpointTimeout.Token);
                    }
                }

                var persistedBeforeStop = stream.GetLatestState();
                Assert.NotNull(persistedBeforeStop);
                Assert.True(persistedBeforeStop.Time > 0);

                source.ThrowOnBeforeSave = true;
                await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));

                Assert.Equal(StreamStateValue.NotStarted, stream.State);
                Assert.Equal(1, GetContext(stream)._stateManager.LastCompletedCheckpointVersion);
                Assert.Equal(persistedBeforeStop.Time, stream.GetLatestState()!.Time);
            }

            var restartedSource = new FailingBeforeSaveSource();
            await using var restarted = Build(streamName, storage, restartedSource, new Sink());
            await restarted.StartAsync();
            await WaitForRunning(restarted);

            Assert.Equal(1, restartedSource.RestoredBarriers);
        }

        [Fact]
        public async Task AStopCompactsPagesCommittedBeforeTheSave()
        {
            using var storage = new CompactionCountingStorage();
            var source = new TreeSource();
            var sink = new Sink();
            await using var stream = Build("stop-compact-" + Guid.NewGuid().ToString("N"), storage, source, sink);
            await stream.StartAsync();
            await WaitForRunning(stream);
            await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(1, storage.CompactionsBeforeCheckpoint);
        }

        private static Base.Engine.DataflowStream Build(string streamName, ReservoirPersistentStorage storage, IngressVertex<string> source, EgressVertex<string> sink)
        {
            source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
            return new DataflowStreamBuilder(streamName)
                .AddIngressBlock("source", source)
                .AddEgressBlock("sink", sink)
                .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
                .Build();
        }

        private static StreamContext GetContext(Base.Engine.DataflowStream stream) =>
            (StreamContext)typeof(Base.Engine.DataflowStream).GetField("streamContext", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(stream)!;

        private static async Task WaitForRunning(Base.Engine.DataflowStream stream)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while (stream.State != StreamStateValue.Running)
            {
                await Task.Delay(10, timeout.Token);
            }
        }

        private sealed class SavedCount
        {
            public int Barriers { get; set; }
        }

        private sealed class Source() : IngressVertex<string>(new DataflowBlockOptions())
        {
            private IObjectState<SavedCount>? _state;

            public int Barriers { get; private set; }

            public int SavedBarriers { get; private set; }

            public int RestoredBarriers { get; private set; }

            public int Hooks { get; private set; }

            public long LastCheckpointTime { get; private set; }

            public volatile bool Ready = true;

            public override bool ReadyToStop => Ready;

            public override string DisplayName => "stop save source";

            protected override Task OnCheckpoint(long checkpointTime)
            {
                Barriers++;
                LastCheckpointTime = checkpointTime;
                return Task.CompletedTask;
            }

            public override async Task BeforeSaveCheckpoint()
            {
                Debug.Assert(_state != null);
                Hooks++;
                _state.Value = new SavedCount { Barriers = Barriers };
                await _state.Commit();
                SavedBarriers = Barriers;
            }

            protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
            {
                _state = await stateManagerClient.GetOrCreateObjectStateAsync<SavedCount>("count");
                RestoredBarriers = _state.Value?.Barriers ?? 0;
            }

            protected override Task SendInitial(IngressOutput<string> output) => Task.CompletedTask;
            protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
            public override Task OnTrigger(string triggerName, object? state) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class Sink() : EgressVertex<string>(new ExecutionDataflowBlockOptions())
        {
            public int Barriers { get; private set; }

            public int SavedBarriers { get; private set; }

            public int Hooks { get; private set; }

            public override string DisplayName => "stop save sink";

            protected override Task OnCheckpoint(long checkpointTime)
            {
                Barriers++;
                return Task.CompletedTask;
            }

            public override Task BeforeSaveCheckpoint()
            {
                Hooks++;
                SavedBarriers = Barriers;
                return Task.CompletedTask;
            }

            protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class GatedBeforeSaveSource() : IngressVertex<string>(new DataflowBlockOptions())
        {
            private int _beforeSaveCalls;
            public volatile bool HoldFirstBeforeSave;
            public int BeforeSaveCalls => Volatile.Read(ref _beforeSaveCalls);
            public TaskCompletionSource FirstBeforeSaveEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource SecondBeforeSaveEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource ReleaseFirstBeforeSave { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public override string DisplayName => "gated before-save source";

            public override async Task BeforeSaveCheckpoint()
            {
                var call = Interlocked.Increment(ref _beforeSaveCalls);
                if (call == 1 && HoldFirstBeforeSave)
                {
                    FirstBeforeSaveEntered.TrySetResult();
                    await ReleaseFirstBeforeSave.Task;
                }
                else if (call == 2)
                {
                    SecondBeforeSaveEntered.TrySetResult();
                }
            }

            protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
            protected override Task SendInitial(IngressOutput<string> output) => Task.CompletedTask;
            protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task OnTrigger(string triggerName, object? state) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class GatedStopSink() : EgressVertex<string>(new ExecutionDataflowBlockOptions())
        {
            private int _beforeSaveCalls;
            public volatile bool HoldStopCheckpoint;
            public int BeforeSaveCalls => Volatile.Read(ref _beforeSaveCalls);
            public TaskCompletionSource StopCheckpointEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource ReleaseStopCheckpoint { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public override string DisplayName => "gated stop sink";

            protected override async Task OnCheckpoint(long checkpointTime)
            {
                if (HoldStopCheckpoint)
                {
                    HoldStopCheckpoint = false;
                    StopCheckpointEntered.TrySetResult();
                    await ReleaseStopCheckpoint.Task;
                }
            }

            public override Task BeforeSaveCheckpoint()
            {
                Interlocked.Increment(ref _beforeSaveCalls);
                return Task.CompletedTask;
            }

            protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class FailingBeforeSaveSource() : IngressVertex<string>(new DataflowBlockOptions())
        {
            private IObjectState<SavedCount>? _state;
            public volatile bool ThrowOnBeforeSave;
            public int Barriers { get; private set; }
            public int RestoredBarriers { get; private set; }

            public override string DisplayName => "failing before-save source";

            protected override Task OnCheckpoint(long checkpointTime)
            {
                Barriers++;
                return Task.CompletedTask;
            }

            public override async Task BeforeSaveCheckpoint()
            {
                Debug.Assert(_state != null);
                _state.Value = new SavedCount { Barriers = Barriers };
                await _state.Commit();
                if (ThrowOnBeforeSave)
                {
                    throw new InvalidOperationException("Simulated BeforeSaveCheckpoint failure on stop.");
                }
            }

            protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
            {
                _state = await stateManagerClient.GetOrCreateObjectStateAsync<SavedCount>("count");
                RestoredBarriers = _state.Value?.Barriers ?? 0;
            }

            protected override Task SendInitial(IngressOutput<string> output) => Task.CompletedTask;
            protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
            public override Task OnTrigger(string triggerName, object? state) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private sealed class CompactionCountingStorage()
            : ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() }), IPersistentStorage
        {
            private int _compactionsBeforeCheckpoint;
            private int _checkpoints;
            public int CompactionsBeforeCheckpoint => Volatile.Read(ref _compactionsBeforeCheckpoint);

            public new ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount)
            {
                if (Volatile.Read(ref _checkpoints) == 0)
                {
                    Interlocked.Increment(ref _compactionsBeforeCheckpoint);
                }
                return base.CompactAsync(changesSinceLastCompact, pageCount);
            }

            public new ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
            {
                Interlocked.Increment(ref _checkpoints);
                return base.CheckpointAsync(metadata, includeIndex);
            }
        }

        private sealed class TreeSource() : IngressVertex<string>(new DataflowBlockOptions())
        {
            private IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>? _tree;
            public override string DisplayName => "tree before-save source";

            public override async Task BeforeSaveCheckpoint()
            {
                Debug.Assert(_tree != null);
                await _tree.Upsert(1, 1);
                await _tree.Commit();
            }

            protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
            {
                _tree = await stateManagerClient.GetOrCreateTree("tree", new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>
                {
                    Comparer = new PrimitiveListComparer<long>(),
                    KeySerializer = new PrimitiveListKeyContainerSerializer<long>(MemoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<long>(MemoryAllocator),
                    MemoryAllocator = MemoryAllocator
                });
            }

            protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
            protected override Task SendInitial(IngressOutput<string> output) => Task.CompletedTask;
            protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
            public override Task OnTrigger(string triggerName, object? state) => Task.CompletedTask;
            public override Task Compact() => Task.CompletedTask;
            public override Task DeleteAsync() => Task.CompletedTask;
        }
    }
}

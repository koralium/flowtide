using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Exchange;

public class StartupCheckpointSchedulingTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ElapsedCheckpointRequestRunsWhenStartupCompletes(bool waitForInitialData)
    {
        var source = new Source();
        var sink = new Sink();
        source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        var stream = new DataflowStreamBuilder("startup-checkpoint-" + Guid.NewGuid().ToString("N"))
            .AddIngressBlock("source", source).AddEgressBlock("sink", sink)
            .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
            .WaitForCheckpointAfterInitialData(waitForInitialData).Build();
        var context = (StreamContext)typeof(FlowtideDotNet.Base.Engine.DataflowStream).GetField("streamContext", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(stream)!;
        try
        {
            var start = stream.StartAsync();
            await sink.Initializing.Task.WaitAsync(TimeSpan.FromSeconds(10));
            foreach (var token in new long[] { 321, 322 })
            {
                Task fired;
                lock (context._checkpointLock)
                {
                    context.TryScheduleCheckpointIn(TimeSpan.FromMilliseconds(1), token);
                    fired = context._scheduleCheckpointTask!;
                }
                // Join the actual timer dispatch while initialization is still held.
                await fired.WaitAsync(TimeSpan.FromSeconds(10));
            }
            Assert.False(sink.CheckpointCompleted.Task.IsCompleted);
            lock (context._checkpointLock)
            {
                // An elapsed request must wait for startup, not for another timer.
                Assert.Null(context._scheduleCheckpointTask);
                Assert.NotNull(context.inQueueCheckpoint);
                Assert.Equal(321L, context._scheduledProvidedCheckpointToken);
            }
            sink.Release.TrySetResult();
            await start.WaitAsync(TimeSpan.FromSeconds(10));
            await sink.CheckpointCompleted.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(2, storage.CurrentVersion);
        }
        finally
        {
            sink.Release.TrySetResult();
            await stream.DisposeAsync();
        }
    }

    private sealed class Sink() : EgressVertex<string>(new ExecutionDataflowBlockOptions())
    {
        public TaskCompletionSource Initializing { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource CheckpointCompleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override string DisplayName => "startup gate";
        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            Initializing.TrySetResult();
            return Release.Task;
        }
        public override Task CheckpointDone(long version) { CheckpointCompleted.TrySetResult(); return Task.CompletedTask; }
        protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
        public override Task Compact() => Task.CompletedTask;
        public override Task DeleteAsync() => Task.CompletedTask;
    }

    private sealed class Source() : IngressVertex<string>(new DataflowBlockOptions())
    {
        public override string DisplayName => "empty source";
        protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task SendInitial(IngressOutput<string> output) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
        public override Task OnTrigger(string triggerName, object? state) => Task.CompletedTask;
        public override Task Compact() => Task.CompletedTask;
        public override Task DeleteAsync() => Task.CompletedTask;
    }
}

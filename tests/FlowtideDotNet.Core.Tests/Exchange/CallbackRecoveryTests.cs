using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Exchange;

public class CallbackRecoveryTests
{
    [Theory]
    [InlineData("initialize")]
    [InlineData("startup-finality")]
    [InlineData("finality")]
    [InlineData("checkpoint")]
    [InlineData("before-save")]
    [InlineData("checkpoint-done")]
    [InlineData("row")]
    [InlineData("dispose-row")]
    [InlineData("stop-finality")]
    [InlineData("stop-checkpoint")]
    [InlineData("stop-checkpoint-done")]
    public async Task SelfRequestedFailureAcknowledgesThenDrainsBeforeRecovery(string phase)
    {
        var source = new EmptySource();
        var sink = new FailureRequestingSink(phase == "dispose-row" ? "row" : phase);
        source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        var stream = new DataflowStreamBuilder("callback-" + Guid.NewGuid().ToString("N"))
            .AddIngressBlock("source", source).AddEgressBlock("sink", sink)
            .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
            .SetStopDrainTimeout(TimeSpan.FromMilliseconds(100)).Build();
        try
        {
            Task? stop = null;
            var start = stream.StartAsync();
            if (phase is not ("initialize" or "startup-finality" or "row" or "dispose-row"))
            {
                await start.WaitAsync(TimeSpan.FromSeconds(10));
                await WaitUntil(() => stream.State == StreamStateValue.Running);
                if (phase.StartsWith("stop-"))
                {
                    sink.Stopping = true;
                    stop = stream.StopAsync();
                }
                else await stream.TriggerCheckpoint();
            }
            await sink.Acknowledged.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Task? dispose = phase == "dispose-row" ? stream.DisposeAsync().AsTask() : null;
            // Retain the callback beyond the old timeout escape. Acknowledgement must
            // not be confused with releasing its resources or completing recovery.
            await Task.Delay(300);
            Assert.False(sink.Disposed.Task.IsCompleted);
            if (dispose != null) Assert.False(dispose.IsCompleted);
            Assert.Equal(stop == null ? StreamStateValue.Failure : StreamStateValue.Stopping, stream.State);
            sink.Release.TrySetResult();
            await sink.Exited.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.False(await sink.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            await start.WaitAsync(TimeSpan.FromSeconds(10));
            if (dispose != null) { await dispose.WaitAsync(TimeSpan.FromSeconds(10)); return; }
            if (stop != null)
            {
                await stop.WaitAsync(TimeSpan.FromSeconds(10));
                sink.Stopping = false;
                await stream.StartAsync();
            }
            Assert.Equal(0, sink.Compactions);
            await WaitUntil(() => sink.Initializations >= 2 && stream.State == StreamStateValue.Running);
            int initializations = sink.Initializations;
            // A saved handler of the old run cannot request a lower restore on the new one.
            await sink.OldHandler!.FailAndRollback(new InvalidOperationException("late failure"), 0);
            await Task.Delay(100);
            Assert.Equal(StreamStateValue.Running, stream.State);
            Assert.Equal(initializations, sink.Initializations);
        }
        finally
        {
            sink.Release.TrySetResult();
            await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition()) await Task.Delay(10, timeout.Token);
    }

    private sealed class FailureRequestingSink(string phase) : EgressVertex<string>(new ExecutionDataflowBlockOptions())
    {
        private int requested;
        public int Initializations;
        public int Compactions;
        public bool Stopping;
        public volatile bool InCallback;
        public FlowtideDotNet.Base.IVertexHandler? OldHandler;
        public TaskCompletionSource Acknowledged { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Exited { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> Disposed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override string DisplayName => "failure requesting sink";
        protected override Task OnCheckpoint(long checkpointTime) => MaybeFail(Stopping ? "stop-checkpoint" : "checkpoint");
        protected override Task OnRecieve(string msg, long time) => MaybeFail("row");
        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            Interlocked.Increment(ref Initializations);
            return MaybeFail("initialize");
        }
        public override Task BeforeSaveCheckpoint() => MaybeFail("before-save");
        public override Task CheckpointDone(long version) => MaybeFail(Stopping ? "stop-checkpoint-done" : "checkpoint-done");
        public override Task CommitVersion(long version) => MaybeFail(Stopping ? "stop-finality" : version == 0 ? "startup-finality" : "finality");
        public override Task Compact() { Interlocked.Increment(ref Compactions); return Task.CompletedTask; }
        public override Task DeleteAsync() => Task.CompletedTask;
        public override ValueTask DisposeAsync()
        {
            Disposed.TrySetResult(InCallback);
            return base.DisposeAsync();
        }
        private async Task MaybeFail(string currentPhase)
        {
            if (phase != currentPhase || Interlocked.Exchange(ref requested, 1) != 0) return;
            InCallback = true;
            OldHandler = (FlowtideDotNet.Base.IVertexHandler)typeof(EgressVertex<string>)
                .GetField("_vertexHandler", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(this)!;
            try
            {
                await FailAndRollback(new InvalidOperationException("self requested failure"));
                await FailAndRollback(new InvalidOperationException("duplicate failure request"));
                Acknowledged.TrySetResult();
                await Release.Task;
            }
            finally { InCallback = false; Exited.TrySetResult(); }
        }
    }

    private sealed class EmptySource : IngressVertex<string>
    {
        public EmptySource() : base(new DataflowBlockOptions()) { }
        public override string DisplayName => "empty source";
        protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task SendInitial(IngressOutput<string> output) => output.SendAsync("row");
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
        public override Task OnTrigger(string triggerName, object? state) => Task.CompletedTask;
        public override Task Compact() => Task.CompletedTask;
        public override Task DeleteAsync() => Task.CompletedTask;
    }
}

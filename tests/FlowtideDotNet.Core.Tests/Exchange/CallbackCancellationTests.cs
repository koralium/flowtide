using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Exchange;

public class CallbackCancellationTests
{
    public static IEnumerable<object[]> CancellationCases()
    {
        foreach (var phase in new[] { "initialize", "startup-finality", "before-save", "checkpoint-done", "finality", "compact", "stop-checkpoint-done", "stop-finality", "stop-compact" })
        foreach (var interruption in new[] { "failure", "self-failure", "dispose" })
            yield return new object[] { phase, interruption };
    }

    [Theory]
    [MemberData(nameof(CancellationCases))]
    public async Task CancellationReachesCallbackBeforeResourcesAreReleased(string phase, string interruption)
    {
        var sink = new Sink(phase) { RequestOwnFailure = interruption == "self-failure" };
        using var storage = CreateStorage();
        var stream = CreateStream(storage, sink);
        Task? disposal = null;
        Task? stop = null;
        try
        {
            stop = await EnterCallback(stream, sink, phase);
            if (interruption == "dispose") disposal = stream.DisposeAsync().AsTask();
            else if (interruption == "failure") await stream.InjectFailureForTests(new InvalidOperationException("Another operator failed."));

            await sink.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            // Cancellation is only a request: the callback still owns resources while
            // finishing its cleanup, including beyond the configured drain timeout.
            await Task.Delay(150);
            Assert.False(sink.Disposed.Task.IsCompleted);
            if (disposal != null) Assert.False(disposal.IsCompleted);
            sink.Finish.TrySetResult();
            await sink.Exited.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.False(await sink.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(10)));

            if (disposal != null)
            {
                await disposal.WaitAsync(TimeSpan.FromSeconds(10));
                if (stop != null) await ObserveStoppedOrDisposed(stop);
            }
            else
            {
                if (stop != null)
                {
                    await stop.WaitAsync(TimeSpan.FromSeconds(10));
                    sink.Stopping = false;
                    await stream.StartAsync();
                }
                await AssertRecovered(stream, sink, storage);
            }
        }
        finally
        {
            sink.Finish.TrySetResult();
            sink.EmergencyRelease.TrySetResult();
            if (disposal != null) await disposal.WaitAsync(TimeSpan.FromSeconds(10));
            else await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    [Theory]
    [InlineData("initialize", false, false)]
    [InlineData("initialize", false, true)]
    [InlineData("initialize", true, false)]
    [InlineData("initialize", true, true)]
    [InlineData("finality", false, false)]
    [InlineData("finality", false, true)]
    [InlineData("finality", true, false)]
    [InlineData("finality", true, true)]
    public async Task TeardownAlsoJoinsCancellationHandlers(string phase, bool dispose, bool handlerThrows)
    {
        using var releaseHandler = new ManualResetEventSlim();
        var sink = new Sink(phase)
        {
            CancellationHandler = () =>
            {
                if (!releaseHandler.Wait(TimeSpan.FromSeconds(15))) throw new TimeoutException("Cancellation handler was not released.");
                if (handlerThrows) throw new InvalidOperationException("Cancellation handler failed.");
            }
        };
        using var storage = CreateStorage();
        var stream = CreateStream(storage, sink);
        Task? disposal = null;
        try
        {
            await EnterCallback(stream, sink, phase);
            if (dispose) disposal = stream.DisposeAsync().AsTask();
            else await stream.InjectFailureForTests(new InvalidOperationException("Request cancellation."));
            await sink.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            sink.Finish.TrySetResult();
            await sink.Exited.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Task.Delay(150);
            Assert.False(sink.Disposed.Task.IsCompleted);
            releaseHandler.Set();
            Assert.False(await sink.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            if (disposal != null) await disposal.WaitAsync(TimeSpan.FromSeconds(10));
            else await AssertRecovered(stream, sink, storage);
        }
        finally
        {
            releaseHandler.Set();
            sink.Finish.TrySetResult();
            sink.EmergencyRelease.TrySetResult();
            if (disposal != null) await disposal.WaitAsync(TimeSpan.FromSeconds(10));
            else await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    [Theory]
    [InlineData("before-save")]
    [InlineData("checkpoint-done")]
    public async Task CancelledRunningCheckpointReleasesOwnershipOnceAndRecovers(string phase)
    {
        var sink = new Sink(phase) { CancelInsteadOfWaiting = true };
        using var storage = CreateStorage();
        var stream = CreateStream(storage, sink);
        try
        {
            await EnterCallback(stream, sink, phase);
            await sink.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(0, sink.Compactions);
            await AssertRecovered(stream, sink, storage);
            Assert.Equal(0, GetContext(stream)._stateManagerWriteCount);
            await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally { await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10)); }
    }

    private static ReservoirPersistentStorage CreateStorage() => new(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });

    private static FlowtideDotNet.Base.Engine.DataflowStream CreateStream(ReservoirPersistentStorage storage, Sink sink)
    {
        var source = new Source();
        source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
        return new DataflowStreamBuilder("callback-cancellation-" + Guid.NewGuid().ToString("N"))
            .AddIngressBlock("source", source).AddEgressBlock("sink", sink)
            .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
            .SetStopDrainTimeout(TimeSpan.FromMilliseconds(100)).Build();
    }

    private static async Task<Task?> EnterCallback(FlowtideDotNet.Base.Engine.DataflowStream stream, Sink sink, string phase)
    {
        sink.Armed = phase is "initialize" or "startup-finality";
        var start = stream.StartAsync();
        Task? stop = null;
        if (!sink.Armed)
        {
            await start.WaitAsync(TimeSpan.FromSeconds(10));
            await WaitUntil(() => stream.State == StreamStateValue.Running);
            sink.Armed = true;
            sink.Stopping = phase.StartsWith("stop-");
            if (sink.Stopping) stop = stream.StopAsync();
            else await stream.TriggerCheckpoint();
        }
        await sink.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        return stop;
    }

    private static async Task AssertRecovered(FlowtideDotNet.Base.Engine.DataflowStream stream, Sink sink, ReservoirPersistentStorage storage)
    {
        await WaitUntil(() => sink.Initializations >= 2 && stream.State == StreamStateValue.Running && GetContext(stream)._stateManagerWriteCount == 0);
        var version = storage.CurrentVersion;
        await stream.TriggerCheckpoint();
        await WaitUntil(() => storage.CurrentVersion > version && stream.CheckpointSchedulingIdleForTests && GetContext(stream)._stateManagerWriteCount == 0);
        Assert.False(sink.TokenIsCancelled);
    }

    private static StreamContext GetContext(FlowtideDotNet.Base.Engine.DataflowStream stream) =>
        (StreamContext)typeof(FlowtideDotNet.Base.Engine.DataflowStream).GetField("streamContext", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(stream)!;

    private static async Task ObserveStoppedOrDisposed(Task stop)
    {
        try { await stop.WaitAsync(TimeSpan.FromSeconds(10)); }
        catch (ObjectDisposedException) { }
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition()) await Task.Delay(10, timeout.Token);
    }

    private sealed class Sink(string phase) : EgressVertex<string>(new ExecutionDataflowBlockOptions())
    {
        public bool Armed, Stopping, RequestOwnFailure, CancelInsteadOfWaiting;
        public volatile bool InCallback;
        public int Initializations, Compactions;
        private int entered;
        private CancellationTokenRegistration registration;
        public Action? CancellationHandler;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finish { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource EmergencyRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Exited { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> Disposed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool TokenIsCancelled => CancellationToken.IsCancellationRequested;
        public override string DisplayName => "cancellation-aware sink";
        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            Interlocked.Increment(ref Initializations);
            return Hold("initialize");
        }
        public override Task CommitVersion(long version) => Hold(Stopping ? "stop-finality" : version == 0 ? "startup-finality" : "finality");
        public override Task BeforeSaveCheckpoint() => Hold("before-save");
        public override Task CheckpointDone(long version) => Hold(Stopping ? "stop-checkpoint-done" : "checkpoint-done");
        public override Task Compact() { Interlocked.Increment(ref Compactions); return Hold(Stopping ? "stop-compact" : "compact"); }

        private async Task Hold(string callback)
        {
            if (!Armed || phase != callback || Interlocked.Exchange(ref entered, 1) != 0) return;
            InCallback = true;
            try
            {
                if (CancelInsteadOfWaiting)
                {
                    Entered.TrySetResult();
                    throw new OperationCanceledException("Callback cancelled its operation.");
                }
                registration = CancellationToken.Register(() => { Cancelled.TrySetResult(); CancellationHandler?.Invoke(); });
                Entered.TrySetResult();
                if (RequestOwnFailure) await FailAndRollback(new InvalidOperationException("Callback requested failure."));
                try { await EmergencyRelease.Task.WaitAsync(CancellationToken); }
                catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested) { }
                await Finish.Task;
            }
            finally { InCallback = false; Exited.TrySetResult(); }
        }

        protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
        public override Task DeleteAsync() => Task.CompletedTask;
        public override ValueTask DisposeAsync()
        {
            Disposed.TrySetResult(InCallback);
            registration.Dispose();
            return base.DisposeAsync();
        }
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

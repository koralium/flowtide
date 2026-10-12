using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Exchange;

public class StopCheckpointNotificationTests
{
    [Theory]
    [InlineData(true, "failure")]
    [InlineData(false, "failure")]
    [InlineData(true, "dispose")]
    [InlineData(false, "dispose")]
    [InlineData(true, "throw")]
    [InlineData(false, "throw")]
    [InlineData(true, "cancel")]
    [InlineData(false, "cancel")]
    public async Task StopRetainsNotificationOwnershipAndSkipsLaterCallbacks(bool ingress, string interruption)
    {
        var sourceProbe = new NotificationProbe(ingress, interruption);
        var sinkProbe = new NotificationProbe(!ingress, interruption);
        var laterSourceProbe = new NotificationProbe(false, interruption);
        var laterSinkProbe = new NotificationProbe(false, interruption);
        var probes = new[] { sourceProbe, laterSourceProbe, sinkProbe, laterSinkProbe };
        var held = ingress ? sourceProbe : sinkProbe;
        var source = new Source(sourceProbe);
        var laterSource = new Source(laterSourceProbe);
        var sink = new Sink(sinkProbe);
        var laterSink = new Sink(laterSinkProbe);
        source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
        laterSource.LinkTo(laterSink, new DataflowLinkOptions { PropagateCompletion = true });
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() });
        var stream = new DataflowStreamBuilder("stop-notification-" + Guid.NewGuid().ToString("N"))
            .AddIngressBlock("source", source).AddIngressBlock("later-source", laterSource)
            .AddEgressBlock("sink", sink).AddEgressBlock("later-sink", laterSink)
            .WithStateOptions(new StateManagerOptions { PersistentStorage = storage })
            .SetStopDrainTimeout(TimeSpan.FromMilliseconds(100)).Build();
        try
        {
            await stream.StartAsync();
            await WaitUntil(() => stream.State == StreamStateValue.Running);
            foreach (var probe in probes) probe.Armed = true;
            var stop = stream.StopAsync();
            await held.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var dispose = interruption == "dispose" ? stream.DisposeAsync().AsTask() : null;

            // The callback outlives the drain timeout. Its ownership must cover this
            // entire interval even though its dataflow block can already complete.
            await Task.Delay(300);
            Assert.False(held.Disposed.Task.IsCompleted);
            Assert.False(stop.IsCompleted);
            if (dispose != null) Assert.False(dispose.IsCompleted);
            held.Release.TrySetResult();
            await held.Exited.Task.WaitAsync(TimeSpan.FromSeconds(10));

            if (dispose != null)
            {
                await dispose.WaitAsync(TimeSpan.FromSeconds(10));
                await Assert.ThrowsAsync<ObjectDisposedException>(() => stop.WaitAsync(TimeSpan.FromSeconds(10)));
            }
            else
            {
                await stop.WaitAsync(TimeSpan.FromSeconds(10));
            }
            Assert.False(await held.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal(0, laterSinkProbe.Notifications);
            if (ingress)
            {
                Assert.Equal(0, laterSourceProbe.Notifications);
                Assert.Equal(0, sinkProbe.Notifications);
            }
            Assert.Equal(0, sink.Compactions);
            Assert.Equal(0, laterSink.Compactions);

            if (dispose == null)
            {
                // A failure/exception must release exactly one ownership claim and
                // leave the stopped stream able to restart and checkpoint again.
                foreach (var probe in probes) probe.Armed = false;
                await stream.StartAsync();
                await WaitUntil(() => stream.State == StreamStateValue.Running);
                var version = storage.CurrentVersion;
                await stream.TriggerCheckpoint();
                await WaitUntil(() => storage.CurrentVersion > version && sink.Compactions > 0);
            }
        }
        finally
        {
            held.Release.TrySetResult();
            await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition()) await Task.Delay(10, timeout.Token);
    }

    private sealed class NotificationProbe(bool hold, string interruption)
    {
        public volatile bool Armed;
        public volatile bool InCallback;
        public int Notifications;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Exited { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> Disposed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task Invoke(Func<Task> requestFailure)
        {
            if (!Armed) return;
            Interlocked.Increment(ref Notifications);
            if (!hold) return;
            InCallback = true;
            try
            {
                if (interruption == "failure") await requestFailure();
                Entered.TrySetResult();
                await Release.Task;
                if (interruption == "throw") throw new InvalidOperationException("Stop notification failed.");
                if (interruption == "cancel") throw new OperationCanceledException("Stop notification cancelled.");
            }
            finally { InCallback = false; Exited.TrySetResult(); }
        }
    }

    private sealed class Source(NotificationProbe probe) : IngressVertex<string>(new DataflowBlockOptions())
    {
        public override string DisplayName => "stop notification source";
        public override Task CheckpointDone(long version) => probe.Invoke(() => FailAndRollback(new InvalidOperationException("Self-requested failure.")));
        protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task SendInitial(IngressOutput<string> output) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>());
        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
        public override Task OnTrigger(string triggerName, object? state) => Task.CompletedTask;
        public override Task Compact() => Task.CompletedTask;
        public override Task DeleteAsync() => Task.CompletedTask;
        public override ValueTask DisposeAsync()
        {
            probe.Disposed.TrySetResult(probe.InCallback);
            return base.DisposeAsync();
        }
    }

    private sealed class Sink(NotificationProbe probe) : EgressVertex<string>(new ExecutionDataflowBlockOptions())
    {
        public int Compactions;
        public override string DisplayName => "stop notification sink";
        public override Task CheckpointDone(long version) => probe.Invoke(() => FailAndRollback(new InvalidOperationException("Self-requested failure.")));
        protected override Task OnCheckpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;
        public override Task Compact() { Interlocked.Increment(ref Compactions); return Task.CompletedTask; }
        public override Task DeleteAsync() => Task.CompletedTask;
        public override ValueTask DisposeAsync()
        {
            probe.Disposed.TrySetResult(probe.InCallback);
            return base.DisposeAsync();
        }
    }
}

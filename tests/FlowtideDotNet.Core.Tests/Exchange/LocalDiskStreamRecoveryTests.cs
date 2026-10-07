using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Exchange;

public class LocalDiskStreamRecoveryTests
{
    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 1)]
    [InlineData(false, 20)]
    [InlineData(true, 20)]
    public async Task CompletedCheckpointsCanRecoverThroughTheEngine(bool cached, int snapshotInterval)
    {
        var parent = Path.GetFullPath(Path.Combine(Path.GetTempPath(), "flowtide-plan-review-retention"));
        var root = Path.GetFullPath(Path.Combine(parent, Guid.NewGuid().ToString("N")));
        Assert.StartsWith(parent + Path.DirectorySeparatorChar, root);
        Directory.CreateDirectory(root);
        var source = new EmptySource();
        var sink = new RecoverySink();
        var failures = new Failures();
        source.LinkTo(sink, new DataflowLinkOptions { PropagateCompletion = true });
        var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions
                {
                    FileProvider = new LocalDiskProvider(Path.Combine(root, "store")),
                    CacheProvider = cached ? new LocalDiskProvider(Path.Combine(root, "cache")) : null,
                    SnapshotCheckpointInterval = snapshotInterval
                });
        var stream = new DataflowStreamBuilder("file-recovery-" + Guid.NewGuid().ToString("N"))
            .AddIngressBlock("source", source)
            .AddEgressBlock("sink", sink)
            .AddFailureListener(failures)
            .WithStateOptions(new StateManagerOptions
            {
                PersistentStorage = storage
            })
            .Build();
        try
        {
            await stream.StartAsync();
            await WaitUntil(() => stream.State == StreamStateValue.Running);
            for (var version = 1; version <= 24; version++)
            {
                await stream.TriggerCheckpoint();
                await WaitUntil(() => Interlocked.Read(ref sink.LastCommitted) >= version);
            }
            Assert.False(failures.StorageFailure.Task.IsCompleted, "Initial checkpoints must succeed.");
            var previousInitializations = Volatile.Read(ref sink.Initializations);
            // Restore the predecessor through the real engine after warming the cache.
            var expectedTime = sink.CheckpointTimes[22];
            await sink.RequestFailure().WaitAsync(TimeSpan.FromSeconds(10));
            await WaitUntil(() => failures.StorageFailure.Task.IsCompleted ||
                (Volatile.Read(ref sink.Initializations) > previousInitializations &&
                 stream.State == StreamStateValue.Running));
            var recoveryError = failures.StorageFailure.Task.IsCompleted
                ? (await failures.StorageFailure.Task).ToString()
                : "Recovery should reach Running without a storage exception.";
            Assert.False(failures.StorageFailure.Task.IsCompleted, recoveryError);
            var context = (StreamContext)typeof(FlowtideDotNet.Base.Engine.DataflowStream).GetField("streamContext",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(stream)!;
            Assert.Equal(expectedTime, context._lastState!.Time);
            Assert.Equal(24, storage.CurrentVersion);
        }
        finally
        {
            await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            storage.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    private static async Task WaitUntil(Func<bool> predicate)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!predicate() && DateTime.UtcNow < deadline) await Task.Delay(10);
        Assert.True(predicate(), "Timed out waiting for stream progress.");
    }

    private sealed class Failures : IFailureListener
    {
        public TaskCompletionSource<Exception> StorageFailure { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void OnFailure(StreamFailureNotification notification)
        {
            if (notification.Exception is { } exception && ContainsIoException(exception))
                StorageFailure.TrySetResult(exception);
        }
        private static bool ContainsIoException(Exception exception) => exception is IOException ||
            (exception is AggregateException aggregate && aggregate.InnerExceptions.Any(ContainsIoException)) ||
            (exception.InnerException is { } inner && ContainsIoException(inner));
    }

    private sealed class RecoverySink : EgressVertex<string>
    {
        public long LastCommitted;
        public int Initializations;
        public RecoverySink() : base(new ExecutionDataflowBlockOptions()) { }
        public Task RequestFailure() => FailAndRollback(new InvalidOperationException("restore predecessor"), 23);
        public List<long> CheckpointTimes { get; } = new();
        public override string DisplayName => "recovery probe sink";
        protected override Task OnCheckpoint(long checkpointTime) { CheckpointTimes.Add(checkpointTime); return Task.CompletedTask; }
        protected override Task OnRecieve(string msg, long time) => Task.CompletedTask;
        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            Interlocked.Increment(ref Initializations);
            return Task.CompletedTask;
        }
        public override Task Compact() => Task.CompletedTask;
        public override Task DeleteAsync() => Task.CompletedTask;
        public override Task CommitVersion(long version)
        {
            Interlocked.Exchange(ref LastCommitted, version);
            return Task.CompletedTask;
        }
    }

    private sealed class EmptySource : IngressVertex<string>
    {
        public EmptySource() : base(new DataflowBlockOptions()) { }
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

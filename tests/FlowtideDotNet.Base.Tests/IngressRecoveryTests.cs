using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Metrics.Internal;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Tests;

public class IngressRecoveryTests
{
    [Fact]
    public async Task OldBlockCompletionCannotDisableTheReplacementRun()
    {
        var scheduler = new HeldScheduler();
        var source = new Source();
        var create = Task.Factory.StartNew(source.CreateBlock, default, TaskCreationOptions.None, scheduler);
        await scheduler.RunNext();
        await create;
        await source.Initialize("source", 0, 1, new Handler(), null);
        source.Complete();
        await source.Completion.WaitAsync(TimeSpan.FromSeconds(10));
        await source.DisposeAsync();

        // The old block has completed, but its completion callback is still queued.
        source.CreateBlock();
        await source.Initialize("source", 0, 1, new Handler(), null);
        try
        {
            await scheduler.RunNext();
            await source.Probe().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(source.Probed, "The previous block's completion disabled the new run.");
        }
        finally
        {
            source.Complete();
            await source.Completion.WaitAsync(TimeSpan.FromSeconds(10));
            await source.DisposeAsync();
        }
    }

    private sealed class HeldScheduler : TaskScheduler
    {
        private readonly ConcurrentQueue<Task> _tasks = new();
        private readonly SemaphoreSlim _queued = new(0);
        protected override IEnumerable<Task> GetScheduledTasks() => _tasks.ToArray();
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;
        protected override void QueueTask(Task task) { _tasks.Enqueue(task); _queued.Release(); }
        public async Task RunNext()
        {
            Assert.True(await _queued.WaitAsync(TimeSpan.FromSeconds(10)), "The expected continuation was not queued.");
            Assert.True(_tasks.TryDequeue(out var task));
            Assert.True(TryExecuteTask(task!));
            await task!;
        }
    }

    private sealed class Handler : IVertexHandler
    {
        public string OperatorId => "source";
        public string StreamName => "ingress-recovery";
        public IMeter Metrics { get; } = new FlowtideMeter(new Meter("ingress-recovery"), new TagList(), () => "source");
        public IStateManagerClient StateClient => null!;
        public ILoggerFactory LoggerFactory => NullLoggerFactory.Instance;
        public IOperatorMemoryManager MemoryManager => null!;
        public void ScheduleCheckpoint(TimeSpan time, long? checkpointVersion) { }
        public Task RegisterTrigger(string name, TimeSpan? scheduledInterval = null) => Task.CompletedTask;
        public Task FailAndRollback(Exception? exception, long? restoreVersion = default) => Task.CompletedTask;
    }

    private sealed class Source() : IngressVertex<string>(new DataflowBlockOptions())
    {
        public bool Probed;
        public Task Probe() => RunTask((_, _) => { Probed = true; return Task.CompletedTask; });
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

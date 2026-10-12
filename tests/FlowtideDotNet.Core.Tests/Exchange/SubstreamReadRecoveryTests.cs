using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Exchange;

public class SubstreamReadRecoveryTests : OperatorTestBase
{
    [Fact]
    public async Task RestartStartsFetchingBeforeTheOldCleanupContinuationRuns()
    {
        var hub = new LocalSubstreamCommunicationHub();
        var point = new SubstreamCommunicationPoint(NullLogger.Instance, "a", "b", hub.CreateFactory("a").GetCommunicationHandler("b", "a"));
        _ = new SubstreamCommunicationPoint(NullLogger.Instance, "b", "a", hub.CreateFactory("b").GetCommunicationHandler("a", "b"));
        var reader = new SubstreamReadOperator(point, new SubstreamExchangeReferenceRelation { SubStreamName = "b", ExchangeTargetId = 1 }, new DataflowBlockOptions());
        await InitializeOperator(reader);
        var scheduler = new HeldScheduler();
        var dispatch = Task.Factory.StartNew(() => reader.DoLockingEvent(new InitWatermarksEvent()), default, TaskCreationOptions.None, scheduler);
        await scheduler.RunNext();
        await dispatch;
        await WaitUntil(() => point.IsSubscribed(1));
        reader.Complete();
        try { await reader.Completion.WaitAsync(TimeSpan.FromSeconds(10)); }
        catch (OperationCanceledException) { }
        await reader.DisposeAsync();

        // The base ingress separately removes completed tasks. Wait for that bookkeeping
        // while deliberately retaining the read operator's own cleanup continuation.
        var ingressType = typeof(IngressVertex<StreamEventBatch>);
        var taskLock = ingressType.GetField("_stateLock", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(reader)!;
        var tasks = (Dictionary<int, Task>)ingressType.GetField("_runningTasks", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(reader)!;
        await WaitUntil(() => { lock (taskLock) return tasks.Count == 0; });

        reader.CreateBlock();
        await ReinitializeOperator(reader);
        try
        {
            reader.DoLockingEvent(new InitWatermarksEvent());
            await WaitUntil(() => point.IsSubscribed(1));
            await scheduler.RunNext();
            Assert.True(point.IsSubscribed(1), "Old cleanup must not retire the replacement fetch loop.");
        }
        finally
        {
            reader.Complete();
            try { await reader.Completion.WaitAsync(TimeSpan.FromSeconds(10)); }
            catch (OperationCanceledException) { }
            await reader.DisposeAsync();
            await scheduler.Drain();
        }
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition()) await Task.Delay(10, timeout.Token);
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
            Assert.True(await _queued.WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.True(_tasks.TryDequeue(out var task));
            Assert.True(TryExecuteTask(task!));
            await task!;
        }
        public async Task Drain()
        {
            while (!_tasks.IsEmpty) await RunNext();
        }
    }
}

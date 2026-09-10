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

using FlowtideDotNet.AcceptanceTests;
using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Xunit.Abstractions;

namespace FlowtideDotNet.Core.Tests.Operators.Window
{
    /// <summary>
    /// Leaf write locks are monitors, so they are thread affine. The bulk window operator held one
    /// across a suspended ComputeRow and released it several async completions later. When a
    /// continuation in between resumed on another thread, Monitor.Exit threw "Object synchronization
    /// method was called from an unsynchronized block of code".
    ///
    /// The test replaces row_number with a function whose every compute suspends and drives the
    /// stream through a scheduler that never resumes a continuation on the thread that queued it,
    /// which makes the failure deterministic instead of a race.
    /// </summary>
    public class BulkWindowLockThreadAffinityTests : FlowtideAcceptanceBase, IDisposable
    {
        private const string RowNumberQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users";

        public record RowNumberResult(string? companyId, int userkey, long value);

        private readonly ForeignThreadTaskScheduler _scheduler = new ForeignThreadTaskScheduler();
        private readonly SuspendingRowNumberDefinition _rowNumber = new SuspendingRowNumberDefinition();

        public BulkWindowLockThreadAffinityTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
            TaskScheduler = _scheduler;
            ((FunctionsRegister)FunctionsRegister).SetBulkWindowFunctionForTests(
                FunctionsArithmetic.Uri,
                FunctionsArithmetic.RowNumber,
                _rowNumber);
        }

        // Runs after the base has disposed the stream.
        public void Dispose()
        {
            _scheduler.Dispose();
        }

        private void AddUser(string companyId, int userKey)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = userKey,
                CompanyId = companyId,
                DoubleValue = userKey
            });
        }

        private List<RowNumberResult> ExpectedRowNumbers()
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g => g.OrderBy(x => x.UserKey)
                    .Select((user, index) => new RowNumberResult(user.CompanyId, user.UserKey, index + 1)))
                .ToList();
        }

        [Fact]
        public async Task SuspendedComputeRowReleasesLeafLockOnOwningThread()
        {
            for (int i = 0; i < 20; i++)
            {
                AddUser("1", i);
                AddUser("2", 100 + i * 10);
            }

            await StartStream(RowNumberQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRowNumbers());

            // Append and insert in the middle, the incremental partition scan.
            AddUser("1", 20);
            AddUser("2", 105);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRowNumbers());

            Assert.Equal(0, FailureNotificationCount);
            // Without these the test passes silently if the suspending path or the thread hops stop being exercised.
            Assert.True(_rowNumber.SuspendCount > 0, "ComputeRow never suspended");
            Assert.True(_scheduler.QueuedTasks > 0, "No task ran on the scheduler");
        }
    }

    /// <summary>
    /// Never inlines and never runs a task on the thread that queued it, every continuation hops threads.
    /// Dedicated workers make the hop a guarantee, the thread pool could hand a requeued item back to the same thread.
    /// </summary>
    internal sealed class ForeignThreadTaskScheduler : TaskScheduler, IDisposable
    {
        private readonly BlockingCollection<Task>[] _queues;
        private readonly Thread[] _workers;
        private readonly CancellationTokenSource _stop = new CancellationTokenSource();
        private int _queuedTasks;
        private int _nextWorker;

        public ForeignThreadTaskScheduler(int workerCount = 8)
        {
            _queues = new BlockingCollection<Task>[workerCount];
            _workers = new Thread[workerCount];
            for (int i = 0; i < workerCount; i++)
            {
                var queue = new BlockingCollection<Task>();
                _queues[i] = queue;
                _workers[i] = new Thread(() => Work(queue))
                {
                    IsBackground = true,
                    Name = $"foreign-scheduler-{i}"
                };
                _workers[i].Start();
            }
        }

        public int QueuedTasks => Volatile.Read(ref _queuedTasks);

        public override int MaximumConcurrencyLevel => _workers.Length;

        protected override void QueueTask(Task task)
        {
            Interlocked.Increment(ref _queuedTasks);
            var index = (Interlocked.Increment(ref _nextWorker) & int.MaxValue) % _workers.Length;
            if (_workers[index] == Thread.CurrentThread)
            {
                index = (index + 1) % _workers.Length;
            }
            _queues[index].Add(task);
            if (_stop.IsCancellationRequested)
            {
                // Stopped workers must not strand a task, the stream still disposes after the test.
                DrainToThreadPool();
            }
        }

        private void Work(BlockingCollection<Task> queue)
        {
            try
            {
                foreach (var task in queue.GetConsumingEnumerable(_stop.Token))
                {
                    TryExecuteTask(task);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private void DrainToThreadPool()
        {
            foreach (var queue in _queues)
            {
                while (queue.TryTake(out var task))
                {
                    ThreadPool.UnsafeQueueUserWorkItem(t => TryExecuteTask(t), task, preferLocal: false);
                }
            }
        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

        protected override IEnumerable<Task> GetScheduledTasks() => _queues.SelectMany(q => q).ToArray();

        public void Dispose()
        {
            _stop.Cancel();
            DrainToThreadPool();
        }
    }

    internal sealed class SuspendingRowNumberDefinition : BulkWindowFunctionDefinition
    {
        public int SuspendCount;

        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            bulkWindowFunction = new SuspendingRowNumberFunction(this);
            return true;
        }
    }

    /// <summary>
    /// Row number whose every compute suspends, forcing the operator's async path.
    /// </summary>
    internal sealed class SuspendingRowNumberFunction : IBulkWindowFunction
    {
        private readonly SuspendingRowNumberDefinition _definition;
        private int _functionIndex;
        private long _nextRowNumber;

        public SuspendingRowNumberFunction(SuspendingRowNumberDefinition definition)
        {
            _definition = definition;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            if (fromPartitionStart || !await seedReader.EnsureRows(1))
            {
                _nextRowNumber = 1;
                return;
            }
            _nextRowNumber = seedReader.GetState(1, _functionIndex).AsLong + 1;
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            return false;
        }

        public async ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Interlocked.Increment(ref _definition.SuspendCount);
            await Task.Yield();
            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(_nextRowNumber);
            _nextRowNumber++;
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}

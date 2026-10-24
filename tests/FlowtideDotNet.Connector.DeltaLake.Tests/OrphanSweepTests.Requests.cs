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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Collections.Concurrent;
using System.Reflection;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // Bounded waits for jobs
        private static readonly TimeSpan s_requestWait = TimeSpan.FromSeconds(30);

        private static readonly TimeSpan s_requestInterval = TimeSpan.FromDays(7);

        private static readonly FieldInfo s_workerRun = typeof(DeltaMaintenanceWorker).GetField("_run", BindingFlags.NonPublic | BindingFlags.Instance)!;

        // A manual-tick worker running only cleanup over a local disk table, with one clock for the budget, the options and the cleanup
        private sealed class RequestRig
        {
            public required FakeTimeProvider Clock { get; init; }

            public required DeltaMaintenanceBudget Budget { get; init; }

            public required DeltaMaintenanceWorker Worker { get; init; }

            public required DeltaTableCleanup Cleanup { get; init; }

            public required IFileStorage Raw { get; init; }

            // The kind of each job the worker settled, in order
            public ConcurrentQueue<JobKind> Kinds { get; } = new ConcurrentQueue<JobKind>();

            // Awaited at every worker hold point after the kinds are recorded
            public Func<string, Task>? Hold { get; set; }

            public DeltaMaintenanceBudget.SharedBudget Source => new DeltaMaintenanceBudget.SharedBudget(existing: Budget);

            public int Count(JobKind kind) => Kinds.Count(x => x == kind);

            public OrphanSweepRequestResult Request() => DeltaLakeMaintenance.RequestOrphanSweep(Raw, "t", Source);

            public bool OrphanDue() => Cleanup.IsDue(JobKind.Orphan, 0, Clock.GetUtcNow());
        }

        // The worker's dispatched job
        private static JobRun RunningJob(DeltaMaintenanceWorker worker) => (JobRun)s_workerRun.GetValue(worker)!;

        // A table at /t on local disk with its own commit 0 publishing the retention, written now
        private static async Task<IFileStorage> RequestTableAsync(AdvanceExecutionKit kit)
        {
            var raw = Files.Of.LocalDisk(kit.NewRoot());
            var published = new Dictionary<string, string>()
            {
                [RetentionProperties.LogKey] = RetentionProperties.Format(AdvanceExecutionKit.Retention),
                [RetentionProperties.DeletedFileKey] = RetentionProperties.Format(AdvanceExecutionKit.Retention)
            };
            await DeltaTestKit.WriteOwnCommit(raw, "t", 0, 0, AdvanceExecutionKit.Now.ToUnixTimeMilliseconds(), DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, published));
            return raw;
        }

        // The cleanup a sink run of the table builds, over the budget's meter
        private static DeltaTableCleanup RequestCleanup(DeltaMaintenanceBudget budget, CleanupConfiguration configuration, IFileStorage raw, string name, Func<IFileStorage?>? current = null)
        {
            var metered = MeteredFileStorage.Wrap(raw, budget.Buckets, configuration.Store.Store.Paging);
            return new DeltaTableCleanup(configuration, metered, budget.Clock, name, null, current ?? (() => raw), new DiscardCounters(), null, name);
        }

        // A worker that runs cleanup without inspection or builds
        private static DeltaMaintenanceWorker RequestWorker(DeltaMaintenanceBudget budget, IFileStorage raw, string name, IMaintenanceCleanup cleanup)
        {
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, budget.Clock) { PeriodicEnabled = false, InspectionEnabled = false }, raw, name, name, null) { Cleanup = cleanup };
            budget.Register(worker);
            return worker;
        }

        private static DeltaMaintenanceBudget RequestBudget(AdvanceExecutionKit kit, FakeTimeProvider clock)
        {
            return new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, kit.NewRoot()), clock, manualTick: true) { Buckets = MaintenanceBucketsTests.Unthrottled() };
        }

        private static async Task<RequestRig> RequestRigAsync(AdvanceExecutionKit kit, TimeSpan? interval)
        {
            var clock = new FakeTimeProvider(AdvanceExecutionKit.Now);
            var budget = RequestBudget(kit, clock);
            var raw = await RequestTableAsync(kit);
            var configuration = CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, interval));
            var cleanup = RequestCleanup(budget, configuration, raw, "t");
            var worker = RequestWorker(budget, raw, "t", cleanup);
            var rig = new RequestRig() { Clock = clock, Budget = budget, Worker = worker, Cleanup = cleanup, Raw = raw };
            worker.HooksForTests = new WorkerHooks()
            {
                Hold = point =>
                {
                    // Once per job, before it settles
                    if (point == "release")
                    {
                        rig.Kinds.Enqueue(RunningJob(worker).Kind);
                    }
                    return rig.Hold?.Invoke(point) ?? Task.CompletedTask;
                }
            };
            worker.Published(0);
            return rig;
        }

        private static async Task StopRequestRigAsync(RequestRig rig)
        {
            await rig.Worker.StopAsync(s_requestWait);
            rig.Budget.ScratchIfCreated?.ReleaseLockForTests();
            rig.Raw.Dispose();
        }

        // Ticks until a tick dispatches nothing
        private static async Task QuiesceRequestRigAsync(RequestRig rig)
        {
            for (int i = 0; i < 20; i++)
            {
                var before = rig.Worker.CurrentJob;
                rig.Budget.TickNow();
                var after = rig.Worker.CurrentJob;
                await after.WaitAsync(s_requestWait);
                if (ReferenceEquals(before, after))
                {
                    return;
                }
            }
            throw new InvalidOperationException("The worker kept dispatching");
        }

        // One long orphan job of the cleanup, settled
        private static Task<CleanupResult> OrphanJobAsync(DeltaTableCleanup cleanup, AdvanceExecutionKit kit) => OrphanJobAsync(cleanup, kit.AdvanceContext(0));

        private static async Task<CleanupResult> OrphanJobAsync(DeltaTableCleanup cleanup, CleanupContext context)
        {
            try
            {
                return await cleanup.RunAsync(JobKind.Orphan, context);
            }
            finally
            {
                await context.SettleAsync();
            }
        }

        // The first grid point after the time, anchored at the offset
        private static DateTimeOffset NextGridPoint(DateTimeOffset time, TimeSpan offset, TimeSpan interval)
        {
            return new DateTimeOffset(offset.Ticks + ((time.UtcTicks - offset.Ticks) / interval.Ticks + 1) * interval.Ticks, TimeSpan.Zero);
        }

        // A wall clock a test can step back
        private sealed class SteppedClock : TimeProvider
        {
            public DateTimeOffset Now { get; set; }

            public override DateTimeOffset GetUtcNow() => Now;
        }

        // A long orphan admission on the clock, ending at the long deadline
        private CleanupContext DeadlineContext(TimeProvider clock)
        {
            return new CleanupContext(new JobRun(0, JobKind.Orphan) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Orphan,
                Long = true,
                ReadLimit = long.MaxValue,
                Deadline = clock.GetUtcNow() + TimeSpan.FromHours(6),
                Head = 0,
                TableId = null,
                Scratch = Scratch(DiskLedger.DefaultCap),
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default
            };
        }

        [Fact]
        public async Task ManualSweepSchedulesCoalescesAndRejectsUnavailable()
        {
            using var kit = new AdvanceExecutionKit();
            var clock = new FakeTimeProvider(AdvanceExecutionKit.Now);
            var budget = RequestBudget(kit, clock);
            var source = new DeltaMaintenanceBudget.SharedBudget(existing: budget);
            var disposables = new List<IDisposable>();
            try
            {
                using var raw = await RequestTableAsync(kit);
                var configuration = CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, null));
                var cleanup = RequestCleanup(budget, configuration, raw, "t");
                RequestWorker(budget, raw, "t", cleanup);

                // Coalesced until an attempt concludes, also when the name is written differently
                Assert.Equal(OrphanSweepRequestResult.Scheduled, DeltaLakeMaintenance.RequestOrphanSweep(raw, "t", source));
                Assert.Equal(OrphanSweepRequestResult.AlreadyPending, DeltaLakeMaintenance.RequestOrphanSweep(raw, "t", source));
                Assert.Equal(OrphanSweepRequestResult.AlreadyPending, DeltaLakeMaintenance.RequestOrphanSweep(raw, "/t", source));
                Assert.True(cleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
                cleanup.OrphanForTests.CoreForTests = _ => Task.FromResult(new CleanupResult(OutcomeKind.Completed));
                Assert.Equal(OutcomeKind.Completed, (await OrphanJobAsync(cleanup, kit)).Kind);
                Assert.False(cleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
                Assert.Equal(OrphanSweepRequestResult.Scheduled, DeltaLakeMaintenance.RequestOrphanSweep(raw, "t", source));

                // Qualified pinned S3 and Azure, whose never-ticked workers reach no endpoint
                foreach (var cloud in new[] { StoreFixtures.S3(), StoreFixtures.Azure() })
                {
                    disposables.Add(cloud);
                    var qualified = CleanupConfiguration.Validate(cloud, "c/t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, s_requestInterval));
                    Assert.True(qualified.Store.Store is { Pinned: true, DirectoryExclusion: true });
                    RequestWorker(budget, cloud, "c/t", RequestCleanup(budget, qualified, cloud, "c/t"));
                    Assert.Equal(OrphanSweepRequestResult.Scheduled, DeltaLakeMaintenance.RequestOrphanSweep(cloud, "c/t", source));
                    Assert.Equal(OrphanSweepRequestResult.AlreadyPending, DeltaLakeMaintenance.RequestOrphanSweep(cloud, "c/t", source));
                }

                // A run with retention off
                var off = Files.Of.LocalDisk(kit.NewRoot());
                disposables.Add(off);
                RequestWorker(budget, off, "t", RequestCleanup(budget, CleanupConfiguration.Validate(off, "t", false, CleanupOptIns.Off), off, "t"));
                // A vouched unknown store, eligible for retention alone
                var unknown = new HookFileStorage(Files.Of.InternalMemory($"./orphan-request-{Guid.NewGuid():N}"));
                var vouched = CleanupConfiguration.Validate(unknown, "t", true, new CleanupOptIns(AdvanceExecutionKit.Retention, null));
                Assert.True(vouched.Store.Eligible);
                RequestWorker(budget, unknown, "t", RequestCleanup(budget, vouched, unknown, "t"));
                // A storage the sink's setting later replaces
                var replaced = Files.Of.LocalDisk(kit.NewRoot());
                disposables.Add(replaced);
                IFileStorage current = replaced;
                var held = CleanupConfiguration.Validate(() => current, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, null));
                var heldCleanup = RequestCleanup(budget, held, replaced, "t", () => current);
                RequestWorker(budget, replaced, "t", heldCleanup);
                Assert.Equal(OrphanSweepRequestResult.Scheduled, DeltaLakeMaintenance.RequestOrphanSweep(replaced, "t", source));
                Assert.True(heldCleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
                var replacement = Files.Of.LocalDisk(kit.NewRoot());
                disposables.Add(replacement);
                current = replacement;
                // The replacement holds the pending request
                Assert.False(heldCleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
                // A worker whose cleanup is not a table's
                var faked = Files.Of.LocalDisk(kit.NewRoot());
                disposables.Add(faked);
                RequestWorker(budget, faked, "t", new DeltaMaintenanceSchedulingTests.FakeCleanup());

                var registered = budget.Registered;
                var none = new DeltaMaintenanceBudget.SharedBudget();
                var sameCloudPath = StoreFixtures.S3();
                disposables.Add(sameCloudPath);
                var unavailable = new (IFileStorage? Storage, string? Table, DeltaMaintenanceBudget.SharedBudget Source)[]
                {
                    (raw, "t", none),
                    (raw, "u", source),
                    (sameCloudPath, "c/t", source),
                    (off, "t", source),
                    (unknown, "t", source),
                    (replaced, "t", source),
                    (faked, "t", source),
                    (null, "t", source),
                    (raw, null, source),
                    // A path no local key forms
                    (raw, "t\0x", source)
                };
                foreach (var (storage, table, from) in unavailable)
                {
                    Assert.Equal(OrphanSweepRequestResult.Unavailable, DeltaLakeMaintenance.RequestOrphanSweep(storage!, table!, from));
                }
                // The lookup creates no budget and registers nothing
                Assert.Null(none.Existing);
                Assert.Equal(registered, budget.Registered);
                // The public overload over the process's budget
                Assert.Equal(OrphanSweepRequestResult.Unavailable, DeltaLakeMaintenance.RequestOrphanSweep(sameCloudPath, "c/t"));
            }
            finally
            {
                foreach (var disposable in disposables)
                {
                    disposable.Dispose();
                }
            }
        }

        [Fact]
        public async Task ManualSweepUsesLongGate()
        {
            using var kit = new AdvanceExecutionKit();
            var rig = await RequestRigAsync(kit, null);
            try
            {
                // The staged sweep and the advance go first, at the run's first chance
                await QuiesceRequestRigAsync(rig);
                Assert.Equal(1, rig.Count(JobKind.StagedSweep));
                Assert.Equal(1, rig.Count(JobKind.Advance));
                Assert.Equal(0, rig.Count(JobKind.Orphan));

                var promoted = new TaskCompletionSource<JobRun>(TaskCreationOptions.RunContinuationsAsynchronously);
                var admitted = new TaskCompletionSource<JobRun>(TaskCreationOptions.RunContinuationsAsynchronously);
                var leavePromoted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var leaveAdmitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                rig.Hold = point =>
                {
                    if (point == "promoted")
                    {
                        promoted.TrySetResult(RunningJob(rig.Worker));
                        return leavePromoted.Task;
                    }
                    // The next admission's cleanup point
                    if (point == "cleanup" && promoted.Task.IsCompleted)
                    {
                        admitted.TrySetResult(RunningJob(rig.Worker));
                        return leaveAdmitted.Task;
                    }
                    return Task.CompletedTask;
                };
                var contexts = new ConcurrentQueue<(CleanupContext Context, int LongRunning)>();
                rig.Cleanup.OrphanForTests.CoreForTests = context =>
                {
                    contexts.Enqueue((context, rig.Budget.LongGate.Running));
                    return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
                };

                Assert.Equal(OrphanSweepRequestResult.Scheduled, rig.Request());
                rig.Budget.TickNow();
                var job = rig.Worker.CurrentJob;

                // Promoted from its short admission, holding no slot
                var held = await promoted.Task.WaitAsync(s_requestWait);
                Assert.Equal((JobKind.Orphan, true), (held.Kind, held.Long));
                Assert.Equal((0, 0, 0), (rig.Budget.LongGate.Running, rig.Budget.Gate.Running, rig.Budget.Leases.Count));
                leavePromoted.SetResult();
                // Then admitted through the long gate before the main gate
                held = await admitted.Task.WaitAsync(s_requestWait);
                Assert.Equal((JobKind.Orphan, true), (held.Kind, held.Long));
                Assert.Equal((1, 1, 1), (rig.Budget.LongGate.Running, rig.Budget.Gate.Running, rig.Budget.Leases.Count));
                leaveAdmitted.SetResult();
                await job.WaitAsync(s_requestWait);

                var (context, longRunning) = Assert.Single(contexts);
                Assert.Equal(JobKind.Orphan, context.Kind);
                Assert.True(context.Long);
                Assert.Equal(long.MaxValue, context.ReadLimit);
                // The long deadline
                Assert.Equal(rig.Clock.GetUtcNow() + TimeSpan.FromHours(6), context.Deadline);
                Assert.Equal(1, longRunning);
                Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
                Assert.Equal(0, rig.Budget.LongGate.Running);
                Assert.Equal(1, rig.Count(JobKind.Orphan));
                // The concluded attempt took the request
                Assert.False(rig.OrphanDue());
                Assert.Equal(OrphanSweepRequestResult.Scheduled, rig.Request());
            }
            finally
            {
                await StopRequestRigAsync(rig);
            }
        }

        [Fact]
        public async Task StartedAdvanceHoldsTheOrphanSweep()
        {
            using var kit = new AdvanceExecutionKit();
            var table = await kit.ChurnedAsync();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            Assert.Equal(OrphanSweepRequestResult.Scheduled, cleanup.RequestOrphanSweep());
            Assert.True(cleanup.IsDue(JobKind.Orphan, 0, AdvanceExecutionKit.Now));
            await kit.PublishAsync(cleanup, table);
            await kit.AdoptAsync(cleanup, table);
            Assert.NotNull(cleanup.ActiveForTests);
            // Held while the advance runs, keeping the request
            Assert.False(cleanup.IsDue(JobKind.Orphan, 0, AdvanceExecutionKit.Now));
            Assert.Equal(OrphanSweepRequestResult.AlreadyPending, cleanup.RequestOrphanSweep());
        }

        [Fact]
        public async Task OrphanSweepOffIsNeverDue()
        {
            using var kit = new AdvanceExecutionKit();
            var rig = await RequestRigAsync(kit, null);
            try
            {
                var cores = 0;
                rig.Cleanup.OrphanForTests.CoreForTests = _ =>
                {
                    Interlocked.Increment(ref cores);
                    return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
                };
                // Four days in six-hour steps
                for (int step = 0; step <= 16; step++)
                {
                    await QuiesceRequestRigAsync(rig);
                    rig.Clock.Advance(TimeSpan.FromHours(6));
                }
                Assert.Equal(0, rig.Count(JobKind.Orphan));
                Assert.Equal(0, Volatile.Read(ref cores));
                // The other kinds ran each day
                Assert.True(rig.Count(JobKind.StagedSweep) >= 4, string.Join(",", rig.Kinds));
                Assert.True(rig.Count(JobKind.Advance) >= 4, string.Join(",", rig.Kinds));

                // A request dispatches it
                Assert.Equal(OrphanSweepRequestResult.Scheduled, rig.Request());
                await QuiesceRequestRigAsync(rig);
                Assert.Equal(1, rig.Count(JobKind.Orphan));
                Assert.Equal(1, Volatile.Read(ref cores));
            }
            finally
            {
                await StopRequestRigAsync(rig);
            }
        }

        [Fact]
        public async Task ScheduledSweepRunsAtIntervalGridPoints()
        {
            using var kit = new AdvanceExecutionKit();
            var clock = new FakeTimeProvider(AdvanceExecutionKit.Now);
            var budget = RequestBudget(kit, clock);
            using var raw = await RequestTableAsync(kit);
            DeltaTableCleanup Cleanup(TimeSpan interval) => RequestCleanup(budget, CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, interval)), raw, "t");
            var cleanup = Cleanup(s_requestInterval);
            bool Due(DeltaTableCleanup of) => of.IsDue(JobKind.Orphan, 0, clock.GetUtcNow());
            TimeSpan? crossing = null;
            cleanup.OrphanForTests.CoreForTests = _ =>
            {
                if (crossing is { } by)
                {
                    clock.Advance(by);
                }
                return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
            };

            // Not at the run's start, then at the next grid point the table's offset anchors
            var first = NextGridPoint(clock.GetUtcNow(), cleanup.AdvanceOffset, s_requestInterval);
            Assert.False(Due(cleanup));
            clock.Advance(first - clock.GetUtcNow() - TimeSpan.FromTicks(1));
            Assert.False(Due(cleanup));
            clock.Advance(TimeSpan.FromTicks(1));
            Assert.True(Due(cleanup));

            // A concluded run waits for the next grid point
            Assert.Equal(OutcomeKind.Completed, (await OrphanJobAsync(cleanup, kit)).Kind);
            Assert.False(Due(cleanup));
            clock.Advance(s_requestInterval - TimeSpan.FromTicks(1));
            Assert.False(Due(cleanup));
            clock.Advance(TimeSpan.FromTicks(1));
            Assert.True(Due(cleanup));
            Assert.Equal(OutcomeKind.Completed, (await OrphanJobAsync(cleanup, kit)).Kind);
            Assert.False(Due(cleanup));

            // A requested run that starts before a grid point and concludes after it leaves that point due
            clock.Advance(TimeSpan.FromDays(1));
            Assert.Equal(OrphanSweepRequestResult.Scheduled, cleanup.RequestOrphanSweep());
            crossing = s_requestInterval;
            Assert.Equal(OutcomeKind.Completed, (await OrphanJobAsync(cleanup, kit)).Kind);
            crossing = null;
            Assert.True(Due(cleanup));

            // A restarted cleanup waits for the next grid point
            var restarted = Cleanup(s_requestInterval);
            Assert.Equal(cleanup.AdvanceOffset, restarted.AdvanceOffset);
            var next = NextGridPoint(clock.GetUtcNow(), restarted.AdvanceOffset, s_requestInterval);
            Assert.False(Due(restarted));
            clock.Advance(next - clock.GetUtcNow() - TimeSpan.FromTicks(1));
            Assert.False(Due(restarted));
            clock.Advance(TimeSpan.FromTicks(1));
            Assert.True(Due(restarted));

            // The longest interval is never due, and its arithmetic never overflows
            var never = Cleanup(TimeSpan.MaxValue);
            never.OrphanForTests.CoreForTests = _ => Task.FromResult(new CleanupResult(OutcomeKind.Completed));
            foreach (var time in new[] { clock.GetUtcNow(), clock.GetUtcNow() + TimeSpan.FromDays(365_000), DateTimeOffset.MinValue, DateTimeOffset.MaxValue })
            {
                Assert.False(never.IsDue(JobKind.Orphan, 0, time));
            }
            Assert.Equal(OrphanSweepRequestResult.Scheduled, never.RequestOrphanSweep());
            Assert.Equal(OutcomeKind.Completed, (await OrphanJobAsync(never, kit)).Kind);
            Assert.False(never.IsDue(JobKind.Orphan, 0, DateTimeOffset.MaxValue));
        }

        public static TheoryData<string, bool> OrphanDeclines => new TheoryData<string, bool>
        {
            { nameof(MaintenanceReason.ListingIneligible), false },
            { nameof(MaintenanceReason.DiskCap), false },
            { nameof(CleanupResult.Defers), false },
            { nameof(MaintenanceReason.OrphanNotEligible), true },
            { nameof(CleanupResult.Oversized), true },
            { nameof(MaintenanceReason.CleanupGate), true },
            { nameof(MaintenanceReason.DeletesDisabled), true }
        };

        [Theory]
        [MemberData(nameof(OrphanDeclines))]
        public async Task NonConcludingOrphanDeclineStaysDueWithBackoff(string decline, bool concludes)
        {
            var result = decline switch
            {
                nameof(CleanupResult.Defers) => new CleanupResult(OutcomeKind.Declined, MaintenanceReason.OrphanNotEligible) { Defers = true },
                nameof(CleanupResult.Oversized) => new CleanupResult(OutcomeKind.Declined, MaintenanceReason.DiskCap) { Oversized = true },
                _ => new CleanupResult(OutcomeKind.Declined, Enum.Parse<MaintenanceReason>(decline))
            };
            using var kit = new AdvanceExecutionKit();

            // A request through the worker
            var rig = await RequestRigAsync(kit, s_requestInterval);
            try
            {
                await QuiesceRequestRigAsync(rig);
                rig.Cleanup.OrphanForTests.CoreForTests = _ => Task.FromResult(result);
                Assert.Equal(OrphanSweepRequestResult.Scheduled, rig.Request());
                await QuiesceRequestRigAsync(rig);
                Assert.Equal(1, rig.Count(JobKind.Orphan));
                Assert.Equal((OutcomeKind.Declined, result.Reason), (rig.Worker.LastOutcomeForTests!.Kind, rig.Worker.LastOutcomeForTests.Reason));
                // Every decline backs the kind off
                Assert.Equal(1, rig.Worker.CleanupFailures(JobKind.Orphan));
                var retryAt = rig.Worker.CleanupRetryAt(JobKind.Orphan);
                Assert.True(retryAt > rig.Clock.GetUtcNow());
                // Only a concluding one takes the request
                Assert.Equal(!concludes, rig.OrphanDue());
                Assert.Equal(concludes ? OrphanSweepRequestResult.Scheduled : OrphanSweepRequestResult.AlreadyPending, rig.Request());
                // The kept or new request waits for the backoff, then runs again
                rig.Clock.Advance(retryAt - rig.Clock.GetUtcNow() - TimeSpan.FromTicks(1));
                await QuiesceRequestRigAsync(rig);
                Assert.Equal(1, rig.Count(JobKind.Orphan));
                rig.Clock.Advance(TimeSpan.FromTicks(1));
                await QuiesceRequestRigAsync(rig);
                Assert.Equal(2, rig.Count(JobKind.Orphan));
                Assert.Equal(2, rig.Worker.CleanupFailures(JobKind.Orphan));
            }
            finally
            {
                await StopRequestRigAsync(rig);
            }

            // A grid point without a request
            var clock = new FakeTimeProvider(AdvanceExecutionKit.Now);
            var budget = RequestBudget(kit, clock);
            using var raw = await RequestTableAsync(kit);
            var cleanup = RequestCleanup(budget, CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, s_requestInterval)), raw, "t");
            cleanup.OrphanForTests.CoreForTests = _ => Task.FromResult(result);
            clock.Advance(NextGridPoint(clock.GetUtcNow(), cleanup.AdvanceOffset, s_requestInterval) - clock.GetUtcNow());
            Assert.True(cleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
            Assert.Same(result, await OrphanJobAsync(cleanup, kit));
            Assert.Equal(!concludes, cleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
        }

        [Fact]
        public async Task RequestDuringARunningSweepJoinsIt()
        {
            using var kit = new AdvanceExecutionKit();
            var clock = new FakeTimeProvider(AdvanceExecutionKit.Now);
            var budget = RequestBudget(kit, clock);
            using var raw = await RequestTableAsync(kit);
            var cleanup = RequestCleanup(budget, CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, null)), raw, "t");
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var leave = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            cleanup.OrphanForTests.CoreForTests = async _ =>
            {
                entered.TrySetResult();
                await leave.Task;
                return new CleanupResult(OutcomeKind.Completed);
            };

            // Taken while a sweep without a request runs, then joined
            var running = OrphanJobAsync(cleanup, kit);
            await entered.Task.WaitAsync(s_requestWait);
            Assert.Equal(OrphanSweepRequestResult.Scheduled, cleanup.RequestOrphanSweep());
            Assert.Equal(OrphanSweepRequestResult.AlreadyPending, cleanup.RequestOrphanSweep());
            leave.SetResult();
            Assert.Equal(OutcomeKind.Completed, (await running.WaitAsync(s_requestWait)).Kind);

            // The running sweep's conclusion answered them
            Assert.False(cleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
            Assert.Equal(OrphanSweepRequestResult.Scheduled, cleanup.RequestOrphanSweep());
        }

        [Fact]
        public async Task ConcludedGridPointNeverRunsTwiceAfterTheClockStepsBack()
        {
            using var kit = new AdvanceExecutionKit();
            using var raw = await RequestTableAsync(kit);
            var clock = new SteppedClock() { Now = AdvanceExecutionKit.Now };
            var configuration = CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, s_requestInterval));
            var metered = MeteredFileStorage.Wrap(raw, MaintenanceBucketsTests.Unthrottled(), configuration.Store.Store.Paging);
            var cleanup = new DeltaTableCleanup(configuration, metered, clock, "t", null, () => raw, new DiscardCounters(), null, "t");
            cleanup.OrphanForTests.CoreForTests = _ => Task.FromResult(new CleanupResult(OutcomeKind.Completed));
            var point = NextGridPoint(clock.Now, cleanup.AdvanceOffset, s_requestInterval);

            // The grid point's sweep concludes
            clock.Now = point;
            Assert.True(cleanup.IsDue(JobKind.Orphan, 0, clock.Now));
            Assert.Equal(OutcomeKind.Completed, (await OrphanJobAsync(cleanup, kit)).Kind);

            // The clock steps back before it, where a requested sweep concludes
            clock.Now = point - TimeSpan.FromMinutes(1);
            Assert.Equal(OrphanSweepRequestResult.Scheduled, cleanup.RequestOrphanSweep());
            Assert.Equal(OutcomeKind.Completed, (await OrphanJobAsync(cleanup, kit)).Kind);

            // Past the concluded grid point again, only the next one is due
            clock.Now = point + TimeSpan.FromMinutes(1);
            Assert.False(cleanup.IsDue(JobKind.Orphan, 0, clock.Now));
            Assert.True(cleanup.IsDue(JobKind.Orphan, 0, point + s_requestInterval));
        }

        [Fact]
        public async Task PassedDeadlineLeavesTheSweepDue()
        {
            using var kit = new AdvanceExecutionKit();
            var rig = await RequestRigAsync(kit, s_requestInterval);
            try
            {
                await QuiesceRequestRigAsync(rig);
                // A sweep that returns at its long deadline without having stopped at it
                rig.Cleanup.OrphanForTests.CoreForTests = context =>
                {
                    rig.Clock.Advance(context.Deadline - rig.Clock.GetUtcNow());
                    return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
                };
                Assert.Equal(OrphanSweepRequestResult.Scheduled, rig.Request());
                rig.Budget.TickNow();
                await rig.Worker.CurrentJob.WaitAsync(s_requestWait);

                Assert.Equal(1, rig.Count(JobKind.Orphan));
                Assert.Equal((OutcomeKind.Declined, MaintenanceReason.JobDeadline), (rig.Worker.LastOutcomeForTests!.Kind, rig.Worker.LastOutcomeForTests.Reason));
                // The request stays, and its retry waits three times the attempt
                Assert.True(rig.OrphanDue());
                Assert.Equal(OrphanSweepRequestResult.AlreadyPending, rig.Request());
                Assert.True(rig.Worker.CleanupRetryAt(JobKind.Orphan) - rig.Clock.GetUtcNow() >= TimeSpan.FromHours(18));
            }
            finally
            {
                await StopRequestRigAsync(rig);
            }

            // A grid point's sweep that passes its deadline leaves the point due
            var clock = new FakeTimeProvider(AdvanceExecutionKit.Now);
            var budget = RequestBudget(kit, clock);
            using var raw = await RequestTableAsync(kit);
            var cleanup = RequestCleanup(budget, CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, s_requestInterval)), raw, "t");
            cleanup.OrphanForTests.CoreForTests = context =>
            {
                clock.Advance(context.Deadline - clock.GetUtcNow());
                return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
            };
            clock.Advance(NextGridPoint(clock.GetUtcNow(), cleanup.AdvanceOffset, s_requestInterval) - clock.GetUtcNow());
            Assert.True(cleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
            var deadline = DeadlineContext(clock);
            Assert.Equal(OutcomeKind.Completed, (await OrphanJobAsync(cleanup, deadline)).Kind);
            Assert.Equal(IssuanceStop.Deadline, deadline.Stopped);
            Assert.True(cleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
        }

        [Fact]
        public async Task OrphanSweepWaitsForConfirmedCoverage()
        {
            using var kit = new AdvanceExecutionKit();
            var clock = new FakeTimeProvider(AdvanceExecutionKit.Now);
            var budget = RequestBudget(kit, clock);
            var cleanup = new DeltaMaintenanceSchedulingTests.FakeCleanup() { Work = kind => kind == JobKind.Orphan };
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./orphan-coverage-{Guid.NewGuid():N}"));
            var settings = new MaintenanceWorkerSettings(100, null, clock) { InspectionEnabled = true, CleanupEligible = true, Retention = AdvanceExecutionKit.Retention };
            var worker = new DeltaMaintenanceWorker(budget, settings, storage, "t", "t", null) { Cleanup = cleanup };
            budget.Register(worker);
            async Task TickAsync()
            {
                budget.TickNow();
                await worker.CurrentJob.WaitAsync(s_requestWait);
            }
            try
            {
                // A legacy lineage that waits for an anchor, so no coverage is confirmed
                await DeltaMaintenanceWorkerTests.Commits(storage, 0, 6);
                await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 4);
                worker.Published(6);
                for (int i = 0; i < 4; i++)
                {
                    await TickAsync();
                }
                Assert.True(worker.RepairPending);
                Assert.Equal(1, cleanup.Count(JobKind.StagedSweep));
                Assert.Equal(0, cleanup.Count(JobKind.Orphan));

                // Once the anchor's repair is confirmed the orphan sweep runs
                await DeltaTestKit.WriteOwnCommit(storage, "t", 7, 7, AdvanceExecutionKit.Now.ToUnixTimeMilliseconds() + 7, DeltaLakeCheckpointBuildTests.Add("f7.parquet"));
                worker.AnchorStaged(worker.AnchorRequest, 7);
                worker.Published(7);
                for (int i = 0; i < 3; i++)
                {
                    await TickAsync();
                }
                Assert.True(worker.CoverageConfirmed);
                Assert.Equal(0, cleanup.Count(JobKind.Orphan));
                await TickAsync();
                Assert.Equal(1, cleanup.Count(JobKind.Orphan));
            }
            finally
            {
                await worker.StopAsync(s_requestWait);
                budget.ScratchIfCreated?.ReleaseLockForTests();
            }
        }
    }
}

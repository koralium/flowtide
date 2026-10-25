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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaMaintenanceSchedulingTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 12, 0, 0, 0, TimeSpan.Zero);

        // Bounded, so a job that never settles fails the test instead of hanging it
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "sch-" + ScratchOwners.ShortId());

        public void Dispose()
        {
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        // Scripted non-build work: which kinds have work, what a job does, and the order calls came in
        internal sealed class FakeCleanup : IMaintenanceCleanup
        {
            public ConcurrentQueue<string> Calls { get; } = new ConcurrentQueue<string>();

            public Func<JobKind, bool> Work { get; set; } = _ => false;

            public Func<JobKind, CleanupContext, Task<CleanupResult>> Run { get; set; } = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.Completed));

            public int? DrainMax;

            // Hints each drain deletes or drops, unless Drain says otherwise
            public int Drained;

            public Func<CleanupContext, DrainResult>? Drain { get; set; }

            public bool HasWork(JobKind kind) => Work(kind);

            public Task<DrainResult> DrainHintsAsync(CleanupContext context, int max)
            {
                DrainMax = max;
                Calls.Enqueue("drain");
                return Task.FromResult(Drain?.Invoke(context) ?? new DrainResult(Drained));
            }

            public Task<CleanupResult> RunAsync(JobKind kind, CleanupContext context)
            {
                Calls.Enqueue(kind.ToString());
                return Run(kind, context);
            }

            public int Count(JobKind kind) => Calls.Count(x => x == kind.ToString());
        }

        private sealed class Rig
        {
            public required FakeTimeProvider BudgetClock { get; init; }

            public required FakeTimeProvider OptionsClock { get; init; }

            public required DeltaMaintenanceBudget Budget { get; init; }

            public required HookFileStorage Storage { get; init; }

            public required DeltaMaintenanceWorker Worker { get; init; }

            public async Task TickAsync()
            {
                Budget.TickNow();
                await Worker.CurrentJob.WaitAsync(s_wait);
            }

            public void Advance(TimeSpan by)
            {
                BudgetClock.Advance(by);
                OptionsClock.Advance(by);
            }

            // Both clocks to an absolute minute after the start
            public void At(int minute)
            {
                var target = s_now + TimeSpan.FromMinutes(minute);
                BudgetClock.Advance(target - BudgetClock.GetUtcNow());
                OptionsClock.Advance(target - OptionsClock.GetUtcNow());
            }
        }

        private DeltaMaintenanceBudget NewBudget(int jobs = 2, long requestsPerSecond = MaintenanceBuckets.DefaultRequestsPerSecond)
        {
            return new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(jobs, Path.Combine(_directory, ScratchOwners.ShortId())) { RequestsPerSecond = requestsPerSecond }, new FakeTimeProvider(s_now), manualTick: true);
        }

        private Rig Create(int interval = 2, TimeSpan? maxAge = null, int jobs = 2, IMaintenanceCleanup? cleanup = null, long? shortReads = null,
            DeltaMaintenanceBudget? budget = null, HookFileStorage? storage = null, bool inspection = true, bool rebuildLegacy = false, int? readAhead = null,
            long requestsPerSecond = MaintenanceBuckets.DefaultRequestsPerSecond)
        {
            storage ??= new HookFileStorage(Files.Of.InternalMemory($"./scheduling-{Guid.NewGuid():N}"));
            budget ??= NewBudget(jobs, requestsPerSecond);
            var options = new FakeTimeProvider(budget.Clock.GetUtcNow());
            var settings = new MaintenanceWorkerSettings(interval, maxAge, options)
            {
                PeriodicEnabled = inspection,
                InspectionEnabled = inspection,
                RebuildLegacyCheckpoints = rebuildLegacy,
                ReadAhead = readAhead
            };
            if (shortReads is { } reads)
            {
                settings = settings with { ShortReads = reads };
            }
            var worker = new DeltaMaintenanceWorker(budget, settings, storage, "t", "t", null) { Cleanup = cleanup };
            budget.Register(worker);
            return new Rig()
            {
                BudgetClock = (FakeTimeProvider)budget.Clock,
                OptionsClock = options,
                Budget = budget,
                Storage = storage,
                Worker = worker
            };
        }

        // Ticks until a tick dispatches nothing
        private static async Task Quiesce(Rig rig)
        {
            for (int i = 0; i < 20; i++)
            {
                var before = rig.Worker.CurrentJob;
                rig.Budget.TickNow();
                var after = rig.Worker.CurrentJob;
                await after.WaitAsync(s_wait);
                if (ReferenceEquals(before, after))
                {
                    return;
                }
            }
            throw new InvalidOperationException("The worker kept dispatching");
        }

        private static Task<List<long>> Checkpoints(Rig rig) => DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage);

        private static TaskCompletionSource Signal() => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Awaits the task and signals once the continuation is registered on it
        private readonly struct Parked<T> : ICriticalNotifyCompletion
        {
            private readonly Task<T> _task;
            private readonly Action _parked;

            public Parked(Task<T> task, Action parked)
            {
                _task = task;
                _parked = parked;
            }

            public Parked<T> GetAwaiter() => this;

            public bool IsCompleted => false;

            public void OnCompleted(Action continuation)
            {
                _task.GetAwaiter().OnCompleted(continuation);
                _parked();
            }

            public void UnsafeOnCompleted(Action continuation)
            {
                _task.GetAwaiter().UnsafeOnCompleted(continuation);
                _parked();
            }

            public T GetResult() => _task.GetAwaiter().GetResult();
        }

        private static int Waiting(DeltaMaintenanceBudget budget, string wait) => wait switch
        {
            "lease" => budget.Leases.Waiting,
            "long" => budget.LongGate.QueueLength,
            "main" => budget.Gate.QueueLength,
            _ => budget.DeleteGate.QueueLength
        };

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AdvanceContextCarriesWhatTheWorkersBuildsUse(bool rebuildLegacy)
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 2, cleanup: cleanup, rebuildLegacy: rebuildLegacy);
            // The build at 6 fails on the sidecar root, which the worker then records as unsupported; without commit 0 the option keeps that root
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 1, 6);
            await DeltaMaintenanceWorkerTests.SidecarCheckpoint(rig.Storage, 3);
            rig.Worker.Published(6);
            await Quiesce(rig);
            Assert.Single(rig.Worker.UnsupportedRoots);

            var contexts = new ConcurrentQueue<CleanupContext>();
            cleanup.Work = x => x == JobKind.Advance && contexts.IsEmpty;
            cleanup.Run = (_, context) =>
            {
                contexts.Enqueue(context);
                return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
            };
            await Quiesce(rig);

            var advance = Assert.Single(contexts);
            Assert.Equal(2, advance.CheckpointInterval);
            Assert.Equal(rebuildLegacy, advance.RebuildLegacyCheckpoints);
            Assert.True(advance.KnownUnsupported.SetEquals(rig.Worker.UnsupportedRoots));
            Assert.Same(rig.Budget.Capacity, advance.Capacity);
        }

        [Fact]
        public async Task StopPausesTheTablesRunnableCapacityAfterItsJob()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            await Quiesce(rig);
            var key = TableLeases.KeyOf(rig.Storage, "t");
            var other = rig.Budget.Capacity.Restore(default, TableLeases.KeyOf(rig.Storage, "u"), "advance-b", new CapacityDemand(10, 0), CapacityState.Runnable);

            // The advance restores its demand only once the stop is waiting to join it
            var running = Signal();
            var release = Signal();
            CapacityLease? restored = null;
            cleanup.Work = kind => kind == JobKind.Advance && restored == null;
            cleanup.Run = async (_, context) =>
            {
                running.TrySetResult();
                await release.Task;
                restored = rig.Budget.Capacity.Restore(context.CapacityOwner, key, "advance-a", new CapacityDemand(100, 0), CapacityState.Runnable);
                return new CleanupResult(OutcomeKind.Completed);
            };
            rig.Budget.TickNow();
            await running.Task.WaitAsync(s_wait);

            var stopping = rig.Worker.StopAsync(s_wait);
            release.SetResult();
            await stopping.WaitAsync(s_wait);
            Assert.Equal(CapacityState.Paused, restored!.State);
            Assert.Equal(CapacityState.Runnable, other.State);
            Assert.Equal(new CapacityDemand(10, 0), rig.Budget.Capacity.Total(CapacityState.Runnable));
            Assert.Equal(new CapacityDemand(100, 0), rig.Budget.Capacity.Total(CapacityState.Paused));
        }

        [Fact]
        public async Task StoppingOneStoreDoesNotPauseAnotherStoresLiveDemand()
        {
            // Two stores holding a table at the same path share its table lease key
            var budget = NewBudget();
            var first = Create(interval: 100, budget: budget);
            var cleanup = new FakeCleanup();
            var second = Create(interval: 100, budget: budget, cleanup: cleanup);
            var key = TableLeases.KeyOf(second.Storage, "t");
            Assert.Equal(TableLeases.KeyOf(first.Storage, "t"), key);
            await DeltaMaintenanceWorkerTests.Commits(second.Storage, 0, 3);
            second.Worker.Published(3);
            await Quiesce(second);

            // The second store's advance restores thirteen hours of runnable demand
            var demand = new CapacityDemand(13 * 3600 * MaintenanceBuckets.DefaultRequestsPerSecond, 0);
            CapacityLease? restored = null;
            cleanup.Work = kind => kind == JobKind.Advance && restored == null;
            cleanup.Run = (_, context) =>
            {
                restored = budget.Capacity.Restore(context.CapacityOwner, key, "advance-a", demand, CapacityState.Runnable);
                return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
            };
            await Quiesce(second);
            Assert.NotNull(restored);

            await first.Worker.StopAsync(s_wait);
            Assert.Equal(CapacityState.Runnable, restored!.State);
            Assert.Equal(demand, budget.Capacity.Current);

            // Its own worker's stop pauses it
            await second.Worker.StopAsync(s_wait);
            Assert.Equal(CapacityState.Paused, restored.State);
            Assert.Equal(default, budget.Capacity.Current);
        }

        [Theory]
        [InlineData("lease")]
        [InlineData("long")]
        [InlineData("main")]
        [InlineData("delete")]
        public async Task DueBuildPreemptsQueuedNonBuildAcrossAllWaitStates(string wait)
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 2, jobs: 1, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 6);
            rig.Worker.Published(4);
            // The inspection builds 4, then the first staged sweep runs
            await Quiesce(rig);
            Assert.Equal(new long[] { 4 }, await Checkpoints(rig));
            Assert.Equal(1, cleanup.Count(JobKind.StagedSweep));

            var kind = wait == "delete" ? JobKind.Deletion : JobKind.Advance;
            var done = false;
            var limits = new ConcurrentQueue<long>();
            cleanup.Work = x => x == kind && !done;
            cleanup.Run = (_, context) =>
            {
                limits.Enqueue(context.ReadLimit);
                // Too many reads for short admission, so the job waits at the long gate
                if (wait == "long" && !context.Long)
                {
                    return Task.FromResult(CleanupResult.Promote);
                }
                done = true;
                return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
            };
            var blockers = new List<IDisposable>();
            switch (wait)
            {
                case "lease":
                    blockers.Add(await rig.Budget.Leases.AcquireAsync(TableLeases.KeyOf(rig.Storage, "t")).WaitAsync(s_wait));
                    break;
                case "long":
                    blockers.Add(await rig.Budget.LongGate.AcquireAsync().WaitAsync(s_wait));
                    break;
                case "main":
                    blockers.Add(await rig.Budget.Gate.AcquireAsync().WaitAsync(s_wait));
                    break;
                default:
                    blockers.Add(await rig.Budget.DeleteGate.AcquireAsync().WaitAsync(s_wait));
                    blockers.Add(await rig.Budget.DeleteGate.AcquireAsync().WaitAsync(s_wait));
                    break;
            }
            rig.Budget.TickNow();
            var queued = rig.Worker.CurrentJob;
            await DeltaMaintenanceWorkerTests.Queued(() => Waiting(rig.Budget, wait) == 1);

            // The due build cancels the wait; the job never ran and nothing backs off
            rig.Worker.Published(6);
            rig.Budget.TickNow();
            await queued.WaitAsync(s_wait);
            Assert.Equal(OutcomeKind.Preempted, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(0, Waiting(rig.Budget, wait));
            Assert.Equal(wait == "long" ? 1 : 0, cleanup.Count(kind));
            Assert.False(done);
            Assert.Equal(0, rig.Worker.CleanupFailures(kind));
            Assert.Equal(DateTimeOffset.MinValue, rig.Worker.CleanupRetryAt(kind));

            // The build runs first, then the preempted work rejoins
            blockers.ForEach(x => x.Dispose());
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));
            await rig.TickAsync();
            Assert.True(done);
            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            // A short admission allows 20,000 counted reads, a long one any number
            Assert.Equal(wait == "long" ? new[] { 20_000L, 20_000L, long.MaxValue } : new[] { 20_000L }, limits);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task PreemptAfterGrantReleasesResources(bool deletion)
        {
            var kind = deletion ? JobKind.Deletion : JobKind.Advance;
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 2, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 6);
            rig.Worker.Published(4);
            await Quiesce(rig);

            var granted = Signal();
            var release = Signal();
            rig.Worker.HooksForTests = new WorkerHooks()
            {
                Hold = point =>
                {
                    if (point != "granted")
                    {
                        return Task.CompletedTask;
                    }
                    granted.TrySetResult();
                    return release.Task;
                }
            };
            cleanup.Work = x => x == kind;
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await granted.Task.WaitAsync(s_wait);
            Assert.Equal(1, kind == JobKind.Deletion ? rig.Budget.DeleteGate.Running : rig.Budget.Gate.Running);

            // The build falls due between the grant and the start
            rig.Worker.Published(6);
            rig.Budget.TickNow();
            release.SetResult();
            await job.WaitAsync(s_wait);
            Assert.Equal(OutcomeKind.Preempted, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(0, cleanup.Count(kind));
            Assert.DoesNotContain("drain", cleanup.Calls);
            Assert.Equal(0, rig.Budget.Gate.Running);
            Assert.Equal(0, rig.Budget.DeleteGate.Running);
            Assert.Equal(0, rig.Budget.Leases.Count);
            Assert.Equal(0, rig.Worker.CleanupFailures(kind));

            rig.Worker.HooksForTests = null;
            cleanup.Work = _ => false;
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));
        }

        [Fact]
        public async Task DemandBuildNeverCancelsItself()
        {
            var rig = Create(interval: 100, maxAge: TimeSpan.FromHours(1), jobs: 1);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 9);
            rig.Worker.Published(9);
            await rig.TickAsync();
            Assert.True(rig.Worker.Inspected);
            Assert.Empty(await Checkpoints(rig));

            var slot = await rig.Budget.Gate.AcquireAsync().WaitAsync(s_wait);
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(5, RootPolicy.Auto)));
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await DeltaMaintenanceWorkerTests.Queued(() => rig.Budget.Gate.QueueLength == 1);

            // Builds are due by age and by the demand while the demand waits, and none cancels it
            rig.Advance(TimeSpan.FromHours(2));
            rig.Budget.TickNow();
            rig.Budget.TickNow();
            Assert.Same(job, rig.Worker.CurrentJob);
            Assert.Equal(1, rig.Budget.Gate.QueueLength);

            // Nor while it builds
            var building = Signal();
            var resume = Signal();
            rig.Worker.HooksForTests = new WorkerHooks()
            {
                Hold = point =>
                {
                    if (point != "build")
                    {
                        return Task.CompletedTask;
                    }
                    building.TrySetResult();
                    return resume.Task;
                }
            };
            slot.Dispose();
            await building.Task.WaitAsync(s_wait);
            rig.Budget.TickNow();
            rig.Budget.TickNow();
            resume.SetResult();
            await job.WaitAsync(s_wait);
            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(new long[] { 5 }, await Checkpoints(rig));
            Assert.Null(rig.Worker.PendingDemand);
        }

        [Fact]
        public async Task NonBuildFailureDoesNotDelayBuilds()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 2, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 8);
            rig.Worker.Published(4);
            await Quiesce(rig);

            cleanup.Work = x => x == JobKind.Advance;
            cleanup.Run = (_, _) => throw new IOException("cleanup fault");
            await rig.TickAsync();
            Assert.Equal(OutcomeKind.Failed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(1, rig.Worker.CleanupFailures(JobKind.Advance));
            Assert.True(rig.Worker.CleanupRetryAt(JobKind.Advance) > rig.OptionsClock.GetUtcNow());
            // The build state is untouched
            Assert.Equal(0, rig.Worker.ConsecutiveFailures);
            Assert.Equal(DateTimeOffset.MinValue, rig.Worker.RetryAt);
            Assert.Null(rig.Worker.OldestOrigin);
            Assert.False(rig.Worker.Skipped);

            // A build due right after it runs at once, while the failed kind waits for its own retry
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));
            await rig.TickAsync();
            Assert.Equal(1, cleanup.Count(JobKind.Advance));
            rig.OptionsClock.Advance(rig.Worker.CleanupRetryAt(JobKind.Advance) - rig.OptionsClock.GetUtcNow());
            await rig.TickAsync();
            Assert.Equal(2, cleanup.Count(JobKind.Advance));
        }

        [Fact]
        public async Task StagedSweepRunsBetweenDeleteSlices()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            await Quiesce(rig);
            Assert.Equal(new[] { "StagedSweep" }, cleanup.Calls);

            var slices = 0;
            var firstSlice = Signal();
            var resume = Signal();
            cleanup.Work = x => x == JobKind.Deletion && Volatile.Read(ref slices) < 3;
            cleanup.Run = async (kind, _) =>
            {
                if (kind == JobKind.Deletion && Interlocked.Increment(ref slices) == 1)
                {
                    firstSlice.TrySetResult();
                    await resume.Task;
                }
                return new CleanupResult(OutcomeKind.Completed);
            };
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await firstSlice.Task.WaitAsync(s_wait);
            // The sweep falls due during the first slice
            rig.Advance(TimeSpan.FromDays(1));
            resume.SetResult();
            await job.WaitAsync(s_wait);
            await Quiesce(rig);

            Assert.Equal(new[] { "StagedSweep", "drain", "Deletion", "StagedSweep", "drain", "Deletion", "drain", "Deletion" }, cleanup.Calls);
            Assert.Equal(128, cleanup.DrainMax);
        }

        [Fact]
        public async Task StagedSweepRunsOnFirstHandOffThenDaily()
        {
            var cleanup = new FakeCleanup();
            var budget = NewBudget();
            var rig = Create(interval: 100, cleanup: cleanup, budget: budget);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);

            // Nothing before the first hand-off
            await rig.TickAsync();
            Assert.Null(rig.Worker.SweepDueAt);
            Assert.Empty(cleanup.Calls);

            // The mandatory inspection first, then the sweep
            rig.Worker.Published(3);
            await rig.TickAsync();
            Assert.True(rig.Worker.Inspected);
            Assert.Equal(0, cleanup.Count(JobKind.StagedSweep));
            await rig.TickAsync();
            Assert.Equal(1, cleanup.Count(JobKind.StagedSweep));

            // Then a day after it completed
            rig.Advance(TimeSpan.FromDays(1) - TimeSpan.FromTicks(1));
            await rig.TickAsync();
            Assert.Equal(1, cleanup.Count(JobKind.StagedSweep));
            rig.Advance(TimeSpan.FromTicks(1));
            await rig.TickAsync();
            Assert.Equal(2, cleanup.Count(JobKind.StagedSweep));

            // The next sink run sweeps at its own first hand-off
            await rig.Worker.StopAsync(s_wait);
            var next = Create(interval: 100, cleanup: cleanup, budget: budget, storage: rig.Storage);
            next.Worker.Published(3);
            await Quiesce(next);
            Assert.Equal(3, cleanup.Count(JobKind.StagedSweep));

            // Without checkpoint inspection the sweep is the first job
            var bareCleanup = new FakeCleanup();
            var bare = Create(interval: 0, cleanup: bareCleanup, inspection: false);
            bare.Worker.Published(0);
            await bare.TickAsync();
            Assert.Equal(new[] { "StagedSweep" }, bareCleanup.Calls);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CutoffTargetSurvivesAdmission(bool rootless)
        {
            var policy = rootless ? RootPolicy.None : RootPolicy.Auto;
            var rig = Create(interval: 100, jobs: 1);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 12);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 3);
            rig.Worker.Published(9);
            await rig.TickAsync();
            Assert.Equal(3, rig.Worker.LastCompleted);

            var slot = await rig.Budget.Gate.AcquireAsync().WaitAsync(s_wait);
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(5, policy)));
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await DeltaMaintenanceWorkerTests.Queued(() => rig.Budget.Gate.QueueLength == 1);
            // Published while the demand waits, so a latest-head admission would build 12
            rig.Worker.Published(12);
            rig.Budget.TickNow();
            slot.Dispose();
            await job.WaitAsync(s_wait);

            Assert.Equal(new long[] { 3, 5 }, await Checkpoints(rig));
            Assert.Equal(5, rig.Worker.LastCompleted);
            Assert.Equal(policy == RootPolicy.Auto ? 3L : null, rig.Worker.LastBuild!.RootVersion);
            Assert.Null(rig.Worker.PendingDemand);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task PromotedBuildKeepsTarget(bool demand)
        {
            // Four commit reads fit a short admission here
            var rig = Create(interval: demand ? 100 : 2, shortReads: 4);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 12);
            if (demand)
            {
                await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 6);
                rig.Worker.Published(9);
                await rig.TickAsync();
                Assert.Equal(6, rig.Worker.LastCompleted);
            }
            var grants = 0;
            rig.Worker.HooksForTests = new WorkerHooks() { Hold = point => { if (point == "granted") Interlocked.Increment(ref grants); return Task.CompletedTask; } };
            var longSlot = await rig.Budget.LongGate.AcquireAsync().WaitAsync(s_wait);
            if (demand)
            {
                // Without a root the replay through 8 reads nine commits; from root 6 it would read two
                Assert.True(rig.Worker.RequestBuild(new BuildDemand(8, RootPolicy.None)));
            }
            else
            {
                // A first build through 9 reads ten commits
                rig.Worker.Published(9);
            }
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await DeltaMaintenanceWorkerTests.Queued(() => rig.Budget.LongGate.QueueLength == 1);
            rig.Worker.Published(12);
            rig.Budget.TickNow();
            longSlot.Dispose();
            await job.WaitAsync(s_wait);

            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(2, grants);
            Assert.Equal(demand ? new long[] { 6, 8 } : new long[] { 9 }, await Checkpoints(rig));
            Assert.Equal(demand ? 8 : 9, rig.Worker.LastCompleted);
            Assert.Null(rig.Worker.LastBuild!.RootVersion);
        }

        [Fact]
        public async Task HistoricalBuildKeepsAgeDating()
        {
            var rig = Create(interval: 100, maxAge: TimeSpan.FromHours(1));
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 40);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 30);
            rig.Worker.Published(30);
            await rig.TickAsync();
            Assert.Equal(30, rig.Worker.LastCompleted);
            Assert.Null(rig.Worker.OldestOrigin);

            rig.At(10);
            rig.Worker.Published(40);
            await rig.TickAsync();
            Assert.Equal(s_now + TimeSpan.FromMinutes(10), rig.Worker.OldestOrigin);

            // A historical demand completes below the frontier
            rig.At(20);
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(20, RootPolicy.Auto)));
            await rig.TickAsync();
            Assert.Equal(new long[] { 20, 30 }, await Checkpoints(rig));
            Assert.Equal(30, rig.Worker.LastCompleted);
            Assert.Equal(s_now + TimeSpan.FromMinutes(10), rig.Worker.OldestOrigin);

            // 31 to 40 were first seen at minute 10, so the age build fires at minute 70
            rig.At(69);
            await rig.TickAsync();
            Assert.Equal(new long[] { 20, 30 }, await Checkpoints(rig));
            rig.At(70);
            await rig.TickAsync();
            Assert.Equal(new long[] { 20, 30, 40 }, await Checkpoints(rig));
        }

        [Fact]
        public async Task HistoricalDemandKeepsDatingOfVersionsSeenWhileItBuilds()
        {
            var rig = Create(interval: 100, maxAge: TimeSpan.FromHours(1));
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 31);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 30);
            rig.Worker.Published(30);
            await rig.TickAsync();
            Assert.Equal(30, rig.Worker.LastCompleted);
            Assert.Null(rig.Worker.OldestOrigin);

            var building = Signal();
            var resume = Signal();
            rig.Worker.HooksForTests = new WorkerHooks()
            {
                Hold = point =>
                {
                    if (point != "build")
                    {
                        return Task.CompletedTask;
                    }
                    building.TrySetResult();
                    return resume.Task;
                }
            };
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(20, RootPolicy.Auto)));
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await building.Task.WaitAsync(s_wait);

            // 31 is first seen at minute 1, while the historical demand builds
            rig.At(1);
            rig.Worker.Published(31);
            rig.Budget.TickNow();
            rig.At(50);
            resume.SetResult();
            await job.WaitAsync(s_wait);
            rig.Worker.HooksForTests = null;
            Assert.Equal(new long[] { 20, 30 }, await Checkpoints(rig));

            // So the age build is due an hour after minute 1, not an hour after the demand settled
            rig.At(60);
            await rig.TickAsync();
            Assert.Equal(new long[] { 20, 30 }, await Checkpoints(rig));
            rig.At(61);
            await rig.TickAsync();
            Assert.Equal(new long[] { 20, 30, 31 }, await Checkpoints(rig));
        }

        [Theory]
        [InlineData(19_999L, false)]
        [InlineData(20_000L, true)]
        public async Task Replay19999StaysShortAnd20000Promotes(long target, bool promoted)
        {
            var rig = Create(interval: 2);
            // A listing needs only the names; the first commit read ends the job
            var inner = rig.Storage.Inner;
            for (long version = 0; version <= target; version++)
            {
                await inner.WriteText($"/t/_delta_log/{version:D20}.json", "{}\n");
            }
            var reads = new ConcurrentQueue<bool>();
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(".json"))
                {
                    reads.Enqueue(rig.Budget.LongGate.Running == 1);
                    throw new IOException("first commit read");
                }
                return Task.CompletedTask;
            };
            var grants = 0;
            rig.Worker.HooksForTests = new WorkerHooks() { Hold = point => { if (point == "granted") Interlocked.Increment(ref grants); return Task.CompletedTask; } };
            rig.Worker.Published(target);
            await rig.TickAsync();

            // A replay without a root through T reads T + 1 commits, and only more than 20,000 is promoted before any is opened
            Assert.Equal(promoted, Assert.Single(reads));
            Assert.Equal(promoted ? 2 : 1, grants);
            Assert.Equal(MaintenanceReason.Transient, rig.Worker.LastOutcomeForTests!.Reason);
        }

        [Fact]
        public async Task ShortPlanningDeadlineStopsIssuanceAndSettles()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            await Quiesce(rig);

            var first = Signal();
            var second = Signal();
            var issued = Signal();
            var almost = Signal();
            var parked = Signal();
            var go = Signal();
            var past = Signal();
            // Completes its awaiter inline
            var finish = new TaskCompletionSource<CleanupResult>();
            var tokens = new ConcurrentQueue<CancellationToken>();
            Task? third = null;
            Task? fourth = null;
            cleanup.Work = x => x == JobKind.Advance && !parked.Task.IsCompleted;
            cleanup.Run = async (kind, context) =>
            {
                _ = context.TryIssue(token => { tokens.Enqueue(token); return first.Task; });
                _ = context.TryIssue(token => { tokens.Enqueue(token); return second.Task; });
                issued.SetResult();
                await go.Task;
                third = context.TryIssue(_ => Task.CompletedTask);
                almost.SetResult();
                await past.Task;
                fourth = context.TryIssue(_ => Task.CompletedTask);
                // Returns without awaiting what it issued, once the test completes it
                return await new Parked<CleanupResult>(finish.Task, () => parked.TrySetResult());
            };
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await issued.Task.WaitAsync(s_wait);

            // Issuance stays open until the short deadline, fifteen minutes after admission
            rig.Advance(TimeSpan.FromMinutes(15) - TimeSpan.FromTicks(1));
            go.SetResult();
            await almost.Task.WaitAsync(s_wait);
            Assert.NotNull(third);
            rig.Advance(TimeSpan.FromTicks(1));
            past.SetResult();
            await parked.Task.WaitAsync(s_wait);
            Assert.Null(fourth);

            // The job's return runs inline on a thread without a context, so an admission released before settling would be gone when this returns
            await Task.Run(() => finish.SetResult(new CleanupResult(OutcomeKind.Completed)));
            // The admission is held until the submitted operations settle, and the deadline cancelled none of them
            Assert.False(job.IsCompleted);
            Assert.Equal(1, rig.Budget.Gate.Running);
            Assert.Equal(1, rig.Budget.Leases.Count);
            Assert.Equal(2, tokens.Count);
            Assert.All(tokens, x => Assert.False(x.IsCancellationRequested));
            first.SetResult();
            second.SetResult();
            await job.WaitAsync(s_wait);
            Assert.Equal(OutcomeKind.Declined, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(MaintenanceReason.JobDeadline, rig.Worker.LastOutcomeForTests.Reason);
            Assert.Equal(0, rig.Budget.Gate.Running);
            Assert.Equal(0, rig.Budget.Leases.Count);
            Assert.Equal(1, rig.Worker.CleanupFailures(JobKind.Advance));
            Assert.Equal(0, rig.Worker.ConsecutiveFailures);
        }

        // Where six hours pass: before the root is fetched, during the replay at commit 5, or at the last commit read before publication
        [Theory]
        [InlineData("short")]
        [InlineData("start")]
        [InlineData("replay")]
        [InlineData("publish")]
        public async Task LongBuildStopsIssuingAtItsDeadline(string at)
        {
            // Two reads promote the build from root 3 through 9; the default keeps it short, where its reads bound it instead of a deadline
            var promoted = at != "short";
            var rig = Create(interval: 2, shortReads: promoted ? 2 : null);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 9);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 3);
            // The root and commit reads under long admission
            var reads = new ConcurrentQueue<string>();
            rig.Storage.Before = (verb, path) =>
            {
                if (verb != "OpenRead" || rig.Budget.LongGate.Running == 0)
                {
                    return Task.CompletedTask;
                }
                if (path.Full.EndsWith(".checkpoint.parquet"))
                {
                    reads.Enqueue("root");
                }
                else if (path.Full.EndsWith(".json") && long.TryParse(path.Name.AsSpan(0, 20), out var version))
                {
                    reads.Enqueue(version.ToString());
                    if ((at == "replay" && version == 5) || (at == "publish" && version == 9))
                    {
                        rig.Advance(TimeSpan.FromHours(6));
                    }
                }
                return Task.CompletedTask;
            };
            var builds = 0;
            rig.Worker.HooksForTests = new WorkerHooks()
            {
                Hold = point =>
                {
                    // The attempt that builds, after a promotion the second
                    if (point == "build" && Interlocked.Increment(ref builds) == (promoted ? 2 : 1) && at is "short" or "start")
                    {
                        rig.Advance(TimeSpan.FromHours(6));
                    }
                    return Task.CompletedTask;
                }
            };
            rig.Worker.Published(9);
            await rig.TickAsync();

            if (!promoted)
            {
                Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
                Assert.Equal(new long[] { 3, 9 }, await Checkpoints(rig));
                return;
            }
            Assert.Equal(OutcomeKind.Declined, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(MaintenanceReason.JobDeadline, rig.Worker.LastOutcomeForTests.Reason);
            Assert.Equal(new long[] { 3 }, await Checkpoints(rig));
            // Nothing is read after the deadline, and nothing is published
            var expected = at switch
            {
                "start" => Array.Empty<string>(),
                "replay" => new[] { "root", "4", "5" },
                _ => new[] { "root", "4", "5", "6", "7", "8", "9" }
            };
            Assert.Equal(expected, reads);
        }

        [Fact]
        public async Task DemandWaitDatesVersionsFromAdmission()
        {
            var rig = Create(interval: 100, maxAge: TimeSpan.FromHours(1), jobs: 1);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 40);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 30);
            rig.Worker.Published(30);
            await rig.TickAsync();
            Assert.Null(rig.Worker.OldestOrigin);

            // Dispatched with nothing uncovered, then 40 is published while the demand waits at the gate
            var slot = await rig.Budget.Gate.AcquireAsync().WaitAsync(s_wait);
            rig.At(10);
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(20, RootPolicy.Auto)));
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await DeltaMaintenanceWorkerTests.Queued(() => rig.Budget.Gate.QueueLength == 1);
            rig.At(15);
            rig.Worker.Published(40);
            rig.Budget.TickNow();
            rig.At(20);
            slot.Dispose();
            await job.WaitAsync(s_wait);
            Assert.Equal(new long[] { 20, 30 }, await Checkpoints(rig));

            // Seen only while queued, 31 to 40 date from the admission at minute 20
            rig.At(79);
            await rig.TickAsync();
            Assert.Equal(new long[] { 20, 30 }, await Checkpoints(rig));
            rig.At(80);
            await rig.TickAsync();
            Assert.Equal(new long[] { 20, 30, 40 }, await Checkpoints(rig));
        }

        [Fact]
        public async Task CleanupDatesVersionsPublishedWhileItRuns()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, maxAge: TimeSpan.FromHours(1), jobs: 1, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            rig.Worker.Published(4);
            await Quiesce(rig);
            Assert.Null(rig.Worker.OldestOrigin);

            cleanup.Work = x => x == JobKind.Advance;
            var slot = await rig.Budget.Gate.AcquireAsync().WaitAsync(s_wait);
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await DeltaMaintenanceWorkerTests.Queued(() => rig.Budget.Gate.QueueLength == 1);

            // Seen at minute 0 while the cleanup waits, so the age build falls due at minute 60 and preempts it
            rig.Worker.Published(6);
            rig.Budget.TickNow();
            Assert.Equal(s_now, rig.Worker.OldestOrigin);
            rig.At(59);
            rig.Budget.TickNow();
            Assert.False(job.IsCompleted);
            rig.At(60);
            rig.Budget.TickNow();
            await job.WaitAsync(s_wait);
            Assert.Equal(OutcomeKind.Preempted, rig.Worker.LastOutcomeForTests!.Kind);

            slot.Dispose();
            cleanup.Work = _ => false;
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));
        }

        [Fact]
        public async Task PromotionPrecedesTheRootFetch()
        {
            // Even root 3 leaves nine commits to 12, more than the four a short admission allows
            var rig = Create(interval: 2, shortReads: 4);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 12);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 3);
            rig.Worker.Published(3);
            await rig.TickAsync();
            Assert.Equal(3, rig.Worker.LastCompleted);

            var rootReads = new ConcurrentQueue<bool>();
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith(".checkpoint.parquet"))
                {
                    rootReads.Enqueue(rig.Budget.LongGate.Running == 1);
                }
                return Task.CompletedTask;
            };
            rig.Worker.Published(12);
            await rig.TickAsync();
            Assert.Equal(new long[] { 3, 12 }, await Checkpoints(rig));
            Assert.Equal(3, rig.Worker.LastBuild!.RootVersion);
            Assert.NotEmpty(rootReads);
            Assert.All(rootReads, Assert.True);
        }

        [Fact]
        public async Task PromotionCountsTheRootActuallyUsed()
        {
            // Root 8 would leave two reads, but the legacy root is dropped for a replay of all eleven, one more than ten
            var rig = Create(interval: 2, shortReads: 10, rebuildLegacy: true);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 10);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 8);
            rig.Worker.Published(8);
            await rig.TickAsync();
            Assert.Equal(8, rig.Worker.LastCompleted);

            var grants = 0;
            rig.Worker.HooksForTests = new WorkerHooks() { Hold = point => { if (point == "granted") Interlocked.Increment(ref grants); return Task.CompletedTask; } };
            rig.Worker.Published(10);
            await rig.TickAsync();
            Assert.Equal(2, grants);
            Assert.Equal(new long[] { 8, 10 }, await Checkpoints(rig));
            Assert.Null(rig.Worker.LastBuild!.RootVersion);
        }

        [Fact]
        public async Task LongBuildReadsAheadOnQualifiedStores()
        {
            var directory = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(directory);
            var storage = TrackedHookFileStorage.Over(Files.Of.LocalDisk(directory));
            // Thirty commit reads promote the first build past the five a short admission allows
            var rig = Create(interval: 2, shortReads: 5, storage: storage);
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 29);
            rig.Worker.Published(29);
            await rig.TickAsync();

            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(new long[] { 29 }, await Checkpoints(rig));
            Assert.Equal(20, rig.Worker.ReadAheadForTests);
            Assert.Equal(20, storage.MaxOutstanding);
        }

        // Guards short builds and stores not qualified for concurrent reads against the read-ahead
        [Fact]
        public async Task ShortBuildsAndUnqualifiedStoresReadOneAtATime()
        {
            // A user's wrapper over in-memory storage classifies as unknown
            var unknown = TrackedHookFileStorage.Over(new HookFileStorage(Files.Of.InternalMemory($"./scheduling-{Guid.NewGuid():N}")));
            var promoted = Create(interval: 2, shortReads: 5, storage: unknown);
            await DeltaMaintenanceWorkerTests.Commits(unknown, 0, 29);
            promoted.Worker.Published(29);
            await promoted.TickAsync();
            Assert.Equal(new long[] { 29 }, await Checkpoints(promoted));
            Assert.Equal(1, promoted.Worker.ReadAheadForTests);
            Assert.Equal(1, unknown.MaxOutstanding);

            // A short build on local disk
            var directory = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(directory);
            var local = TrackedHookFileStorage.Over(Files.Of.LocalDisk(directory));
            var rig = Create(interval: 2, storage: local);
            await DeltaMaintenanceWorkerTests.Commits(local, 0, 29);
            rig.Worker.Published(29);
            await rig.TickAsync();
            Assert.Equal(new long[] { 29 }, await Checkpoints(rig));
            Assert.Equal(1, local.MaxOutstanding);
        }

        [Fact]
        public void ReadAheadFollowsTheBudgetsRequestRate()
        {
            var budget = NewBudget(requestsPerSecond: 5000);
            var settings = new MaintenanceWorkerSettings(2, null, new FakeTimeProvider(s_now));
            var directory = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(directory);
            using var local = Files.Of.LocalDisk(directory);
            using var s3 = StoreFixtures.S3();

            Assert.Equal(500, new DeltaMaintenanceWorker(budget, settings, local, "t", "t", null).ReadAheadForTests);
            Assert.Equal(1, new DeltaMaintenanceWorker(budget, settings, s3, "t", "t", null).ReadAheadForTests);
            Assert.Equal(20, new DeltaMaintenanceWorker(NewBudget(), settings, local, "t", "t", null).ReadAheadForTests);
            // A depth the settings name is kept
            Assert.Equal(3, new DeltaMaintenanceWorker(budget, settings with { ReadAhead = 3 }, local, "t", "t", null).ReadAheadForTests);
        }

        [Fact]
        public async Task CleanupAndDemandsWaitForTheInspection()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 2, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 4);
            var ran = false;
            cleanup.Work = x => x == JobKind.Advance && !ran;
            cleanup.Run = (kind, _) =>
            {
                ran |= kind == JobKind.Advance;
                return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
            };
            var fail = true;
            rig.Storage.Before = (verb, _) => fail && verb == "Ls" ? throw new IOException("listing fault") : Task.CompletedTask;
            var grants = 0;
            rig.Worker.HooksForTests = new WorkerHooks() { Hold = point => { if (point == "granted") Interlocked.Increment(ref grants); return Task.CompletedTask; } };
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.False(rig.Worker.Inspected);
            Assert.Equal(1, grants);

            // While the inspection backs off, neither cleanup nor a demand runs
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(2, RootPolicy.Auto)));
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(1, grants);
            Assert.Empty(cleanup.Calls);
            Assert.Equal(0, rig.Worker.DemandFailures);

            fail = false;
            rig.OptionsClock.Advance(rig.Worker.RetryAt - rig.OptionsClock.GetUtcNow());
            await Quiesce(rig);
            Assert.True(rig.Worker.Inspected);
            Assert.Equal(new long[] { 2, 4 }, await Checkpoints(rig));
            Assert.Equal(new[] { "StagedSweep", "Advance" }, cleanup.Calls);
        }

        [Fact]
        public async Task ExecutingCleanupYieldsToADueBuild()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 2, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 6);
            rig.Worker.Published(4);
            await Quiesce(rig);

            var started = Signal();
            var resume = Signal();
            bool? dueAtStart = null;
            long? head = null;
            Task? beforeDue = null;
            Task? afterDue = null;
            cleanup.Work = x => x == JobKind.Deletion && afterDue == null;
            cleanup.Run = async (_, context) =>
            {
                dueAtStart = context.BuildDue;
                head = context.Head;
                beforeDue = context.TryIssue(_ => Task.CompletedTask);
                started.SetResult();
                await resume.Task;
                afterDue = context.TryIssue(_ => Task.CompletedTask);
                return new CleanupResult(OutcomeKind.Completed);
            };
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await started.Task.WaitAsync(s_wait);
            // The build falls due while the slice executes
            rig.Worker.Published(6);
            rig.Budget.TickNow();
            resume.SetResult();
            await job.WaitAsync(s_wait);

            Assert.False(dueAtStart);
            Assert.Equal(4, head);
            Assert.NotNull(beforeDue);
            Assert.Null(afterDue);
            Assert.Equal(OutcomeKind.Preempted, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(0, rig.Worker.CleanupFailures(JobKind.Deletion));
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));
        }

        [Theory]
        [InlineData(nameof(JobKind.Deletion))]
        [InlineData(nameof(JobKind.StagedSweep))]
        public async Task DeleteClassWorkPromotesThroughTheLongGate(string name)
        {
            var kind = Enum.Parse<JobKind>(name);
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            await Quiesce(rig);
            Assert.Equal(1, cleanup.Count(JobKind.StagedSweep));

            var done = false;
            var seen = new ConcurrentQueue<(bool Long, long ReadLimit, TimeSpan Deadline, int LongRunning, int DeleteRunning, int MainRunning)>();
            cleanup.Work = x => x == kind && !done;
            cleanup.Run = (_, context) =>
            {
                seen.Enqueue((context.Long, context.ReadLimit, context.Deadline - rig.OptionsClock.GetUtcNow(), rig.Budget.LongGate.Running, rig.Budget.DeleteGate.Running, rig.Budget.Gate.Running));
                if (!context.Long)
                {
                    return Task.FromResult(CleanupResult.Promote);
                }
                done = true;
                return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
            };
            var promoted = Signal();
            var resume = Signal();
            rig.Worker.HooksForTests = new WorkerHooks()
            {
                Hold = point =>
                {
                    if (point != "promoted")
                    {
                        return Task.CompletedTask;
                    }
                    promoted.TrySetResult();
                    return resume.Task;
                }
            };
            if (kind == JobKind.StagedSweep)
            {
                rig.Advance(TimeSpan.FromDays(1));
            }
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await Task.WhenAny(promoted.Task, job).WaitAsync(s_wait);
            Assert.True(promoted.Task.IsCompleted, $"Not promoted: {rig.Worker.LastOutcomeForTests}");

            // The lease, then the long gate, then the delete gate, holding the long slot while it waits there
            var longBlocker = await rig.Budget.LongGate.AcquireAsync().WaitAsync(s_wait);
            resume.SetResult();
            await DeltaMaintenanceWorkerTests.Queued(() => rig.Budget.LongGate.QueueLength == 1);
            Assert.Equal(0, rig.Budget.DeleteGate.Running);
            var deleteBlockers = new[] { await rig.Budget.DeleteGate.AcquireAsync().WaitAsync(s_wait), await rig.Budget.DeleteGate.AcquireAsync().WaitAsync(s_wait) };
            longBlocker.Dispose();
            await DeltaMaintenanceWorkerTests.Queued(() => rig.Budget.DeleteGate.QueueLength == 1);
            Assert.Equal(1, rig.Budget.LongGate.Running);
            Assert.Equal(0, rig.Budget.Gate.Running);
            Assert.False(job.IsCompleted);
            // The freed delete slot goes to it while the other stays taken
            deleteBlockers[0].Dispose();
            await job.WaitAsync(s_wait);
            deleteBlockers[1].Dispose();

            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.True(done);
            // Promoted, it reads without a limit until the long deadline and never touches the main gate
            Assert.Equal(new[]
            {
                (false, 20_000L, TimeSpan.FromMinutes(15), 0, 1, 0),
                (true, long.MaxValue, TimeSpan.FromHours(6), 1, 2, 0)
            }, seen);
            Assert.Equal(0, rig.Worker.CleanupFailures(kind));
            Assert.Equal(0, rig.Budget.LongGate.Running);
            Assert.Equal(0, rig.Budget.DeleteGate.Running);
            Assert.Equal(0, rig.Budget.Leases.Count);
        }

        [Fact]
        public async Task HintDrainPromotesTheWholeTurn()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            await Quiesce(rig);

            var done = false;
            var slices = new ConcurrentQueue<bool>();
            cleanup.Work = x => x == JobKind.Deletion && !done;
            // The hint gate needs more reads than short admission allows
            cleanup.Drain = context => context.Long ? new DrainResult(2) : DrainResult.Promote;
            cleanup.Run = (_, context) =>
            {
                slices.Enqueue(context.Long);
                done = true;
                return Task.FromResult(new CleanupResult(OutcomeKind.NoWork));
            };
            await rig.TickAsync();

            // The slice runs only under long admission, and the drained hints complete the turn
            Assert.Equal(new[] { "StagedSweep", "drain", "drain", "Deletion" }, cleanup.Calls);
            Assert.Equal(new[] { true }, slices);
            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(0, rig.Worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(0, rig.Budget.LongGate.Running);
            Assert.Equal(0, rig.Budget.DeleteGate.Running);
        }

        [Fact]
        public async Task LongWorkAskingForPromotionAgainFails()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            await Quiesce(rig);

            cleanup.Work = x => x == JobKind.Deletion;
            cleanup.Run = (_, _) => Task.FromResult(CleanupResult.Promote);
            await rig.TickAsync();
            Assert.Equal(OutcomeKind.Failed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(MaintenanceReason.Unexpected, rig.Worker.LastOutcomeForTests.Reason);
            Assert.Equal(2, cleanup.Count(JobKind.Deletion));
            Assert.Equal(1, rig.Worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(0, rig.Budget.LongGate.Running);
            Assert.Equal(0, rig.Budget.DeleteGate.Running);
        }

        // Only delete-class work yields to a newly due build; planning may delay its own table's build
        [Theory]
        [InlineData(nameof(JobKind.Advance), false)]
        [InlineData(nameof(JobKind.Advance), true)]
        [InlineData(nameof(JobKind.Orphan), true)]
        public async Task ExecutingPlanningFinishesBeforeADueBuild(string name, bool promoted)
        {
            var kind = Enum.Parse<JobKind>(name);
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 2, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 6);
            rig.Worker.Published(4);
            await Quiesce(rig);

            var started = Signal();
            var resume = Signal();
            var done = false;
            IssuanceStop? stop = null;
            Task? afterDue = null;
            cleanup.Work = x => x == kind && !done;
            cleanup.Run = async (_, context) =>
            {
                if (promoted && !context.Long)
                {
                    return CleanupResult.Promote;
                }
                started.TrySetResult();
                await resume.Task;
                stop = context.CheckStop();
                afterDue = context.TryIssue(_ => Task.CompletedTask);
                done = true;
                return new CleanupResult(OutcomeKind.Completed);
            };
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await started.Task.WaitAsync(s_wait);
            // The build falls due while the job executes
            rig.Worker.Published(6);
            rig.Budget.TickNow();
            rig.Budget.TickNow();
            Assert.Same(job, rig.Worker.CurrentJob);
            resume.SetResult();
            await job.WaitAsync(s_wait);

            Assert.Equal(IssuanceStop.None, stop);
            Assert.NotNull(afterDue);
            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(promoted ? 2 : 1, cleanup.Count(kind));
            Assert.Equal(0, rig.Worker.CleanupFailures(kind));
            // The build waited for it and runs next
            Assert.Equal(new long[] { 4 }, await Checkpoints(rig));
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));
        }

        // A short job that asks for long admission while its own table's build is due gives way instead of taking the long slot
        [Theory]
        [InlineData(nameof(JobKind.Advance))]
        [InlineData(nameof(JobKind.Deletion))]
        public async Task PromotionGivesWayToADueBuild(string name)
        {
            var kind = Enum.Parse<JobKind>(name);
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 2, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 6);
            rig.Worker.Published(4);
            await Quiesce(rig);

            var started = Signal();
            var resume = Signal();
            var longRuns = 0;
            cleanup.Work = x => x == kind && longRuns == 0;
            cleanup.Run = async (_, context) =>
            {
                if (context.Long)
                {
                    longRuns++;
                    return new CleanupResult(OutcomeKind.Completed);
                }
                started.TrySetResult();
                await resume.Task;
                return CleanupResult.Promote;
            };
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await started.Task.WaitAsync(s_wait);
            // The build falls due while the short attempt executes
            rig.Worker.Published(6);
            rig.Budget.TickNow();
            resume.SetResult();
            await job.WaitAsync(s_wait);

            Assert.Equal(OutcomeKind.Preempted, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(0, longRuns);
            Assert.Equal(0, rig.Budget.LongGate.Running);
            Assert.Equal(0, rig.Worker.CleanupFailures(kind));
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));
        }

        [Fact]
        public async Task NoWorkWithWorkStillQueuedWaitsAFloor()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            await Quiesce(rig);

            // Hints the gate keeps retaining: the turn drains none and finds no slice
            var queued = true;
            var empties = false;
            cleanup.Work = x => x == JobKind.Deletion && queued;
            cleanup.Run = (_, _) =>
            {
                if (empties)
                {
                    queued = false;
                }
                return Task.FromResult(new CleanupResult(OutcomeKind.NoWork));
            };
            for (int i = 0; i < 10; i++)
            {
                await rig.TickAsync();
            }
            Assert.Equal(1, cleanup.Count(JobKind.Deletion));
            Assert.Equal(OutcomeKind.NoWork, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(0, rig.Worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(rig.OptionsClock.GetUtcNow() + TimeSpan.FromMinutes(1), rig.Worker.CleanupRetryAt(JobKind.Deletion));

            // Due again once the floor passes, and the floor never grows
            rig.Advance(TimeSpan.FromMinutes(1) - TimeSpan.FromTicks(1));
            await rig.TickAsync();
            Assert.Equal(1, cleanup.Count(JobKind.Deletion));
            rig.Advance(TimeSpan.FromTicks(1));
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(2, cleanup.Count(JobKind.Deletion));
            Assert.Equal(0, rig.Worker.CleanupFailures(JobKind.Deletion));
            Assert.Equal(rig.OptionsClock.GetUtcNow() + TimeSpan.FromMinutes(1), rig.Worker.CleanupRetryAt(JobKind.Deletion));

            // Nothing queued at settlement leaves no floor, so work arriving later runs at once
            rig.Advance(TimeSpan.FromMinutes(1));
            empties = true;
            await rig.TickAsync();
            Assert.Equal(3, cleanup.Count(JobKind.Deletion));
            Assert.Equal(DateTimeOffset.MinValue, rig.Worker.CleanupRetryAt(JobKind.Deletion));
            queued = true;
            await rig.TickAsync();
            Assert.Equal(4, cleanup.Count(JobKind.Deletion));
        }

        [Fact]
        public async Task DrainedHintsCompleteATurnWithoutASlice()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            await Quiesce(rig);

            // More hints stay queued after each turn
            cleanup.Work = x => x == JobKind.Deletion;
            cleanup.Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.NoWork));
            cleanup.Drained = 3;
            await rig.TickAsync();
            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(DateTimeOffset.MinValue, rig.Worker.CleanupRetryAt(JobKind.Deletion));
            await rig.TickAsync();
            Assert.Equal(2, cleanup.Count(JobKind.Deletion));

            // Nothing drained and no slice is no work
            cleanup.Drained = 0;
            await rig.TickAsync();
            Assert.Equal(OutcomeKind.NoWork, rig.Worker.LastOutcomeForTests!.Kind);
            await rig.TickAsync();
            Assert.Equal(3, cleanup.Count(JobKind.Deletion));
        }

        [Fact]
        public async Task StagedSweepIsDueAPeriodAfterAnyAttemptThatRan()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            await Quiesce(rig);
            Assert.Equal(1, cleanup.Count(JobKind.StagedSweep));
            Assert.Equal(rig.BudgetClock.GetUtcNow() + TimeSpan.FromDays(1), rig.Worker.SweepDueAt);

            // A decline waits the period, not a backoff
            var result = new CleanupResult(OutcomeKind.Declined, MaintenanceReason.CleanupGate);
            cleanup.Run = (_, _) => Task.FromResult(result);
            rig.Advance(TimeSpan.FromDays(1));
            await rig.TickAsync();
            Assert.Equal(2, cleanup.Count(JobKind.StagedSweep));
            Assert.Equal(rig.BudgetClock.GetUtcNow() + TimeSpan.FromDays(1), rig.Worker.SweepDueAt);
            Assert.Equal(0, rig.Worker.CleanupFailures(JobKind.StagedSweep));
            Assert.Equal(DateTimeOffset.MinValue, rig.Worker.CleanupRetryAt(JobKind.StagedSweep));
            rig.Advance(TimeSpan.FromMinutes(30));
            await rig.TickAsync();
            Assert.Equal(2, cleanup.Count(JobKind.StagedSweep));

            // As does an attempt with nothing to do
            result = new CleanupResult(OutcomeKind.NoWork);
            rig.Advance(rig.Worker.SweepDueAt!.Value - rig.BudgetClock.GetUtcNow());
            await rig.TickAsync();
            Assert.Equal(3, cleanup.Count(JobKind.StagedSweep));
            Assert.Equal(rig.BudgetClock.GetUtcNow() + TimeSpan.FromDays(1), rig.Worker.SweepDueAt);

            // Only a failure backs off, and the sweep stays due
            result = new CleanupResult(OutcomeKind.Failed, MaintenanceReason.Transient);
            rig.Advance(rig.Worker.SweepDueAt!.Value - rig.BudgetClock.GetUtcNow());
            var due = rig.Worker.SweepDueAt;
            await rig.TickAsync();
            Assert.Equal(4, cleanup.Count(JobKind.StagedSweep));
            Assert.Equal(due, rig.Worker.SweepDueAt);
            Assert.Equal(1, rig.Worker.CleanupFailures(JobKind.StagedSweep));
            Assert.Equal(rig.OptionsClock.GetUtcNow() + TimeSpan.FromMinutes(1), rig.Worker.CleanupRetryAt(JobKind.StagedSweep));
            await rig.TickAsync();
            Assert.Equal(4, cleanup.Count(JobKind.StagedSweep));
            result = new CleanupResult(OutcomeKind.Completed);
            rig.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.Equal(5, cleanup.Count(JobKind.StagedSweep));
            Assert.Equal(0, rig.Worker.CleanupFailures(JobKind.StagedSweep));
            Assert.Equal(rig.BudgetClock.GetUtcNow() + TimeSpan.FromDays(1), rig.Worker.SweepDueAt);

            // An attempt that yields to a due demand stays due
            rig.Advance(TimeSpan.FromDays(1));
            due = rig.Worker.SweepDueAt;
            var running = Signal();
            var resume = Signal();
            cleanup.Run = async (kind, context) =>
            {
                running.TrySetResult();
                await resume.Task;
                _ = context.TryIssue(_ => Task.CompletedTask);
                return new CleanupResult(OutcomeKind.Completed);
            };
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await running.Task.WaitAsync(s_wait);
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(2, RootPolicy.Auto)));
            rig.Budget.TickNow();
            resume.SetResult();
            await job.WaitAsync(s_wait);
            Assert.Equal(OutcomeKind.Preempted, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(6, cleanup.Count(JobKind.StagedSweep));
            Assert.Equal(due, rig.Worker.SweepDueAt);
            Assert.Equal(0, rig.Worker.CleanupFailures(JobKind.StagedSweep));

            // The demand builds first, then the sweep runs again
            cleanup.Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.Completed));
            await rig.TickAsync();
            Assert.Equal(new long[] { 2 }, await Checkpoints(rig));
            await rig.TickAsync();
            Assert.Equal(7, cleanup.Count(JobKind.StagedSweep));
            Assert.Equal(rig.BudgetClock.GetUtcNow() + TimeSpan.FromDays(1), rig.Worker.SweepDueAt);
        }

        [Fact]
        public async Task ADeclinedDemandRequestedAgainKeepsItsRetryTime()
        {
            var rig = Create(interval: 100);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.Equal(4, rig.Worker.LastCompleted);

            // Without a root, the existing checkpoint at 4 is never replaced
            var demand = new BuildDemand(4, RootPolicy.None);
            Assert.True(rig.Worker.RequestBuild(demand));
            await rig.TickAsync();
            var declined = rig.Worker.LastOutcomeForTests!;
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.CheckpointExists), (declined.Kind, declined.Reason));
            Assert.Null(rig.Worker.PendingDemand);
            Assert.Equal(1, rig.Worker.DemandFailures);

            // Asked again at once, it waits for the target's retry time
            Assert.True(rig.Worker.RequestBuild(demand));
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Same(declined, rig.Worker.LastOutcomeForTests);
            Assert.Equal(demand, rig.Worker.PendingDemand);
            Assert.Equal(1, rig.Worker.DemandFailures);
            rig.OptionsClock.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.NotSame(declined, rig.Worker.LastOutcomeForTests);
            Assert.Equal(OutcomeKind.Declined, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(2, rig.Worker.DemandFailures);
            Assert.Null(rig.Worker.PendingDemand);

            // Another target starts afresh
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(5, RootPolicy.Auto)));
            Assert.Equal(0, rig.Worker.DemandFailures);
            await rig.TickAsync();
            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(new long[] { 4, 5 }, await Checkpoints(rig));
            Assert.Null(rig.Worker.PendingDemand);
        }

        [Fact]
        public async Task RequestBuildRefusesWhenInspectionIsOff()
        {
            var rig = Create(interval: 0, inspection: false);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 5);
            rig.Worker.Published(5);
            Assert.False(rig.Worker.RequestBuild(new BuildDemand(3, RootPolicy.Auto)));
            Assert.Null(rig.Worker.PendingDemand);
            await rig.TickAsync();
            Assert.Empty(await Checkpoints(rig));
        }

        [Fact]
        public async Task IssuedOperationsSettleBetweenStepsWithoutClosingIssuance()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 100, cleanup: cleanup);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            await Quiesce(rig);

            var stage = Signal();
            bool? waitedForStage = null;
            IssuanceStop? stop = null;
            Task? failed = null;
            Task? payload = null;
            var done = false;
            cleanup.Work = x => x == JobKind.Deletion && !done;
            cleanup.Run = async (kind, context) =>
            {
                failed = context.TryIssue(_ => Task.FromException(new IOException("stage delete fault")));
                _ = context.TryIssue(_ => stage.Task);
                var settling = context.AwaitIssuedAsync();
                waitedForStage = !settling.IsCompleted;
                stage.SetResult();
                // A failed operation is the job's to observe through its own task
                await settling;
                stop = context.Stopped;
                payload = context.TryIssue(_ => Task.CompletedTask);
                done = true;
                return new CleanupResult(OutcomeKind.Completed);
            };
            await rig.TickAsync();

            Assert.True(waitedForStage);
            Assert.True(failed!.IsFaulted);
            Assert.Equal(IssuanceStop.None, stop);
            Assert.NotNull(payload);
            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);

            // A job that returns without awaiting its wait still holds its admission until the operation settles
            var late = Signal();
            var parked = Signal();
            var finish = new TaskCompletionSource<CleanupResult>();
            done = false;
            cleanup.Run = async (kind, context) =>
            {
                _ = context.TryIssue(_ => late.Task);
                _ = context.AwaitIssuedAsync();
                done = true;
                return await new Parked<CleanupResult>(finish.Task, () => parked.TrySetResult());
            };
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await parked.Task.WaitAsync(s_wait);
            // The return runs inline, so an admission released before settling would be gone when this returns
            await Task.Run(() => finish.SetResult(new CleanupResult(OutcomeKind.Completed)));
            Assert.False(job.IsCompleted);
            Assert.Equal(1, rig.Budget.DeleteGate.Running);
            late.SetResult();
            await job.WaitAsync(s_wait);
            Assert.Equal(0, rig.Budget.DeleteGate.Running);
            Assert.Equal(0, rig.Budget.Leases.Count);
        }

        [Fact]
        public async Task DemandFailureBacksOffOnItsOwnAndADeclineEndsIt()
        {
            var rig = Create(interval: 100);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.Equal(4, rig.Worker.LastCompleted);

            // A handed-off target not yet in the log fails; only the demand backs off, and it stays pending
            rig.Worker.Published(9);
            var demand = new BuildDemand(9, RootPolicy.Auto);
            Assert.True(rig.Worker.RequestBuild(demand));
            Assert.False(rig.Worker.RequestBuild(new BuildDemand(5, RootPolicy.Auto)));
            await rig.TickAsync();
            Assert.Equal(OutcomeKind.Failed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(1, rig.Worker.DemandFailures);
            Assert.Equal(demand, rig.Worker.PendingDemand);
            Assert.Equal(0, rig.Worker.ConsecutiveFailures);
            Assert.Equal(DateTimeOffset.MinValue, rig.Worker.RetryAt);
            var failed = rig.Worker.LastOutcomeForTests;
            await rig.TickAsync();
            Assert.Same(failed, rig.Worker.LastOutcomeForTests);

            // Retried at the same target after its backoff
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 7, 9);
            rig.Worker.Published(9);
            rig.OptionsClock.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 9 }, await Checkpoints(rig));
            Assert.Null(rig.Worker.PendingDemand);
            Assert.Equal(0, rig.Worker.DemandFailures);

            // A decline is final: without a root, an existing checkpoint at the target is never replaced; the target keeps its retry time
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(9, RootPolicy.None)));
            await rig.TickAsync();
            Assert.Equal(OutcomeKind.Declined, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(MaintenanceReason.CheckpointExists, rig.Worker.LastOutcomeForTests.Reason);
            Assert.Null(rig.Worker.PendingDemand);
            Assert.Equal(1, rig.Worker.DemandFailures);
            Assert.Equal(0, rig.Worker.ConsecutiveFailures);
        }

        [Fact]
        public async Task OneMainSlotStillRunsLongJobs()
        {
            // Two commit reads fit a short admission, so the first build through 9 is promoted
            var rig = Create(interval: 2, jobs: 1, shortReads: 2);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 9);
            var promoted = Signal();
            var resume = Signal();
            rig.Worker.HooksForTests = new WorkerHooks()
            {
                Hold = point =>
                {
                    if (point != "promoted")
                    {
                        return Task.CompletedTask;
                    }
                    promoted.TrySetResult();
                    return resume.Task;
                }
            };
            rig.Worker.Published(9);
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            await promoted.Task.WaitAsync(s_wait);

            // Another table takes the only main slot while the promoted job is between admissions
            var other = await rig.Budget.Gate.AcquireAsync().WaitAsync(s_wait);
            resume.SetResult();
            await DeltaMaintenanceWorkerTests.Queued(() => rig.Budget.Gate.QueueLength == 1);
            // The long slot is taken before the main slot is queued for
            Assert.Equal(1, rig.Budget.LongGate.Running);

            other.Dispose();
            await job.WaitAsync(s_wait);
            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(new long[] { 9 }, await Checkpoints(rig));
            Assert.Equal(0, rig.Budget.LongGate.Running);
            Assert.Equal(0, rig.Budget.Gate.Running);
        }

        [Fact]
        public async Task KitSettleFailsOnceItsDeadlinePasses()
        {
            var kit = new MaintenanceKit();
            // A delete turn due at every tick
            var cleanup = new FakeCleanup() { Work = kind => kind == JobKind.Deletion, Drained = 1 };
            var worker = new DeltaMaintenanceWorker(kit.Budget, new MaintenanceWorkerSettings(0, null, TimeProvider.System) { PeriodicEnabled = false, InspectionEnabled = false },
                new HookFileStorage(Files.Of.InternalMemory($"./kit-{Guid.NewGuid():N}")), "t", "t", null) { Cleanup = cleanup };
            kit.Budget.Register(worker);
            try
            {
                worker.Published(0);
                var settle = kit.TickAndSettleAsync(0, TimeSpan.FromSeconds(2));
                Assert.Same(settle, await Task.WhenAny(settle, Task.Delay(s_wait)));
                var timeout = await Assert.ThrowsAsync<TimeoutException>(() => settle);
                Assert.Contains("still started", timeout.Message);
            }
            finally
            {
                await worker.StopAsync(s_wait);
                kit.DeleteScratch();
            }
        }
    }
}

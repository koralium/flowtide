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

using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaMaintenanceWorkerTests
    {
        private static readonly TimeSpan s_hour = TimeSpan.FromHours(1);

        // Holds a named point until released, and counts how often it was reached
        private sealed class HoldPoint
        {
            private readonly TaskCompletionSource _reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public HoldPoint(string name)
            {
                Name = name;
            }

            public string Name { get; }

            public int Reached;

            // Bounded, so a hold that is never reached fails the test instead of hanging it
            public Task WhenReached => _reached.Task.WaitAsync(TimeSpan.FromMinutes(1));

            public void Release() => _release.TrySetResult();

            public Task Hold(string point)
            {
                if (point != Name)
                {
                    return Task.CompletedTask;
                }
                Interlocked.Increment(ref Reached);
                _reached.TrySetResult();
                return _release.Task;
            }
        }

        private static HoldPoint HoldAt(Rig rig, string point)
        {
            var hold = new HoldPoint(point);
            rig.Worker.HooksForTests = new WorkerHooks() { Hold = hold.Hold };
            return hold;
        }

        // Moves both clocks to an absolute minute after the start
        private static void At(Rig rig, int minute)
        {
            var target = s_now + TimeSpan.FromMinutes(minute);
            rig.BudgetClock.Advance(target - rig.BudgetClock.GetUtcNow());
            if (rig.OptionsClock.GetUtcNow() < target)
            {
                rig.OptionsClock.Advance(target - rig.OptionsClock.GetUtcNow());
            }
        }

        // No dispatch from the first release step until settlement, including after the lease and slot are back
        [Theory]
        [InlineData("release")]
        [InlineData("settle")]
        public async Task NoDispatchWhileTheJobReleases(string point)
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 9);
            var hold = HoldAt(rig, point);
            var admitted = 0;
            var held = rig.Worker.HooksForTests!.Hold!;
            rig.Worker.HooksForTests = new WorkerHooks() { Hold = point => { if (point == "admitted") Interlocked.Increment(ref admitted); return held(point); } };
            rig.Worker.Published(5);
            rig.Budget.TickNow();
            await hold.WhenReached;
            var job = rig.Worker.CurrentJob;

            // Far past every trigger, yet the releasing job still occupies the worker
            rig.Worker.Published(9);
            rig.Advance(TimeSpan.FromHours(5));
            rig.Budget.TickNow();
            rig.Budget.TickNow();
            // A tick dispatches synchronously, so a successor would already be the current job here
            Assert.Same(job, rig.Worker.CurrentJob);
            Assert.Equal(1, admitted);
            Assert.False(rig.Worker.Idle);

            hold.Release();
            await rig.Worker.CurrentJob;
            Assert.True(rig.Worker.Idle);
        }

        [Theory]
        [InlineData("release")]
        [InlineData("settle")]
        public async Task JobReleasingDuringStopIsCountedDetached(string point)
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 5);
            var hold = HoldAt(rig, point);
            rig.Worker.Published(5);
            rig.Budget.TickNow();
            await hold.WhenReached;
            var job = rig.Worker.CurrentJob;

            await rig.Worker.StopAsync(TimeSpan.FromMilliseconds(50));
            Assert.Equal(1, rig.Worker.DetachedJobs);
            Assert.Equal(1, rig.Budget.DetachedJobs);

            hold.Release();
            await job;
            Assert.Equal(0, rig.Worker.DetachedJobs);
            Assert.Equal(0, rig.Budget.DetachedJobs);
        }

        [Fact]
        public async Task RepeatedStopCountsADetachedJobOnce()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 5);
            var hold = HoldAt(rig, "build");
            rig.Worker.Published(5);
            rig.Budget.TickNow();
            await hold.WhenReached;
            var job = rig.Worker.CurrentJob;

            await rig.Worker.StopAsync(TimeSpan.Zero);
            await rig.Worker.StopAsync(TimeSpan.Zero);
            Assert.Equal(1, rig.Worker.DetachedJobs);
            Assert.Equal(1, rig.Budget.DetachedJobs);

            hold.Release();
            await job;
            Assert.Equal(0, rig.Budget.DetachedJobs);
        }

        [Fact]
        public async Task EveryStoppedWorkerWithADetachedJobIsCounted()
        {
            var older = Create(interval: 2);
            await Commits(older.Storage, 0, 5);
            var hold = HoldAt(older, "build");
            older.Worker.Published(5);
            older.Budget.TickNow();
            await hold.WhenReached;
            var job = older.Worker.CurrentJob;
            await older.Worker.StopAsync(TimeSpan.Zero);
            var stopped = DeltaLakeSink.Detached(Array.Empty<DeltaMaintenanceWorker>(), older.Worker);

            // The next run's worker only waited, so it stops with nothing detached and the older job stays counted
            var newer = Create(interval: 2);
            await newer.Worker.StopAsync(TimeSpan.Zero);
            stopped = DeltaLakeSink.Detached(stopped, newer.Worker);
            Assert.Same(older.Worker, Assert.Single(stopped));
            Assert.Equal(1, stopped.Sum(x => x.DetachedJobs));

            hold.Release();
            await job;
            Assert.Empty(DeltaLakeSink.Detached(stopped, newer.Worker));
        }

        [Fact]
        public async Task DetachedCounterNeverLeaksUnderRaces()
        {
            var budget = NewBudget(jobs: 4);
            var storage = Files.Of.InternalMemory($"./races-{Guid.NewGuid():N}");
            await Commits(storage, 0, 0);
            for (int i = 0; i < 10_000; i++)
            {
                var rig = Create(storage, interval: 10, budget: budget);
                // Held at admission the job is cancelled on its way to settling; held at release it settles right away
                var hold = HoldAt(rig, i % 2 == 0 ? "admitted" : "release");
                rig.Worker.Published(0);
                rig.Budget.TickNow();
                var job = rig.Worker.CurrentJob;
                await hold.WhenReached;
                // The job settles while the stop counts it
                var stop = Task.Run(() => rig.Worker.StopAsync(TimeSpan.Zero));
                hold.Release();
                await stop;
                await job;
                Assert.Equal(0, rig.Worker.DetachedJobs);
            }
            Assert.Equal(0, budget.DetachedJobs);
        }

        [Fact]
        public async Task SuccessorSeesThePredecessorsScratchReleased()
        {
            var budget = NewBudget(jobs: 2);
            var predecessor = Create(interval: 2, budget: budget);
            await Commits(predecessor.Storage, 0, 5);
            var release = HoldAt(predecessor, "release");
            predecessor.Worker.Published(5);
            predecessor.Budget.TickNow();
            await release.WhenReached;
            var folder = predecessor.Worker.LastJobFolderForTests!;
            Assert.True(Directory.Exists(folder));

            var successor = Create(predecessor.Storage, interval: 2, budget: budget);
            bool? sawFolder = null;
            long reserved = -1;
            var admitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            successor.Worker.HooksForTests = new WorkerHooks()
            {
                Hold = point =>
                {
                    if (point == "admitted")
                    {
                        sawFolder = Directory.Exists(folder);
                        reserved = budget.Scratch.Ledger.Reserved;
                        admitted.TrySetResult();
                    }
                    return Task.CompletedTask;
                }
            };
            // A lease released before the scratch hands it to the successor at once, so its admission is awaited; in order the successor still waits
            budget.Scratch.DeleteFailsForTests = path =>
            {
                if (path == folder && budget.Leases.Waiting == 0)
                {
                    admitted.Task.Wait(TimeSpan.FromSeconds(30));
                }
                return false;
            };
            successor.Worker.Published(5);
            successor.Budget.TickNow();
            // The successor waits on the table lease while the predecessor still holds its scratch
            await Queued(() => budget.Leases.Waiting == 1);
            Assert.False(admitted.Task.IsCompleted);

            release.Release();
            await admitted.Task.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.False(sawFolder);
            Assert.Equal(0, reserved);
            await predecessor.Worker.CurrentJob;
            await successor.Worker.CurrentJob;
        }

        [Fact]
        public async Task DispatchFailureSettlesTheRun()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 3);
            var faults = 1;
            rig.Worker.HooksForTests = new WorkerHooks() { Fault = point => { if (point == "dispatch" && Interlocked.Decrement(ref faults) >= 0) throw new InvalidOperationException("no thread"); } };
            rig.Worker.Published(3);
            rig.Budget.TickNow();
            Assert.True(rig.Worker.Idle);
            Assert.Equal(1, rig.Worker.ConsecutiveFailures);
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.DispatchFailed)));

            rig.PastRetry();
            await rig.TickAsync();
            Assert.True(rig.Worker.Inspected);
            Assert.Equal(new long[] { 3 }, await Checkpoints(rig.Storage));
        }

        [Theory]
        [InlineData("release")]
        [InlineData("slot")]
        [InlineData("outcome")]
        public async Task ReleaseFailureStillSettlesTheRun(string point)
        {
            var budget = NewBudget(jobs: 2);
            var rig = Create(interval: 2, budget: budget);
            await Commits(rig.Storage, 0, 3);
            rig.Worker.HooksForTests = new WorkerHooks() { Fault = at => { if (at == point) throw new InvalidOperationException($"{at} fault"); } };
            rig.Worker.Published(3);
            await rig.TickAsync();
            Assert.True(rig.Worker.Idle);
            Assert.Equal(1, rig.Logs.Warnings(point == "outcome" ? nameof(MaintenanceReason.Unexpected) : nameof(MaintenanceReason.ReleaseFailed)));

            // The lease was released, so a successor on the same table is admitted
            var successor = Create(rig.Storage, interval: 2, budget: budget);
            successor.Worker.Published(3);
            successor.Budget.TickNow();
            await successor.Worker.CurrentJob.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.True(successor.Worker.Inspected);
        }

        [Fact]
        public async Task TickFromASuppressedContextDispatches()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 3);
            rig.Worker.Published(3);
            using (ExecutionContext.SuppressFlow())
            {
                rig.Budget.TickNow();
            }
            await rig.Worker.CurrentJob;
            Assert.True(rig.Worker.Inspected);
            Assert.Equal(0, rig.Worker.ConsecutiveFailures);
        }

        // Interval 100 never fires here, so every build is due by age (1 h)
        private async Task<Rig> AgeRig(long commits = 40, int jobs = 2)
        {
            var rig = Create(interval: 100, maxAge: s_hour, jobs: jobs);
            await Commits(rig.Storage, 0, commits);
            return rig;
        }

        // Arms at minute 0 with a first job that only inspects
        private static async Task ArmAt0(Rig rig, long version)
        {
            rig.Worker.Published(version);
            await rig.TickAsync();
            Assert.True(rig.Worker.Inspected);
        }

        [Fact]
        public async Task AgeTriggerFiresAtCompletionForAVersionSeenAtMinute10()
        {
            var rig = await AgeRig();
            await ArmAt0(rig, 5);
            Assert.Empty(await Checkpoints(rig.Storage));

            // The build of 5 is admitted at minute 60 and held for 90 minutes
            var hold = HoldAt(rig, "build");
            At(rig, 60);
            rig.Budget.TickNow();
            await hold.WhenReached;
            At(rig, 70);
            rig.Worker.Published(6);
            rig.Budget.TickNow();
            At(rig, 150);
            hold.Release();
            await rig.Worker.CurrentJob;
            Assert.Equal(new long[] { 5 }, await Checkpoints(rig.Storage));

            // Version 6 was first seen at minute 70, so it is due at completion
            rig.Worker.HooksForTests = null;
            await rig.TickAsync();
            Assert.Equal(new long[] { 5, 6 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task AgeTriggerDatesVersionsWhenFirstObserved()
        {
            var rig = await AgeRig();
            await ArmAt0(rig, 5);
            var hold = HoldAt(rig, "build");
            At(rig, 60);
            rig.Budget.TickNow();
            await hold.WhenReached;
            At(rig, 140);
            rig.Worker.Published(6);
            rig.Budget.TickNow();
            At(rig, 150);
            hold.Release();
            await rig.Worker.CurrentJob;
            rig.Worker.HooksForTests = null;

            await rig.TickAsync();
            Assert.Equal(new long[] { 5 }, await Checkpoints(rig.Storage));
            At(rig, 199);
            await rig.TickAsync();
            Assert.Equal(new long[] { 5 }, await Checkpoints(rig.Storage));
            At(rig, 200);
            await rig.TickAsync();
            Assert.Equal(new long[] { 5, 6 }, await Checkpoints(rig.Storage));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task HeldWaitDoesNotMisdateLaterVersions(bool lease)
        {
            var rig = await AgeRig(jobs: 1);
            // The first job waits from minute 0 to 60, at the gate or at the table lease
            var blocker = lease ? await rig.Budget.Leases.AcquireAsync(TableLeases.KeyOf(rig.Storage, "t")) : await rig.Budget.Gate.AcquireAsync();
            rig.Worker.Published(5);
            rig.Budget.TickNow();
            At(rig, 10);
            rig.Worker.Published(30);
            rig.Budget.TickNow();

            var hold = HoldAt(rig, "build");
            At(rig, 60);
            blocker.Dispose();
            await hold.WhenReached;
            At(rig, 80);
            rig.Worker.Published(31);
            rig.Budget.TickNow();
            At(rig, 90);
            hold.Release();
            await rig.Worker.CurrentJob;
            rig.Worker.HooksForTests = null;
            Assert.Equal(new long[] { 30 }, await Checkpoints(rig.Storage));

            // 31 was first seen at minute 80, so it fires at 140, not at completion
            await rig.TickAsync();
            At(rig, 139);
            await rig.TickAsync();
            Assert.Equal(new long[] { 30 }, await Checkpoints(rig.Storage));
            At(rig, 140);
            await rig.TickAsync();
            Assert.Equal(new long[] { 30, 31 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task HeldGateDoesNotMisdateLaterVersions() => await HeldWaitDoesNotMisdateLaterVersions(lease: false);

        [Fact]
        public async Task HeldTableLeaseDoesNotMisdateLaterVersions() => await HeldWaitDoesNotMisdateLaterVersions(lease: true);

        [Fact]
        public async Task PublicationAfterTheLastTickIsDatedAtCompletion()
        {
            var rig = await AgeRig();
            await ArmAt0(rig, 5);
            var hold = HoldAt(rig, "build");
            At(rig, 60);
            rig.Budget.TickNow();
            await hold.WhenReached;
            At(rig, 100);
            // Published with no tick before the build completes
            rig.Worker.Published(6);
            At(rig, 110);
            hold.Release();
            await rig.Worker.CurrentJob;
            rig.Worker.HooksForTests = null;

            At(rig, 169);
            await rig.TickAsync();
            Assert.Equal(new long[] { 5 }, await Checkpoints(rig.Storage));
            At(rig, 170);
            await rig.TickAsync();
            Assert.Equal(new long[] { 5, 6 }, await Checkpoints(rig.Storage));
        }

        // Dispatch target 10 at minute 0, admission at minute 60 with T = 30
        private async Task<(Rig Rig, HoldPoint Inspect)> QueuedInspection(long checkpoint)
        {
            var rig = await AgeRig(jobs: 1);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", checkpoint);
            var slot = await rig.Budget.Gate.AcquireAsync();
            rig.Worker.Published(10);
            rig.Budget.TickNow();
            At(rig, 10);
            rig.Worker.Published(30);
            rig.Budget.TickNow();
            var hold = HoldAt(rig, "inspect");
            At(rig, 60);
            slot.Dispose();
            await hold.WhenReached;
            return (rig, hold);
        }

        [Fact]
        public async Task InspectionCoveringTheAdmittedTargetDatesResidualWorkAtObservation()
        {
            var (rig, inspect) = await QueuedInspection(30);
            At(rig, 80);
            rig.Worker.Published(31);
            rig.Budget.TickNow();
            At(rig, 90);
            inspect.Release();
            await rig.Worker.CurrentJob;
            rig.Worker.HooksForTests = null;
            Assert.Equal(30, rig.Worker.LastCompleted);

            At(rig, 120);
            await rig.TickAsync();
            Assert.Equal(new long[] { 30 }, await Checkpoints(rig.Storage));
            At(rig, 140);
            await rig.TickAsync();
            Assert.Equal(new long[] { 30, 31 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task CoveringInspectionDatesLaterVersionsAtAdmission()
        {
            var (rig, inspect) = await QueuedInspection(10);
            inspect.Release();
            await rig.Worker.CurrentJob;
            rig.Worker.HooksForTests = null;
            Assert.Equal(10, rig.Worker.LastCompleted);

            At(rig, 119);
            await rig.TickAsync();
            Assert.Equal(new long[] { 10 }, await Checkpoints(rig.Storage));
            At(rig, 120);
            await rig.TickAsync();
            Assert.Equal(new long[] { 10, 30 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task CoveringInspectionDoesNotTriggerAnEarlyAgeBuild()
        {
            var (rig, inspect) = await QueuedInspection(10);
            At(rig, 90);
            inspect.Release();
            await rig.Worker.CurrentJob;
            // The job admitted at minute 60 found 10 covered; 30 was first seen then, so no build before 120
            Assert.Equal(new long[] { 10 }, await Checkpoints(rig.Storage));
            Assert.Equal(s_now + TimeSpan.FromMinutes(60), rig.Worker.OldestOrigin);
        }

        [Fact]
        public async Task InspectionBelowTheDispatchTargetKeepsTheArmingOrigin()
        {
            var (rig, inspect) = await QueuedInspection(5);
            inspect.Release();
            await rig.Worker.CurrentJob;
            // Nothing up to the dispatch target was covered, so 30 keeps the origin of minute 0 and is due at admission
            Assert.Equal(new long[] { 5, 30 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task AgeOriginSurvivesFailures()
        {
            var rig = await AgeRig();
            await ArmAt0(rig, 5);
            Assert.Equal(s_now, rig.Worker.OldestOrigin);
            rig.Hooks!.Before = (verb, path) => verb == "OpenWrite" && path.Full.EndsWith(".checkpoint.parquet") ? throw new IOException("upload fault") : Task.CompletedTask;
            At(rig, 60);
            await rig.TickAsync();
            Assert.Equal(1, rig.Worker.ConsecutiveFailures);
            Assert.Equal(s_now, rig.Worker.OldestOrigin);
        }

        // Every attempt fails at the listing
        private async Task<Rig> FailingRig(Func<int, string>? message = null)
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 3);
            var attempt = 0;
            rig.Hooks!.Before = (verb, path) => verb == "Ls" ? throw new IOException(message?.Invoke(Interlocked.Increment(ref attempt)) ?? "listing fault") : Task.CompletedTask;
            rig.Worker.Published(3);
            return rig;
        }

        [Fact]
        public async Task BackoffDoublesToThirtyMinutes()
        {
            var rig = await FailingRig();
            var delays = new List<double>();
            for (int i = 0; i < 7; i++)
            {
                rig.PastRetry();
                await rig.TickAsync();
                delays.Add((rig.Worker.RetryAt - rig.OptionsClock.GetUtcNow()).TotalMinutes);
            }
            Assert.Equal(new double[] { 1, 2, 4, 8, 16, 30, 30 }, delays);
        }

        [Fact]
        public async Task BackoffStaysCappedAfterManyFailures()
        {
            var rig = await FailingRig();
            for (int i = 0; i < 40; i++)
            {
                rig.PastRetry();
                await rig.TickAsync();
            }
            Assert.Equal(40, rig.Worker.ConsecutiveFailures);
            Assert.Equal(TimeSpan.FromMinutes(30), rig.Worker.RetryAt - rig.OptionsClock.GetUtcNow());
        }

        [Fact]
        public async Task BackoffUsesTheOptionsClock()
        {
            // The two clocks start at different epochs and move independently
            var options = new FakeTimeProvider(new DateTimeOffset(2031, 1, 1, 0, 0, 0, TimeSpan.Zero));
            var rig = Create(interval: 2, optionsClock: options);
            await Commits(rig.Storage, 0, 3);
            rig.Hooks!.Before = (verb, path) => verb == "Ls" ? throw new IOException("listing fault") : Task.CompletedTask;
            rig.Worker.Published(3);
            await rig.TickAsync();
            Assert.Equal(options.GetUtcNow() + TimeSpan.FromMinutes(1), rig.Worker.RetryAt);

            rig.Hooks.ClearRequests();
            rig.BudgetClock.Advance(TimeSpan.FromHours(3));
            await rig.TickAsync();
            Assert.Empty(rig.Hooks.Requests);

            options.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.NotEmpty(rig.Hooks.Requests);
        }

        [Fact]
        public async Task DeclineWarnsOncePerReasonCode()
        {
            var rig = await FailingRig();
            for (int i = 0; i < 5; i++)
            {
                rig.PastRetry();
                await rig.TickAsync();
            }
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.Transient)));
            Assert.True(rig.Logs.Entries.Count(x => x.Level == LogLevel.Debug && x.Message.Contains(nameof(MaintenanceReason.Transient))) >= 4);
        }

        [Fact]
        public async Task WarningSetStaysBoundedUnderDistinctMessages()
        {
            var rig = await FailingRig(attempt => $"listing fault {attempt} {Guid.NewGuid()}");
            for (int i = 0; i < 20; i++)
            {
                rig.PastRetry();
                await rig.TickAsync();
            }
            Assert.Equal(1, rig.Logs.Entries.Count(x => x.Level == LogLevel.Warning));
        }

        [Fact]
        public async Task IdleTickMakesNoRequest()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 4);
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.Equal(4, rig.Worker.LastCompleted);

            rig.Hooks!.ClearRequests();
            for (int i = 0; i < 5; i++)
            {
                rig.Advance(TimeSpan.FromMinutes(5));
                await rig.TickAsync();
            }
            Assert.Empty(rig.Hooks.Requests);
        }

        [Fact]
        public async Task BusyWorkerIsSkippedByTheTick()
        {
            var rig = Create(interval: 2);
            rig.Worker.Published(3);
            using var held = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            var holder = Task.Run(() =>
            {
                lock (rig.Worker.SyncForTests)
                {
                    held.Set();
                    release.Wait();
                }
            });
            held.Wait();
            try
            {
                var tick = Task.Run(rig.Budget.TickNow);
                Assert.True(tick.Wait(TimeSpan.FromSeconds(5)), "The tick blocked on a busy worker");
            }
            finally
            {
                release.Set();
                await holder;
            }
            // The skipped tick dispatched nothing
            Assert.True(rig.Worker.Idle);
        }

        [Fact]
        public async Task StopRacingATickAlwaysJoinsTheJob()
        {
            var storage = Files.Of.InternalMemory($"./stop-race-{Guid.NewGuid():N}");
            await Commits(storage, 0, 0);
            var budget = NewBudget(jobs: 8);
            for (int i = 0; i < 10_000; i++)
            {
                var rig = Create(storage, interval: 10, budget: budget);
                rig.Worker.Published(0);
                using var start = new Barrier(2);
                var tick = Task.Run(() =>
                {
                    start.SignalAndWait();
                    rig.Budget.TickNow();
                });
                var stop = Task.Run(async () =>
                {
                    start.SignalAndWait();
                    await rig.Worker.StopAsync(TimeSpan.Zero);
                    // When the stop returns, a dispatched job is counted detached or settled; a detached job may settle between the two reads
                    return rig.Worker.DetachedJobs == 1 || rig.Worker.Idle;
                });
                await tick;
                Assert.True(await stop, $"iteration {i}: a job escaped the stop");
                await rig.Worker.CurrentJob;
            }
            Assert.Equal(0, budget.DetachedJobs);
        }

        [Fact]
        public void HandOffAllocatesNothingAndRunsNoWorkerCode()
        {
            var rig = Create(interval: 2);
            rig.Worker.Published(1);
            var calls = rig.Worker.WorkerCodeCallsForTests;
            var before = GC.GetAllocatedBytesForCurrentThread();
            rig.Worker.Published(5);
            rig.Worker.Published(3);
            var after = GC.GetAllocatedBytesForCurrentThread();
            Assert.Equal(0, after - before);
            Assert.Equal(calls, rig.Worker.WorkerCodeCallsForTests);
            Assert.Equal(5, rig.Worker.Target);
        }

        [Fact]
        public async Task TwoBudgetsOnOneTableLeaveValidCheckpointsAndRepairTheHint()
        {
            // Two processes, each with its own budget and therefore its own leases
            var first = Create(interval: 2);
            await Commits(first.Storage, 0, 5);
            var second = Create(first.Storage, interval: 2);
            first.Worker.Published(5);
            second.Worker.Published(5);
            await Task.WhenAll(Task.Run(first.TickAsync), Task.Run(second.TickAsync));

            var bytes = await DeltaTestKit.ReadBytes(first.Storage, CheckpointPath(5));
            var local = Path.Combine(_directory, "two-budgets.parquet");
            Directory.CreateDirectory(_directory);
            await File.WriteAllBytesAsync(local, bytes);
            var memory = new JobMemory();
            var scan = CheckpointPageScan.Scan(local, memory);
            memory.Release(scan.ModelBytes);

            // The next run's inspection repairs a hint that names the other writer's bytes
            var next = Create(first.Storage, interval: 2);
            next.Worker.Published(5);
            await next.TickAsync();
            var hint = (await Hint(first.Storage))!;
            Assert.Equal(5, hint.Version);
            Assert.Equal(bytes.Length, hint.SizeInBytes);
        }
    }
}

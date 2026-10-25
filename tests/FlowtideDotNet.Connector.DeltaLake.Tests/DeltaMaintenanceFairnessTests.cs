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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using FakeCleanup = FlowtideDotNet.Connector.DeltaLake.Tests.DeltaMaintenanceSchedulingTests.FakeCleanup;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaMaintenanceFairnessTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 12, 0, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);
        private const int Tables = 300;

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "fair-" + ScratchOwners.ShortId());

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

        // Workers of tables t0 to t299 on one budget and one in-memory store, on manual ticks and one fake clock
        private sealed class Fleet
        {
            public required DeltaMaintenanceBudget Budget { get; init; }

            public required FakeTimeProvider Clock { get; init; }

            public required HookFileStorage Store { get; init; }

            public List<DeltaMaintenanceWorker> Workers { get; } = new List<DeltaMaintenanceWorker>();

            public List<FakeCleanup> Cleanups { get; } = new List<FakeCleanup>();

            // Ticks until a tick starts no job, waiting on every job but the skipped worker's
            public async Task QuiesceAsync(int skip = -1)
            {
                for (int round = 0; round < 100; round++)
                {
                    var before = Workers.Select(x => x.CurrentJob).ToArray();
                    Budget.TickNow();
                    var after = Workers.Select(x => x.CurrentJob).ToArray();
                    await Task.WhenAll(after.Where((_, i) => i != skip)).WaitAsync(s_wait);
                    if (before.Zip(after).All(x => ReferenceEquals(x.First, x.Second)))
                    {
                        return;
                    }
                }
                throw new InvalidOperationException("The fleet kept dispatching");
            }
        }

        private DeltaMaintenanceBudget NewBudget(FakeTimeProvider clock, int jobs = MaintenanceBudgetSettings.DefaultConcurrentJobs)
        {
            return new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(jobs, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true)
            {
                Buckets = MaintenanceBucketsTests.Unthrottled()
            };
        }

        // Every table published at 0 with only cleanup work, which the fakes script
        private Fleet NewFleet(int jobs = MaintenanceBudgetSettings.DefaultConcurrentJobs)
        {
            var clock = new FakeTimeProvider(s_now);
            var fleet = new Fleet()
            {
                Budget = NewBudget(clock, jobs),
                Clock = clock,
                Store = OwnedHookFileStorage.Over(Files.Of.InternalMemory($"./fleet-{Guid.NewGuid():N}"))
            };
            for (int i = 0; i < Tables; i++)
            {
                var cleanup = new FakeCleanup();
                var settings = new MaintenanceWorkerSettings(100, null, clock) { PeriodicEnabled = false, InspectionEnabled = false };
                var worker = new DeltaMaintenanceWorker(fleet.Budget, settings, fleet.Store, $"t{i}", $"t{i}", null) { Cleanup = cleanup };
                fleet.Budget.Register(worker);
                worker.Published(0);
                fleet.Workers.Add(worker);
                fleet.Cleanups.Add(cleanup);
            }
            return fleet;
        }

        private static TaskCompletionSource Signal() => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        private static void AssertReturned(DeltaMaintenanceBudget budget)
        {
            Assert.Equal((0, 0, 0), (budget.Gate.Running, budget.LongGate.Running, budget.DeleteGate.Running));
            Assert.Equal((0, 0, 0), (budget.Gate.QueueLength, budget.LongGate.QueueLength, budget.DeleteGate.QueueLength));
            Assert.Equal(0, budget.Leases.Count);
            Assert.Equal(0, budget.Transfer.Charged);
            Assert.Equal(0, budget.ScratchIfCreated?.Ledger.Reserved ?? 0);
            Assert.Equal((default(CapacityDemand), default(CapacityDemand), default(CapacityDemand)),
                (budget.Capacity.Total(CapacityState.Runnable), budget.Capacity.Total(CapacityState.Paused), budget.Capacity.Total(CapacityState.Suspended)));
        }

        [Theory]
        [InlineData("none")]
        [InlineData("deletion")]
        [InlineData("long")]
        [InlineData("validation")]
        [InlineData("one-main-slot")]
        public async Task PermitsReturnAfterEveryScenario(string scenario)
        {
            var fleet = NewFleet(jobs: scenario == "one-main-slot" ? 1 : MaintenanceBudgetSettings.DefaultConcurrentJobs);
            var kind = scenario is "deletion" or "validation" ? JobKind.Deletion : JobKind.Advance;
            var left = Enumerable.Repeat(scenario == "deletion" ? 2 : 1, Tables).ToArray();
            var parked = Signal();
            for (int i = 0; i < Tables; i++)
            {
                var table = i;
                var key = TableLeases.KeyOf(fleet.Store, $"t{i}");
                fleet.Cleanups[i].Work = x => x == kind && Volatile.Read(ref left[table]) > 0;
                fleet.Cleanups[i].Run = async (x, context) =>
                {
                    // The first table's first job holds a delete permit and a scratch file until its worker stops
                    if (table == 0 && x == JobKind.StagedSweep)
                    {
                        await context.Scratch.Create(context.Scratch.NewPath(".tmp")).WriteAsync(new byte[4096]);
                        parked.TrySetResult();
                        await new TaskCompletionSource().Task.WaitAsync(context.Stop);
                    }
                    if (x != kind)
                    {
                        return new CleanupResult(OutcomeKind.Completed);
                    }
                    if (scenario == "long" && !context.Long)
                    {
                        return CleanupResult.Promote;
                    }
                    if (scenario is "long" or "validation")
                    {
                        // Restored demand, transfer, scratch and request charges, as a repair or a full validation holds them
                        using var lease = context.Capacity!.Restore(context.CapacityOwner, key, $"advance-{table}", new CapacityDemand(100, 1 << 20), CapacityState.Runnable);
                        using var transfer = await context.Transfer.AdmitAsync(1 << 20, context.Stop);
                        await context.Scratch.Create(context.Scratch.NewPath(".tmp")).WriteAsync(new byte[1 << 16]);
                        await fleet.Budget.Buckets.ChargeRequestsAsync(10, context.Stop);
                    }
                    Interlocked.Decrement(ref left[table]);
                    return new CleanupResult(OutcomeKind.Completed);
                };
            }

            await fleet.QuiesceAsync(skip: 0);
            await parked.Task.WaitAsync(s_wait);
            Assert.All(left.Skip(1), x => Assert.Equal(0, x));
            Assert.Equal(1, fleet.Budget.DeleteGate.Running);

            // The held job is cancelled by its worker's stop
            await fleet.Workers[0].StopAsync(s_wait);
            Assert.Equal(OutcomeKind.Cancelled, fleet.Workers[0].LastOutcomeForTests!.Kind);
            AssertReturned(fleet.Budget);
        }

        [Fact]
        public async Task NoTableStarves()
        {
            var budget = NewBudget(new FakeTimeProvider(s_now));
            // Each table asks for a delete admission in turn, and asks again at the tail once its job ends
            var arrivals = new List<(int Table, JobAdmission Admission, Task Admitted)>();
            void Arrive(int table)
            {
                var admission = new JobAdmission();
                arrivals.Add((table, admission, budget.AdmitAsync($"/t{table}", admission, AdmissionClass.Delete, default)));
            }
            for (int table = 0; table < Tables; table++)
            {
                Arrive(table);
            }
            for (int turn = 0; turn < 3 * Tables; turn++)
            {
                var (table, admission, admitted) = arrivals[turn];
                await admitted.WaitAsync(s_wait);
                // Admitted in arrival order: only the permits' holders run, every later arrival waits
                var outstanding = arrivals.Count - turn;
                var running = Math.Min(budget.Settings.DeleteJobs, outstanding);
                Assert.Equal((running, outstanding - running), (budget.DeleteGate.Running, budget.DeleteGate.QueueLength));
                admission.Slot!.Dispose();
                admission.Lease!.Dispose();
                if (turn < 2 * Tables)
                {
                    Arrive(table);
                }
            }
            // Every table admitted once per round, within as many admissions as there are tables
            Assert.Equal(Enumerable.Range(0, 3).SelectMany(_ => Enumerable.Range(0, Tables)), arrivals.Select(x => x.Table));
            Assert.All(arrivals, x => Assert.True(x.Admitted.IsCompletedSuccessfully));
            Assert.Equal((0, 0), (budget.DeleteGate.Running, budget.Leases.Count));

            // On the fleet every table's delete turns run, none left behind
            var fleet = NewFleet();
            var turns = new int[Tables];
            for (int i = 0; i < Tables; i++)
            {
                var table = i;
                fleet.Cleanups[i].Work = x => x == JobKind.Deletion && Volatile.Read(ref turns[table]) < 3;
                fleet.Cleanups[i].Run = (x, _) =>
                {
                    if (x == JobKind.Deletion)
                    {
                        Interlocked.Increment(ref turns[table]);
                    }
                    return Task.FromResult(new CleanupResult(OutcomeKind.Completed));
                };
            }
            await fleet.QuiesceAsync();
            Assert.All(turns, x => Assert.Equal(3, x));
            AssertReturned(fleet.Budget);
        }

        // A table at the name with commit 0 and one-byte payload files part-0 to part-(files - 1)
        private static async Task<(CleanupConfiguration Configuration, IFileStorage Metered)> PayloadTableAsync(IFileStorage store, MaintenanceBuckets buckets, string table, int files)
        {
            await DeltaMaintenanceWorkerTests.Commits(store, 0, 0, table);
            for (int i = 0; i < files; i++)
            {
                await using var stream = await store.OpenWrite($"/{table}/part-{i}.parquet");
                await stream.WriteAsync(new byte[1]);
            }
            var configuration = CleanupConfiguration.Validate(store, table, true, CleanupOptIns.Off);
            Assert.True(configuration.Store.Eligible);
            return (configuration, MeteredFileStorage.Wrap(store, buckets, configuration.Store.Store.Paging));
        }

        // One slice over the table's payload files behind a passing log gate, as a delete turn runs it
        private static async Task<SliceResult> SliceAsync(CleanupConfiguration configuration, IFileStorage metered, int files, TimeProvider clock, CleanupContext context,
            Func<DeletionTarget, Task> beforeDelete, DeletionRunSettings? settings = null)
        {
            var executor = new DeletionExecutor(configuration, metered) { BeforeDeleteForTests = beforeDelete };
            var targets = new List<DeletionTarget>();
            for (int i = 0; i < files; i++)
            {
                Assert.Equal(KeepReason.None, executor.TryTarget($"part-{i}.parquet", TargetKind.Payload, out var target));
                targets.Add(target);
            }
            var gate = ChunkGate.ForLog(configuration, metered, (_, _) => Task.FromResult(new GateResult(GateVerdict.Passed)));
            return await new DeletionRun(executor, gate, new ListDeletionSource(targets), clock, settings).RunSliceAsync(context);
        }

        [Fact]
        public async Task DeleteIssuanceStaysWithinThirtySeconds()
        {
            var fleet = NewFleet();
            var (configuration, metered) = await PayloadTableAsync(fleet.Store, fleet.Budget.Buckets, "t0", 100);
            // Serial deletes, each taking four seconds of the fleet's clock
            var issued = new List<TimeSpan>();
            DateTimeOffset? first = null;
            SliceResult? slice = null;
            var done = false;
            fleet.Cleanups[0].Work = x => x == JobKind.Deletion && !done;
            fleet.Cleanups[0].Run = async (x, context) =>
            {
                if (x == JobKind.Deletion)
                {
                    slice = await SliceAsync(configuration, metered, 100, fleet.Clock, context, _ =>
                    {
                        first ??= fleet.Clock.GetUtcNow();
                        issued.Add(fleet.Clock.GetUtcNow() - first.Value);
                        fleet.Clock.Advance(TimeSpan.FromSeconds(4));
                        return Task.CompletedTask;
                    }, new DeletionRunSettings() { MaxOutstanding = 1 });
                    done = true;
                }
                return new CleanupResult(OutcomeKind.Completed);
            };

            await fleet.QuiesceAsync();

            // Issued at 0, 4, ... 28 seconds; the one due at 32 waits for a later slice
            Assert.Equal(Enumerable.Range(0, 8).Select(x => TimeSpan.FromSeconds(4 * x)), issued);
            Assert.Equal(SliceStatus.Progressed, slice!.Status);
            Assert.Equal(8, slice.Issued);
            Assert.True(slice.IssuanceWindow <= new DeletionRunSettings().IssuanceTime, $"{slice.IssuanceWindow}");
            Assert.Equal(92, (await fleet.Store.Ls("/t0/")).Count(x => x.Name.StartsWith("part-", StringComparison.Ordinal)));
            AssertReturned(fleet.Budget);
        }

        [Fact]
        public async Task AtMostEightDeleteAdmissionsAheadOfABuild()
        {
            var fleet = NewFleet();
            const int files = 10;
            var tables = new (CleanupConfiguration Configuration, IFileStorage Metered)[Tables];
            for (int i = 0; i < Tables; i++)
            {
                tables[i] = await PayloadTableAsync(fleet.Store, fleet.Budget.Buckets, $"t{i}", files);
            }
            // Every delete parks until the build has made its first request, once each permit's slice has all its deletes outstanding
            var inFlight = 0;
            var release = Signal();
            var parked = Signal();
            var outstanding = fleet.Budget.Settings.DeleteJobs * DeletionRun.MaxOutstanding;
            async Task Park(DeletionTarget _)
            {
                if (release.Task.IsCompleted)
                {
                    return;
                }
                if (Interlocked.Increment(ref inFlight) == outstanding)
                {
                    parked.TrySetResult();
                }
                await release.Task;
                Interlocked.Decrement(ref inFlight);
            }
            var turns = new int[Tables];
            var statuses = new SliceStatus?[Tables];
            var deleting = false;
            for (int i = 0; i < Tables; i++)
            {
                var table = i;
                fleet.Cleanups[i].Work = x => x == JobKind.Deletion && Volatile.Read(ref deleting) && Volatile.Read(ref turns[table]) == 0;
                fleet.Cleanups[i].Run = async (x, context) =>
                {
                    if (x == JobKind.Deletion)
                    {
                        Interlocked.Increment(ref turns[table]);
                        statuses[table] = (await SliceAsync(tables[table].Configuration, tables[table].Metered, files, fleet.Clock, context, Park)).Status;
                    }
                    return new CleanupResult(OutcomeKind.Completed);
                };
            }
            // Every table's staged sweep, then its delete turn: two run and park, the others wait for a permit
            await fleet.QuiesceAsync();
            Volatile.Write(ref deleting, true);
            fleet.Budget.TickNow();
            await parked.Task.WaitAsync(s_wait);

            // A build of another table becomes due; its first request sees what delete work is ahead of it
            var buildStore = new HookFileStorage(fleet.Store.Inner);
            await DeltaMaintenanceWorkerTests.Commits(buildStore, 0, 2, "b");
            var ahead = new List<int>();
            buildStore.Before = (_, _) =>
            {
                if (ahead.Count == 0)
                {
                    ahead.Add(Volatile.Read(ref inFlight) + fleet.Budget.Buckets.Requests.QueueLength);
                }
                return Task.CompletedTask;
            };
            var builder = new DeltaMaintenanceWorker(fleet.Budget, new MaintenanceWorkerSettings(2, null, fleet.Clock) { PeriodicEnabled = true, InspectionEnabled = true }, buildStore, "b", "b", null);
            fleet.Budget.Register(builder);
            builder.Published(2);
            fleet.Budget.TickNow();
            await builder.CurrentJob.WaitAsync(s_wait);
            Assert.Equal(new long[] { 2 }, await DeltaMaintenanceWorkerTests.Checkpoints(buildStore, "b"));
            var count = Assert.Single(ahead);
            Assert.True(count <= 8, $"{count} delete admissions were ahead of the build");
            // Each permit's slice has its deletes outstanding
            Assert.Equal(outstanding, count);
            Assert.Equal(fleet.Budget.Settings.DeleteJobs, fleet.Budget.DeleteGate.Running);

            release.SetResult();
            await fleet.QuiesceAsync();
            Assert.All(turns, x => Assert.Equal(1, x));
            Assert.All(statuses, x => Assert.Equal(SliceStatus.Completed, x));
            Assert.Empty((await fleet.Store.Ls("/t0/")).Where(x => x.Name.StartsWith("part-", StringComparison.Ordinal)));
            AssertReturned(fleet.Budget);
        }

        [Fact]
        public async Task OneMainSlotBuildWaitsForLongJob()
        {
            var clock = new FakeTimeProvider(s_now);
            var budget = NewBudget(clock, jobs: 1);
            var store = new HookFileStorage(Files.Of.InternalMemory($"./one-slot-{Guid.NewGuid():N}"));
            var cleanup = new FakeCleanup();
            var running = Signal();
            var release = Signal();
            var advanced = false;
            cleanup.Work = x => x == JobKind.Advance && !advanced;
            cleanup.Run = async (x, context) =>
            {
                if (x != JobKind.Advance)
                {
                    return new CleanupResult(OutcomeKind.Completed);
                }
                if (!context.Long)
                {
                    return CleanupResult.Promote;
                }
                running.TrySetResult();
                await release.Task;
                advanced = true;
                return new CleanupResult(OutcomeKind.Completed);
            };
            var granted = new List<(string Table, DateTimeOffset At)>();
            WorkerHooks Hooks(string table) => new WorkerHooks()
            {
                Hold = point =>
                {
                    if (point == "granted")
                    {
                        lock (granted)
                        {
                            granted.Add((table, clock.GetUtcNow()));
                        }
                    }
                    return Task.CompletedTask;
                }
            };
            var workers = new List<DeltaMaintenanceWorker>();
            foreach (var table in new[] { "x", "y" })
            {
                await DeltaMaintenanceWorkerTests.Commits(store, 0, 1, table);
                var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(2, null, clock) { PeriodicEnabled = true, InspectionEnabled = true }, store, table, table, null)
                {
                    Cleanup = table == "x" ? cleanup : null,
                    HooksForTests = Hooks(table)
                };
                budget.Register(worker);
                worker.Published(1);
                workers.Add(worker);
            }
            var (x, y) = (workers[0], workers[1]);
            async Task TickAsync()
            {
                budget.TickNow();
                await Task.WhenAll(x.CurrentJob, y.CurrentJob).WaitAsync(s_wait);
            }
            // Both inspections, then x's staged sweep
            await TickAsync();
            await TickAsync();

            // x's advance promotes and runs as a long job, holding the one main slot
            budget.TickNow();
            await running.Task.WaitAsync(s_wait);
            Assert.Equal((1, 1), (budget.LongGate.Running, budget.Gate.Running));

            // Both tables fall due for a build, which waits for the long job: x's behind its own table's job, y's for the main slot
            foreach (var table in new[] { "x", "y" })
            {
                await DeltaMaintenanceWorkerTests.Commits(store, 2, 3, table);
            }
            x.Published(3);
            y.Published(3);
            budget.TickNow();
            clock.Advance(TimeSpan.FromMinutes(10));
            budget.TickNow();
            Assert.Equal((600, 600), (x.OldestUncheckpointedSeconds, y.OldestUncheckpointedSeconds));
            Assert.DoesNotContain(3, await DeltaMaintenanceWorkerTests.Checkpoints(store, "y"));

            release.SetResult();
            await x.CurrentJob.WaitAsync(s_wait);
            await y.CurrentJob.WaitAsync(s_wait);
            await TickAsync();
            await TickAsync();
            Assert.True(advanced);
            Assert.Contains(3, await DeltaMaintenanceWorkerTests.Checkpoints(store, "x"));
            Assert.Contains(3, await DeltaMaintenanceWorkerTests.Checkpoints(store, "y"));
            Assert.Equal((0.0, 0.0), (x.OldestUncheckpointedSeconds, y.OldestUncheckpointedSeconds));
            // Neither build was admitted before the long job ended
            Assert.Equal(s_now + TimeSpan.FromMinutes(10), granted.Last(g => g.Table == "x").At);
            Assert.Equal(s_now + TimeSpan.FromMinutes(10), granted.Last(g => g.Table == "y").At);
            AssertReturned(budget);
        }
    }
}

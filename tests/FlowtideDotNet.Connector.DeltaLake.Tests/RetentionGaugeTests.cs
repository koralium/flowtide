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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Diagnostics.Metrics;
using System.Reflection;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RetentionGaugeTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = AdvanceExecutionKit.Now;
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);
        private const string TableId = "table-a";

        private readonly AdvanceExecutionKit _kit = new AdvanceExecutionKit();
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "rg-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public void Dispose()
        {
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            _owners.Release();
            _kit.Dispose();
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

        // A started advance adopted by a cleanup on a fake clock, with its lease in a ledger of its own
        private sealed class Started
        {
            public required ExecutionTable Table { get; init; }

            public required DeltaTableCleanup Cleanup { get; init; }

            public required ActiveAdvance Active { get; init; }

            public required CapacityLedger Ledger { get; init; }

            public required FakeTimeProvider Clock { get; init; }
        }

        private async Task<Started> StartedAsync(bool logsComplete = false)
        {
            var table = await _kit.ChurnedAsync();
            var clock = new FakeTimeProvider(s_now);
            var cleanup = AdvanceExecutionKit.Cleanup(table, clock: clock);
            await _kit.PublishAsync(cleanup, table, logsComplete: logsComplete);
            var ledger = new CapacityLedger(() => default);
            var active = await _kit.AdoptAsync(cleanup, table, ledger);
            return new Started() { Table = table, Cleanup = cleanup, Active = active, Ledger = ledger, Clock = clock };
        }

        private Task<CleanupResult> TurnAsync(Started started) => AdvanceExecutionKit.TurnAsync(started.Cleanup, _kit.DeletionContext(started.Table.Head, started.Ledger));

        // Each scripted slice in turn, the last from then on
        private static AdvancePhaseSlice Scripted(params Func<ActiveAdvance, SliceResult>[] steps)
        {
            var next = 0;
            return (active, turn, context) => Task.FromResult(steps[Math.Min(next++, steps.Length - 1)](active));
        }

        private static Func<ActiveAdvance, SliceResult> Still(SliceStatus status, MaintenanceReason reason = MaintenanceReason.None) => _ => new SliceResult(status, reason, 0);

        private static RetentionState State(string name) => Enum.Parse<RetentionState>(name);

        [Theory]
        [InlineData("Off", 0)]
        [InlineData("Idle", 1)]
        [InlineData("Coverage", 2)]
        [InlineData("Runnable", 3)]
        [InlineData("Paused", 4)]
        [InlineData("Rebasing", 5)]
        [InlineData("Suspended", 6)]
        [InlineData("RepairBlocked", 7)]
        public void StateCodesAreTheGaugesValues(string state, int code)
        {
            Assert.Equal(code, (int)State(state));
        }

        [Theory]
        [InlineData("Off", "Runnable", "Off")]
        [InlineData("Off", "Idle", "Off")]
        [InlineData("Idle", "Runnable", "Runnable")]
        [InlineData("Coverage", "Paused", "Paused")]
        [InlineData("RepairBlocked", "Rebasing", "Rebasing")]
        [InlineData("RepairBlocked", "Suspended", "Suspended")]
        [InlineData("RepairBlocked", "Idle", "RepairBlocked")]
        [InlineData("Coverage", "Idle", "Coverage")]
        [InlineData("Idle", "Idle", "Idle")]
        public void StateIsOffThenTheStartedAdvanceThenCoverage(string coverage, string advance, string expected)
        {
            Assert.Equal(State(expected), RetentionStates.Of(State(coverage), State(advance)));
        }

        // A worker over an in-memory table on manual ticks and fake clocks, with retention on an eligible store unless off
        private (DeltaMaintenanceWorker Worker, HookFileStorage Storage, FakeTimeProvider Options, DeltaMaintenanceBudget Budget) NewWorker(bool retention = true, int interval = 100)
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./gauges-{Guid.NewGuid():N}"));
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, ScratchOwners.ShortId())), new FakeTimeProvider(s_now), manualTick: true)
            {
                Buckets = MaintenanceBucketsTests.Unthrottled()
            };
            var options = new FakeTimeProvider(s_now);
            var settings = new MaintenanceWorkerSettings(interval, null, options)
            {
                PeriodicEnabled = true,
                InspectionEnabled = true,
                CleanupEligible = true,
                Retention = retention ? TimeSpan.FromDays(7) : null
            };
            var worker = new DeltaMaintenanceWorker(budget, settings, storage, "t", "t", null);
            budget.Register(worker);
            return (worker, storage, options, budget);
        }

        private static async Task TickAsync(DeltaMaintenanceBudget budget, DeltaMaintenanceWorker worker)
        {
            budget.TickNow();
            await worker.CurrentJob.WaitAsync(s_wait);
        }

        [Fact]
        public void WorkerWithoutRetentionIsOff()
        {
            Assert.Equal(RetentionState.Off, NewWorker(retention: false).Worker.CoverageState);
            Assert.Equal(RetentionState.Coverage, NewWorker().Worker.CoverageState);
        }

        [Fact]
        public async Task RepairWaitCountsFromWhenTheRepairBeganUntilCoverageIsConfirmed()
        {
            var (worker, storage, options, budget) = NewWorker();
            // Commits without an epoch under a marked checkpoint, then the first own commit adopts at 6
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 5);
            await _kit.BuildAsync(storage, 4);
            await WriteOwnCommit(storage, "t", 6, 6, s_now.ToUnixTimeMilliseconds() + 6, DeltaLakeCheckpointBuildTests.Add("f6.parquet"));
            worker.Published(6);
            Assert.Equal(0, worker.RepairWaitSeconds);

            // The inspection finds the repair, which then waits as the options clock moves
            await TickAsync(budget, worker);
            Assert.True(worker.RepairPending);
            Assert.Equal(RetentionState.Coverage, worker.CoverageState);
            options.Advance(TimeSpan.FromSeconds(90));
            Assert.Equal(90, worker.RepairWaitSeconds);

            // Its build keeps the wait it began with until a destination read confirms coverage
            await TickAsync(budget, worker);
            Assert.True(worker.RepairPending);
            options.Advance(TimeSpan.FromSeconds(30));
            Assert.Equal(120, worker.RepairWaitSeconds);
            await TickAsync(budget, worker);
            Assert.True(worker.CoverageConfirmed);
            Assert.Equal(0, worker.RepairWaitSeconds);
            Assert.Equal(RetentionState.Idle, worker.CoverageState);
        }

        [Fact]
        public async Task RepairThatCannotSucceedShowsBlocked()
        {
            var (worker, storage, _, budget) = NewWorker(interval: 2);
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 4);
            // The first own commit enables a feature no checkpoint build supports
            await WriteOwnCommit(storage, "t", 7, 7, s_now.ToUnixTimeMilliseconds() + 7, ProtocolWith("catalogManaged"), DeltaLakeCheckpointBuildTests.Add("f7.parquet"));
            worker.Published(7);
            await TickAsync(budget, worker);
            Assert.Equal(RetentionState.Coverage, worker.CoverageState);

            await TickAsync(budget, worker);
            Assert.NotNull(worker.Blocked);
            Assert.Equal(RetentionState.RepairBlocked, worker.CoverageState);
            Assert.Equal(0, worker.RepairWaitSeconds);
        }

        [Fact]
        public async Task RepairDiscoveredAgainKeepsTheWaitItBeganWith()
        {
            var (worker, storage, options, budget) = NewWorker();
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 4);
            await WriteOwnCommit(storage, "t", 7, 7, s_now.ToUnixTimeMilliseconds() + 7, DeltaLakeCheckpointBuildTests.Add("f7.parquet"));
            worker.Published(7);
            await TickAsync(budget, worker);
            Assert.Equal(new BuildDemand(7, RootPolicy.None), worker.PendingDemand);

            // Another writer's checkpoint at the target declines the build, then goes
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 7);
            await TickAsync(budget, worker);
            Assert.Equal(MaintenanceReason.CheckpointExists, worker.LastOutcomeForTests!.Reason);
            await storage.Rm($"/t/_delta_log/{7:D20}.checkpoint.parquet");
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 4);
            options.Advance(TimeSpan.FromMinutes(1));

            // Discovered again after the backoff
            await TickAsync(budget, worker);
            Assert.Equal(new BuildDemand(7, RootPolicy.None), worker.PendingDemand);
            Assert.Equal(60, worker.RepairWaitSeconds);
        }

        [Fact]
        public async Task AdvanceGaugesFollowTheStartedAdvanceUntilItSuspends()
        {
            var table = await _kit.ChurnedAsync();
            var clock = new FakeTimeProvider(s_now);
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table, clock: clock), table);
            // A restarted run's cleanup, unknown before it learns anything
            var cleanup = AdvanceExecutionKit.Cleanup(table, clock: clock);
            Assert.Equal((-1L, -1L, -1.0), (cleanup.OldestVersion, cleanup.AdoptionVersion, cleanup.BacklogSeconds));
            Assert.Equal((RetentionState.Idle, 0L, 0), (cleanup.AdvanceGaugeState, cleanup.PayloadRemaining, cleanup.FailedSlices));

            var ledger = new CapacityLedger(() => default);
            var active = await _kit.AdoptAsync(cleanup, table, ledger);
            var started = new Started() { Table = table, Cleanup = cleanup, Active = active, Ledger = ledger, Clock = clock };

            // Handed off: O, A and the cutoff of its start, every planned path left
            Assert.Equal((published.Started.O, published.Started.A), (cleanup.OldestVersion, cleanup.AdoptionVersion));
            Assert.Equal(0, cleanup.BacklogSeconds);
            Assert.Equal((RetentionState.Runnable, published.Started.Totals.Emitted), (cleanup.AdvanceGaugeState, cleanup.PayloadRemaining));
            Assert.True(cleanup.PayloadRemaining > 0);
            // A day later the cutoff moved on by a day
            clock.Advance(TimeSpan.FromDays(1));
            Assert.Equal(TimeSpan.FromDays(1).TotalSeconds, cleanup.BacklogSeconds);

            // Paused by a disabled slice, runnable after a passed one
            cleanup.LogPhaseForTests = Scripted(Still(SliceStatus.Disabled, MaintenanceReason.DeletesDisabled), Still(SliceStatus.Progressed),
                Still(SliceStatus.Failed, MaintenanceReason.DeleteFailed));
            await TurnAsync(started);
            Assert.Equal(RetentionState.Paused, cleanup.AdvanceGaugeState);
            await TurnAsync(started);
            Assert.Equal(RetentionState.Runnable, cleanup.AdvanceGaugeState);

            // Failed slices counted until the third suspends it, which keeps its authority after it leaves the turns
            for (int failures = 1; failures <= AdvanceExecution.MaxDurableFailures; failures++)
            {
                await TurnAsync(started);
                Assert.Equal(failures, cleanup.FailedSlices);
            }
            Assert.Null(cleanup.ActiveForTests);
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);
            Assert.Equal(published.Started.Totals.Emitted, cleanup.PayloadRemaining);
            Assert.Equal(published.Started.O, cleanup.OldestVersion);
        }

        [Theory]
        [InlineData("done")]
        [InlineData("suspended")]
        public async Task RetiredAdvanceLeavesOnlyItsGauges(string end)
        {
            var table = await _kit.ChurnedAsync(local: false);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, partPaths: 4, logsComplete: true);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            var ledger = new CapacityLedger(() => default);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Kind);
            var active = cleanup.ActiveForTests!;
            // Another writer re-adds a remaining planned path, which the rebase excludes
            await WriteCommit(table.Raw, "t", ++table.Head, DeltaLakeCheckpointBuildTests.Add("part-0009.parquet"));
            Assert.Equal(OutcomeKind.NoWork, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Kind);
            Assert.Equal(1, active.Exclusions.Count);

            if (end == "done")
            {
                await _kit.ExecuteAsync(cleanup, table, ledger);
                Assert.Equal((0L, 0), (cleanup.PayloadRemaining, cleanup.FailedSlices));
            }
            else
            {
                cleanup.PayloadPhaseForTests = Scripted(Still(SliceStatus.Failed, MaintenanceReason.DeleteFailed));
                for (int failures = 1; failures <= AdvanceExecution.MaxDurableFailures; failures++)
                {
                    await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger));
                }
                Assert.Equal(AdvanceState.Suspended, active.State);
                Assert.True(active.Started.Totals.Emitted > active.Processed);
                Assert.Equal((active.Started.Totals.Emitted - active.Processed, AdvanceExecution.MaxDurableFailures), (cleanup.PayloadRemaining, cleanup.FailedSlices));
            }
            Assert.Null(cleanup.ActiveForTests);

            // Nothing of the advance outlives its turns on the cleanup the sink run keeps
            var fields = typeof(DeltaTableCleanup).GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            Assert.Empty(fields.Where(x => x.GetValue(cleanup) is ActiveAdvance or ExclusionSet).Select(x => x.Name));
        }

        [Fact]
        public async Task RebasingAdvanceShowsRebasing()
        {
            var started = await StartedAsync(logsComplete: true);
            started.Cleanup.PayloadPhaseForTests = Scripted(Still(SliceStatus.Rebase, MaintenanceReason.HistoryChanged));

            await TurnAsync(started);

            Assert.Equal(AdvanceState.Rebasing, started.Active.State);
            Assert.Equal(RetentionState.Rebasing, started.Cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task RestartedResumeSuspensionShowsSuspended()
        {
            var table = await _kit.ChurnedAsync();
            // C listed as its header names it, with another digest
            var c = (await DeltaTransactionReader.ListLog(table.Raw, "t")).Checkpoints.Single(x => x.Version == 12).IOEntry;
            var fixture = CleanupRecordKit.Fixture("table-id", 12, 0, 0, table.Head, 0) with
            {
                Destination = new CheckpointSummary(c.Size!.Value, CleanupRecordKit.Hash("another checkpoint"), c.Size.Value, c.LastModificationTime!.Value.UtcTicks)
            };
            var part = CleanupRecordKit.Part(fixture, 0, "/t/part-0003.parquet");
            var header = CleanupRecordKit.Header(fixture, part);
            foreach (var record in new[] { part, header, CleanupRecordKit.Started(fixture, header) })
            {
                await CleanupRecordKit.PlantAsync(table.Raw, "t", record);
            }
            var cleanup = AdvanceExecutionTests.WithoutExclusions(AdvanceExecutionKit.Cleanup(table));
            var ledger = new CapacityLedger(() => default);

            var result = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));

            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.HistoryChanged), (result.Kind, result.Reason));
            Assert.True(ledger.Total(CapacityState.Suspended).Requests > 0);
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task ManifestSuspensionShowsSuspendedUntilARecoveryLeavesNothingUnresolved()
        {
            var table = await _kit.ChurnedAsync();
            var torn = await PlantTornStartedAsync(table);
            var cleanup = new DeltaTableCleanup(EightDays(table), table.Metered, new FakeTimeProvider(s_now), "t", null, () => table.Raw, new DiscardCounters(), null, "t");
            var ambient = default(CapacityDemand);
            var ledger = new CapacityLedger(() => ambient);

            var result = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.ManifestSuspended), (result.Kind, result.Reason));
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);

            // A listing decline, then a closed ledger's decline once the record is gone, neither after a recovery
            Assert.Equal(MaintenanceReason.ListingIneligible, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head + 5, ledger))).Reason);
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);
            File.Delete(torn);
            ambient = Closed;
            Assert.Equal(MaintenanceReason.DeleteBacklog, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Reason);
            Assert.Equal(0, ledger.Total(CapacityState.Suspended).Requests);
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);

            // A recovery with nothing unresolved, before the retention decline
            ambient = default;
            Assert.Equal(MaintenanceReason.RetentionNotPublished, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Reason);
            Assert.Equal(RetentionState.Idle, cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task FreshDeclineWithoutAResumeShowsIdle()
        {
            var table = await _kit.ChurnedAsync();
            // A fresh run whose ordinary decline has no started advance behind it
            var cleanup = AdvanceExecutionTests.WithoutExclusions(new DeltaTableCleanup(EightDays(table), table.Metered, new FakeTimeProvider(s_now), "t", null, () => table.Raw,
                new DiscardCounters(), null, "t"));
            var ledger = new CapacityLedger(() => default);

            var result = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));

            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.RetentionNotPublished), (result.Kind, result.Reason));
            Assert.Equal(RetentionState.Idle, cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task PausedResumeWhoseRecordsAreGoneShowsIdle()
        {
            var table = await _kit.ChurnedAsync();
            await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table);
            var cleanup = AdvanceExecutionTests.WithoutExclusions(new DeltaTableCleanup(EightDays(table), table.Metered, new FakeTimeProvider(s_now), "t", null, () => table.Raw,
                new DiscardCounters(), null, "t"));
            var ledger = new CapacityLedger(() => default);
            var first = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.Equal(MaintenanceReason.RetentionNotPublished, first.Reason);
            Assert.Equal(RetentionState.Paused, cleanup.AdvanceGaugeState);

            // A decline before the listing keeps the lease
            TearDownRecords(table);
            Assert.Equal(MaintenanceReason.ListingIneligible, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head + 5, ledger))).Reason);
            Assert.True(ledger.Total(CapacityState.Paused).Requests > 0);
            Assert.Equal(RetentionState.Paused, cleanup.AdvanceGaugeState);

            // A preparation that lists no records drops it
            var second = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));

            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.RetentionNotPublished), (second.Kind, second.Reason));
            Assert.Equal(0, ledger.Total(CapacityState.Paused).Requests);
            Assert.Equal(0, ledger.Total(CapacityState.Runnable).Requests);
            Assert.Equal(RetentionState.Idle, cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task SuspendedResumeWhoseRecordsAreGoneShowsIdle()
        {
            var table = await _kit.ChurnedAsync();
            // C listed as its header names it, with another digest
            var c = (await DeltaTransactionReader.ListLog(table.Raw, "t")).Checkpoints.Single(x => x.Version == 12).IOEntry;
            var fixture = CleanupRecordKit.Fixture("table-id", 12, 0, 0, table.Head, 0) with
            {
                Destination = new CheckpointSummary(c.Size!.Value, CleanupRecordKit.Hash("another checkpoint"), c.Size.Value, c.LastModificationTime!.Value.UtcTicks)
            };
            var part = CleanupRecordKit.Part(fixture, 0, "/t/part-0003.parquet");
            var header = CleanupRecordKit.Header(fixture, part);
            foreach (var record in new[] { part, header, CleanupRecordKit.Started(fixture, header) })
            {
                await CleanupRecordKit.PlantAsync(table.Raw, "t", record);
            }
            var cleanup = AdvanceExecutionTests.WithoutExclusions(AdvanceExecutionKit.Cleanup(table));
            var ambient = default(CapacityDemand);
            var ledger = new CapacityLedger(() => ambient);
            var first = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.Equal(MaintenanceReason.HistoryChanged, first.Reason);
            Assert.True(ledger.Total(CapacityState.Suspended).Requests > 0);
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);

            // A fresh preparation after the records are gone declines on a closed ledger with no lease left
            TearDownRecords(table);
            ambient = Closed;
            var second = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));

            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.DeleteBacklog), (second.Kind, second.Reason));
            Assert.Equal(0, ledger.Total(CapacityState.Suspended).Requests);
            Assert.Equal(RetentionState.Idle, cleanup.AdvanceGaugeState);
        }

        // A sink with a retention the table does not publish
        private static CleanupConfiguration EightDays(ExecutionTable table) => CleanupConfiguration.Validate(table.Raw, "t", false, new CleanupOptIns(TimeSpan.FromDays(8), null));

        // Every cleanup record of the table, as an operator removes them
        private static void TearDownRecords(ExecutionTable table)
        {
            foreach (var file in Directory.GetFiles(Path.Combine(table.Root!, "t", "_delta_log"), ".flowtide-cleanup.*"))
            {
                File.Delete(file);
            }
        }

        // Another advance's started record at the boundary that no preparation can interpret; returns its path
        private static async Task<string> PlantTornStartedAsync(ExecutionTable table, long boundary = 12)
        {
            var path = Path.Combine(table.Root!, "t", "_delta_log", $".flowtide-cleanup.{boundary:D20}.advance.0123456789abcdef.attempt.started");
            await File.WriteAllTextAsync(path, "{}");
            return path;
        }

        // Ambient demand that closes admission
        private static CapacityDemand Closed => new CapacityDemand((long)(CapacityLedger.CloseSeconds + 1) * MaintenanceBuckets.DefaultRequestsPerSecond, 0);

        [Theory]
        [InlineData("retention", nameof(CapacityState.Paused), nameof(RetentionState.Paused))]
        [InlineData("raised", nameof(CapacityState.Paused), nameof(RetentionState.Paused))]
        [InlineData("retry", nameof(CapacityState.Runnable), nameof(RetentionState.Runnable))]
        public async Task RestartedResumeThatDoesNotHandOffShowsItsLease(string outcome, string lease, string expected)
        {
            var table = await _kit.ChurnedAsync();
            await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table);
            var configuration = table.Configuration;
            var raised = TimeSpan.FromDays(30);
            if (outcome == "retention")
            {
                // A sink with a retention the table does not publish
                configuration = CleanupConfiguration.Validate(table.Raw, "t", false, new CleanupOptIns(TimeSpan.FromDays(8), null));
            }
            else if (outcome == "raised")
            {
                // Thirty days, published from the next commit, which C is not yet older than
                var formatted = RetentionProperties.Format(raised);
                await WriteOwnCommit(table.Raw, "t", table.Head + 1, 0, BoundarySelectorTests.Old(table.Head + 1),
                    Metadata(UserSchema, new Dictionary<string, string>() { [RetentionProperties.LogKey] = formatted, [RetentionProperties.DeletedFileKey] = formatted }));
                table.Head++;
                configuration = CleanupConfiguration.Validate(table.Raw, "t", false, new CleanupOptIns(raised, null));
            }
            var clock = new FakeTimeProvider(s_now);
            var cleanup = AdvanceExecutionTests.WithoutExclusions(new DeltaTableCleanup(configuration, table.Metered, clock, "t", null, () => table.Raw, new DiscardCounters(), null, "t"));
            if (outcome == "retry")
            {
                // A retried exclusion read, under recovery's runnable lease
                cleanup.AdvanceForTests.ResumeExclusionsForTests = static (_, _, _, _, _, _, _) =>
                    Task.FromResult(new ExclusionResult(null, MaintenanceReason.Transient, "The listing is retried") { Action = GateAction.Retry });
            }
            var ledger = new CapacityLedger(() => default);

            await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));

            Assert.Null(cleanup.ActiveForTests);
            Assert.True(ledger.Total(Enum.Parse<CapacityState>(lease)).Requests > 0);
            Assert.Equal(State(expected), cleanup.AdvanceGaugeState);
            if (outcome == "raised")
            {
                // Past the new cutoff the resume hands off
                clock.Advance(raised);
                await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
                Assert.NotNull(cleanup.ActiveForTests);
                Assert.Equal(RetentionState.Runnable, cleanup.AdvanceGaugeState);
            }
        }

        [Theory]
        [InlineData("paused", nameof(RetentionState.Paused))]
        [InlineData("retry", nameof(RetentionState.Runnable))]
        public async Task ResumeThenAnotherTornStartShowsSuspendedOverTheResumesLease(string outcome, string resumed)
        {
            var table = await _kit.ChurnedAsync();
            await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table);
            var configuration = outcome == "paused" ? EightDays(table) : table.Configuration;
            var cleanup = AdvanceExecutionTests.WithoutExclusions(new DeltaTableCleanup(configuration, table.Metered, new FakeTimeProvider(s_now), "t", null, () => table.Raw,
                new DiscardCounters(), null, "t"));
            if (outcome == "retry")
            {
                cleanup.AdvanceForTests.ResumeExclusionsForTests = static (_, _, _, _, _, _, _) =>
                    Task.FromResult(new ExclusionResult(null, MaintenanceReason.Transient, "The listing is retried") { Action = GateAction.Retry });
            }
            var ledger = new CapacityLedger(() => default);
            await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.Equal(State(resumed), cleanup.AdvanceGaugeState);

            // Another advance's torn start beside the resume's records
            var torn = await PlantTornStartedAsync(table);
            var fenced = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.ManifestSuspended), (fenced.Kind, fenced.Reason));
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);

            // The torn start gone, the resume's lease alone under a recovery that left the resume unresolved
            File.Delete(torn);
            var again = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.NotEqual(MaintenanceReason.ManifestSuspended, again.Reason);
            Assert.Equal(0, ledger.Total(CapacityState.Suspended).Requests);
            Assert.True(ledger.Total(Enum.Parse<CapacityState>(resumed)).Requests > 0);
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task StaleResumeThenOtherSuspensionThenListingDeclineShowsSuspended()
        {
            var table = await _kit.ChurnedAsync();
            await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table);
            var cleanup = AdvanceExecutionTests.WithoutExclusions(new DeltaTableCleanup(EightDays(table), table.Metered, new FakeTimeProvider(s_now), "t", null, () => table.Raw,
                new DiscardCounters(), null, "t"));
            var ledger = new CapacityLedger(() => default);
            await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.Equal(RetentionState.Paused, cleanup.AdvanceGaugeState);

            // The resume's records give way to another advance's torn start
            TearDownRecords(table);
            await PlantTornStartedAsync(table);
            Assert.Equal(MaintenanceReason.ManifestSuspended, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Reason);
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);

            var declined = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head + 5, ledger));

            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.ListingIneligible), (declined.Kind, declined.Reason));
            Assert.True(ledger.Total(CapacityState.Suspended).Requests > 0);
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task TurnSuspensionShowsSuspendedWhileItsLeaseIsHeld()
        {
            var started = await StartedAsync();
            started.Cleanup.LogPhaseForTests = Scripted(Still(SliceStatus.Suspended, MaintenanceReason.HistoryChanged));

            var turned = await TurnAsync(started);

            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.HistoryChanged), (turned.Kind, turned.Reason));
            Assert.Null(started.Cleanup.ActiveForTests);
            Assert.True(started.Ledger.Total(CapacityState.Suspended).Requests > 0);
            Assert.Equal(RetentionState.Suspended, started.Cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task DoneWhoseTeardownFindsAnotherAdvanceShowsItsSuspension()
        {
            var started = await StartedAsync();
            // Another advance's torn start above C, listed from the done's teardown on
            started.Cleanup.AdvanceHooksForTests.Step = async step =>
            {
                if (step == AdvanceSteps.Teardown)
                {
                    await PlantTornStartedAsync(started.Table, started.Active.C + 1);
                }
            };

            await _kit.ExecuteAsync(started.Cleanup, started.Table, started.Ledger);

            Assert.Equal(AdvancePhase.Done, started.Active.Phase);
            Assert.Equal(0, started.Ledger.Total(CapacityState.Runnable).Requests);
            Assert.True(started.Ledger.Total(CapacityState.Suspended).Requests > 0);
            Assert.Equal(RetentionState.Suspended, started.Cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task TableLeasesShowTheMostSevereStateOfThisOwnersLeases()
        {
            var table = await _kit.ChurnedAsync();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            var ledger = new CapacityLedger(() => default);
            using var owner = new CancellationTokenSource();
            using var other = new CancellationTokenSource();
            var key = TableLeases.KeyOf(RawStorage.Unwrap(table.Raw), table.Configuration.Store.Table);
            var demand = new CapacityDemand(1, 0);
            ledger.Restore(owner.Token, key, "runnable", demand, CapacityState.Runnable);
            ledger.Restore(owner.Token, key, "paused", demand, CapacityState.Paused);
            // Suspensions of another owner and of another table
            ledger.Restore(other.Token, key, "other-owner", demand, CapacityState.Suspended);
            ledger.Restore(owner.Token, key + "-other", "other-table", demand, CapacityState.Suspended);

            // Declined at the listing, before preparation changes any lease
            var declined = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head + 5, ledger, owner.Token));

            Assert.Equal(MaintenanceReason.ListingIneligible, declined.Reason);
            Assert.Equal(RetentionState.Paused, cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task AdvanceGateListingsAreCounted()
        {
            var table = await _kit.ChurnedAsync(local: false);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            await _kit.PublishAsync(cleanup, table);
            var ledger = new CapacityLedger(() => default);
            await _kit.AdoptAsync(cleanup, table, ledger);
            var counters = cleanup.Counters;

            // A passed gate probed an absent successor
            await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger));
            Assert.Equal((1L, 0L, 0L), (counters.Probes(ProbeResult.Absent), counters.Probes(ProbeResult.Present), counters.Probes(ProbeResult.Error)));
            Assert.All(Enum.GetValues<ListingCheck>(), check => Assert.Equal(0, counters.ListingDeclines(check)));

            // The next gate's listing does not show the commit its probe finds
            await WriteOwnCommit(table.Raw, "t", table.Head + 1, 0, s_now.ToUnixTimeMilliseconds(), DeltaLakeCheckpointBuildTests.Add("part-late.parquet"));
            var hidden = $"{table.Head + 1:D20}.json";
            table.Hooks!.Hidden = path => path.Full.EndsWith(hidden, StringComparison.Ordinal);
            var turned = await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger));

            Assert.Equal(MaintenanceReason.ListingIneligible, turned.Reason);
            Assert.Equal((1L, 1L, 0L), (counters.ListingDeclines(ListingCheck.SuccessorPresent), counters.Probes(ProbeResult.Present), counters.Probes(ProbeResult.Error)));
            Assert.Equal(1, counters.Probes(ProbeResult.Absent));
        }

        [Fact]
        public async Task AdvanceGateThatDeclinesAfterItsListingCountsNoProbe()
        {
            var started = await StartedAsync();
            // Other bytes in C at the size and modification time its header names
            var path = Path.Combine(started.Table.Root!, "t", "_delta_log", $"{started.Active.C:D20}.checkpoint.parquet");
            var modified = File.GetLastWriteTimeUtc(path);
            var bytes = File.ReadAllBytes(path);
            bytes[bytes.Length / 2] ^= 0xFF;
            File.WriteAllBytes(path, bytes);
            File.SetLastWriteTimeUtc(path, modified);

            var turned = await TurnAsync(started);

            Assert.Equal(MaintenanceReason.HistoryChanged, turned.Reason);
            Assert.All(Enum.GetValues<ProbeResult>(), result => Assert.Equal(0, started.Cleanup.Counters.Probes(result)));
            Assert.All(Enum.GetValues<ListingCheck>(), check => Assert.Equal(0, started.Cleanup.Counters.ListingDeclines(check)));
        }

        [Fact]
        public async Task PayloadRemainingIsThePlannedPathsNotYetProcessed()
        {
            var table = await _kit.ChurnedAsync();
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            await _kit.PublishAsync(cleanup, table, logsComplete: true);
            var ledger = new CapacityLedger(() => default);
            var active = await _kit.AdoptAsync(cleanup, table, ledger);
            // Slices of one path each
            cleanup.Settings = cleanup.Settings with { MaxPaths = 1 };
            var emitted = active.Started.Totals.Emitted;
            Assert.True(emitted > 1);

            await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger));

            Assert.Equal((AdvancePhase.Payload, 1L), (active.Phase, active.Processed));
            Assert.Equal(emitted - 1, cleanup.PayloadRemaining);
        }

        [Fact]
        public async Task AdvanceSliceAChangedStorageDisabledIsAStorageDecline()
        {
            var table = await _kit.ChurnedAsync();
            IFileStorage? current = table.Raw;
            var cleanup = new DeltaTableCleanup(table.Configuration, table.Metered, new FakeTimeProvider(s_now), "t", null, () => current, new DiscardCounters(), null, "t");
            await _kit.PublishAsync(cleanup, table);
            var ledger = new CapacityLedger(() => default);
            await _kit.AdoptAsync(cleanup, table, ledger);
            // The sink no longer holds the classified storage
            current = null;

            var turned = await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger));

            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.DeletesDisabled), (turned.Kind, turned.Reason));
            Assert.Equal(1, cleanup.Counters.ListingDeclines(ListingCheck.StorageChanged));
            Assert.Equal(RetentionState.Paused, cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task ChangedStorageIsCountedOncePerPause()
        {
            var table = await _kit.ChurnedAsync();
            IFileStorage? current = table.Raw;
            var cleanup = new DeltaTableCleanup(table.Configuration, table.Metered, new FakeTimeProvider(s_now), "t", null, () => current, new DiscardCounters(), null, "t");
            await _kit.PublishAsync(cleanup, table);
            var ledger = new CapacityLedger(() => default);
            await _kit.AdoptAsync(cleanup, table, ledger);
            current = null;

            // Every retry of the paused advance is disabled again
            for (int retry = 0; retry < 3; retry++)
            {
                Assert.Equal(MaintenanceReason.DeletesDisabled, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Reason);
            }
            Assert.Equal(1, cleanup.Counters.ListingDeclines(ListingCheck.StorageChanged));

            // Runnable once the storage is back, then paused by another change
            current = table.Raw;
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
            Assert.Equal(RetentionState.Runnable, cleanup.AdvanceGaugeState);
            current = null;
            await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger));
            Assert.Equal(2, cleanup.Counters.ListingDeclines(ListingCheck.StorageChanged));
        }

        [Fact]
        public async Task RebaseThatRejectsAPathIsCountedOnce()
        {
            var started = await StartedAsync(logsComplete: true);
            var table = started.Table;
            started.Cleanup.PayloadPhaseForTests = Scripted(Still(SliceStatus.Rebase, MaintenanceReason.HistoryChanged));
            await TurnAsync(started);
            // Another writer references a path outside the table
            table.Head++;
            await WriteCommit(table.Raw, "t", table.Head, DeltaLakeCheckpointBuildTests.Add("s3://elsewhere/t/part-0004.parquet"));

            var rebased = await AdvanceExecutionKit.JobAsync(started.Cleanup, _kit.AdvanceContext(table.Head, started.Ledger));

            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.PathRejected), (rebased.Kind, rebased.Reason));
            Assert.Equal(1, started.Cleanup.Counters.PathRejections);
            Assert.Equal(RetentionState.Suspended, started.Cleanup.AdvanceGaugeState);
        }

        [Fact]
        public async Task SliceTimingsAreTheLatestSlices()
        {
            var started = await StartedAsync();
            var counters = started.Cleanup.Counters;
            started.Cleanup.LogPhaseForTests = Scripted(
                _ => new SliceResult(SliceStatus.Progressed, MaintenanceReason.None, 0) { GateElapsed = TimeSpan.FromMilliseconds(1500), Elapsed = TimeSpan.FromSeconds(4) },
                _ => new SliceResult(SliceStatus.Retry, MaintenanceReason.Transient, 0) { GateElapsed = TimeSpan.FromMilliseconds(250) });

            await TurnAsync(started);
            Assert.Equal((1500.0, 4000.0), (counters.LastGateMs, counters.LastSliceMs));

            // A slice whose gate did not pass issued nothing
            await TurnAsync(started);
            Assert.Equal((250.0, 0.0), (counters.LastGateMs, counters.LastSliceMs));
        }

        [Fact]
        public async Task DisabledSliceLeavesTheLatestTimings()
        {
            var started = await StartedAsync();
            var counters = started.Cleanup.Counters;
            started.Cleanup.LogPhaseForTests = Scripted(
                _ => new SliceResult(SliceStatus.Progressed, MaintenanceReason.None, 0) { GateElapsed = TimeSpan.FromMilliseconds(1500), Elapsed = TimeSpan.FromSeconds(4) },
                Still(SliceStatus.Disabled, MaintenanceReason.DeletesDisabled));
            await TurnAsync(started);

            // The kill switch pauses the advance before the slice's gate
            await TurnAsync(started);

            Assert.Equal(RetentionState.Paused, started.Cleanup.AdvanceGaugeState);
            Assert.Equal((1500.0, 4000.0), (counters.LastGateMs, counters.LastSliceMs));
        }

        [Fact]
        public async Task SliceDisabledAfterItsGateRecordsItsTimings()
        {
            var started = await StartedAsync();
            var counters = started.Cleanup.Counters;
            started.Cleanup.LogPhaseForTests = Scripted(
                _ => new SliceResult(SliceStatus.Progressed, MaintenanceReason.None, 0) { GateElapsed = TimeSpan.FromMilliseconds(1500), Elapsed = TimeSpan.FromSeconds(4) },
                _ => new SliceResult(SliceStatus.Disabled, MaintenanceReason.DeletesDisabled, 0)
                {
                    Gate = new GateResult(GateVerdict.Passed),
                    Issued = 2,
                    GateElapsed = TimeSpan.FromMilliseconds(250),
                    Elapsed = TimeSpan.FromSeconds(1)
                });
            await TurnAsync(started);

            // The kill switch stops issuance after the gate passed and two deletes settled
            await TurnAsync(started);

            Assert.Equal(RetentionState.Paused, started.Cleanup.AdvanceGaugeState);
            Assert.Equal((250.0, 1000.0), (counters.LastGateMs, counters.LastSliceMs));
            Assert.Equal(1, counters.Probes(ProbeResult.Absent));
        }

        [Fact]
        public async Task ClassTallyMatchesTheStoresRemovesAndTheExcludedPath()
        {
            var table = await _kit.ChurnedAsync(local: false);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            // Slices of two paths: progress records settle and their predecessors go
            cleanup.Settings = cleanup.Settings with { MaxPaths = 2 };
            var ledger = new CapacityLedger(() => default);
            var hooks = table.Hooks!;
            hooks.ClearRequests();
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Kind);
            var active = cleanup.ActiveForTests!;
            for (int turn = 0; turn < 40 && active.Phase == AdvancePhase.Log; turn++)
            {
                Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
            }
            Assert.Equal(AdvancePhase.Payload, active.Phase);
            // Another writer re-adds a remaining planned path, which the rebase excludes
            using (hooks.Unrecorded())
            {
                await WriteCommit(table.Raw, "t", ++table.Head, DeltaLakeCheckpointBuildTests.Add("part-0009.parquet"));
            }
            Assert.Equal(OutcomeKind.NoWork, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Kind);
            Assert.Equal(1, active.Exclusions.Count);

            await _kit.ExecuteAsync(cleanup, table, ledger);

            // Each remove the store received by its class, and the excluded path kept, through the teardown
            Assert.True(await table.Raw.Exists("/t/part-0009.parquet"));
            var expected = new long[20];
            foreach (var request in hooks.Requests.Where(x => x.StartsWith("Rm ", StringComparison.Ordinal)))
            {
                var name = request.Substring(request.LastIndexOf('/') + 1);
                var kind = !request.Contains(DeltaTransactionReader.DeltaLogDirName, StringComparison.Ordinal) ? TargetKind.Payload
                    : name.StartsWith(DeltaTransactionReader.CleanupControlPrefix, StringComparison.Ordinal) ? TargetKind.Control : TargetKind.Log;
                expected[(int)kind * 4 + (int)DeleteStatus.Deleted]++;
            }
            expected[(int)TargetKind.Payload * 4 + (int)DeleteStatus.Kept] = 1;
            Assert.Equal(expected, Cells(cleanup.Counters));
            Assert.All(new[] { TargetKind.Log, TargetKind.Payload, TargetKind.Control }, kind => Assert.True(expected[(int)kind * 4 + (int)DeleteStatus.Deleted] > 0, kind.ToString()));
        }

        [Fact]
        public async Task TeardownDeletesAreCounted()
        {
            var table = await _kit.ChurnedAsync();
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            long[]? atTeardown = null;
            var records = 0;
            cleanup.AdvanceHooksForTests.Step = async step =>
            {
                if (step == AdvanceSteps.Teardown)
                {
                    atTeardown = Cells(cleanup.Counters);
                    records = (await table.ControlsAsync()).Length;
                }
            };
            var ledger = new CapacityLedger(() => default);

            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Kind);
            await _kit.ExecuteAsync(cleanup, table, ledger);

            // Every record but the done went through the teardown's executor, and nothing else
            Assert.True(records > 1);
            Assert.Single(await table.ControlsAsync());
            Assert.Equal(ControlDeletes(records - 1), Cells(cleanup.Counters).Zip(atTeardown!, (after, before) => after - before));
        }

        // Only the given control deletes, class-major
        private static long[] ControlDeletes(long deleted)
        {
            var cells = new long[20];
            cells[(int)TargetKind.Control * 4 + (int)DeleteStatus.Deleted] = deleted;
            return cells;
        }

        [Fact]
        public async Task RestartKnowsOAndAAgainAtPreparationAndCountsItsTeardown()
        {
            var table = await _kit.ChurnedAsync();
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var counters = new RetentionCounters();
            DeltaTableCleanup Cleanup() => new DeltaTableCleanup(table.Configuration, table.Metered, new FakeTimeProvider(s_now), "t", null, () => table.Raw, new DiscardCounters(), null, "t", null, counters);
            var first = Cleanup();
            // The done's teardown fails, leaving its records for a later recovery
            first.AdvanceHooksForTests.Step = step => step == AdvanceSteps.Teardown ? throw new IOException("Teardown fails") : Task.CompletedTask;
            var ledger = new CapacityLedger(() => default);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(first, _kit.AdvanceContext(table.Head, ledger))).Kind);
            var started = first.ActiveForTests!.Started;
            var c = first.ActiveForTests.C;
            await _kit.ExecuteAsync(first, table, ledger);
            Assert.Equal(c, first.OldestVersion);
            var records = (await table.ControlsAsync()).Length;
            Assert.True(records > 1);
            var before = Cells(counters);

            // A restarted run's cleanup over the same counters knows neither O nor A
            var restarted = Cleanup();
            Assert.Equal((-1L, -1L, -1.0), (restarted.OldestVersion, restarted.AdoptionVersion, restarted.BacklogSeconds));
            Assert.Equal(before, Cells(counters));

            // Its next advance job's preparation tears the done advance down and learns O and A
            await AdvanceExecutionKit.JobAsync(restarted, _kit.AdvanceContext(table.Head, ledger));
            Assert.Single(await table.ControlsAsync());
            Assert.Equal(ControlDeletes(records - 1), Cells(counters).Zip(before, (after, previous) => after - previous));
            Assert.Equal((c, started.A), (restarted.OldestVersion, restarted.AdoptionVersion));
        }

        [Fact]
        public async Task StagedSweepDeletesAreCountedByClass()
        {
            var table = await _kit.ChurnedAsync();
            // A stage whose commit at 15 was published from it
            var staged = await DeltaTransactionWriter.StageCommit(table.Raw, "t", 15, new List<DeltaAction>()
            {
                new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = "stage-15", AdoptedAt = 0, Timestamp = 2015, Operation = "WRITE" } },
                DeltaLakeCheckpointBuildTests.Add("part-0015.parquet")
            });
            await CleanupRecordKit.PlantAsync(table.Raw, "t", $"{15:D20}.json", await ReadBytes(table.Raw, table.LogPath(staged.FileName)));
            table.Head = 15;
            var cleanup = AdvanceExecutionKit.Cleanup(table);

            Assert.Equal(OutcomeKind.Completed, (await cleanup.RunAsync(JobKind.StagedSweep, JobContext(JobKind.StagedSweep, table.Head))).Kind);

            Assert.False(await table.Raw.Exists(table.LogPath(staged.FileName)));
            Assert.Equal(1, cleanup.LastSweepForTests!.LogFilesDeleted);
            Assert.Equal(1, cleanup.Counters.Deletes(TargetKind.Stage, DeleteStatus.Deleted));
            Assert.Equal(1, Cells(cleanup.Counters).Sum());
        }

        // Each class and outcome's count, class-major
        private static long[] Cells(RetentionCounters counters)
        {
            var cells = new long[20];
            foreach (var kind in Enum.GetValues<TargetKind>())
            {
                foreach (var status in Enum.GetValues<DeleteStatus>())
                {
                    cells[(int)kind * 4 + (int)status] = counters.Deletes(kind, status);
                }
            }
            return cells;
        }

        [Fact]
        public async Task PlannerPathRejectionIsCountedOncePerJob()
        {
            var table = await _kit.ChurnedAsync();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            var reason = MaintenanceReason.PathRejected;
            cleanup.AdvanceForTests.PlanForTests = (_, _, _, _, _, _) => Task.FromResult(new AdvancePlan() { Status = AdvancePlanStatus.Declined, Reason = reason, Detail = "A planned path is rejected" });

            var rejected = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head));
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.PathRejected), (rejected.Kind, rejected.Reason));
            Assert.Equal(1, cleanup.Counters.PathRejections);

            // Another decline is not a rejection
            reason = MaintenanceReason.CleanupGate;
            Assert.Equal(MaintenanceReason.CleanupGate, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head))).Reason);
            Assert.Equal(1, cleanup.Counters.PathRejections);
        }

        // An eligible local table at t with a queued run's cleanup and its producer handle
        private sealed class DrainRig
        {
            public required string TableDir { get; init; }

            public required HookFileStorage Storage { get; init; }

            public required DeltaTableCleanup Cleanup { get; init; }

            public required DiscardHandle Producer { get; init; }

            public required RetentionCounters Counters { get; init; }

            public IFileStorage? Current { get; set; }

            public string Put(string relative)
            {
                var path = Path.Combine(TableDir, relative.Replace('/', Path.DirectorySeparatorChar));
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, "x");
                return relative;
            }

            public bool Exists(string relative) => File.Exists(Path.Combine(TableDir, relative.Replace('/', Path.DirectorySeparatorChar)));

            public void Queue(long version, params string[] paths) => Producer.Entry.Enqueue(new DiscardGroup(TableId, 0, version, $"stage-{version}", paths));
        }

        private async Task<DrainRig> DrainRigAsync(long head, Func<IFileStorage, IFileStorage>? below = null)
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(Path.Combine(root, "t"));
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var storage = OwnedHookFileStorage.Over(below?.Invoke(local) ?? local);
            var configuration = CleanupConfiguration.Validate(storage, "t", false, CleanupOptIns.Off);
            Assert.True(configuration.Store.Eligible);
            var handOff = new DiscardHandOff();
            var counters = new RetentionCounters();
            DrainRig? rig = null;
            var metered = MeteredFileStorage.Wrap(storage, MaintenanceBucketsTests.Unthrottled(), ListingPaging.LocalDisk);
            var cleanup = new DeltaTableCleanup(configuration, metered, TimeProvider.System, "t", handOff, () => rig!.Current, new DiscardCounters(), null, "t", null, counters);
            rig = new DrainRig()
            {
                TableDir = Path.Combine(root, "t"),
                Storage = storage,
                Cleanup = cleanup,
                Producer = handOff.Acquire(DiscardRoute.Of(storage, "t")),
                Counters = counters,
                Current = storage
            };
            var metaData = Metadata(UserSchema);
            metaData.MetaData!.Id = TableId;
            await WriteOwnCommit(storage, "t", 0, 0, 1000, Protocol(), metaData);
            for (long version = 1; version <= head; version++)
            {
                await WriteOwnCommit(storage, "t", version, 0, 1000 + version, DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet"));
            }
            return rig;
        }

        // A short admission of the kind, a delete turn's unless given
        private CleanupContext JobContext(JobKind kind = JobKind.Deletion, long head = -1)
        {
            var clock = new FakeTimeProvider(s_now);
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()))).CreateJob();
            _disposables.Add(scratch);
            return new CleanupContext(new JobRun(head, kind) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = kind,
                Long = false,
                ReadLimit = MaintenanceWorkerSettings.DefaultShortReads,
                Deadline = s_now + TimeSpan.FromMinutes(15),
                Head = head,
                TableId = null,
                Scratch = scratch,
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default
            };
        }

        [Fact]
        public async Task DrainCountsItsDeletesRejectionsAndAbsentSuccessor()
        {
            var rig = await DrainRigAsync(1);
            var a = rig.Put("_change_data/a.parquet");
            var b = rig.Put("_change_data/b.parquet");
            // A name inside the log is never a discarded output's
            rig.Queue(1, a, b, "_delta_log/misplaced.parquet");

            Assert.Equal(3, (await rig.Cleanup.DrainHintsAsync(JobContext(), 128)).Drained);

            Assert.False(rig.Exists(a) || rig.Exists(b));
            Assert.Equal((2L, 1L, 0L, 0L), (rig.Counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted), rig.Counters.Deletes(TargetKind.Payload, DeleteStatus.Kept),
                rig.Counters.Deletes(TargetKind.Payload, DeleteStatus.Absent), rig.Counters.Deletes(TargetKind.Payload, DeleteStatus.Failed)));
            Assert.Equal(1, rig.Counters.PathRejections);
            Assert.Equal((1L, 0L, 0L), (rig.Counters.Probes(ProbeResult.Absent), rig.Counters.Probes(ProbeResult.Present), rig.Counters.Probes(ProbeResult.Error)));
            Assert.All(Enum.GetValues<ListingCheck>(), check => Assert.Equal(0, rig.Counters.ListingDeclines(check)));
        }

        [Fact]
        public async Task DrainCountsAPresentSuccessorAndAFailedProbe()
        {
            var rig = await DrainRigAsync(1);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(1, a);
            // The listing does not show commit 2, which the probe finds
            await WriteOwnCommit(rig.Storage, "t", 2, 0, 1002, DeltaLakeCheckpointBuildTests.Add("part-0002.parquet"));
            rig.Storage.Hidden = path => path.Full.EndsWith($"{2:D20}.json", StringComparison.Ordinal);

            await rig.Cleanup.DrainHintsAsync(JobContext(), 128);
            Assert.Equal((1L, 1L), (rig.Counters.ListingDeclines(ListingCheck.SuccessorPresent), rig.Counters.Probes(ProbeResult.Present)));

            // The probe itself fails
            rig.Storage.Hidden = null;
            rig.Storage.Before = (verb, path) => verb == "Stat" && path.Full.EndsWith($"{3:D20}.json", StringComparison.Ordinal) ? throw new IOException("probe failed") : Task.CompletedTask;
            await rig.Cleanup.DrainHintsAsync(JobContext(), 128);

            Assert.True(rig.Exists(a));
            Assert.Equal((1L, 1L), (rig.Counters.ListingDeclines(ListingCheck.ProbeError), rig.Counters.Probes(ProbeResult.Error)));
            Assert.Equal(0, rig.Counters.Probes(ProbeResult.Absent));
            Assert.Equal(0, rig.Counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted));
        }

        [Fact]
        public async Task StorageChangeDeclinesAreCounted()
        {
            var rig = await DrainRigAsync(1);
            rig.Queue(1, rig.Put("_change_data/a.parquet"));
            var replacement = Files.Of.LocalDisk(Path.GetDirectoryName(rig.TableDir)!);
            _disposables.Add(replacement);
            rig.Current = replacement;

            await rig.Cleanup.DrainHintsAsync(JobContext(), 128);

            Assert.Equal(1, rig.Counters.ListingDeclines(ListingCheck.StorageChanged));
            Assert.Equal(0, rig.Counters.Probes(ProbeResult.Absent));
            Assert.True(rig.Exists("_change_data/a.parquet"));
        }

        [Fact]
        public async Task ChangedStorageIsCountedOncePerDrop()
        {
            var rig = await DrainRigAsync(1);
            rig.Queue(1, rig.Put("_change_data/a.parquet"));
            var replacement = Files.Of.LocalDisk(Path.GetDirectoryName(rig.TableDir)!);
            _disposables.Add(replacement);
            rig.Current = replacement;

            // Every delete turn drains the route again, only the first drops hints
            for (int turn = 0; turn < 3; turn++)
            {
                await rig.Cleanup.DrainHintsAsync(JobContext(), 128);
            }
            Assert.Equal(1, rig.Counters.ListingDeclines(ListingCheck.StorageChanged));

            // Hints queued later are dropped and counted again
            rig.Queue(2, rig.Put("_change_data/b.parquet"));
            await rig.Cleanup.DrainHintsAsync(JobContext(), 128);
            Assert.Equal(2, rig.Counters.ListingDeclines(ListingCheck.StorageChanged));
            Assert.True(rig.Exists("_change_data/a.parquet") && rig.Exists("_change_data/b.parquet"));
        }

        [Fact]
        public async Task DrainSliceAfterAStorageChangeIsAStorageDecline()
        {
            var rig = await DrainRigAsync(1);
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(1, a);
            var replacement = Files.Of.LocalDisk(Path.GetDirectoryName(rig.TableDir)!);
            _disposables.Add(replacement);
            // The sink's storage is replaced once the turn's listing probed its successor
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "Stat" && path.Full.EndsWith($"{2:D20}.json", StringComparison.Ordinal))
                {
                    rig.Current = replacement;
                }
                return Task.CompletedTask;
            };

            await rig.Cleanup.DrainHintsAsync(JobContext(), 128);

            Assert.Equal((1L, 1L), (rig.Counters.ListingDeclines(ListingCheck.StorageChanged), rig.Counters.Probes(ProbeResult.Absent)));
            Assert.True(rig.Exists(a));
        }

        [Fact]
        public async Task EstablishmentAfterAStorageChangeIsAStorageDecline()
        {
            TestStorage? below = null;
            var rig = await DrainRigAsync(1, local => below = new TestStorage(local));
            var a = rig.Put("_change_data/a.parquet");
            rig.Queue(1, a);
            var replacement = Files.Of.LocalDisk(Path.GetDirectoryName(rig.TableDir)!);
            _disposables.Add(replacement);
            // The storage below the meter is replaced while the turn lists the log
            below!.BeforeLs = folder =>
            {
                if (folder.Full.Contains(DeltaTransactionReader.DeltaLogDirName, StringComparison.Ordinal))
                {
                    below.Inner = replacement;
                }
                return Task.CompletedTask;
            };

            await rig.Cleanup.DrainHintsAsync(JobContext(), 128);

            Assert.Equal((1L, 1L), (rig.Counters.ListingDeclines(ListingCheck.StorageChanged), rig.Counters.Probes(ProbeResult.Absent)));
            Assert.True(rig.Exists(a));
        }

        [Theory]
        [InlineData(nameof(ListingCheck.Passed), nameof(ProbeResult.Absent))]
        [InlineData(nameof(ListingCheck.SuccessorPresent), nameof(ProbeResult.Present))]
        [InlineData(nameof(ListingCheck.ProbeError), nameof(ProbeResult.Error))]
        [InlineData(nameof(ListingCheck.Overflow), nameof(ProbeResult.Error))]
        [InlineData(nameof(ListingCheck.RangeGap), null)]
        [InlineData(nameof(ListingCheck.StorageChanged), null)]
        public void ListingCountsItsProbeAndItsDecline(string check, string? probe)
        {
            var counters = new RetentionCounters();
            var listed = Enum.Parse<ListingCheck>(check);

            counters.AddListing(listed);

            Assert.Equal(Enum.GetValues<ProbeResult>().Select(x => x.ToString() == probe ? 1L : 0L), Enum.GetValues<ProbeResult>().Select(counters.Probes));
            Assert.Equal(Enum.GetValues<ListingCheck>().Select(x => x == listed && x != ListingCheck.Passed ? 1L : 0L), Enum.GetValues<ListingCheck>().Select(counters.ListingDeclines));
        }

        [Fact]
        public async Task BucketOldestWaitShowsHowLongTheHeadWaited()
        {
            var clock = new FakeTimeProvider(s_now);
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true)
            {
                Buckets = new MaintenanceBuckets(clock, requestsPerSecond: 1, requestBurst: 1)
            };
            using var meter = new Meter("bucket-wait-" + Guid.NewGuid().ToString("N"));
            DeltaMaintenanceBudget.SharedBudget.RegisterGauges(meter, budget);
            await budget.Buckets.Requests.AcquireAsync(1);
            Assert.Equal(0, ProcessGauge(meter, "flowtide_delta_maintenance_bucket_oldest_wait_seconds"));

            // The second request waits for the next token, a third behind it
            var waiter = budget.Buckets.Requests.AcquireAsync(1).AsTask();
            clock.Advance(TimeSpan.FromMilliseconds(300));
            var behind = budget.Buckets.Requests.AcquireAsync(1).AsTask();
            clock.Advance(TimeSpan.FromMilliseconds(100));
            Assert.Equal(0.4, ProcessGauge(meter, "flowtide_delta_maintenance_bucket_oldest_wait_seconds"), 6);
            // The gate wait gauge is the gates' alone
            Assert.Equal(0, ProcessGauge(meter, "flowtide_delta_maintenance_oldest_wait_seconds"));

            // Once the head is granted, the one behind it is the head
            clock.Advance(TimeSpan.FromMilliseconds(600));
            await waiter.WaitAsync(s_wait);
            Assert.Equal(0.7, ProcessGauge(meter, "flowtide_delta_maintenance_bucket_oldest_wait_seconds"), 6);
            clock.Advance(TimeSpan.FromSeconds(1));
            await behind.WaitAsync(s_wait);
            Assert.Equal(0, ProcessGauge(meter, "flowtide_delta_maintenance_bucket_oldest_wait_seconds"));
        }

        // One observation of a process gauge registered on this meter
        private static double ProcessGauge(Meter meter, string name)
        {
            double? value = null;
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, owner) =>
            {
                if (ReferenceEquals(instrument.Meter, meter) && instrument.Name == name)
                {
                    owner.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<double>((_, measurement, _, _) => value = measurement);
            listener.Start();
            listener.RecordObservableInstruments();
            Assert.True(value.HasValue, $"No {name} gauge");
            return value.Value;
        }

        // Every measurement of a gauge of the stream's sink, long, int or double, with its tags
        internal static List<(double Value, Dictionary<string, string> Tags)> SinkGauge(DeltaLakeSinkStream stream, string name)
        {
            var measurements = new List<(double, Dictionary<string, string>)>();
            var found = false;
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, owner) =>
            {
                if (instrument.Meter.Name.StartsWith($"flowtide.{stream.StreamName}.operator.", StringComparison.Ordinal) && instrument.Name == "flowtide_" + name)
                {
                    found = true;
                    owner.EnableMeasurementEvents(instrument);
                }
            };
            void Add(double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
            {
                var copy = new Dictionary<string, string>();
                foreach (var tag in tags)
                {
                    copy[tag.Key] = (string)tag.Value!;
                }
                measurements.Add((value, copy));
            }
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) => Add(value, tags));
            listener.SetMeasurementEventCallback<int>((_, value, tags, _) => Add(value, tags));
            listener.SetMeasurementEventCallback<double>((_, value, tags, _) => Add(value, tags));
            listener.Start();
            listener.RecordObservableInstruments();
            Assert.True(found, $"No {name} gauge");
            return measurements;
        }

        private static double SinkValue(DeltaLakeSinkStream stream, string name) => Assert.Single(SinkGauge(stream, name)).Value;

        // Each measurement's tag, which must be the snake case name of a value of the enum, and every value appears once
        private static void AssertEnumTags<T>(List<(double Value, Dictionary<string, string> Tags)> measurements, string key, params T[] excluded) where T : struct, Enum
        {
            var expected = Enum.GetValues<T>().Except(excluded).Select(x => RetentionCounters.TagOf(x)).Order(StringComparer.Ordinal).ToList();
            var tags = measurements.Select(x => x.Tags[key]).Distinct().Order(StringComparer.Ordinal).ToList();
            Assert.Equal(expected, tags);
            Assert.All(tags, tag => Assert.Matches("^[a-z]+(_[a-z]+)*$", tag));
        }

        [Fact]
        public async Task SinkGaugesCarryOnlyEnumTagsAndFollowAnAdvance()
        {
            var testName = nameof(SinkGaugesCarryOnlyEnumTagsAndFollowAnAdvance);
            var directory = StoreFixtures.TempDirectory(testName);
            using var storage = Files.Of.LocalDisk(directory);
            var clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.HistoryRetention = TimeSpan.FromDays(1);
                options.CheckpointInterval = 0;
                options.TimeProvider = clock;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(5);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            // Before any advance: nothing known, coverage not yet confirmed
            Assert.Equal(-1, SinkValue(stream, "delta_retention_oldest_version"));
            Assert.Equal(-1, SinkValue(stream, "delta_retention_adoption_version"));
            Assert.Equal(-1, SinkValue(stream, "delta_retention_backlog_seconds"));
            Assert.Equal((int)RetentionState.Coverage, SinkValue(stream, "delta_retention_state"));
            foreach (var name in new[] { "delta_retention_repair_wait_seconds", "delta_retention_repair_blocked", "delta_retention_payload_remaining", "delta_retention_failed_slices",
                "delta_retention_last_gate_ms", "delta_retention_last_slice_ms", "delta_cleanup_path_rejections" })
            {
                Assert.Equal(0, SinkValue(stream, name));
            }
            var deletes = SinkGauge(stream, "delta_cleanup_deletes");
            Assert.Equal(20, deletes.Count);
            AssertEnumTags<TargetKind>(deletes, "class");
            AssertEnumTags<DeleteStatus>(deletes, "outcome");
            Assert.Equal(20, deletes.Select(x => (x.Tags["class"], x.Tags["outcome"])).Distinct().Count());
            var declines = SinkGauge(stream, "delta_cleanup_listing_declines");
            AssertEnumTags(declines, "check", ListingCheck.Passed);
            var probes = SinkGauge(stream, "delta_cleanup_successor_probes");
            AssertEnumTags<ProbeResult>(probes, "result");
            Assert.Equal(new[] { "absent", "present", "error" }, probes.Select(x => x.Tags["result"]));

            // The hand-off confirms coverage, then a day passes with each commit until the advance to 1 is done
            await stream.Maintenance.TickAndSettleAsync(0);
            Assert.Equal((int)RetentionState.Idle, SinkValue(stream, "delta_retention_state"));
            for (long version = 1; version <= 3; version++)
            {
                clock.Advance(TimeSpan.FromDays(1));
                stream.Generate(5);
                await WaitForVersion(storage, "test", stream, version);
                await stream.Maintenance.TickAndSettleAsync(version);
            }
            var sink = Assert.Single(stream.Sinks);
            var counters = sink.RetentionCountersForTests;
            Assert.Equal(1, SinkValue(stream, "delta_retention_oldest_version"));
            Assert.Equal((int)RetentionState.Idle, SinkValue(stream, "delta_retention_state"));
            Assert.Equal(0, SinkValue(stream, "delta_retention_backlog_seconds"));
            Assert.Equal(0, SinkValue(stream, "delta_retention_payload_remaining"));
            Assert.True(counters.Deletes(TargetKind.Log, DeleteStatus.Deleted) > 0);
            deletes = SinkGauge(stream, "delta_cleanup_deletes");
            Assert.Equal(counters.Deletes(TargetKind.Log, DeleteStatus.Deleted), Assert.Single(deletes, x => x.Tags["class"] == "log" && x.Tags["outcome"] == "deleted").Value);

            // Distinct values for the gauges read at zero so far
            counters.AddSlice(TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(4));
            for (int i = 0; i < 3; i++)
            {
                counters.AddPathRejection();
            }
            var table = await _kit.ChurnedAsync();
            var kitCleanup = AdvanceExecutionKit.Cleanup(table);
            var published = await _kit.PublishAsync(kitCleanup, table);
            var held = await _kit.AdoptAsync(kitCleanup, table);
            // A suspended advance no turn runs, one planned path left after two failed slices
            held.State = AdvanceState.Suspended;
            held.Processed = published.Started.Totals.Emitted - 1;
            held.Failures = 2;
            ((DeltaTableCleanup)sink.StartedWorkerForTests!.Cleanup!).Adopt(held);
            Assert.Equal(250, SinkValue(stream, "delta_retention_last_gate_ms"));
            Assert.Equal(4000, SinkValue(stream, "delta_retention_last_slice_ms"));
            Assert.Equal(3, SinkValue(stream, "delta_cleanup_path_rejections"));
            Assert.Equal(1, SinkValue(stream, "delta_retention_payload_remaining"));
            Assert.Equal(2, SinkValue(stream, "delta_retention_failed_slices"));
        }

        [Fact]
        public async Task SinkRepairWaitIsTheWorkersWaitAndNotItsBlock()
        {
            var testName = nameof(SinkRepairWaitIsTheWorkersWaitAndNotItsBlock);
            var directory = StoreFixtures.TempDirectory(testName);
            using var storage = Files.Of.LocalDisk(directory);
            var clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.HistoryRetention = TimeSpan.FromDays(1);
                options.CheckpointInterval = 0;
                options.TimeProvider = clock;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(5);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            // Another writer commits above the sink's head before coverage is discovered
            await WriteCommit(storage, "test", 1, new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["operation"] = "FOREIGN" } } });

            await stream.Maintenance.TickAndSettleAsync(0);
            clock.Advance(TimeSpan.FromSeconds(90));

            Assert.Equal((int)RetentionState.Coverage, SinkValue(stream, "delta_retention_state"));
            Assert.Equal(90, SinkValue(stream, "delta_retention_repair_wait_seconds"));
            Assert.Equal(0, SinkValue(stream, "delta_retention_repair_blocked"));
        }

        [Fact]
        public async Task SinkRepairThatCannotSucceedShowsBlocked()
        {
            var testName = nameof(SinkRepairThatCannotSucceedShowsBlocked);
            var directory = StoreFixtures.TempDirectory(testName);
            using var storage = Files.Of.LocalDisk(directory);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.HistoryRetention = TimeSpan.FromDays(1);
                options.CheckpointInterval = 0;
                options.TimeProvider = new FakeTimeProvider(new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero));
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(5);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            // Another writer enables a feature no checkpoint build supports above the sink's head
            await WriteCommit(storage, "test", 1, ProtocolWith("catalogManaged"));

            // The worker asks for an anchor, whose repair build then declines
            await stream.Maintenance.TickAndSettleAsync(0);
            await WaitForVersionCheckpointing(storage, "test", stream, 2);
            await stream.Maintenance.TickAndSettleAsync(2);

            Assert.Equal(1, SinkValue(stream, "delta_retention_repair_blocked"));
            Assert.Equal((int)RetentionState.RepairBlocked, SinkValue(stream, "delta_retention_state"));
        }
    }
}

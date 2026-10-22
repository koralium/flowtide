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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Text;
using FakeCleanup = FlowtideDotNet.Connector.DeltaLake.Tests.DeltaMaintenanceSchedulingTests.FakeCleanup;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RetentionDemandTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 12, 0, 0, 0, TimeSpan.Zero);

        // Bounded: a job that never settles fails the test
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "rd-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();

        public void Dispose()
        {
            _owners.Release();
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

        private sealed class Rig
        {
            public required FakeTimeProvider BudgetClock { get; init; }

            public required FakeTimeProvider OptionsClock { get; init; }

            public required DeltaMaintenanceBudget Budget { get; init; }

            public required HookFileStorage Storage { get; init; }

            public required DeltaMaintenanceWorker Worker { get; init; }

            public required DeltaMaintenanceWorkerTests.CollectingLogger Logs { get; init; }

            // One tick and the job it dispatched, if any
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
        }

        // Retention on an eligible store; interval 100 keeps periodic builds out of the way
        private Rig Create(int interval = 100, bool periodic = true, bool rebuildLegacy = false, IMaintenanceCleanup? cleanup = null, HookFileStorage? storage = null, string? tableId = null,
            long shortReads = MaintenanceWorkerSettings.DefaultShortReads, TimeSpan? longDeadline = null)
        {
            storage ??= new HookFileStorage(Files.Of.InternalMemory($"./retention-{Guid.NewGuid():N}"));
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, ScratchOwners.ShortId())), new FakeTimeProvider(s_now), manualTick: true)
            {
                Buckets = MaintenanceBucketsTests.Unthrottled()
            };
            var options = new FakeTimeProvider(s_now);
            var settings = new MaintenanceWorkerSettings(interval, null, options)
            {
                PeriodicEnabled = periodic,
                InspectionEnabled = true,
                CleanupEligible = true,
                Retention = TimeSpan.FromDays(7),
                RebuildLegacyCheckpoints = rebuildLegacy,
                ShortReads = shortReads
            };
            if (longDeadline is { } deadline)
            {
                settings = settings with { LongDeadline = deadline };
            }
            var logs = new DeltaMaintenanceWorkerTests.CollectingLogger();
            var worker = new DeltaMaintenanceWorker(budget, settings, storage, "t", "t", logs) { Cleanup = cleanup };
            budget.Register(worker);
            if (tableId != null)
            {
                worker.SetTableId(tableId);
            }
            return new Rig()
            {
                BudgetClock = (FakeTimeProvider)budget.Clock,
                OptionsClock = options,
                Budget = budget,
                Storage = storage,
                Worker = worker,
                Logs = logs
            };
        }

        // An own commit adopted at the epoch; commit 0 also holds protocol and metaData
        private static Task Own(IFileStorage storage, long version, long adoptedAt, params DeltaAction[] extra)
        {
            var actions = new List<DeltaAction>();
            if (version == 0)
            {
                actions.Add(DeltaTestKit.Protocol());
                actions.Add(DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            }
            actions.AddRange(extra);
            actions.Add(DeltaLakeCheckpointBuildTests.Add($"f{version}.parquet"));
            return DeltaTestKit.WriteOwnCommit(storage, "t", version, adoptedAt, s_now.ToUnixTimeMilliseconds() + version, actions.ToArray());
        }

        private static async Task OwnRange(IFileStorage storage, long from, long to, long adoptedAt)
        {
            for (var version = from; version <= to; version++)
            {
                await Own(storage, version, adoptedAt);
            }
        }

        // Commits without commitInfo, as a Phase 3 sink or another writer leaves them
        private static Task Legacy(IFileStorage storage, long from, long to) => DeltaMaintenanceWorkerTests.Commits(storage, from, to);

        // A marked checkpoint built and published as a worker would, rooted as Auto chooses
        private async Task Build(IFileStorage storage, long version)
        {
            var directory = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            try
            {
                using var scratch = directory.CreateJob();
                var context = new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = "t",
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Clock = new FakeTimeProvider(s_now)
                };
                var result = await CheckpointBuilder.BuildAsync(context, new BuildRequest() { Version = version });
                Assert.Equal(BuildStatus.Completed, result.Status);
            }
            finally
            {
                _owners.Release(directory);
            }
        }

        private static Task<List<long>> Checkpoints(Rig rig) => DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage);

        private static string CheckpointName(long version) => $"{version:D20}.checkpoint.parquet";

        [Fact]
        public async Task PhaseThreeBornTableUpgradeBuildsAutoAtT()
        {
            var rig = Create();
            // Phase 3 left commits without an epoch and a marked checkpoint over the whole history
            await Legacy(rig.Storage, 0, 5);
            await Build(rig.Storage, 4);
            // The first Phase 4 commit adopts at its own version
            await Own(rig.Storage, 6, 6);
            rig.Worker.Published(6);

            // Root 4 is marked and gap-free but below the epoch: Auto at 6
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(6, RootPolicy.Auto), rig.Worker.PendingDemand);
            Assert.True(rig.Worker.RepairPending);
            Assert.False(rig.Worker.CoverageConfirmed);

            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));
            Assert.Equal(4, rig.Worker.LastBuild!.RootVersion);
            Assert.False(rig.Worker.LastBuild.InheritedGap);
            // Held until a destination read proves the coverage
            Assert.True(rig.Worker.RepairPending);

            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
            Assert.False(rig.Worker.RepairPending);
            Assert.Null(rig.Worker.PendingDemand);
        }

        [Fact]
        public async Task ForeignCommitOnTruncatedTableRepairsWithAuto()
        {
            var rig = Create();
            // Our own history with a marked checkpoint at 5, then commits below 3 truncated
            await OwnRange(rig.Storage, 0, 8, 0);
            await Build(rig.Storage, 5);
            for (long version = 0; version < 3; version++)
            {
                await rig.Storage.Rm(DeltaTestKit.CommitPath("t", version));
            }
            // A foreign commit, then our next commit adopts at its own version
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 9, DeltaLakeCheckpointBuildTests.Add("foreign.parquet"));
            await Own(rig.Storage, 10, 10);
            rig.Worker.Published(10);

            // Without history from zero, the marked gap-free root 5 still repairs with Auto
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(10, RootPolicy.Auto), rig.Worker.PendingDemand);
            await rig.TickAsync();
            Assert.Equal(new long[] { 5, 10 }, await Checkpoints(rig));
            Assert.Equal(5, rig.Worker.LastBuild!.RootVersion);
            Assert.False(rig.Worker.LastBuild.InheritedGap);
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
            Assert.False(rig.Worker.RepairPending);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task LegacyLineageRepairsFromZero(bool rebuildLegacy)
        {
            var rig = Create(rebuildLegacy: rebuildLegacy);
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await Own(rig.Storage, 7, 7);
            rig.Worker.Published(7);

            // The unmarked root would pass its gap on: a replay from zero
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), rig.Worker.PendingDemand);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 7 }, await Checkpoints(rig));
            Assert.Null(rig.Worker.LastBuild!.RootVersion);
            Assert.False(rig.Worker.LastBuild.InheritedGap);
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        [Fact]
        public async Task NoPeriodicBuildOvertakesPendingRepair()
        {
            var rig = Create(interval: 2);
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            rig.Worker.Published(6);

            // The head has no epoch: an anchor is asked for, and the inspection builds nothing although the interval is reached
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);
            Assert.Equal(new long[] { 4 }, await Checkpoints(rig));

            // Versions keep arriving while the anchor is outstanding, and no periodic build runs
            await Legacy(rig.Storage, 7, 10);
            rig.Worker.Published(10);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(new long[] { 4 }, await Checkpoints(rig));
            Assert.True(rig.Worker.Idle);

            // The anchor at 11 is staged and handed off; the repair replays from zero
            await Own(rig.Storage, 11, 11);
            rig.Worker.AnchorStaged(generation, 11);
            rig.Worker.Published(11);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(11, RootPolicy.None), rig.Worker.PendingDemand);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 11 }, await Checkpoints(rig));
            await rig.TickAsync();
            Assert.False(rig.Worker.RepairPending);

            // Periodic builds resume and inherit the repair's coverage
            await Own(rig.Storage, 12, 11);
            await Own(rig.Storage, 13, 11);
            rig.Worker.Published(13);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 11, 13 }, await Checkpoints(rig));
            Assert.Equal(11, rig.Worker.LastBuild!.RootVersion);
            Assert.False(rig.Worker.LastBuild.InheritedGap);
        }

        [Fact]
        public async Task RepairDeclineUnblocksPeriodicBuilds()
        {
            var rig = Create(interval: 2);
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            // The first Phase 4 commit enables a feature no checkpoint build supports
            await Own(rig.Storage, 7, 7, DeltaTestKit.ProtocolWith("catalogManaged"));
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), rig.Worker.PendingDemand);

            // A decline a retry cannot fix blocks the repair for a day instead of backing off
            await rig.TickAsync();
            Assert.Equal((rig.OptionsClock.GetUtcNow() + DeltaMaintenanceWorker.BlockedFor, MaintenanceReason.ExcludedFeature), rig.Worker.Blocked!.Value);
            Assert.False(rig.Worker.RepairPending);
            Assert.Null(rig.Worker.PendingDemand);
            Assert.Equal(0, rig.Worker.DemandFailures);
            Assert.Equal(1, rig.Logs.Warnings("ExcludedFeature"));

            // Periodic builds resume
            await Own(rig.Storage, 8, 7, DeltaTestKit.Protocol());
            rig.Worker.Published(8);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 8 }, await Checkpoints(rig));
            Assert.Equal(4, rig.Worker.LastBuild!.RootVersion);
            Assert.NotNull(rig.Worker.Blocked);
        }

        [Fact]
        public async Task RepairPastItsLongDeadlineBlocksAndPeriodicBuildsResume()
        {
            // A replay from zero needs long admission, and its long deadline always passes
            var rig = Create(interval: 2, shortReads: 7, longDeadline: TimeSpan.Zero);
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await Own(rig.Storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), rig.Worker.PendingDemand);

            // Three simulated days with two commits an hour
            var version = 7L;
            var blocks = new HashSet<DateTimeOffset>();
            for (var hour = 0; hour < 72; hour++)
            {
                await Own(rig.Storage, ++version, 7);
                await Own(rig.Storage, ++version, 7);
                rig.Worker.Published(version);
                for (var i = 0; i < 3; i++)
                {
                    await rig.TickAsync();
                    if (rig.Worker.Blocked is { } blocked)
                    {
                        blocks.Add(blocked.Until);
                    }
                }
                rig.Advance(TimeSpan.FromHours(1));
            }

            // Periodic builds keep up with the head, and each expiry blocks the recomputed repair again
            Assert.Equal(version, (await Checkpoints(rig)).Max());
            Assert.Equal(MaintenanceReason.JobDeadline, rig.Worker.Blocked?.Reason);
            Assert.False(rig.Worker.RepairPending);
            Assert.Equal(3, blocks.Count);
        }

        [Fact]
        public async Task RepairDeclinedByARaceBacksOffInsteadOfBlocking()
        {
            var rig = Create();
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await Own(rig.Storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), rig.Worker.PendingDemand);

            // Another writer checkpoints at the target first
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 7);
            await rig.TickAsync();
            Assert.Equal(MaintenanceReason.CheckpointExists, rig.Worker.LastOutcomeForTests!.Reason);
            Assert.Null(rig.Worker.Blocked);
            Assert.True(rig.Worker.RepairPending);
            Assert.Equal(1, rig.Worker.DemandFailures);

            // Discovered again after the backoff: with the head checkpointed it asks for an anchor
            rig.OptionsClock.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);
            Assert.NotEqual(0, rig.Worker.AnchorRequest);
        }

        [Fact]
        public async Task BlockedRecomputesAboveNewCheckpoints()
        {
            var rig = Create(interval: 3);
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await Own(rig.Storage, 7, 7, DeltaTestKit.ProtocolWith("catalogManaged"));
            rig.Worker.Published(7);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(MaintenanceReason.ExcludedFeature, rig.Worker.Blocked!.Value.Reason);

            // While blocked, a periodic build adds a gapped checkpoint at 8
            await Own(rig.Storage, 8, 7, DeltaTestKit.Protocol());
            rig.Worker.Published(8);
            await rig.TickAsync();
            await Own(rig.Storage, 9, 7);
            await Own(rig.Storage, 10, 7);
            rig.Worker.Published(10);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 8 }, await Checkpoints(rig));

            // Still blocked a moment before the day is over
            rig.Advance(DeltaMaintenanceWorker.BlockedFor - TimeSpan.FromTicks(1));
            await rig.TickAsync();
            Assert.NotNull(rig.Worker.Blocked);
            Assert.Null(rig.Worker.PendingDemand);

            // At expiry the target is recomputed above every listed checkpoint, not the old 7
            rig.Advance(TimeSpan.FromTicks(1));
            await rig.TickAsync();
            Assert.Null(rig.Worker.Blocked);
            Assert.Equal(new BuildDemand(10, RootPolicy.None), rig.Worker.PendingDemand);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 8, 10 }, await Checkpoints(rig));
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        [Fact]
        public async Task CutoffBuildKeepsExactTarget()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 4, cleanup: cleanup);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
            // The staged sweep
            await rig.TickAsync();

            // The advance finds no eligible checkpoint and asks for the cutoff build at 5
            var asked = false;
            cleanup.Work = kind => kind == JobKind.Advance && !asked;
            cleanup.Run = (_, _) =>
            {
                asked = true;
                return Task.FromResult(new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint) { Demand = new BuildDemand(5, RootPolicy.Auto) });
            };
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(5, RootPolicy.Auto), rig.Worker.PendingDemand);

            // A periodic build falls due too; the demand runs first, at exactly 5
            await OwnRange(rig.Storage, 7, 9, 0);
            rig.Worker.Published(9);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 5 }, await Checkpoints(rig));
            Assert.Equal(4, rig.Worker.LastBuild!.RootVersion);
            Assert.Null(rig.Worker.PendingDemand);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 5, 9 }, await Checkpoints(rig));
        }

        [Fact]
        public async Task DurableListedButNotHandedOffVersionCannotBecomeDemandTarget()
        {
            var rig = Create();
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await OwnRange(rig.Storage, 7, 10, 7);
            // Only 8 is handed off; 9 and 10 are durable and listed
            rig.Worker.Published(8);
            Assert.False(rig.Worker.RequestBuild(new BuildDemand(10, RootPolicy.None)));

            await rig.TickAsync();
            Assert.Equal(new BuildDemand(8, RootPolicy.None), rig.Worker.PendingDemand);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 8 }, await Checkpoints(rig));

            // A cutoff build above the head its advance observed is ignored
            var cleanup = new FakeCleanup();
            var covered = Create(cleanup: cleanup);
            await OwnRange(covered.Storage, 0, 10, 0);
            await Build(covered.Storage, 4);
            covered.Worker.Published(8);
            await covered.TickAsync();
            await covered.TickAsync();
            Assert.True(covered.Worker.CoverageConfirmed);
            cleanup.Work = kind => kind == JobKind.Advance && cleanup.Count(JobKind.Advance) == 0;
            cleanup.Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint) { Demand = new BuildDemand(9, RootPolicy.Auto) });
            await covered.TickAsync();
            Assert.Equal(1, cleanup.Count(JobKind.Advance));
            Assert.Null(covered.Worker.PendingDemand);
        }

        [Fact]
        public async Task CutoffDemandIgnoredBeforeCoverage()
        {
            var cleanup = new FakeCleanup()
            {
                Work = kind => kind == JobKind.Advance,
                Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint) { Demand = new BuildDemand(6, RootPolicy.Auto) })
            };
            var rig = Create(cleanup: cleanup);
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            rig.Worker.Published(6);

            // Waiting for an anchor, the table has no confirmed coverage: no advance runs and no cutoff build is taken
            for (int i = 0; i < 4; i++)
            {
                await rig.TickAsync();
            }
            Assert.True(rig.Worker.RepairPending);
            Assert.Equal(1, cleanup.Count(JobKind.StagedSweep));
            Assert.Equal(0, cleanup.Count(JobKind.Advance));
            Assert.Null(rig.Worker.PendingDemand);

            // Once the repair is confirmed the advance runs
            await Own(rig.Storage, 7, 7);
            rig.Worker.AnchorStaged(rig.Worker.AnchorRequest, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
            await rig.TickAsync();
            Assert.Equal(1, cleanup.Count(JobKind.Advance));
            Assert.Equal(new BuildDemand(6, RootPolicy.Auto), rig.Worker.PendingDemand);
        }

        [Fact]
        public async Task CutoffDemandOfAnAdvanceAdmittedUnderAnotherTableIdIsDropped()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(cleanup: cleanup, tableId: "a");
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            // The staged sweep
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);

            // Dispatched over confirmed coverage, admitted under another table id
            cleanup.Work = kind => kind == JobKind.Advance && cleanup.Count(JobKind.Advance) == 0;
            cleanup.Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint) { Demand = new BuildDemand(5, RootPolicy.Auto) });
            rig.Worker.SetTableId("b");
            await rig.TickAsync();
            Assert.Equal(1, cleanup.Count(JobKind.Advance));
            Assert.Null(rig.Worker.PendingDemand);
            Assert.False(rig.Worker.CoverageConfirmed);

            // Discovered again under the new id
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
            Assert.Null(rig.Worker.PendingDemand);
        }

        [Fact]
        public async Task RetentionNotPublishedRequestsAnchor()
        {
            var cleanup = new FakeCleanup()
            {
                Work = kind => kind == JobKind.Advance,
                Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.Declined, MaintenanceReason.RetentionNotPublished))
            };
            var rig = Create(cleanup: cleanup);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);

            // The advance declines: the anchor commit republishes the properties
            await rig.TickAsync();
            Assert.Equal(1, cleanup.Count(JobKind.Advance));
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);

            // Declined again while it is outstanding, the same generation is asked for
            rig.OptionsClock.Advance(rig.Worker.CleanupRetryAt(JobKind.Advance) - rig.OptionsClock.GetUtcNow());
            await rig.TickAsync();
            Assert.Equal(2, cleanup.Count(JobKind.Advance));
            Assert.Equal(generation, rig.Worker.AnchorRequest);

            // Staged and not yet handed off, a decline asks for nothing more
            await Own(rig.Storage, 7, 0);
            rig.Worker.AnchorStaged(generation, 7);
            Assert.Equal(0, rig.Worker.AnchorRequest);
            rig.OptionsClock.Advance(rig.Worker.CleanupRetryAt(JobKind.Advance) - rig.OptionsClock.GetUtcNow());
            await rig.TickAsync();
            Assert.Equal(3, cleanup.Count(JobKind.Advance));
            Assert.Equal(0, rig.Worker.AnchorRequest);

            // Handed off, the anchor is discovered once, and a later decline asks again
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(3, cleanup.Count(JobKind.Advance));
            Assert.True(rig.Worker.CoverageConfirmed);
            rig.OptionsClock.Advance(rig.Worker.CleanupRetryAt(JobKind.Advance) - rig.OptionsClock.GetUtcNow());
            await rig.TickAsync();
            Assert.Equal(4, cleanup.Count(JobKind.Advance));
            Assert.Equal(generation + 1, rig.Worker.AnchorRequest);
        }

        [Fact]
        public async Task TableIdChangeClearsRepairAndBlocked()
        {
            var rig = Create(interval: 2, tableId: "a");
            // A truncated legacy lineage: no repair can run
            await Legacy(rig.Storage, 0, 8);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await Own(rig.Storage, 9, 9);
            await rig.Storage.Rm(DeltaTestKit.CommitPath("t", 0));
            await rig.Storage.Rm(DeltaTestKit.CommitPath("t", 1));
            rig.Worker.Published(9);
            await rig.TickAsync();
            Assert.Equal((s_now + DeltaMaintenanceWorker.BlockedFor, MaintenanceReason.MissingHistory), rig.Worker.Blocked!.Value);
            // The periodic build ran in the same job
            Assert.Equal(new long[] { 4, 9 }, await Checkpoints(rig));
            var generation = rig.Worker.RequestAnchor();
            Assert.NotEqual(0, generation);

            // The next admission under another table id drops the block and the anchor
            rig.Advance(TimeSpan.FromHours(1));
            rig.Worker.SetTableId("b");
            await Own(rig.Storage, 10, 9);
            await Own(rig.Storage, 11, 9);
            rig.Worker.Published(11);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 9, 11 }, await Checkpoints(rig));
            Assert.Null(rig.Worker.Blocked);
            Assert.Equal(0, rig.Worker.AnchorRequest);
            Assert.Null(rig.Worker.AnchorStagedVersion);

            // Discovery runs again at once, not at expiry; with checkpoint 11 at the head it asks for an anchor
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);
            Assert.Null(rig.Worker.Blocked);
            Assert.Equal(generation + 1, rig.Worker.AnchorRequest);
        }

        [Fact]
        public async Task SaveDataNeverBlocksOnBusyWorker()
        {
            var rig = Create();
            var generation = rig.Worker.RequestAnchor();
            var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var exited = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var release = new ManualResetEventSlim();
            // A thread of its own holds the worker's lock, as a settling job does
            var holder = new Thread(() =>
            {
                try
                {
                    lock (rig.Worker.SyncForTests)
                    {
                        held.SetResult();
                        release.Wait();
                    }
                }
                finally
                {
                    exited.SetResult();
                }
            });
            holder.IsBackground = true;
            holder.Start();
            await held.Task.WaitAsync(s_wait);
            try
            {
                // The anchor calls SaveData makes: the request read, then the acknowledgement after staging
                var calls = Task.Run(() =>
                {
                    var wanted = rig.Worker.AnchorRequest;
                    rig.Worker.AnchorStaged(wanted, 7);
                    return wanted;
                });
                Assert.Same(calls, await Task.WhenAny(calls, Task.Delay(TimeSpan.FromSeconds(5))));
                Assert.Equal(generation, await calls);
            }
            finally
            {
                release.Set();
                await exited.Task.WaitAsync(s_wait);
            }
            Assert.Equal(0, rig.Worker.AnchorRequest);
            Assert.Equal(7, rig.Worker.AnchorStagedVersion);
        }

        [Fact]
        public async Task IdleCheckpointedHeadAsksForOneAnchorUntilHandedOff()
        {
            var rig = Create(interval: 2);
            // Fully checkpointed, but the head carries no epoch
            await Legacy(rig.Storage, 0, 6);
            await Build(rig.Storage, 6);
            rig.Worker.Published(6);
            await rig.TickAsync();
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);
            Assert.True(rig.Worker.RepairPending);

            // Ticks while it is outstanding ask for nothing more
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(generation, rig.Worker.AnchorRequest);

            // Staged at 7, it is answered, and nothing runs before the hand-off
            await Own(rig.Storage, 7, 7);
            rig.Worker.AnchorStaged(generation, 7);
            Assert.Equal(0, rig.Worker.AnchorRequest);
            var job = rig.Worker.CurrentJob;
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Same(job, rig.Worker.CurrentJob);
            Assert.Equal(0, rig.Worker.AnchorRequest);

            // Handed off, the anchor is the exact target, rooted on the marked checkpoint at 6
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.Auto), rig.Worker.PendingDemand);
            Assert.Equal(0, rig.Worker.AnchorRequest);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(new long[] { 6, 7 }, await Checkpoints(rig));
            Assert.True(rig.Worker.CoverageConfirmed);
            Assert.Equal(0, rig.Worker.AnchorRequest);
        }

        [Fact]
        public async Task GappedRootsNeedAnAnchorThenARootlessRepair()
        {
            var rig = Create(interval: 1);
            // Gapped checkpoints 4 and 8 over a legacy root, uncheckpointed 7 at the epoch, head 8
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 2);
            await Build(rig.Storage, 4);
            await Own(rig.Storage, 7, 7);
            await Own(rig.Storage, 8, 7);
            await Build(rig.Storage, 8);
            rig.Worker.Published(8);

            // No handed-off version is above checkpoint 8: an anchor
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);
            Assert.Null(rig.Worker.PendingDemand);
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);

            // Anchor 9 over the gapped root 8: a replay from zero
            await Own(rig.Storage, 9, 7);
            rig.Worker.AnchorStaged(generation, 9);
            rig.Worker.Published(9);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(9, RootPolicy.None), rig.Worker.PendingDemand);
            await rig.TickAsync();
            Assert.Null(rig.Worker.LastBuild!.RootVersion);
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);

            // Auto at 10 roots on 9 and inherits complete coverage
            await Own(rig.Storage, 10, 7);
            rig.Worker.Published(10);
            await rig.TickAsync();
            Assert.Equal(new long[] { 2, 4, 8, 9, 10 }, await Checkpoints(rig));
            Assert.Equal(9, rig.Worker.LastBuild!.RootVersion);
            Assert.False(rig.Worker.LastBuild.InheritedGap);
        }

        [Fact]
        public async Task BuildInheritingAGapEndsCoverageAndStartsARepair()
        {
            var rig = Create(interval: 2);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));

            // Another writer checkpoints at 10 without a marker
            await OwnRange(rig.Storage, 7, 10, 0);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 10);
            await OwnRange(rig.Storage, 11, 12, 0);
            rig.Worker.Published(12);

            // The periodic build roots on it and inherits its gap
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6, 10, 12 }, await Checkpoints(rig));
            Assert.True(rig.Worker.LastBuild!.InheritedGap);
            Assert.False(rig.Worker.CoverageConfirmed);

            // Discovered again at once: with the head checkpointed, an anchor starts the repair
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);

            // The anchor replays from zero
            await Own(rig.Storage, 13, 0);
            rig.Worker.AnchorStaged(generation, 13);
            rig.Worker.Published(13);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(13, RootPolicy.None), rig.Worker.PendingDemand);
        }

        [Fact]
        public async Task CheckpointAboveTheHandOffNeedsAnAnchorBeforeCoverage()
        {
            var rig = Create(interval: 2);
            await OwnRange(rig.Storage, 0, 8, 0);
            await Build(rig.Storage, 4);
            // Another writer checkpoints own commit 6 without a marker, above the hand-off at 5
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 6);
            rig.Worker.Published(5);
            await rig.TickAsync();
            Assert.False(rig.Worker.CoverageConfirmed);
            Assert.True(rig.Worker.RepairPending);
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);

            // No periodic build roots on the unmarked checkpoint
            rig.Worker.Published(8);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));

            // The anchor at 9 replays from zero
            await Own(rig.Storage, 9, 0);
            rig.Worker.AnchorStaged(generation, 9);
            rig.Worker.Published(9);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(9, RootPolicy.None), rig.Worker.PendingDemand);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6, 9 }, await Checkpoints(rig));
            Assert.False(rig.Worker.LastBuild!.InheritedGap);
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        [Fact]
        public async Task CutoffBuildInheritingAGapEndsCoverage()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(cleanup: cleanup);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            // The staged sweep
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);

            // Another writer checkpoints at 10 without a marker
            await OwnRange(rig.Storage, 7, 10, 0);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 10);
            await OwnRange(rig.Storage, 11, 12, 0);
            rig.Worker.Published(12);

            // The advance's cutoff build at 12 roots on 10 and inherits its gap
            cleanup.Work = kind => kind == JobKind.Advance && cleanup.Count(JobKind.Advance) == 0;
            cleanup.Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint) { Demand = new BuildDemand(12, RootPolicy.Auto) });
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(12, RootPolicy.Auto), rig.Worker.PendingDemand);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 10, 12 }, await Checkpoints(rig));
            Assert.Equal(10, rig.Worker.LastBuild!.RootVersion);
            Assert.True(rig.Worker.LastBuild.InheritedGap);
            Assert.False(rig.Worker.CoverageConfirmed);

            // Discovered again at once: with the head checkpointed, an anchor starts the repair
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);
            Assert.NotEqual(0, rig.Worker.AnchorRequest);
        }

        [Fact]
        public async Task IntervalZeroBuildsTwoSuccessiveCutoffDemands()
        {
            var targets = new Queue<long>(new long[] { 5, 8 });
            var cleanup = new FakeCleanup();
            var rig = Create(interval: 0, periodic: false, cleanup: cleanup);
            await OwnRange(rig.Storage, 0, 10, 0);
            await Build(rig.Storage, 4);
            rig.Worker.Published(10);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);

            cleanup.Work = kind => kind == JobKind.Advance && targets.Count > 0;
            cleanup.Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint) { Demand = new BuildDemand(targets.Dequeue(), RootPolicy.Auto) });
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 5 }, await Checkpoints(rig));

            // The next advance, after the no-work floor
            rig.OptionsClock.Advance(rig.Worker.CleanupRetryAt(JobKind.Advance) - rig.OptionsClock.GetUtcNow());
            await rig.TickAsync();
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 5, 8 }, await Checkpoints(rig));
            Assert.Equal(2, cleanup.Count(JobKind.Advance));
        }

        [Fact]
        public async Task RepairReleasedOnlyByUncachedDestinationRead()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./retention-{Guid.NewGuid():N}")) { StampIdentity = true };
            var rig = Create(storage: storage, tableId: "table-id");
            await Legacy(storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 4);
            await Own(storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 7 }, await Checkpoints(rig));
            Assert.True(rig.Worker.RepairPending);

            // A failed destination read keeps the repair pending
            var fail = true;
            storage.Before = (verb, path) => fail && verb == "OpenRead" && path.Full.EndsWith(CheckpointName(7)) ? throw new IOException("read fault") : Task.CompletedTask;
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);
            Assert.False(rig.Worker.CoverageConfirmed);

            // After the backoff it reads the destination once, while the probe is served from the cache
            fail = false;
            storage.ClearRequests();
            rig.OptionsClock.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.False(rig.Worker.RepairPending);
            Assert.True(rig.Worker.CoverageConfirmed);
            Assert.Single(storage.Requests, x => x.StartsWith("OpenRead ") && x.EndsWith(CheckpointName(7)));
        }

        // The checkpoint's bytes change under its cached identity: same length, a marker of an unknown format
        private static async Task RefuteAsync(HookFileStorage storage, long version)
        {
            var path = $"/t/_delta_log/{CheckpointName(version)}";
            byte[] bytes;
            using (var copy = (MemoryStream)(await storage.OpenRead(path))!)
            {
                bytes = copy.ToArray();
            }
            var marker = Encoding.UTF8.GetBytes($"{{\"format\":1,\"version\":{version},");
            var at = bytes.AsSpan().IndexOf(marker);
            Assert.True(at >= 0);
            Assert.Equal(-1, bytes.AsSpan(at + 1).IndexOf(marker));
            bytes[at + "{\"format\":".Length] = (byte)'2';
            await using (var write = await storage.Inner.OpenWrite(path))
            {
                await write.WriteAsync(bytes);
            }
        }

        [Fact]
        public async Task RepairStaysPendingWhenTheDestinationIsNotGapFree()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./retention-{Guid.NewGuid():N}")) { StampIdentity = true };
            var rig = Create(storage: storage, tableId: "table-id");
            await Legacy(storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 4);
            await Own(storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 7 }, await Checkpoints(rig));
            Assert.True(rig.Worker.RepairPending);

            await RefuteAsync(storage, 7);

            // The cached probe still sees a gap-free root; the destination read refutes it and an anchor is asked for
            storage.ClearRequests();
            await rig.TickAsync();
            Assert.Single(storage.Requests, x => x.StartsWith("OpenRead ") && x.EndsWith(CheckpointName(7)));
            Assert.True(rig.Worker.RepairPending);
            Assert.False(rig.Worker.CoverageConfirmed);
            Assert.NotEqual(0, rig.Worker.AnchorRequest);
        }

        [Fact]
        public async Task FirstInspectionSurvivesADiscoveryReadFailure()
        {
            var rig = Create();
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            // The head commit's provenance read fails once
            var faults = 0;
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith($"{6:D20}.json") && ++faults == 1)
                {
                    throw new IOException("read fault");
                }
                return Task.CompletedTask;
            };
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.Equal(1, faults);
            Assert.True(rig.Worker.Inspected);
            Assert.Equal(4, rig.Worker.LastCompleted);
            Assert.False(rig.Worker.CoverageConfirmed);
            Assert.False(rig.Worker.RepairPending);
            Assert.Equal(1, rig.Logs.Warnings("Transient"));

            // Discovered again after its backoff
            rig.OptionsClock.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        [Fact]
        public async Task HeadCommitGoneAfterTheListingDecidesNothing()
        {
            var rig = Create();
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            // Commit 6 is removed between the discovery's listing and its read
            rig.Storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith($"{6:D20}.json"))
                {
                    rig.Storage.Hidden = x => x.Full.EndsWith($"{6:D20}.json");
                }
                return Task.CompletedTask;
            };
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.Equal(0, rig.Worker.AnchorRequest);
            Assert.False(rig.Worker.RepairPending);
            Assert.False(rig.Worker.CoverageConfirmed);

            // Listed again after the backoff
            rig.Storage.Before = null;
            rig.Storage.Hidden = null;
            rig.OptionsClock.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        [Fact]
        public async Task DiscoveryPromotesBeforeItsReadsExceedShortAdmission()
        {
            const long shortReads = 3;
            var rig = Create(periodic: false, shortReads: shortReads);
            // Inspected before the roots below are added
            await Legacy(rig.Storage, 0, 0);
            rig.Worker.Published(0);
            await rig.TickAsync();
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);

            // Six torn roots below an own head
            await OwnRange(rig.Storage, 1, 6, 1);
            for (long version = 0; version < 6; version++)
            {
                await rig.Storage.WriteText($"/t/_delta_log/{CheckpointName(version)}", "torn");
            }
            var promoted = false;
            var rootReads = 0;
            rig.Worker.HooksForTests = new WorkerHooks()
            {
                Hold = point =>
                {
                    promoted |= point == "promoted";
                    return Task.CompletedTask;
                }
            };
            rig.Storage.Before = (verb, path) =>
            {
                if (!promoted && verb == "OpenRead" && path.Full.EndsWith(".checkpoint.parquet"))
                {
                    rootReads++;
                }
                return Task.CompletedTask;
            };
            rig.Worker.AnchorStaged(generation, 1);
            rig.Worker.Published(6);
            await rig.TickAsync();

            Assert.True(rootReads <= shortReads, $"The discovery read {rootReads} roots under a short admission of {shortReads} reads; promoted {promoted}");
            Assert.True(promoted);
            // Decided under long admission: a replay from zero at 6
            Assert.Equal(new BuildDemand(6, RootPolicy.None), rig.Worker.PendingDemand);
        }

        [Fact]
        public async Task LongDiscoveryStopsAtItsDeadline()
        {
            const long shortReads = 1;
            var rig = Create(periodic: false, shortReads: shortReads, longDeadline: TimeSpan.Zero);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);

            // Promoted at the inspection's discovery, whose long deadline has already passed
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.True(rig.Worker.Inspected);
            Assert.False(rig.Worker.CoverageConfirmed);
            Assert.Equal(1, rig.Logs.Warnings("JobDeadline"));
        }

        [Fact]
        public async Task DemandAdmittedUnderANewTableIdLeavesItsStateAlone()
        {
            var rig = Create(tableId: "a");
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await Own(rig.Storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), rig.Worker.PendingDemand);

            // Dispatched for table a, admitted once the id changed, then declined for missing history
            var granted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            rig.Worker.HooksForTests = new WorkerHooks()
            {
                Hold = async point =>
                {
                    if (point == "granted")
                    {
                        granted.TrySetResult();
                        await release.Task;
                    }
                }
            };
            rig.Budget.TickNow();
            await granted.Task.WaitAsync(s_wait);
            rig.Worker.SetTableId("b");
            await rig.Storage.Rm(DeltaTestKit.CommitPath("t", 0));
            await rig.Storage.Rm(DeltaTestKit.CommitPath("t", 1));
            release.SetResult();
            await rig.Worker.CurrentJob.WaitAsync(s_wait);
            Assert.Equal(MaintenanceReason.MissingHistory, rig.Worker.LastOutcomeForTests!.Reason);

            // The new table is neither blocked nor backed off by it
            Assert.Null(rig.Worker.Blocked);
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(7, RootPolicy.None)));
            Assert.Equal(0, rig.Worker.DemandFailures);
        }

        [Fact]
        public async Task CutoffBuildDeclineBacksOffWithoutBlocking()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(cleanup: cleanup);
            await OwnRange(rig.Storage, 0, 4, 0);
            await Build(rig.Storage, 4);
            // Commit 5 enables a feature no checkpoint build supports
            await Own(rig.Storage, 5, 0, DeltaTestKit.ProtocolWith("catalogManaged"));
            await Own(rig.Storage, 6, 0);
            rig.Worker.Published(6);
            await rig.TickAsync();
            // The staged sweep
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);

            cleanup.Work = kind => kind == JobKind.Advance && cleanup.Count(JobKind.Advance) == 0;
            cleanup.Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint) { Demand = new BuildDemand(5, RootPolicy.Auto) });
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(5, RootPolicy.Auto), rig.Worker.PendingDemand);

            // A lasting decline of a cutoff build backs off; coverage stays and nothing is blocked
            await rig.TickAsync();
            Assert.Equal(MaintenanceReason.ExcludedFeature, rig.Worker.LastOutcomeForTests!.Reason);
            Assert.Null(rig.Worker.Blocked);
            Assert.True(rig.Worker.CoverageConfirmed);
            Assert.Null(rig.Worker.PendingDemand);
            Assert.Equal(1, rig.Worker.DemandFailures);
        }

        [Fact]
        public async Task CutoffBuildThatKeepsFailingIsDroppedOnceItsBackoffSaturates()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(cleanup: cleanup);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            // The staged sweep
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);

            cleanup.Work = kind => kind == JobKind.Advance && cleanup.Count(JobKind.Advance) == 0;
            cleanup.Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint) { Demand = new BuildDemand(5, RootPolicy.Auto) });
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(5, RootPolicy.Auto), rig.Worker.PendingDemand);

            // Every read of commit 5 fails
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full.EndsWith($"{5:D20}.json") ? throw new IOException("read fault") : Task.CompletedTask;
            for (var attempt = 0; rig.Worker.PendingDemand != null; attempt++)
            {
                Assert.True(attempt < 10, $"A failing cutoff build never cleared: {rig.Worker.DemandFailures} failures");
                await rig.TickAsync();
                Assert.Equal(OutcomeKind.Failed, rig.Worker.LastOutcomeForTests!.Kind);
                rig.Advance(TimeSpan.FromMinutes(30));
            }
            Assert.Equal(6, rig.Worker.DemandFailures);
            Assert.Null(rig.Worker.Blocked);
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AdvanceEndingCoverageRediscoversAfterTheBackoff(bool gate)
        {
            // A wait on a gapped cutoff root, or a head the advance cannot establish
            var ended = gate ? new CleanupResult(OutcomeKind.Declined, MaintenanceReason.CleanupGate) { ForeignHead = true } : new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.NoAdmissibleRoot);
            var cleanup = new FakeCleanup();
            var rig = Create(cleanup: cleanup);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            // The staged sweep
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);

            cleanup.Work = kind => kind == JobKind.Advance && cleanup.Count(JobKind.Advance) == 0;
            cleanup.Run = (_, _) => Task.FromResult(ended);
            await rig.TickAsync();
            Assert.False(rig.Worker.CoverageConfirmed);

            // Not discovered again before the backoff
            await rig.TickAsync();
            Assert.False(rig.Worker.CoverageConfirmed);
            rig.OptionsClock.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AdvanceDeclineWithoutAForeignHeadKeepsCoverage(bool capacity)
        {
            var reason = capacity ? MaintenanceReason.DeleteBacklog : MaintenanceReason.CleanupGate;
            var cleanup = new FakeCleanup();
            var rig = Create(cleanup: cleanup);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            // The staged sweep
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);

            // Deferred behind another advance's records, or by capacity
            cleanup.Work = kind => kind == JobKind.Advance && cleanup.Count(JobKind.Advance) == 0;
            cleanup.Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.Declined, reason));
            await rig.TickAsync();
            Assert.Equal(1, cleanup.Count(JobKind.Advance));
            Assert.Equal((OutcomeKind.Declined, reason), (rig.Worker.LastOutcomeForTests!.Kind, rig.Worker.LastOutcomeForTests.Reason));
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        [Fact]
        public async Task RepairStartsWithoutTheFailuresOfAnEarlierDemand()
        {
            var cleanup = new FakeCleanup();
            var rig = Create(cleanup: cleanup);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            await Build(rig.Storage, 5);
            rig.Worker.Published(6);
            await rig.TickAsync();
            // The staged sweep
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);

            // Five declines of a requested build at an existing checkpoint, the last one's backoff still running
            for (var attempt = 0; attempt < 5; attempt++)
            {
                rig.Advance(TimeSpan.FromMinutes(31));
                Assert.True(rig.Worker.RequestBuild(new BuildDemand(5, RootPolicy.None)));
                await rig.TickAsync();
                Assert.Equal(MaintenanceReason.CheckpointExists, rig.Worker.LastOutcomeForTests!.Reason);
            }
            Assert.Equal(5, rig.Worker.DemandFailures);

            // A foreign head ends coverage at the advance, and discovery asks for an anchor above it
            await Legacy(rig.Storage, 7, 7);
            cleanup.Work = kind => kind == JobKind.Advance && cleanup.Count(JobKind.Advance) == 0;
            cleanup.Run = (_, _) => Task.FromResult(new CleanupResult(OutcomeKind.Declined, MaintenanceReason.CleanupGate) { ForeignHead = true });
            await rig.TickAsync();
            rig.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);
            await Own(rig.Storage, 8, 8);
            rig.Worker.AnchorStaged(generation, 8);
            rig.Worker.Published(8);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(8, RootPolicy.Auto), rig.Worker.PendingDemand);

            // One failed attempt of the repair only backs off
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full.EndsWith($"{8:D20}.json") ? throw new IOException("read fault") : Task.CompletedTask;
            await rig.TickAsync();
            Assert.Equal(OutcomeKind.Failed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Null(rig.Worker.Blocked);
            Assert.Equal(1, rig.Worker.DemandFailures);
            Assert.True(rig.Worker.RepairPending);

            // Built after its backoff and confirmed by a destination read, it leaves no failures behind
            rig.Storage.Before = null;
            rig.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.Null(rig.Worker.PendingDemand);
            Assert.Equal(1, rig.Worker.DemandFailures);
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
            Assert.Equal(0, rig.Worker.DemandFailures);
        }

        // A marked gap-free root of every add up to the version, uncompressed, with a string length past its page that only decoding sees
        private byte[] UndecodableMarkedRoot(long version)
        {
            Directory.CreateDirectory(_directory);
            var path = Path.Combine(_directory, ScratchOwners.ShortId() + ".parquet");
            using (var writer = new CheckpointFileWriter(path, new DiskLedger(), new JobMemory(), ParquetSharp.Compression.Uncompressed, new CoverageMarker(version, 0, 0, null, true).ToJson()))
            {
                var actions = new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema) }.Concat(Enumerable.Range(0, (int)version + 1).Select(i => DeltaLakeCheckpointBuildTests.Add($"f{i}.parquet")));
                foreach (var record in actions.SelectMany(x => TestRecords.FromAction(x)))
                {
                    writer.Append(record);
                }
                writer.Close();
            }
            var bytes = File.ReadAllBytes(path);
            File.Delete(path);
            var at = bytes.AsSpan().IndexOf(Encoding.UTF8.GetBytes($"f{version}.parquet"));
            Assert.True(at > 4);
            BitConverter.GetBytes(0x7FFFFFF0).CopyTo(bytes, at - 4);
            return bytes;
        }

        [Fact]
        public async Task RepairWhoseRootFailsToDecodeIsDiscoveredAgain()
        {
            var rig = Create();
            await Legacy(rig.Storage, 0, 6);
            await using (var write = await rig.Storage.OpenWrite($"/t/_delta_log/{CheckpointName(4)}"))
            {
                await write.WriteAsync(UndecodableMarkedRoot(4));
            }
            await Own(rig.Storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.Auto), rig.Worker.PendingDemand);

            // The root is recorded unsupported and the repair is discovered again after the backoff, not retried as Auto
            await rig.TickAsync();
            Assert.Equal(MaintenanceReason.RootDecodeFailure, rig.Worker.LastOutcomeForTests!.Reason);
            Assert.Single(rig.Worker.UnsupportedRoots);
            Assert.Null(rig.Worker.PendingDemand);
            rig.OptionsClock.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), rig.Worker.PendingDemand);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 7 }, await Checkpoints(rig));
            Assert.Null(rig.Worker.LastBuild!.RootVersion);
            Assert.True(rig.Worker.CoverageConfirmed);
            Assert.Null(rig.Worker.Blocked);
        }

        [Fact]
        public async Task RepairWhoseTargetLeftTheLogIsDiscoveredAgain()
        {
            var rig = Create();
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await Own(rig.Storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), rig.Worker.PendingDemand);

            // Commit 7 is gone before the build, and the next commit adopts at 8
            await rig.Storage.Rm(DeltaTestKit.CommitPath("t", 7));
            await Own(rig.Storage, 8, 8);
            rig.Worker.Published(8);
            await rig.TickAsync();
            Assert.Equal(MaintenanceReason.TargetNotInLog, rig.Worker.LastOutcomeForTests!.Reason);
            Assert.Null(rig.Worker.PendingDemand);

            // Discovered again after the backoff at the new head, whose history is incomplete
            rig.OptionsClock.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.Equal((rig.OptionsClock.GetUtcNow() + DeltaMaintenanceWorker.BlockedFor, MaintenanceReason.MissingHistory), rig.Worker.Blocked!.Value);
        }

        [Fact]
        public async Task RepairThatKeepsFailingBlocksAndPeriodicBuildsResume()
        {
            var rig = Create(interval: 2);
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await Own(rig.Storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), rig.Worker.PendingDemand);

            // The replay from zero cannot read commit 0: the repair blocks once its backoff saturates
            rig.Storage.Before = (verb, path) => verb == "OpenRead" && path.Full.EndsWith($"{0:D20}.json") ? throw new IOException("archived") : Task.CompletedTask;
            for (var attempt = 0; rig.Worker.Blocked == null; attempt++)
            {
                Assert.True(attempt < 10, $"A failing repair never blocked: {rig.Worker.DemandFailures} failures, pending {rig.Worker.RepairPending}");
                await rig.TickAsync();
                rig.Advance(TimeSpan.FromMinutes(30));
            }
            Assert.Equal(MaintenanceReason.Transient, rig.Worker.Blocked!.Value.Reason);
            Assert.Equal(6, rig.Worker.DemandFailures);
            Assert.False(rig.Worker.RepairPending);
            Assert.Null(rig.Worker.PendingDemand);

            // Periodic builds resume over the legacy root, which never reads commit 0
            await Own(rig.Storage, 8, 7);
            await Own(rig.Storage, 9, 7);
            rig.Worker.Published(9);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 9 }, await Checkpoints(rig));
        }

        [Fact]
        public async Task RepairLosingEveryRaceAcrossTargetsBlocks()
        {
            var rig = Create();
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            var version = 7L;
            await Own(rig.Storage, version, 7);
            rig.Worker.Published(version);
            await rig.TickAsync();

            // Another writer checkpoints each target first, and each discovery after it repairs at a newer head
            for (var attempt = 0; rig.Worker.Blocked == null; attempt++)
            {
                Assert.True(attempt < 10, $"A repair losing every race never blocked: {rig.Worker.DemandFailures} failures");
                Assert.Equal(new BuildDemand(version, RootPolicy.None), rig.Worker.PendingDemand);
                await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", version);
                await rig.TickAsync();
                Assert.Equal(MaintenanceReason.CheckpointExists, rig.Worker.LastOutcomeForTests!.Reason);
                await Own(rig.Storage, ++version, 7);
                rig.Worker.Published(version);
                rig.Advance(TimeSpan.FromMinutes(30));
                await rig.TickAsync();
            }
            Assert.Equal(MaintenanceReason.CheckpointExists, rig.Worker.Blocked!.Value.Reason);
            Assert.Equal(6, rig.Worker.DemandFailures);
            Assert.False(rig.Worker.RepairPending);
        }

        // Races lost at each repair target until the repair blocks; the discovery after each loss asks for an anchor at the same head
        private static async Task<int> LoseEveryRaceAtTheTarget(Rig rig, long version, MaintenanceReason reason, bool listingLags)
        {
            var races = 0;
            while (true)
            {
                Assert.True(races < 10, $"A repair losing every race at its target never blocked: {rig.Worker.DemandFailures} failures");
                Assert.Equal(new BuildDemand(version, RootPolicy.None), rig.Worker.PendingDemand);
                // Another writer checkpoints the target first; a lagging listing hides both from the build
                await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", version);
                var lagging = $"{version:D20}.";
                rig.Storage.Hidden = listingLags ? path => path.Full.Contains(lagging) : null;
                await rig.TickAsync();
                rig.Storage.Hidden = null;
                races++;
                Assert.Equal(reason, rig.Worker.LastOutcomeForTests!.Reason);
                rig.Advance(TimeSpan.FromMinutes(30));
                if (rig.Worker.Blocked != null)
                {
                    return races;
                }

                // Discovered again at the same head, which is checkpointed
                await rig.TickAsync();
                if (rig.Worker.Blocked != null)
                {
                    return races;
                }
                Assert.True(rig.Worker.RepairPending);
                Assert.NotEqual(0, rig.Worker.AnchorRequest);
                version++;
                Assert.Equal(version + 1, await StageRequestedAnchor(rig, version));
                await rig.TickAsync();
                rig.Advance(TimeSpan.FromMinutes(30));
            }
        }

        [Fact]
        public async Task RepairLosingEveryRaceAtItsTargetBlocksAfterAsManyRaces()
        {
            var rig = Create();
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await Own(rig.Storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();

            var races = await LoseEveryRaceAtTheTarget(rig, 7, MaintenanceReason.CheckpointExists, listingLags: false);
            Assert.Equal((6, MaintenanceReason.CheckpointExists), (races, rig.Worker.Blocked!.Value.Reason));
            Assert.Equal(6, rig.Worker.DemandFailures);
            Assert.False(rig.Worker.RepairPending);
        }

        [Fact]
        public async Task RepairWhoseTargetEveryBuildMissesBlocksAfterAsManyRaces()
        {
            var rig = Create();
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await Own(rig.Storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();

            var races = await LoseEveryRaceAtTheTarget(rig, 7, MaintenanceReason.TargetNotInLog, listingLags: true);
            Assert.Equal((6, MaintenanceReason.TargetNotInLog), (races, rig.Worker.Blocked!.Value.Reason));
            Assert.Equal(6, rig.Worker.DemandFailures);
            Assert.False(rig.Worker.RepairPending);
        }

        [Fact]
        public async Task RepairWhoseReleaseStaysUndecidedBlocks()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./retention-{Guid.NewGuid():N}")) { StampIdentity = true };
            var rig = Create(interval: 2, storage: storage, tableId: "table-id");
            await Legacy(storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 4);
            await Own(storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 7 }, await Checkpoints(rig));
            Assert.True(rig.Worker.RepairPending);

            // Every destination read of the repair fails: the repair blocks once the discovery's backoff saturates
            storage.Before = (verb, path) => verb == "OpenRead" && path.Full.EndsWith(CheckpointName(7)) ? throw new IOException("read fault") : Task.CompletedTask;
            for (var attempt = 0; rig.Worker.Blocked == null; attempt++)
            {
                Assert.True(attempt < 10, "A release that stays undecided never blocked");
                await rig.TickAsync();
                rig.Advance(TimeSpan.FromMinutes(30));
            }
            Assert.Equal(MaintenanceReason.Transient, rig.Worker.Blocked!.Value.Reason);
            Assert.False(rig.Worker.RepairPending);

            // Periodic builds resume
            storage.Before = null;
            await Own(storage, 8, 7);
            await Own(storage, 9, 7);
            rig.Worker.Published(9);
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 7, 9 }, await Checkpoints(rig));
        }

        [Fact]
        public async Task ForeignCoWriterAboveEveryAnchorDoesNotHoldPeriodicBuildsForever()
        {
            var rig = Create(interval: 2);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            // Another writer commits above the hand-off before the inspection lists
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 7, DeltaLakeCheckpointBuildTests.Add("foreign-7.parquet"));
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);

            // The other writer commits again above each anchor before the next discovery lists
            var version = 8L;
            for (var round = 0; rig.Worker.Blocked == null; round++)
            {
                Assert.True(round < 10, $"Periodic builds stayed held through {round} anchors: pending {rig.Worker.RepairPending}, checkpoints {string.Join(",", await Checkpoints(rig))}");
                var generation = rig.Worker.AnchorRequest;
                Assert.NotEqual(0, generation);
                await Own(rig.Storage, version, version);
                rig.Worker.AnchorStaged(generation, version);
                await DeltaTestKit.WriteCommit(rig.Storage, "t", version + 1, DeltaLakeCheckpointBuildTests.Add($"foreign-{version + 1}.parquet"));
                rig.Worker.Published(version);
                await rig.TickAsync();
                rig.Advance(TimeSpan.FromMinutes(30));
                version += 2;
            }
            Assert.Equal(MaintenanceReason.HistoryChanged, rig.Worker.Blocked!.Value.Reason);
            Assert.Equal(6, rig.Worker.DemandFailures);
            Assert.False(rig.Worker.RepairPending);
            // The block is warned and asks for no further anchor
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.HistoryChanged)));
            Assert.Equal(0, rig.Worker.AnchorRequest);

            // Periodic builds resume
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, version - 2 }, await Checkpoints(rig));
        }

        [Fact]
        public async Task ForeignCoWriterAfterEachRepairBuildBeforeConfirmationDoesNotHoldPeriodicBuildsForever()
        {
            var rig = Create(interval: 2);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 7, DeltaLakeCheckpointBuildTests.Add("foreign-7.parquet"));
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);

            // Each anchor's repair build completes, then the co-writer commits above it before the confirmation discovery lists
            var version = 8L;
            for (var round = 0; rig.Worker.Blocked == null; round++)
            {
                Assert.True(round < 10, $"Periodic builds stayed held through {round} unconfirmed repairs: demandFailures={rig.Worker.DemandFailures}");
                var generation = rig.Worker.AnchorRequest;
                Assert.NotEqual(0, generation);
                await Own(rig.Storage, version, version);
                rig.Worker.AnchorStaged(generation, version);
                rig.Worker.Published(version);
                // Discovery queues the exact repair at the anchor, and the next tick completes that build
                await rig.TickAsync();
                Assert.Equal(new BuildDemand(version, RootPolicy.Auto), rig.Worker.PendingDemand);
                await rig.TickAsync();
                Assert.Null(rig.Worker.PendingDemand);
                Assert.True(rig.Worker.RepairPending);

                // The co-writer commits above the repaired anchor before the confirmation discovery runs
                await DeltaTestKit.WriteCommit(rig.Storage, "t", version + 1, DeltaLakeCheckpointBuildTests.Add($"foreign-{version + 1}.parquet"));
                await rig.TickAsync();
                rig.Advance(TimeSpan.FromMinutes(30));
                version += 2;
            }
            Assert.Equal(MaintenanceReason.HistoryChanged, rig.Worker.Blocked!.Value.Reason);
            Assert.False(rig.Worker.RepairPending);
        }

        [Fact]
        public async Task ShortForeignBurstDoesNotBlockRetentionForADay()
        {
            var rig = Create(interval: 2);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 7, DeltaLakeCheckpointBuildTests.Add("foreign-7.parquet"));
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);

            // Another writer commits above each of six anchors, one a minute, then stops
            var version = 8L;
            for (var round = 0; round < 6; round++)
            {
                var generation = rig.Worker.AnchorRequest;
                Assert.NotEqual(0, generation);
                await Own(rig.Storage, version, version);
                rig.Worker.AnchorStaged(generation, version);
                await DeltaTestKit.WriteCommit(rig.Storage, "t", version + 1, DeltaLakeCheckpointBuildTests.Add($"foreign-{version + 1}.parquet"));
                rig.Worker.Published(version);
                await rig.TickAsync();
                rig.Advance(TimeSpan.FromMinutes(1));
                version += 2;
            }

            Assert.True(rig.Worker.Blocked == null, $"A six-minute foreign burst blocked retention until {rig.Worker.Blocked?.Until} ({rig.Worker.Blocked?.Reason}) after {rig.Worker.DemandFailures} anchors");
        }

        // The sink stages a requested anchor at the next version
        private static async Task<long> StageRequestedAnchor(Rig rig, long version)
        {
            var generation = rig.Worker.AnchorRequest;
            if (generation == 0)
            {
                return version;
            }
            await Own(rig.Storage, version, version);
            rig.Worker.AnchorStaged(generation, version);
            rig.Worker.Published(version);
            return version + 1;
        }

        // One minute: a requested anchor is staged, its discovery and a due repair build run, and another writer commits before the confirmation lists
        private static async Task<long> ForeignMinuteAfterRepairBuild(Rig rig, long version)
        {
            version = await StageRequestedAnchor(rig, version);
            await rig.TickAsync();
            await rig.TickAsync();
            await DeltaTestKit.WriteCommit(rig.Storage, "t", version, DeltaLakeCheckpointBuildTests.Add($"foreign-{version}.parquet"));
            await rig.TickAsync();
            rig.Advance(TimeSpan.FromMinutes(1));
            return version + 1;
        }

        [Fact]
        public async Task ShortForeignBurstAfterEachRepairBuildDoesNotBlockRetentionForADay()
        {
            var rig = Create(interval: 2);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 7, DeltaLakeCheckpointBuildTests.Add("foreign-7.parquet"));
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);

            // Another writer commits once a minute for six minutes, after each repair build, then stops
            var version = 8L;
            for (var minute = 0; minute < 6; minute++)
            {
                version = await ForeignMinuteAfterRepairBuild(rig, version);
            }
            Assert.True(rig.Worker.Blocked == null, $"A six-minute foreign burst after each repair build blocked retention until {rig.Worker.Blocked?.Until} ({rig.Worker.Blocked?.Reason}) after {rig.Worker.DemandFailures} anchors");

            // The next repair is built after its backoff and confirmed
            for (var round = 0; !rig.Worker.CoverageConfirmed; round++)
            {
                Assert.True(round < 10, $"The repair stayed unconfirmed after the burst: pending {rig.Worker.RepairPending}, blocked {rig.Worker.Blocked?.Reason}");
                version = await StageRequestedAnchor(rig, version);
                await rig.TickAsync();
                await rig.TickAsync();
                rig.Advance(TimeSpan.FromMinutes(30));
            }
            Assert.Null(rig.Worker.Blocked);
            Assert.Equal(0, rig.Worker.DemandFailures);
            Assert.Equal(0, rig.Logs.Warnings(nameof(MaintenanceReason.HistoryChanged)));
        }

        [Fact]
        public async Task SustainedForeignWriterAfterEachRepairBuildBlocksPastTheBackoff()
        {
            var rig = Create(interval: 2);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 7, DeltaLakeCheckpointBuildTests.Add("foreign-7.parquet"));
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.True(rig.Worker.RepairPending);

            // Another writer keeps committing once a minute, after each repair build
            var version = 8L;
            for (var minute = 0; rig.Worker.Blocked == null; minute++)
            {
                Assert.True(minute < 120, $"A sustained foreign writer never blocked the repair: {rig.Worker.DemandFailures} anchors");
                version = await ForeignMinuteAfterRepairBuild(rig, version);
            }
            Assert.True(rig.OptionsClock.GetUtcNow() - s_now >= TimeSpan.FromMinutes(30), $"Blocked after {rig.OptionsClock.GetUtcNow() - s_now}, inside the backoff");
            Assert.Equal(MaintenanceReason.HistoryChanged, rig.Worker.Blocked!.Value.Reason);
            Assert.Equal(6, rig.Worker.DemandFailures);
            Assert.False(rig.Worker.RepairPending);
            // The block is warned and asks for no further anchor
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.HistoryChanged)));
            Assert.Equal(0, rig.Worker.AnchorRequest);
        }

        // Ticks a minute at a time until the repair blocks; after each completed repair build the table changes before its confirmation lists
        private static async Task BlockAfterEachRepairBuild(Rig rig, Func<Task> beforeConfirmation)
        {
            for (var minute = 0; rig.Worker.Blocked == null; minute++)
            {
                Assert.True(minute < 60, $"Unconfirmed repair builds never blocked: {rig.Worker.DemandFailures} failures");
                await rig.TickAsync();
                if (rig.Worker.PendingDemand == null && rig.Worker.RepairPending)
                {
                    await beforeConfirmation();
                    await rig.TickAsync();
                }
                rig.Advance(TimeSpan.FromMinutes(1));
            }
            Assert.True(rig.OptionsClock.GetUtcNow() - s_now >= TimeSpan.FromMinutes(30), $"Blocked after {rig.OptionsClock.GetUtcNow() - s_now}, inside the backoff");
            Assert.Equal(MaintenanceReason.HistoryChanged, rig.Worker.Blocked!.Value.Reason);
            Assert.Equal(6, rig.Worker.DemandFailures);
            Assert.False(rig.Worker.RepairPending);
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.HistoryChanged)));
        }

        [Fact]
        public async Task RefutedRepairBelowANewerOwnCommitBlocksPastTheBackoff()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./retention-{Guid.NewGuid():N}")) { StampIdentity = true };
            var rig = Create(storage: storage, tableId: "table-id");
            await Legacy(storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 4);
            await Own(storage, 7, 7);
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), rig.Worker.PendingDemand);

            // Each repair checkpoint is refuted by its destination read, and the sink commits above it
            var version = 7L;
            await BlockAfterEachRepairBuild(rig, async () =>
            {
                await RefuteAsync(storage, version);
                version++;
                await Own(storage, version, 7);
                rig.Worker.Published(version);
            });
        }

        [Fact]
        public async Task ForeignAndAdoptingOwnCommitAboveEachRepairBuildBlocksPastTheBackoff()
        {
            var rig = Create(interval: 2);
            await OwnRange(rig.Storage, 0, 6, 0);
            await Build(rig.Storage, 4);
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 7, DeltaLakeCheckpointBuildTests.Add("foreign-7.parquet"));
            rig.Worker.Published(6);
            await rig.TickAsync();
            var version = await StageRequestedAnchor(rig, 8);
            await rig.TickAsync();
            Assert.Equal(new BuildDemand(8, RootPolicy.Auto), rig.Worker.PendingDemand);

            // Another writer commits above each repair build, then the sink commits above it and adopts there
            await BlockAfterEachRepairBuild(rig, async () =>
            {
                await DeltaTestKit.WriteCommit(rig.Storage, "t", version, DeltaLakeCheckpointBuildTests.Add($"foreign-{version}.parquet"));
                await Own(rig.Storage, version + 1, version + 1);
                rig.Worker.Published(version + 1);
                version += 2;
            });
        }

        [Fact]
        public async Task TornRebuildAtInspectionDiscoversFromAFreshListing()
        {
            // Local disk, whose listings carry sizes
            var rig = Create(interval: 2, storage: HookFileStorage.Wrap(Files.Of.LocalDisk(Path.Combine(_directory, ScratchOwners.ShortId()))));
            // An unmarked root at 4, a torn checkpoint at 6 and own commits from 7
            await Legacy(rig.Storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            await rig.Storage.WriteText($"/t/_delta_log/{CheckpointName(6)}", "torn");
            await OwnRange(rig.Storage, 7, 9, 7);
            rig.Worker.Published(9);

            // The inspection rebuilds 6 over the unmarked root; the discovery after it asks for the repair, which no periodic build overtakes
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6 }, await Checkpoints(rig));
            Assert.Equal(new BuildDemand(9, RootPolicy.None), rig.Worker.PendingDemand);
            Assert.True(rig.Worker.RepairPending);

            // The next build at 9 is that repair, a replay from zero
            await rig.TickAsync();
            Assert.Equal(new long[] { 4, 6, 9 }, await Checkpoints(rig));
            Assert.Null(rig.Worker.LastBuild!.RootVersion);
            Assert.False(rig.Worker.LastBuild.InheritedGap);
        }

        [Fact]
        public async Task RestartedWorkerReconstructsThePendingRepair()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./retention-{Guid.NewGuid():N}"));
            var first = Create(storage: storage);
            await Legacy(storage, 0, 6);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 4);
            await Own(storage, 7, 7);
            first.Worker.Published(7);
            await first.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), first.Worker.PendingDemand);
            await first.Worker.StopAsync(s_wait);

            // The next sink run's worker finds the same exact repair from the log alone
            var second = Create(storage: storage);
            second.Worker.Published(7);
            await second.TickAsync();
            Assert.Equal(new BuildDemand(7, RootPolicy.None), second.Worker.PendingDemand);
            Assert.True(second.Worker.RepairPending);
            await second.TickAsync();
            Assert.Equal(new long[] { 4, 7 }, await Checkpoints(second));
        }

        // A retention worker over local disk with a real cleanup and periodic builds off; one clock drives the budget, the options and the advance's daily slot
        private sealed class CleanupRig
        {
            public required FakeTimeProvider Clock { get; init; }

            public required DeltaMaintenanceBudget Budget { get; init; }

            public required IFileStorage Storage { get; init; }

            public required DeltaMaintenanceWorker Worker { get; init; }

            public required DeltaTableCleanup Cleanup { get; init; }

            public async Task TickAsync()
            {
                Budget.TickNow();
                await Worker.CurrentJob.WaitAsync(s_wait);
            }

            public async Task DaysAsync(int days, int ticks)
            {
                for (var day = 0; day < days; day++)
                {
                    Clock.Advance(s_day);
                    for (var tick = 0; tick < ticks; tick++)
                    {
                        await TickAsync();
                    }
                }
            }
        }

        private static readonly TimeSpan s_day = TimeSpan.FromDays(1);

        private CleanupRig CreateWithCleanup()
        {
            var storage = Files.Of.LocalDisk(Path.Combine(_directory, ScratchOwners.ShortId()));
            var clock = new FakeTimeProvider(s_now);
            var configuration = CleanupConfiguration.Validate(storage, "t", false, new CleanupOptIns(s_day, null));
            // Due at the first chance, then daily twelve hours after midnight UTC
            var cleanup = new DeltaTableCleanup(configuration, storage, clock, "t", null, () => storage, new DiscardCounters(), null, "t") { AdvanceOffset = TimeSpan.FromHours(12) };
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true)
            {
                Buckets = MaintenanceBucketsTests.Unthrottled()
            };
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock)
            {
                PeriodicEnabled = false,
                CleanupEligible = true,
                Retention = s_day
            }, storage, "t", "t", null) { Cleanup = cleanup };
            budget.Register(worker);
            return new CleanupRig() { Clock = clock, Budget = budget, Storage = storage, Worker = worker, Cleanup = cleanup };
        }

        // An own commit ten days old; commit 0 also publishes the one-day retention
        private static Task Aged(IFileStorage storage, long version, long adoptedAt)
        {
            var add = DeltaLakeCheckpointBuildTests.Add($"f{version}.parquet");
            var published = RetentionProperties.Format(s_day);
            var actions = version == 0
                ? new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, new Dictionary<string, string>() { [RetentionProperties.LogKey] = published, [RetentionProperties.DeletedFileKey] = published }), add }
                : new[] { add };
            return DeltaTestKit.WriteOwnCommit(storage, "t", version, adoptedAt, (s_now - TimeSpan.FromDays(10)).ToUnixTimeMilliseconds() + version, actions);
        }

        // Own commits 0 to 6 with a marked root at 4, inspected and covered
        private async Task<CleanupRig> CoveredWithCleanupAsync()
        {
            var rig = CreateWithCleanup();
            for (long version = 0; version <= 6; version++)
            {
                await Aged(rig.Storage, version, 0);
            }
            await Build(rig.Storage, 4);
            rig.Worker.Published(6);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
            return rig;
        }

        [Fact]
        public async Task ForeignEpochOverAnUnmarkedRootIsRepairedWithIntervalZero()
        {
            var rig = await CoveredWithCleanupAsync();
            // A foreign commit, own commits adopting at 8, and an unmarked checkpoint at 10 from another writer
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 7, DeltaLakeCheckpointBuildTests.Add("foreign-7.parquet"));
            for (long version = 8; version <= 10; version++)
            {
                await Aged(rig.Storage, version, 8);
            }
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 10);
            await Aged(rig.Storage, 11, 8);
            await Aged(rig.Storage, 12, 8);
            rig.Worker.Published(12);

            // The advance waiting on the unmarked root ends coverage; the repair lands above it
            await rig.DaysAsync(3, 8);
            Assert.Equal(new long[] { 4, 10, 12 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        [Fact]
        public async Task CapacityDeclineKeepsTheDaysAdvanceDueAndCoverage()
        {
            var rig = await CoveredWithCleanupAsync();
            var selections = 0;
            rig.Cleanup.AdvanceForTests.SelectedHookForTests = _ =>
            {
                if (++selections == 1)
                {
                    throw AdvanceJob.Declined(MaintenanceReason.DeleteBacklog, "Runnable maintenance demand passed twelve hours");
                }
            };

            await rig.TickAsync();
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.DeleteBacklog), (rig.Worker.LastOutcomeForTests!.Kind, rig.Worker.LastOutcomeForTests.Reason));
            Assert.True(rig.Cleanup.IsDue(JobKind.Advance, 6, rig.Clock.GetUtcNow()));
            Assert.True(rig.Worker.CoverageConfirmed);

            // Retried after its backoff the same day, the advance concludes it
            rig.Clock.Advance(TimeSpan.FromMinutes(1));
            await rig.TickAsync();
            Assert.Equal(2, selections);
            Assert.NotEqual(OutcomeKind.Declined, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.False(rig.Cleanup.IsDue(JobKind.Advance, 6, rig.Clock.GetUtcNow()));
        }

        [Fact]
        public async Task ForeignHeadOnAnIdleCoveredTableAsksForAnAnchor()
        {
            var rig = await CoveredWithCleanupAsync();
            // Another writer commits above the idle sink's last hand-off
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 7, DeltaLakeCheckpointBuildTests.Add("foreign-7.parquet"));

            // The advance cannot establish the foreign head; coverage is discovered again and an anchor is asked for above it
            await rig.DaysAsync(2, 4);
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);

            // The anchor at 8 adopts at its own version: an exact Auto repair rooted on 4
            await Aged(rig.Storage, 8, 8);
            rig.Worker.AnchorStaged(generation, 8);
            rig.Worker.Published(8);
            for (var tick = 0; tick < 3; tick++)
            {
                await rig.TickAsync();
            }
            Assert.Equal(new long[] { 4, 8 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
            Assert.Equal(4, rig.Worker.LastBuild!.RootVersion);
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        [Fact]
        public async Task ForeignEpochCheckpointedAtCutoffBoundaryEndsCoverageAndRepairs()
        {
            var rig = await CoveredWithCleanupAsync();
            // A foreign commit, an own head adopting at 8 and an unmarked checkpoint at 8 from another writer
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 7, DeltaLakeCheckpointBuildTests.Add("foreign-7.parquet"));
            await Aged(rig.Storage, 8, 8);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 8);
            rig.Worker.Published(8);

            // Every version a cutoff build could take is checkpointed: coverage ends and an anchor is asked for
            await rig.DaysAsync(2, 4);
            Assert.False(rig.Worker.CoverageConfirmed);
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);

            // The anchor at 9 gets an exact repair replayed from zero
            await Aged(rig.Storage, 9, 8);
            rig.Worker.AnchorStaged(generation, 9);
            rig.Worker.Published(9);
            for (var tick = 0; tick < 3; tick++)
            {
                await rig.TickAsync();
            }
            Assert.Equal(new long[] { 4, 8, 9 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
            Assert.Null(rig.Worker.LastBuild!.RootVersion);
            Assert.True(rig.Worker.CoverageConfirmed);

            // The next daily advance takes the repair as its boundary; its delete turns follow it
            var observed = BoundarySelectorTests.Observed(rig.Cleanup);
            await rig.DaysAsync(1, 4);
            Assert.Equal(9, observed.Boundary!.C);
            Assert.Equal(9, (await DeltaMaintenanceWorkerTests.Hint(rig.Storage))!.Version);
        }

        [Fact]
        public async Task FullyCheckpointedForeignEpochAsksForAnAnchor()
        {
            var rig = await CoveredWithCleanupAsync();
            // Foreign commits, then an own head adopting at 10 that another writer checkpointed unmarked
            for (long version = 7; version <= 9; version++)
            {
                await DeltaTestKit.WriteCommit(rig.Storage, "t", version, DeltaLakeCheckpointBuildTests.Add($"foreign-{version}.parquet"));
            }
            await DeltaTestKit.WriteOwnCommit(rig.Storage, "t", 10, 10, rig.Clock.GetUtcNow().ToUnixTimeMilliseconds(), DeltaLakeCheckpointBuildTests.Add("f10.parquet"));
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 10);
            rig.Worker.Published(10);

            await rig.DaysAsync(3, 8);
            Assert.Equal(new long[] { 4, 10 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
            Assert.False(rig.Worker.CoverageConfirmed);
            Assert.True(rig.Worker.RepairPending);
            var generation = rig.Worker.AnchorRequest;
            Assert.NotEqual(0, generation);

            // The anchor at 11 gets an exact repair replayed from zero
            await DeltaTestKit.WriteOwnCommit(rig.Storage, "t", 11, 10, rig.Clock.GetUtcNow().ToUnixTimeMilliseconds(), DeltaLakeCheckpointBuildTests.Add("f11.parquet"));
            rig.Worker.AnchorStaged(generation, 11);
            rig.Worker.Published(11);
            for (var tick = 0; tick < 3; tick++)
            {
                await rig.TickAsync();
            }
            Assert.Equal(new long[] { 4, 10, 11 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
            Assert.Null(rig.Worker.LastBuild!.RootVersion);
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        // Scripted reads for the pure discovery rules
        private sealed class ScriptedReads : ICoverageReads
        {
            // Null when the commit is gone
            public CommitProvenance? Provenance { get; set; }

            public RootPrediction Prediction { get; set; } = new RootPrediction(null, false, null, Array.Empty<string>());

            public bool? Confirmation { get; set; }

            public int Confirms { get; private set; }

            public Task<CommitProvenance?> ProvenanceAsync(LogTransactionFile commit) => Task.FromResult(Provenance);

            public Task<RootPrediction> PredictAsync(IReadOnlyList<LogTransactionFile> candidates) => Task.FromResult(Prediction);

            public Task<bool?> ConfirmAsync(LogTransactionFile checkpoint)
            {
                Confirms++;
                return Task.FromResult(Confirmation);
            }
        }

        private static RootPrediction Rooted(DeltaLogSegment segment, long version, bool gapFree)
        {
            var file = segment.Listing.Checkpoints.First(x => x.Version == version);
            var marker = new CoverageMarker(version, 0, 0, gapFree ? null : new CoverageGap(0, new[] { CoverageMarker.Remove }, 0), true);
            return new RootPrediction(new ProbedCheckpoint(ProbeClass.Usable, ObjectIdentity.Of("t", file), marker, 1, 1, null), false, null, Array.Empty<string>());
        }

        [Fact]
        public async Task DiscoveryDecidesFromItsReads()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./retention-{Guid.NewGuid():N}"));
            await OwnRange(storage, 0, 6, 0);
            await Build(storage, 4);
            await Build(storage, 6);
            var segment = await DeltaLogSegment.ListAsync(storage, "t");
            var reads = new ScriptedReads();

            // A head gone after the listing decides nothing, while one without provenance asks for an anchor
            Assert.Equal(DiscoveryKind.Undecided, (await CoverageDiscovery.DiscoverAsync(segment, 6, false, false, reads)).Kind);
            reads.Provenance = CommitProvenance.Unknown(6);
            Assert.Equal(DiscoveryKind.Anchor, (await CoverageDiscovery.DiscoverAsync(segment, 6, false, false, reads)).Kind);
            reads.Provenance = new CommitProvenance(6, "stage-6", 0, 1);

            // A root whose state is unknown decides nothing
            reads.Prediction = new RootPrediction(null, false, "Checkpoint 6 changed after the listing", Array.Empty<string>());
            Assert.Equal(DiscoveryKind.Undecided, (await CoverageDiscovery.DiscoverAsync(segment, 6, false, false, reads)).Kind);

            // Without a pending repair the probe confirms coverage, with one only the destination does
            reads.Prediction = Rooted(segment, 6, gapFree: true);
            Assert.Equal(DiscoveryKind.Covered, (await CoverageDiscovery.DiscoverAsync(segment, 6, false, false, reads)).Kind);
            Assert.Equal(0, reads.Confirms);
            reads.Confirmation = null;
            Assert.Equal(DiscoveryKind.Undecided, (await CoverageDiscovery.DiscoverAsync(segment, 6, true, false, reads)).Kind);
            reads.Confirmation = true;
            Assert.Equal(DiscoveryKind.Covered, (await CoverageDiscovery.DiscoverAsync(segment, 6, true, false, reads)).Kind);

            // Refuted at the head checkpoint: an anchor
            reads.Confirmation = false;
            Assert.Equal(DiscoveryKind.Anchor, (await CoverageDiscovery.DiscoverAsync(segment, 6, true, false, reads)).Kind);

            // Refuted below the head: never Auto over that root
            await Own(storage, 7, 0);
            segment = await DeltaLogSegment.ListAsync(storage, "t");
            reads.Provenance = new CommitProvenance(7, "stage-7", 0, 1);
            reads.Prediction = Rooted(segment, 6, gapFree: true);
            var refuted = await CoverageDiscovery.DiscoverAsync(segment, 7, true, false, reads);
            Assert.Equal((DiscoveryKind.Repair, new BuildDemand(7, RootPolicy.None)), (refuted.Kind, refuted.Demand));

            // A gapped root without history from zero declines
            await storage.Rm(DeltaTestKit.CommitPath("t", 0));
            segment = await DeltaLogSegment.ListAsync(storage, "t");
            reads.Prediction = Rooted(segment, 6, gapFree: false);
            var declined = await CoverageDiscovery.DiscoverAsync(segment, 7, false, false, reads);
            Assert.Equal((DiscoveryKind.Declined, MaintenanceReason.MissingHistory), (declined.Kind, declined.Reason));
        }
    }
}

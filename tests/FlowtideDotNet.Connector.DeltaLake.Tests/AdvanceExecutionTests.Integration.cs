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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using System.Collections.Concurrent;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class AdvanceExecutionTests
    {
        // A short advance admission with the given reads over the ledger
        private CleanupContext ShortAdvanceContext(ExecutionTable table, long reads, CapacityLedger ledger)
        {
            var basis = _kit.AdvanceContext(table.Head);
            return new CleanupContext(new JobRun(table.Head, JobKind.Advance) { Cancellation = new CancellationTokenSource() }, new FakeTimeProvider(s_now))
            {
                Kind = JobKind.Advance,
                Long = false,
                ReadLimit = reads,
                Deadline = s_now + TimeSpan.FromHours(1),
                Head = table.Head,
                TableId = null,
                Scratch = basis.Scratch,
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default,
                Capacity = ledger
            };
        }

        // The demand a restart's recovery would restore from the advance's records, at its evidence and the cleanup's latency
        private async Task<CapacityDemand> RestoredDemandAsync(Rig rig)
        {
            var configuration = rig.Table.Configuration;
            var context = _kit.AdvanceContext(rig.Table.Head);
            var log = await AdvancePreparation.ListAsync(configuration, rig.Table.Metered, context.Head, context);
            var recovered = await AdvanceRecovery.RecoverAsync(configuration, rig.Table.Metered, log, rig.Active.Evidence, context);
            await context.SettleAsync();
            AssertReleased(context);
            var group = Assert.Single(recovered.Unresolved);
            return CapacityEstimator.Advance(AdvanceRecovery.Remaining(configuration, recovered.Log, LogClassifier.Classify(recovered.Log), rig.Active.Evidence, group, rig.Cleanup.Latency), MaintenanceBuckets.DefaultRequestsPerSecond);
        }

        // A worker on the shared budget whose cleanup's delete turns run the real phases
        private static (DeltaMaintenanceWorker Worker, DeltaTableCleanup Cleanup) RealSink(DeltaMaintenanceBudget budget, FakeTimeProvider clock, ExecutionTable table,
            CleanupConfiguration configuration)
        {
            var cleanup = new DeltaTableCleanup(configuration, table.Metered, clock, "t", null, () => table.Raw, new DiscardCounters(), null, "t");
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock)
            {
                PeriodicEnabled = false,
                CleanupEligible = true,
                Retention = configuration.OptIns.Retention,
                Paging = configuration.Store.Store.Paging
            }, table.Raw, "t", "t", null) { Cleanup = cleanup };
            budget.Register(worker);
            return (worker, cleanup);
        }

        // Started with logs below C and files for its paths, then another writer's commits and an own head in a new epoch
        private async Task<(ExecutionTable Table, PublishedAdvance Published)> InterruptedAsync(int foreign, params DeltaAction[] actions)
        {
            var table = await _kit.ChurnedAsync(local: false);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table);
            for (int i = 0; i < foreign; i++)
            {
                await ForeignAsync(table, i == 0 ? actions : new[] { Add($"foreign-{i}.parquet") });
            }
            await OwnAsync(table, table.Head + 1, Add("part-0099.parquet"));
            return (table, published);
        }

        [Fact]
        public async Task ResumeRetriesAStageThatVanishesThenCompletes()
        {
            var (table, published) = await InterruptedAsync(1, Add("part-0003.parquet"));
            var stage = await DeltaTransactionWriter.StageCommit(table.Raw, "t", table.Head + 1, new List<DeltaAction>() { Add("part-0005.parquet") });
            var stagePath = table.LogPath(stage.FileName);
            // Listed, then gone at its read
            table.Hooks!.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full == stagePath)
                {
                    await table.Hooks.Inner.Rm(stagePath);
                }
            };
            var records = await table.SnapshotAsync();
            var ledger = new CapacityLedger(() => default);
            var cleanup = AdvanceExecutionKit.Cleanup(table);

            var retried = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));

            // A retry that keeps the day's advance due, the records and recovery's runnable lease
            AssertDeclined(retried, MaintenanceReason.Transient);
            Assert.Contains("Missing", retried.Detail);
            Assert.Null(cleanup.ActiveForTests);
            Assert.True(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
            Assert.Equal(records, await table.SnapshotAsync());
            Assert.NotEqual(default, ledger.Current);
            Assert.False(await table.Raw.Exists(stagePath));

            table.Hooks.Before = null;
            var resumed = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.True(resumed.Kind == OutcomeKind.Completed, $"{resumed.Kind} {resumed.Reason}: {resumed.Detail}");
            var active = cleanup.ActiveForTests!;
            Assert.Equal(published.Association, active.Association);
            AssertExcluded(active.Exclusions, "/t/part-0003.parquet");

            // It completes with the re-referenced path kept
            await _kit.ExecuteAsync(cleanup, table, ledger);
            await AdvanceExecutionKit.AssertAdvancedAsync(table, published.C, true, "/t/part-0003.parquet");
            Assert.Equal(default, ledger.Current);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ResumePromotesWhenItsShortReadsCannotCoverItsExclusions(bool plannedHead)
        {
            var (table, published) = await InterruptedAsync(4, Add("part-0003.parquet"));
            var commits = (int)(table.Head - published.Started.Head);
            // The reads a short resume counts before its exclusions
            long before = -1;
            var probe = AdvanceExecutionKit.Cleanup(table);
            probe.AdvanceForTests.ResumeExclusionsForTests = (configuration, storage, log, evidence, started, from, context) =>
            {
                before = context.ReadsUsed;
                return AdvanceExclusions.ForResumeAsync(configuration, storage, log, evidence, started, from, context);
            };
            var probed = await AdvanceExecutionKit.JobAsync(probe, ShortAdvanceContext(table, MaintenanceWorkerSettings.DefaultShortReads, new CapacityLedger(() => default)));
            Assert.True(probed.Kind == OutcomeKind.Completed && before > 0, $"{probed.Kind} {probed.Reason}: {probed.Detail}, {before} reads");
            var records = await table.SnapshotAsync();
            var ledger = new CapacityLedger(() => default);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            // None left for the planned head, or enough for it and one short of the commits after it
            var promoted = await AdvanceExecutionKit.JobAsync(cleanup, ShortAdvanceContext(table, plannedHead ? before : before + 1 + commits - 1, ledger));

            Assert.True(promoted.NeedsLongAdmission, $"{promoted.Kind} {promoted.Reason}: {promoted.Detail}");
            Assert.Null(cleanup.ActiveForTests);
            Assert.True(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
            Assert.Equal(records, await table.SnapshotAsync());

            // The long admission resumes it
            var resumed = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.True(resumed.Kind == OutcomeKind.Completed, $"{resumed.Kind} {resumed.Reason}: {resumed.Detail}");
            AssertExcluded(cleanup.ActiveForTests!.Exclusions, "/t/part-0003.parquet");
        }

        [Fact]
        public async Task RecoveryIsNotBlockedByAdmissionDebtExecutesAndReachesDone()
        {
            var table = await _kit.ChurnedAsync();
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table);
            // Another table's thirteen hours close admission
            var ledger = new CapacityLedger(() => default);
            var other = ledger.Restore(default, "other-table", ControlTokens.New(), Hours(13), CapacityState.Runnable);
            Assert.False(ledger.Admits());
            var cleanup = AdvanceExecutionKit.Cleanup(table);

            var resumed = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));

            Assert.True(resumed.Kind == OutcomeKind.Completed, $"{resumed.Kind} {resumed.Reason}: {resumed.Detail}");
            Assert.Equal((published.Association, AdvancePhase.Log), (cleanup.ActiveForTests!.Association, cleanup.ActiveForTests.Phase));
            // Its log, payload, done and teardown, with only the done kept and its lease released
            await _kit.ExecuteAsync(cleanup, table, ledger);
            await AdvanceExecutionKit.AssertAdvancedAsync(table, published.C);
            Assert.Equal(other.Demand, ledger.Current);
            Assert.Equal((default(CapacityDemand), default(CapacityDemand)), (ledger.Total(CapacityState.Paused), ledger.Total(CapacityState.Suspended)));

            // The next new advance waits on the debt
            AssertDeclined(await AdvanceExecutionKit.JobAsync(AdvanceExecutionKit.Cleanup(table), _kit.AdvanceContext(table.Head, ledger)), MaintenanceReason.DeleteBacklog);
        }

        [Fact]
        public async Task DirectForeignHeadRebasesAndSurvivesRestart()
        {
            var table = await _kit.ChurnedAsync(local: false);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            // Parts of four paths
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, partPaths: 4, logsComplete: true);
            var ledger = new CapacityLedger(() => default);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Kind);
            var active = cleanup.ActiveForTests!;
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);

            // Between slices another writer re-adds a path of the next part and one of the last
            await ForeignAsync(table, Add("part-0005.parquet"), Add("part-0009.parquet"));
            Assert.Equal(OutcomeKind.NoWork, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
            Assert.Equal(AdvanceState.Rebasing, active.State);
            Assert.True(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
            var rebased = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.True(rebased.Kind == OutcomeKind.Completed, $"{rebased.Kind} {rebased.Reason}: {rebased.Detail}");
            AssertExcluded(active.Exclusions, "/t/part-0005.parquet", "/t/part-0009.parquet");
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
            Assert.True(await table.Raw.Exists("/t/part-0005.parquet"));
            Assert.False(await table.Raw.Exists("/t/part-0004.parquet") || await table.Raw.Exists("/t/part-0006.parquet") || await table.Raw.Exists("/t/part-0007.parquet"));

            // After an own commit a restarted process rebuilds the exclusions from the planned head for the paths after its progress and finishes
            await OwnAsync(table, table.Head + 1, Add("part-0099.parquet"));
            var restartedLedger = new CapacityLedger(() => default);
            var restarted = AdvanceExecutionKit.Cleanup(table);
            var resumed = await AdvanceExecutionKit.JobAsync(restarted, _kit.AdvanceContext(table.Head, restartedLedger));
            Assert.True(resumed.Kind == OutcomeKind.Completed, $"{resumed.Kind} {resumed.Reason}: {resumed.Detail}");
            AssertExcluded(restarted.ActiveForTests!.Exclusions, "/t/part-0009.parquet");
            await _kit.ExecuteAsync(restarted, table, restartedLedger);
            await AdvanceExecutionKit.AssertAdvancedAsync(table, published.C, false, "/t/part-0005.parquet", "/t/part-0009.parquet");
            Assert.Equal(default, restartedLedger.Current);
        }

        [Fact]
        public async Task DirectForeignHeadResumesAfterARestartOverTheSinksAnchor()
        {
            var table = await _kit.ChurnedAsync(local: false);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, partPaths: 4, logsComplete: true);
            var ledger = new CapacityLedger(() => default);
            var first = AdvanceExecutionKit.Cleanup(table);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(first, _kit.AdvanceContext(table.Head, ledger))).Kind);
            // Rebased onto another writer's head and a slice validated there, then the process ends
            await ForeignAsync(table, Add("part-0005.parquet"), Add("part-0009.parquet"));
            Assert.Equal(OutcomeKind.NoWork, (await AdvanceExecutionKit.TurnAsync(first, _kit.DeletionContext(table.Head, ledger))).Kind);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(first, _kit.AdvanceContext(table.Head, ledger))).Kind);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(first, _kit.DeletionContext(table.Head, ledger))).Kind);
            var foreignHead = table.Head;

            // A restarted sink, idle at the foreign head
            await using var stream = new DeltaLakeSinkStream(nameof(DirectForeignHeadResumesAfterARestartOverTheSinksAnchor), table.Raw, options =>
            {
                options.HistoryRetention = AdvanceExecutionKit.Retention;
                options.AssumeCompleteStorageListing = true;
                options.CheckpointInterval = 0;
                options.TimeProvider = new FakeTimeProvider(s_now);
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(DeltaTestKit.UserInsert.Replace("test", "t"));
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
            while (!stream.Maintenance.Workers.Any(x => x.Target >= foreignHead))
            {
                Assert.True(DateTime.UtcNow < deadline, $"No worker reached version {foreignHead}");
                await stream.TriggerCheckpoint();
                await stream.SchedulerTick();
                await Task.Delay(10);
            }
            var worker = stream.Maintenance.Workers.Single();
            var steps = new ConcurrentQueue<string>();
            ((DeltaTableCleanup)worker.Cleanup!).AdvanceHooksForTests.Step = step =>
            {
                steps.Enqueue(step);
                return Task.CompletedTask;
            };

            // The worker asks for an anchor, which the next idle checkpoint stages as an own commit
            await stream.Maintenance.TickAndSettleAsync(foreignHead);
            await DeltaTestKit.WaitForVersionCheckpointing(table.Raw, "t", stream, foreignHead + 1);
            Assert.NotNull((await DeltaTestKit.ReadCommitActions(table.Raw, "t", foreignHead + 1))[0].CommitInfo);
            table.Head = foreignHead + 1;

            // Over it the advance resumes and finishes with the re-added paths kept
            await stream.Maintenance.TickAndSettleAsync(table.Head);
            Assert.Contains(AdvanceSteps.Resumed, steps);
            await AdvanceExecutionKit.AssertAdvancedAsync(table, published.C, false, "/t/part-0005.parquet", "/t/part-0009.parquet");
        }

        [Fact]
        public async Task ResumeExcludesOnlyThePathsFromItsProgressOn()
        {
            var table = await _kit.ChurnedAsync(local: false);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            // Parts of four paths, the first deleted
            await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, partPaths: 4, logsComplete: true);
            var first = AdvanceExecutionKit.Cleanup(table);
            var ledger = new CapacityLedger(() => default);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(first, _kit.AdvanceContext(table.Head, ledger))).Kind);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(first, _kit.DeletionContext(table.Head, ledger))).Kind);
            Assert.Equal(new RecordCursor(1, 0, ControlRecords.FirstPathOffset), (await NewestPayloadProgressAsync(table))!.Next);

            // Another writer re-adds a deleted path and a remaining one, then an own commit in a new epoch and a restart
            await ForeignAsync(table, Add("part-0001.parquet"), Add("part-0005.parquet"));
            await OwnAsync(table, table.Head + 1, Add("part-0099.parquet"));
            var restarted = AdvanceExecutionKit.Cleanup(table);
            var resumed = await AdvanceExecutionKit.JobAsync(restarted, _kit.AdvanceContext(table.Head, new CapacityLedger(() => default)));

            Assert.True(resumed.Kind == OutcomeKind.Completed, $"{resumed.Kind} {resumed.Reason}: {resumed.Detail}");
            AssertExcluded(restarted.ActiveForTests!.Exclusions, "/t/part-0005.parquet");
        }

        [Fact]
        public async Task SecondForeignCommitExtendsExclusions()
        {
            var table = await _kit.ChurnedAsync(local: false);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, partPaths: 4, logsComplete: true);
            // The first foreign commit, then an own commit and a restart
            await ForeignAsync(table, Add("part-0005.parquet"));
            await OwnAsync(table, table.Head + 1, Add("part-0098.parquet"));
            var ledger = new CapacityLedger(() => default);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Kind);
            var active = cleanup.ActiveForTests!;
            AssertExcluded(active.Exclusions, "/t/part-0005.parquet");

            // The second after it
            await ForeignAsync(table, Add("part-0009.parquet"));
            Assert.Equal(OutcomeKind.NoWork, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
            var rebased = await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger));
            Assert.True(rebased.Kind == OutcomeKind.Completed, $"{rebased.Kind} {rebased.Reason}: {rebased.Detail}");

            // Both kept, never only the latest commit's
            AssertExcluded(active.Exclusions, "/t/part-0005.parquet", "/t/part-0009.parquet");
            await _kit.ExecuteAsync(cleanup, table, ledger);
            await AdvanceExecutionKit.AssertAdvancedAsync(table, published.C, false, "/t/part-0005.parquet", "/t/part-0009.parquet");
        }

        [Fact]
        public async Task MissingForeignRangeSuspends()
        {
            var table = await _kit.ChurnedAsync(local: false);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, partPaths: 4, logsComplete: true);
            var logs = new DeltaMaintenanceWorkerTests.CollectingLogger();
            var ledger = new CapacityLedger(() => default);
            var cleanup = AdvanceExecutionKit.Cleanup(table, logs);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger))).Kind);
            var active = cleanup.ActiveForTests!;
            await ForeignAsync(table, Add("part-0003.parquet"));
            await ForeignAsync(table, Add("part-0005.parquet"));
            Assert.Equal(OutcomeKind.NoWork, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
            Assert.Equal(AdvanceState.Rebasing, active.State);
            var demand = active.Lease!.Demand;
            var records = await table.SnapshotAsync();
            var steps = new ConcurrentQueue<string>();
            cleanup.AdvanceHooksForTests.Step = step =>
            {
                steps.Enqueue(step);
                return Task.CompletedTask;
            };
            // A commit after the planned head goes missing before the rebase
            var missing = table.LogPath(Json(table.Head - 1));
            table.Hooks!.Hidden = path => path.Full == missing;

            AssertDeclined(await AdvanceExecutionKit.JobAsync(cleanup, _kit.AdvanceContext(table.Head, ledger)), MaintenanceReason.HistoryChanged);

            Assert.DoesNotContain(AdvanceSteps.Rebase, steps);
            Assert.Equal((AdvanceState.Suspended, CapacityState.Suspended), (active.State, active.Lease!.State));
            Assert.Equal((demand, default(CapacityDemand)), (ledger.Total(CapacityState.Suspended), ledger.Current));
            Assert.Null(cleanup.ActiveForTests);
            Assert.Equal(1, logs.Warnings(nameof(MaintenanceReason.HistoryChanged)));
            table.Hooks.Hidden = null;
            Assert.Equal(records, await table.SnapshotAsync());
            Assert.Equal(published.C, await AdvanceExecutionKit.FloorAsync(table));
            Assert.True(await table.Raw.Exists("/t/part-0000.parquet"));
        }

        [Fact]
        public async Task LogPhaseChecksOnlyRetainedRangeAfterIntentionalDeletesAcrossARestart()
        {
            var table = await _kit.ChurnedAsync(local: false);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table);
            async Task<string[]> LeftAsync() => LogTargets.Order(LogClassifier.Classify(await table.Raw.Ls($"/t/{DeltaTransactionReader.DeltaLogDirName}")), published.C)
                .Select(x => x.Entry.Name).ToArray();
            var all = await LeftAsync();
            var ledger = new CapacityLedger(() => default);
            var first = AdvanceExecutionKit.Cleanup(table);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(first, _kit.AdvanceContext(table.Head, ledger))).Kind);
            first.Settings = first.Settings with { MaxPaths = 2 };

            // Two log targets go, then the process ends before the third
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(first, _kit.DeletionContext(table.Head, ledger))).Kind);
            var left = await LeftAsync();
            Assert.Equal(all.Skip(2), left);

            // A new worker resumes over the gap below C and finishes
            var restartedLedger = new CapacityLedger(() => default);
            var restarted = AdvanceExecutionKit.Cleanup(table);
            var resumed = await AdvanceExecutionKit.JobAsync(restarted, _kit.AdvanceContext(table.Head, restartedLedger));
            Assert.True(resumed.Kind == OutcomeKind.Completed, $"{resumed.Kind} {resumed.Reason}: {resumed.Detail}");
            Assert.Equal((AdvancePhase.Log, (long)left.Length), (restarted.ActiveForTests!.Phase, restarted.ActiveForTests.LogTargetsLeft));
            await _kit.ExecuteAsync(restarted, table, restartedLedger);
            await AdvanceExecutionKit.AssertAdvancedAsync(table, published.C);
        }

        [Fact]
        public async Task ResumeRepeatsAtMostOneSliceAcrossAWorkerRestart()
        {
            // One part of eleven paths in slices of three
            static DeltaTableCleanup Slices(DeltaTableCleanup cleanup)
            {
                cleanup.Settings = cleanup.Settings with { MaxPaths = 3 };
                return cleanup;
            }
            var table = await _kit.ChurnedAsync();
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, partPaths: 11, logsComplete: true);
            var planned = Enumerable.Range(0, 11).Select(x => $"part-{x:D4}.parquet").ToArray();
            var first = Slices(AdvanceExecutionKit.Cleanup(table));
            var ledger = new CapacityLedger(() => default);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(first, _kit.AdvanceContext(table.Head, ledger))).Kind);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(first, _kit.DeletionContext(table.Head, ledger))).Kind);
            var recorded = (await NewestPayloadProgressAsync(table))!;
            Assert.Equal(3, recorded.Processed);

            // The next slice deletes without its progress, and the worker ends
            first.AdvanceHooksForTests.BeforePublish = (kind, _) => kind == ControlKind.Progress ? throw new IOException("injected") : Task.CompletedTask;
            AssertDeclined(await AdvanceExecutionKit.TurnAsync(first, _kit.DeletionContext(table.Head, ledger)), MaintenanceReason.Transient);
            Assert.False(await table.Raw.Exists($"/t/{planned[5]}"));

            // A restarted worker's advance job resumes it from the recorded cursor
            var restarted = Slices(AdvanceExecutionKit.Cleanup(table));
            var restartedLedger = new CapacityLedger(() => default);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(restarted, _kit.AdvanceContext(table.Head, restartedLedger))).Kind);
            Assert.Equal((recorded.Next, 3L), (restarted.ActiveForTests!.Cursor, restarted.ActiveForTests.Processed));
            var issued = new ConcurrentQueue<string>();
            restarted.AdvanceHooksForTests.Settled = (target, _) =>
            {
                if (target.Kind == TargetKind.Payload)
                {
                    issued.Enqueue(PayloadName(target.Key));
                }
            };
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(restarted, _kit.DeletionContext(table.Head, restartedLedger))).Kind);

            // The interrupted slice again, nothing before it
            Assert.Equal(planned.Skip(3).Take(3), issued.Order(StringComparer.Ordinal));
            await _kit.ExecuteAsync(restarted, table, restartedLedger);
            await AdvanceExecutionKit.AssertAdvancedAsync(table, published.C, false);
        }

        [Fact]
        public async Task UnsetPausesAndKeepsReservationWithAHeldLogDelete()
        {
            var table = await _kit.ChurnedAsync();
            var directory = Path.Combine(Path.GetTempPath(), "ae-" + ScratchOwners.ShortId());
            var owners = new ScratchOwners();
            var clock = new FakeTimeProvider(s_now);
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, directory), clock, manualTick: true)
            {
                Buckets = MaintenanceBucketsTests.Unthrottled()
            };
            owners.Add(budget.Scratch);
            try
            {
                // Sink A starts an advance
                var (sinkA, cleanupA) = RealSink(budget, clock, table, table.Configuration);
                var deletes = new LogDeletes(cleanupA.AdvanceHooksForTests);
                var held = Signal();
                var release = Signal();
                deletes.Hold = async _ =>
                {
                    if (held.TrySetResult())
                    {
                        await release.Task;
                    }
                };
                sinkA.Published(table.Head);
                for (var tick = 0; tick < 8 && cleanupA.ActiveForTests == null; tick++)
                {
                    await TickAsync(budget, sinkA);
                }
                var active = cleanupA.ActiveForTests;
                Assert.NotNull(active);

                // Its delete turn holds its first log delete while the sink is disposed
                for (var tick = 0; tick < 8 && !held.Task.IsCompleted; tick++)
                {
                    budget.TickNow();
                    await Task.WhenAny(held.Task, sinkA.CurrentJob).WaitAsync(s_wait);
                }
                Assert.True(held.Task.IsCompleted);
                var stopping = sinkA.StopAsync(s_wait);
                release.SetResult();
                await stopping;

                // The held delete settled within the joined turn, and nothing was issued after it
                Assert.Single(deletes.Issued);
                deletes.AssertOneAtATime();
                Assert.Equal(0, budget.DetachedJobs);
                var demand = active.Lease!.Demand;
                Assert.Equal(demand, budget.Capacity.Total(CapacityState.Paused));
                var records = await table.SnapshotAsync();
                var listed = (await table.Raw.Ls($"/t/{DeltaTransactionReader.DeltaLogDirName}")).Select(x => x.Name).Order(StringComparer.Ordinal).ToArray();

                // Sink B over the same table and budget without retention, for two days
                var (sinkB, cleanupB) = RealSink(budget, clock, table, CleanupConfiguration.Validate(table.Raw, "t", false, CleanupOptIns.Off));
                var issuedB = 0;
                cleanupB.AdvanceHooksForTests.BeforeDelete = _ =>
                {
                    Interlocked.Increment(ref issuedB);
                    return Task.CompletedTask;
                };
                sinkB.Published(table.Head);
                for (var day = 0; day < 2; day++)
                {
                    clock.Advance(TimeSpan.FromDays(1));
                    for (var tick = 0; tick < 4; tick++)
                    {
                        await TickAsync(budget, sinkB);
                    }
                }
                await sinkB.StopAsync(s_wait);

                // Nothing issued, the records, log and reservation kept, the demand reported paused
                Assert.Equal(0, issuedB);
                Assert.Null(cleanupB.ActiveForTests);
                Assert.Equal(records, await table.SnapshotAsync());
                Assert.Equal(listed, (await table.Raw.Ls($"/t/{DeltaTransactionReader.DeltaLogDirName}")).Select(x => x.Name).Order(StringComparer.Ordinal));
                Assert.Equal(active.C, await AdvanceExecutionKit.FloorAsync(table));
                // Below the held delete's commit
                var below = await TryBuildAsync(owners, directory, table.Raw, active.C - 2);
                Assert.Equal((BuildStatus.Declined, MaintenanceReason.CleanupGate), (below.Status, below.Reason));
                Assert.Equal(demand, budget.Capacity.Total(CapacityState.Paused));
                Assert.Equal(default, budget.Capacity.Current);
            }
            finally
            {
                owners.Release();
                try
                {
                    Directory.Delete(directory, true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RaisingRRechecksBothPhasesWithRealPhases(bool logsComplete)
        {
            var table = await _kit.ChurnedAsync();
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, logsComplete: logsComplete);
            // The sink is rebuilt with thirty days, which the table publishes from its next commit
            var raised = TimeSpan.FromDays(30);
            var formatted = RetentionProperties.Format(raised);
            await DeltaTestKit.WriteOwnCommit(table.Raw, "t", table.Head + 1, 0, BoundarySelectorTests.Old(table.Head + 1),
                DeltaTestKit.Metadata(DeltaTestKit.UserSchema, new Dictionary<string, string>() { [RetentionProperties.LogKey] = formatted, [RetentionProperties.DeletedFileKey] = formatted }));
            table.Head++;
            var configuration = CleanupConfiguration.Validate(table.Raw, "t", false, new CleanupOptIns(raised, null));
            var clock = new FakeTimeProvider(s_now);
            var cleanup = new DeltaTableCleanup(configuration, table.Metered, clock, "t", null, () => table.Raw, new DiscardCounters(), null, "t");
            var issued = 0;
            cleanup.AdvanceHooksForTests.BeforeDelete = _ =>
            {
                Interlocked.Increment(ref issued);
                return Task.CompletedTask;
            };
            var ledger = new CapacityLedger(() => default);
            var records = await table.SnapshotAsync();

            var waiting = await AdvanceAsync(cleanup, table, ledger);

            // C is after the new cutoff: nothing is issued in either phase while it waits with its reservation
            Assert.True(waiting is { Kind: OutcomeKind.NoWork, Reason: MaintenanceReason.NoEligibleCheckpoint, ForeignHead: false }, $"{waiting.Kind} {waiting.Reason}: {waiting.Detail}");
            Assert.Equal(OutcomeKind.NoWork, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
            Assert.Equal(0, issued);
            Assert.Equal(records, await table.SnapshotAsync());
            Assert.True(await table.Raw.Exists("/t/part-0000.parquet") && await table.Raw.Exists(table.LogPath(Json(0))));
            Assert.True(ledger.Total(CapacityState.Paused).Requests > 0);

            // Past the new cutoff the advance resumes in the phase its records reached and finishes
            clock.Advance(raised);
            var resumed = await AdvanceAsync(cleanup, table, ledger);
            Assert.True(resumed.Kind == OutcomeKind.Completed, $"{resumed.Kind} {resumed.Reason}: {resumed.Detail}");
            Assert.Equal(logsComplete ? AdvancePhase.Payload : AdvancePhase.Log, cleanup.ActiveForTests!.Phase);
            await _kit.ExecuteAsync(cleanup, table, ledger);
            await AdvanceExecutionKit.AssertAdvancedAsync(table, published.C, !logsComplete);
            Assert.Equal((default(CapacityDemand), default(CapacityDemand)), (ledger.Current, ledger.Total(CapacityState.Paused)));
        }

        [Fact]
        public async Task CatchUpRunsBackToBackWithRealPhases()
        {
            var table = await _kit.ChurnedAsync();
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            // Checkpoints at 4 and 8, both eligible
            await table.Raw.Rm(table.LogPath($"{12:D20}.checkpoint.parquet"));
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            var ledger = new CapacityLedger(() => default);
            var reservations = 0;
            ledger.BetweenCheckAndAddForTests = () => reservations++;
            // A short admission whose reads leave each step capped at the lowest eligible checkpoint
            Assert.True((await AdvanceExecutionKit.JobAsync(cleanup, ShortContext(table, 1))).NeedsLongAdmission);

            var advances = new List<ActiveAdvance>();
            for (int advance = 0; advance < 2; advance++)
            {
                Assert.True(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
                var started = await AdvanceAsync(cleanup, table, ledger);
                Assert.True(started.Kind == OutcomeKind.Completed, $"{started.Kind} {started.Reason}: {started.Detail}");
                advances.Add(cleanup.ActiveForTests!);
                Assert.False(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
                await _kit.ExecuteAsync(cleanup, table, ledger);
            }

            // Two advances the same day, each with its own selection and reservation, then one that finds no eligible checkpoint
            Assert.Equal(new long[] { 4, 8 }, advances.Select(x => x.C));
            Assert.NotEqual(advances[0].Association.AdvanceId, advances[1].Association.AdvanceId);
            Assert.Equal(2, reservations);
            Assert.True(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
            var none = await AdvanceAsync(cleanup, table, ledger);
            Assert.True(none is { Kind: OutcomeKind.NoWork, Reason: MaintenanceReason.NoEligibleCheckpoint }, $"{none.Kind} {none.Reason}: {none.Detail}");
            Assert.False(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
            await AdvanceExecutionKit.AssertAdvancedAsync(table, 8);
            Assert.Equal(default, ledger.Current);
        }

        [Fact]
        public async Task LeaseDemandFollowsEachValidatedProgress()
        {
            // Parts of four, four and three paths
            var rig = await PayloadAdvanceAsync();
            Assert.Equal(await RestoredDemandAsync(rig), rig.Active.Lease!.Demand);

            for (int turn = 0; turn < 2; turn++)
            {
                Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);

                // As a restart would restore it from the validated progress
                var newest = (await NewestPayloadProgressAsync(rig.Table))!;
                Assert.Equal((newest.Next, newest.Processed), (rig.Active.Cursor, rig.Active.Processed));
                var restored = await RestoredDemandAsync(rig);
                Assert.Equal(restored, rig.Active.Lease!.Demand);
                Assert.Equal(restored, rig.Ledger.Current);
            }

            // A failed progress leaves it as it was
            var before = rig.Active.Lease!.Demand;
            rig.Cleanup.AdvanceHooksForTests.BeforePublish = (kind, _) => kind == ControlKind.Progress ? throw new IOException("injected") : Task.CompletedTask;
            AssertDeclined(await TurnAsync(rig), MaintenanceReason.Transient);
            Assert.Equal((before, before, before), (rig.Active.Lease!.Demand, rig.Ledger.Current, await RestoredDemandAsync(rig)));

            rig.Cleanup.AdvanceHooksForTests.BeforePublish = null;
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            Assert.Equal((AdvancePhase.Done, default(CapacityDemand)), (rig.Active.Phase, rig.Ledger.Current));
        }

        [Fact]
        public async Task HintRaisedBeforeFirstDeletionIsAtOrAboveCAtTheFirstLogDelete()
        {
            var table = await _kit.ChurnedAsync();
            // A hint below every boundary
            await new StowageCheckpointTransport(table.Raw, "t").WriteHintAsync(new LastCheckpointInfo() { Version = 4, Size = 1 }, default);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            long? hint = null;
            var logDeletes = 0;
            cleanup.AdvanceHooksForTests.BeforeDelete = async target =>
            {
                if (target.Kind == TargetKind.Log && Interlocked.Increment(ref logDeletes) == 1)
                {
                    hint = (await DeltaMaintenanceWorkerTests.Hint(table.Raw))?.Version;
                }
            };
            var ledger = new CapacityLedger(() => default);

            Assert.Equal(OutcomeKind.Completed, (await AdvanceAsync(cleanup, table, ledger)).Kind);
            var c = cleanup.ActiveForTests!.C;
            await _kit.ExecuteAsync(cleanup, table, ledger);

            Assert.True(logDeletes > 0 && hint >= c, $"{logDeletes} log deletes, hint {hint} at the first, C {c}");
        }
    }
}

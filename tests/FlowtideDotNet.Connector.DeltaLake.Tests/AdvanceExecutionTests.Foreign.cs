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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class AdvanceExecutionTests
    {
        private static readonly string[] s_reReferenced = { "/t/part-0002.parquet", "/t/part-0003.parquet", "/t/part-0005.parquet" };

        // An advance in its payload phase, published by one cleanup and adopted by another, optionally under a newer root than C
        private async Task<Rig> PayloadAsync(bool local = false, bool newerRoot = false, int partPaths = 2)
        {
            var table = await _kit.ChurnedAsync(local);
            var logs = new DeltaMaintenanceWorkerTests.CollectingLogger();
            await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table, logs), table, partPaths, logsComplete: true);
            if (newerRoot)
            {
                await AdvanceExecutionKit.CommitAsync(table, 2);
                await _kit.BuildAsync(table.Raw, table.Head);
            }
            var cleanup = AdvanceExecutionKit.Cleanup(table, logs);
            var ledger = new CapacityLedger(() => default);
            var active = await _kit.AdoptAsync(cleanup, table, ledger);
            Assert.Equal(AdvancePhase.Payload, active.Phase);
            return new Rig() { Table = table, Cleanup = cleanup, Active = active, Ledger = ledger, Logs = logs };
        }

        // Another writer's commit after the head, without commitInfo
        private static async Task ForeignAsync(ExecutionTable table, params DeltaAction[] actions)
        {
            table.Head++;
            await DeltaTestKit.WriteCommit(table.Raw, "t", table.Head, actions);
        }

        private static async Task OwnAsync(ExecutionTable table, long adoptedAt, params DeltaAction[] actions)
        {
            table.Head++;
            await DeltaTestKit.WriteOwnCommit(table.Raw, "t", table.Head, adoptedAt, BoundarySelectorTests.Old(table.Head), actions);
        }

        private static async Task RewriteAsync(ExecutionTable table, long version, params DeltaAction[] actions)
        {
            await table.Raw.Rm(table.LogPath($"{version:D20}.json"));
            await DeltaTestKit.WriteCommit(table.Raw, "t", version, actions);
        }

        private static DeltaAction Add(string path) => DeltaLakeCheckpointBuildTests.Add(path);

        private static DeltaAction Published(string? id = null)
        {
            var retention = RetentionProperties.Format(AdvanceExecutionKit.Retention);
            var metadata = DeltaTestKit.Metadata(DeltaTestKit.UserSchema, new Dictionary<string, string>() { [RetentionProperties.LogKey] = retention, [RetentionProperties.DeletedFileKey] = retention });
            metadata.MetaData!.Id = id ?? metadata.MetaData.Id;
            return metadata;
        }

        // A payload slice whose gate found a foreign or new-epoch head
        private async Task RebasingAsync(Rig rig)
        {
            rig.Cleanup.PayloadPhaseForTests = Scripted(Still(SliceStatus.Rebase));
            Assert.Equal(OutcomeKind.NoWork, (await TurnAsync(rig)).Kind);
            Assert.Equal(AdvanceState.Rebasing, rig.Active.State);
        }

        // The advance job that rebases, as a worker runs it; no result of it ends the worker's coverage
        private async Task<CleanupResult> RebaseAsync(Rig rig, JobMemory? memory = null)
        {
            Assert.True(rig.Cleanup.IsDue(JobKind.Advance, rig.Table.Head, s_now));
            var context = _kit.AdvanceContext(rig.Table.Head, rig.Ledger, memory: memory);
            CleanupResult result;
            try
            {
                result = await rig.Cleanup.RunAsync(JobKind.Advance, context);
            }
            finally
            {
                await context.SettleAsync();
            }
            AssertReleased(context);
            Assert.False(result.ForeignHead || result is { Kind: OutcomeKind.NoWork, Reason: MaintenanceReason.NoAdmissibleRoot }, $"{result.Kind} {result.Reason}: {result.Detail}");
            return result;
        }

        // The advance's next delete-turn gate
        private async Task<GateResult> GateAsync(Rig rig)
        {
            var gate = ChunkGate.ForAdvance(rig.Table.Configuration, rig.Table.Metered, rig.Active, (log, _) => Task.FromResult(new GateResult(GateVerdict.Passed) { Head = log.Head }));
            var context = _kit.DeletionContext(rig.Table.Head);
            var result = await gate.PassAsync(context);
            AssertReleased(context);
            return result;
        }

        private async Task<ExclusionResult> ComputeAsync(Rig rig, CleanupContext? context = null, int cap = AdvanceExclusions.MaxExclusions, Func<Task>? afterListing = null,
            IFileStorage? storage = null)
        {
            var listing = _kit.AdvanceContext(rig.Table.Head);
            var log = await AdvancePreparation.ListAsync(rig.Table.Configuration, rig.Table.Metered, listing.Head, listing);
            await listing.SettleAsync();
            AssertReleased(listing);
            context ??= _kit.AdvanceContext(rig.Table.Head);
            if (afterListing != null)
            {
                await afterListing();
            }
            var result = await AdvanceExclusions.ComputeAsync(rig.Table.Configuration, storage ?? rig.Table.Metered, log, rig.Active, context, cap);
            await context.SettleAsync();
            AssertReleased(context);
            return result;
        }

        private static void AssertReleased(CleanupContext context)
        {
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
            Assert.Empty(context.Scratch.Files);
        }

        // Kept exclusions of 985-character paths, at the bytes their computation charges
        private static ExclusionSet LongExclusions(int count)
        {
            var directory = string.Join('/', Enumerable.Repeat(new string('x', 240), 4));
            var paths = Enumerable.Range(0, count).Select(x => $"/t/{x:D5}/{directory}/file.parquet").ToHashSet(StringComparer.Ordinal);
            return ExclusionSet.Of(paths, paths.Sum(ExclusionSet.BytesOf));
        }

        private static void AssertExcluded(ExclusionSet? set, params string[] expected)
        {
            Assert.NotNull(set);
            Assert.Equal(expected.Length, set.Count);
            Assert.All(expected, x => Assert.True(set.Contains(x), x));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReReferenceUnderAnotherSpellingIsExcludedAfterARebase(bool local)
        {
            var rig = await PayloadAsync(local);
            // Percent-encoded and in other case
            await ForeignAsync(rig.Table, Add("PART%2D0003.parquet"));
            await RebasingAsync(rig);

            var result = await RebaseAsync(rig);

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            Assert.Equal((AdvanceState.Runnable, CapacityState.Runnable), (rig.Active.State, rig.Active.Lease!.State));
            AssertExcluded(rig.Active.Exclusions, "/t/part-0003.parquet");
            // The next gate passes at the accepted head
            var gate = await GateAsync(rig);
            Assert.True(gate.Passed, $"{gate.Verdict}: {gate.Detail}");
            Assert.Equal(rig.Table.Head, gate.Head);
        }

        [Fact]
        public async Task SecondForeignCommitGrowsTheExclusions()
        {
            var rig = await PayloadAsync();
            await ForeignAsync(rig.Table, Add("part-0002.parquet"));
            await RebasingAsync(rig);
            Assert.Equal(OutcomeKind.Completed, (await RebaseAsync(rig)).Kind);
            AssertExcluded(rig.Active.Exclusions, "/t/part-0002.parquet");

            await ForeignAsync(rig.Table, Add("part-0005.parquet"));
            var gate = await GateAsync(rig);
            Assert.Equal((GateVerdict.ForeignHead, GateAction.Rebase), (gate.Verdict, gate.Action));
            await RebasingAsync(rig);
            Assert.Equal(OutcomeKind.Completed, (await RebaseAsync(rig)).Kind);

            // Recomputed from the planned head, never only the latest commit
            AssertExcluded(rig.Active.Exclusions, "/t/part-0002.parquet", "/t/part-0005.parquet");
            Assert.True((await GateAsync(rig)).Passed);
        }

        [Fact]
        public async Task ExcludedPathIsCountedAsAKeptDelete()
        {
            var rig = await PayloadAsync();
            await AdvanceExecutionKit.PayloadFilesAsync(rig.Table);
            await ForeignAsync(rig.Table, Add("part-0003.parquet"));
            await RebasingAsync(rig);
            Assert.Equal(OutcomeKind.Completed, (await RebaseAsync(rig)).Kind);
            AssertExcluded(rig.Active.Exclusions, "/t/part-0003.parquet");
            rig.Cleanup.PayloadPhaseForTests = AdvancePayloadPhase.RunSliceAsync;

            await _kit.ExecuteAsync(rig.Cleanup, rig.Table, rig.Ledger);

            // Passed without a delete and counted as kept
            Assert.True(await rig.Table.Raw.Exists("/t/part-0003.parquet"));
            Assert.True(rig.Cleanup.Counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted) > 0);
            Assert.Equal(1, rig.Cleanup.Counters.Deletes(TargetKind.Payload, DeleteStatus.Kept));
        }

        [Theory]
        [InlineData("none")]
        [InlineData("fenced")]
        public async Task RebaseOfAnUnchangedHeadResumesAndAFailedListingRetries(string change)
        {
            var rig = await PayloadAsync();
            var demand = rig.Active.Lease!.Demand;
            await RebasingAsync(rig);
            if (change == "fenced")
            {
                await CleanupRecordKit.PlantAsync(rig.Table.Raw, "t", $"{DeltaTransactionReader.CleanupControlPrefix}{rig.Active.C:D20}.unknown", new byte[1]);
            }

            var result = await RebaseAsync(rig);

            Assert.Same(ExclusionSet.Empty, rig.Active.Exclusions);
            Assert.Equal((CapacityState.Runnable, demand), (rig.Active.Lease!.State, rig.Ledger.Current));
            Assert.Same(rig.Active, rig.Cleanup.ActiveForTests);
            if (change == "none")
            {
                Assert.Equal(OutcomeKind.Completed, result.Kind);
                Assert.Equal(AdvanceState.Runnable, rig.Active.State);
                return;
            }
            // Keeps rebasing with its authority
            AssertDeclined(result, MaintenanceReason.ListingIneligible);
            Assert.Equal(AdvanceState.Rebasing, rig.Active.State);
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.ListingIneligible)));
        }

        [Fact]
        public async Task MissingCommitAfterThePlannedHeadSuspendsTheRebase()
        {
            var rig = await PayloadAsync();
            await ForeignAsync(rig.Table, Add("part-0003.parquet"));
            await ForeignAsync(rig.Table, Add("part-0005.parquet"));
            var records = await rig.Table.SnapshotAsync();
            var demand = rig.Active.Lease!.Demand;
            await RebasingAsync(rig);
            await rig.Table.Raw.Rm(rig.Table.LogPath($"{rig.Table.Head - 1:D20}.json"));

            AssertDeclined(await RebaseAsync(rig), MaintenanceReason.HistoryChanged);

            Assert.Equal(AdvanceState.Suspended, rig.Active.State);
            Assert.Equal(demand, rig.Ledger.Total(CapacityState.Suspended));
            Assert.Null(rig.Cleanup.ActiveForTests);
            Assert.Same(ExclusionSet.Empty, rig.Active.Exclusions);
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.HistoryChanged)));
            Assert.Equal(records, await rig.Table.SnapshotAsync());
            Assert.Equal(rig.Active.C, await AdvanceExecutionKit.FloorAsync(rig.Table));
        }

        [Theory]
        [InlineData("retention")]
        [InlineData("checkpoint")]
        [InlineData("table")]
        public async Task RebaseRechecksRetentionCAndTheTable(string change)
        {
            var rig = await PayloadAsync(local: change == "checkpoint", newerRoot: change == "checkpoint");
            var demand = rig.Active.Lease!.Demand;
            switch (change)
            {
                case "retention":
                    // Published without the retention
                    await ForeignAsync(rig.Table, DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("part-0003.parquet"));
                    break;
                case "checkpoint":
                    await ForeignAsync(rig.Table, Add("part-0003.parquet"));
                    Assert.DoesNotContain(rig.Active.Evidence.Capability.Sources, x => x.IsCheckpoint && x.Version == rig.Active.C);
                    break;
                case "table":
                    await ForeignAsync(rig.Table, Published("another-table"), Add("part-0003.parquet"));
                    break;
            }
            await RebasingAsync(rig);
            if (change == "checkpoint")
            {
                // Other bytes at the size and modification time the header names
                var path = Path.Combine(rig.Table.Root!, "t", "_delta_log", $"{rig.Active.C:D20}.checkpoint.parquet");
                var modified = File.GetLastWriteTimeUtc(path);
                var bytes = File.ReadAllBytes(path);
                bytes[bytes.Length / 2] ^= 0xFF;
                File.WriteAllBytes(path, bytes);
                File.SetLastWriteTimeUtc(path, modified);
            }

            var result = await RebaseAsync(rig);

            if (change != "retention")
            {
                AssertDeclined(result, MaintenanceReason.HistoryChanged);
                Assert.Equal((AdvanceState.Suspended, CapacityState.Suspended), (rig.Active.State, rig.Active.Lease!.State));
                Assert.Null(rig.Cleanup.ActiveForTests);
                return;
            }
            // Waits rebasing with its demand paused
            AssertDeclined(result, MaintenanceReason.RetentionNotPublished);
            Assert.Equal((AdvanceState.Rebasing, CapacityState.Paused), (rig.Active.State, rig.Active.Lease!.State));
            Assert.Equal((demand, default), (rig.Ledger.Total(CapacityState.Paused), rig.Ledger.Current));
            Assert.Same(rig.Active, rig.Cleanup.ActiveForTests);

            // An own anchor commit publishes it again in a new epoch
            await OwnAsync(rig.Table, rig.Table.Head + 1, Published());
            Assert.Equal(OutcomeKind.Completed, (await RebaseAsync(rig)).Kind);

            Assert.Equal((AdvanceState.Runnable, CapacityState.Runnable), (rig.Active.State, rig.Active.Lease!.State));
            Assert.Equal(demand, rig.Ledger.Current);
            AssertExcluded(rig.Active.Exclusions, "/t/part-0003.parquet");
            // Anchored in the new epoch, which later own commits continue
            Assert.Equal(rig.Table.Head, rig.Active.Evidence.Anchor.Version);
            await OwnAsync(rig.Table, rig.Table.Head, Add("part-0099.parquet"));
            Assert.True((await GateAsync(rig)).Passed);
        }

        [Theory]
        [InlineData("none")]
        [InlineData("head")]
        [InlineData("source")]
        public async Task AcceptedHeadPassesUntilANewerOrChangedHead(string change)
        {
            var table = await _kit.ChurnedAsync(local: false);
            var configuration = table.Configuration;
            var context = _kit.AdvanceContext(table.Head);
            async Task<GateResult> ValidateAsync(CleanupEvidence evidence)
            {
                var (check, log) = await ListingEvidence.ReadLogAsync(configuration, table.Metered, table.Head, default);
                Assert.Equal(ListingCheck.Passed, check);
                return await evidence.ValidateAsync(configuration, table.Metered, log!, context);
            }
            async Task<CleanupEvidence> EstablishAsync(bool durable)
            {
                var (_, log) = await ListingEvidence.ReadLogAsync(configuration, table.Metered, table.Head, default);
                var established = await CleanupEvidence.EstablishAsync(configuration, table.Metered, log!, log!.Head, null, durable, context);
                Assert.True(established.Passed, $"{established.Verdict}: {established.Detail}");
                return established.Evidence!;
            }
            var evidence = await EstablishAsync(true);
            var volatileEvidence = await EstablishAsync(false);

            // Another writer changes the table's metadata
            await ForeignAsync(table, Published(), Add("x-15.parquet"));
            var first = await ValidateAsync(evidence);
            Assert.Equal((GateVerdict.ForeignHead, GateAction.Rebase, 15L), (first.Verdict, first.Action, first.HeadProof!.Version));
            Assert.Equal((15L, evidence.TableId), (first.Current!.Version, first.Current.TableId));
            Assert.False(volatileEvidence.AcceptHead(first.HeadProof!, first.Current!));
            Assert.True(evidence.AcceptHead(first.HeadProof!, first.Current!));
            Assert.Equal((15L, 15L), (evidence.Frontier, evidence.LastValidatedHead));
            Assert.Same(first.Current, evidence.Capability);
            Assert.Equal(GateVerdict.Passed, (await ValidateAsync(evidence)).Verdict);

            // A newer head rebases again
            await ForeignAsync(table, Add("x-16.parquet"));
            var second = await ValidateAsync(evidence);
            Assert.Equal((GateVerdict.ForeignHead, GateAction.Rebase), (second.Verdict, second.Action));
            Assert.True(evidence.AcceptHead(second.HeadProof!, second.Current!));
            Assert.Equal(GateVerdict.Passed, (await ValidateAsync(evidence)).Verdict);
            Assert.False(evidence.AcceptHead(first.HeadProof!, first.Current!));

            switch (change)
            {
                case "head":
                    await RewriteAsync(table, 16, Add("x-16-other.parquet"));
                    break;
                case "source":
                    // The metadata the accepted capability came from
                    await RewriteAsync(table, 15, Published(), Add("x-15-other.parquet"));
                    break;
            }
            var result = await ValidateAsync(evidence);

            Assert.Equal(change switch
            {
                "head" => GateVerdict.Rollback,
                "source" => GateVerdict.SourceReplaced,
                _ => GateVerdict.Passed
            }, result.Verdict);
            AssertReleased(context);
        }

        [Theory]
        [InlineData("uninterrupted")]
        [InlineData("new-epoch")]
        [InlineData("rewritten")]
        [InlineData("rewritten-new-epoch")]
        [InlineData("missing")]
        public async Task ResumeNeedsExclusionsOnlyAfterAnInterruptedEpoch(string history)
        {
            var table = await _kit.ChurnedAsync(local: false);
            var configuration = table.Configuration;
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, logsComplete: true);
            Assert.Equal(table.Head, published.Started.Head);
            switch (history)
            {
                case "uninterrupted":
                    // Own commits of the planned epoch, which preserve its protection
                    await OwnAsync(table, 0, Add("part-0003.parquet"));
                    break;
                case "new-epoch":
                    await ForeignAsync(table, Add("part-0003.parquet"));
                    await OwnAsync(table, table.Head + 1, Add("part-0099.parquet"));
                    break;
                case "rewritten":
                case "rewritten-new-epoch":
                    // The planned head replaced in place by an own commit re-adding a planned candidate
                    await table.Raw.Rm(table.LogPath($"{table.Head:D20}.json"));
                    await DeltaTestKit.WriteOwnCommit(table.Raw, "t", table.Head, 0, BoundarySelectorTests.Old(table.Head), Add("part-0014.parquet"), Add("part-0002.parquet"));
                    await OwnAsync(table, history == "rewritten" ? 0 : table.Head + 1, Add("part-0003.parquet"));
                    break;
                case "missing":
                    // Gone under a checkpoint that roots the head
                    await OwnAsync(table, 0, Add("part-0003.parquet"));
                    await _kit.BuildAsync(table.Raw, table.Head);
                    await table.Raw.Rm(table.LogPath($"{table.Head - 1:D20}.json"));
                    break;
            }
            var context = _kit.AdvanceContext(table.Head);
            var log = await AdvancePreparation.ListAsync(configuration, table.Metered, context.Head, context);
            var established = await CleanupEvidence.EstablishAsync(configuration, table.Metered, log, log.Head, null, durable: true, context);
            Assert.True(established.Passed, $"{established.Verdict}: {established.Detail}");

            var result = await AdvanceExclusions.ForResumeAsync(configuration, table.Metered, log, established.Evidence!, published.Started, ActiveAdvance.Start(published.Started.Totals), context);
            await context.SettleAsync();

            AssertReleased(context);
            if (history == "uninterrupted")
            {
                Assert.Same(ExclusionSet.Empty, result.Set);
                return;
            }
            if (history != "new-epoch")
            {
                // Never taken for an interrupted epoch
                Assert.True(result.Action == GateAction.Suspend && result.Reason == MaintenanceReason.HistoryChanged, $"{result.Action} {result.Reason}: {result.Detail}");
                Assert.Null(result.Set);
                return;
            }
            AssertExcluded(result.Set, "/t/part-0003.parquet");
        }

        [Theory]
        [InlineData("none")]
        [InlineData("rebase")]
        [InlineData("resume")]
        public async Task ReplacedPlannedHeadStopsDeletionAfterARebaseOrAResume(string after)
        {
            var rig = await PayloadAsync();
            var table = rig.Table;
            var configuration = table.Configuration;
            var plannedHead = rig.Active.Started.Head;
            switch (after)
            {
                case "rebase":
                    // An own commit in a new epoch, rebased onto
                    await OwnAsync(table, table.Head + 1, Add("part-0099.parquet"));
                    await RebasingAsync(rig);
                    Assert.Equal(OutcomeKind.Completed, (await RebaseAsync(rig)).Kind);
                    break;
                case "resume":
                    // A restarted worker's evidence is anchored at the listed head
                    await OwnAsync(table, 0, Add("part-0099.parquet"));
                    var context = _kit.AdvanceContext(table.Head);
                    var log = await AdvancePreparation.ListAsync(configuration, table.Metered, context.Head, context);
                    var established = await CleanupEvidence.EstablishAsync(configuration, table.Metered, log, log.Head, null, durable: true, context);
                    Assert.True(established.Passed, $"{established.Verdict}: {established.Detail}");
                    var resumed = await AdvanceExclusions.ForResumeAsync(configuration, table.Metered, log, established.Evidence!, rig.Active.Started, rig.Active.Cursor, context);
                    await context.SettleAsync();
                    AssertReleased(context);
                    Assert.Same(ExclusionSet.Empty, resumed.Set);
                    rig.Active.Evidence = established.Evidence!;
                    break;
            }
            Assert.Equal(after == "none", rig.Active.Evidence.Anchor.Version == plannedHead);
            // The planned head replaced in place, re-adding a planned candidate
            await table.Raw.Rm(table.LogPath($"{plannedHead:D20}.json"));
            await DeltaTestKit.WriteOwnCommit(table.Raw, "t", plannedHead, 0, BoundarySelectorTests.Old(plannedHead), Add("part-0014.parquet"), Add("part-0002.parquet"));
            await using (var stream = await table.Raw.OpenWrite("/t/part-0002.parquet"))
            {
                await stream.WriteAsync(new byte[1]);
            }
            var executor = new DeletionExecutor(configuration, table.Metered);
            rig.Cleanup.PayloadPhaseForTests = (active, turn, slice) => new DeletionRun(executor,
                ChunkGate.ForAdvance(configuration, table.Metered, active, (listed, _) => Task.FromResult(new GateResult(GateVerdict.Passed) { Head = listed.Head })),
                new ListDeletionSource(new[] { executor.TargetOf("/t/part-0002.parquet") }), new FakeTimeProvider(s_now)).RunSliceAsync(slice);

            AssertDeclined(await TurnAsync(rig), MaintenanceReason.HistoryChanged);

            Assert.Equal((AdvanceState.Suspended, CapacityState.Suspended), (rig.Active.State, rig.Active.Lease!.State));
            Assert.True(await table.Raw.Exists("/t/part-0002.parquet"));
            Assert.Equal(0L, executor.Tally.Deleted);
        }

        [Fact]
        public async Task ExclusionsCoverOnlyThePathsFromTheCursorOn()
        {
            var rig = await PayloadAsync();
            await ForeignAsync(rig.Table, s_reReferenced.Select(x => Add(x.Substring(3))).ToArray());
            var parts = rig.Active.Started.Totals.Parts;
            // Two paths a part, in key order from part-0000
            var cases = new (RecordCursor Cursor, string[] Expected)[]
            {
                (new RecordCursor(0, 0, ControlRecords.FirstPathOffset), s_reReferenced),
                (new RecordCursor(1, 1, 0), s_reReferenced.Skip(1).ToArray()),
                (new RecordCursor(2, 0, ControlRecords.FirstPathOffset), s_reReferenced.Skip(2).ToArray()),
                (new RecordCursor(3, 0, ControlRecords.FirstPathOffset), Array.Empty<string>()),
                (new RecordCursor(parts, 0, 0), Array.Empty<string>())
            };

            foreach (var (cursor, expected) in cases)
            {
                rig.Active.Cursor = cursor;
                AssertExcluded((await ComputeAsync(rig)).Set, expected);
            }
        }

        [Theory]
        [InlineData("none", nameof(GateAction.Proceed), nameof(MaintenanceReason.None))]
        [InlineData("stage", nameof(GateAction.Proceed), nameof(MaintenanceReason.None))]
        [InlineData("unknown-below", nameof(GateAction.Proceed), nameof(MaintenanceReason.None))]
        [InlineData("gap", nameof(GateAction.Suspend), nameof(MaintenanceReason.HistoryChanged))]
        [InlineData("gone", nameof(GateAction.Suspend), nameof(MaintenanceReason.HistoryChanged))]
        [InlineData("planned-head", nameof(GateAction.Suspend), nameof(MaintenanceReason.HistoryChanged))]
        [InlineData("v2", nameof(GateAction.Suspend), nameof(MaintenanceReason.SidecarRoot))]
        [InlineData("unknown", nameof(GateAction.Suspend), nameof(MaintenanceReason.UnclassifiedLogFile))]
        [InlineData("outside", nameof(GateAction.Suspend), nameof(MaintenanceReason.PathRejected))]
        [InlineData("header", nameof(GateAction.Suspend), nameof(MaintenanceReason.ManifestSuspended))]
        [InlineData("part", nameof(GateAction.Suspend), nameof(MaintenanceReason.ManifestSuspended))]
        [InlineData("directory", nameof(GateAction.Suspend), nameof(MaintenanceReason.PathRejected))]
        [InlineData("over-cap", nameof(GateAction.Suspend), nameof(MaintenanceReason.EntriesOverCap))]
        [InlineData("short", nameof(GateAction.Promote), nameof(MaintenanceReason.None))]
        [InlineData("memory", nameof(GateAction.Retry), nameof(MaintenanceReason.ManagedMemoryCap))]
        public async Task ChangedHistoryEitherExcludesOrStopsTheAdvance(string change, string action, string reason)
        {
            var rig = await PayloadAsync(local: change == "directory");
            var table = rig.Table;
            await ForeignAsync(table, Add("part-0003.parquet"));
            await ForeignAsync(table, Add("part-0005.parquet"));
            var expected = new[] { "/t/part-0003.parquet", "/t/part-0005.parquet" };
            CleanupContext? context = null;
            Func<Task>? afterListing = null;
            async Task<string> NameAsync(Func<ControlName, bool> which) => (await table.ControlsAsync()).Single(x => ControlName.TryParse(x, out var parsed) && which(parsed));
            async Task ReplaceAsync(string name, byte[] bytes)
            {
                await table.Raw.Rm(table.LogPath(name));
                await CleanupRecordKit.PlantAsync(table.Raw, "t", name, bytes);
            }
            switch (change)
            {
                case "stage":
                    await DeltaTransactionWriter.StageCommit(table.Raw, "t", table.Head + 1, new List<DeltaAction>() { Add("part-0002.parquet") });
                    expected = s_reReferenced;
                    break;
                case "unknown-below":
                    await CleanupRecordKit.PlantAsync(table.Raw, "t", $"{rig.Active.Started.Head - 1:D20}.unknown", new byte[1]);
                    break;
                case "gap":
                    await table.Raw.Rm(table.LogPath($"{table.Head - 1:D20}.json"));
                    break;
                case "gone":
                    afterListing = () => table.Raw.Rm(table.LogPath($"{table.Head:D20}.json"));
                    break;
                case "planned-head":
                    await RewriteAsync(table, rig.Active.Started.Head, Add("part-0014.parquet"), Add("part-0002.parquet"));
                    break;
                case "v2":
                    await CleanupRecordKit.PlantAsync(table.Raw, "t", $"{rig.Active.Started.Head:D20}.checkpoint.{Guid.NewGuid():D}.parquet", new byte[1]);
                    break;
                case "unknown":
                    await CleanupRecordKit.PlantAsync(table.Raw, "t", $"{table.Head:D20}.unknown", new byte[1]);
                    break;
                case "outside":
                    await ForeignAsync(table, Add("s3://elsewhere/t/part-0004.parquet"));
                    break;
                case "header":
                    // A valid header whose kept count differs, before its trailer, descriptors and totals
                    var header = await NameAsync(x => x.Kind == ControlKind.Header);
                    var bytes = await DeltaTestKit.ReadBytes(table.Raw, table.LogPath(header));
                    bytes[bytes.Length - 36 - rig.Active.Started.Totals.Parts * 76 - 20 - 8] ^= 1;
                    await ReplaceAsync(header, CleanupRecordKit.Reseal(bytes));
                    break;
                case "part":
                    // Another valid part of the same length
                    var later = await DeltaTestKit.ReadBytes(table.Raw, table.LogPath(await NameAsync(x => x.Kind == ControlKind.Part && x.Ordinal == 3)));
                    await ReplaceAsync(await NameAsync(x => x.Kind == ControlKind.Part && x.Ordinal == 2), later);
                    break;
                case "directory":
                    // A remaining planned name that is now a folder has no candidate key
                    Directory.CreateDirectory(Path.Combine(table.Root!, "t", "part-0007.parquet"));
                    break;
                case "short":
                    context = _kit.DeletionContext(table.Head);
                    // Reads for both commits, none for the planned head
                    while (context.ReadsRemaining > 2)
                    {
                        context.TryCountRead();
                    }
                    break;
                case "memory":
                    var basis = _kit.AdvanceContext(table.Head);
                    context = new CleanupContext(new JobRun(table.Head, JobKind.Advance) { Cancellation = new CancellationTokenSource() }, new FakeTimeProvider(s_now))
                    {
                        Kind = JobKind.Advance,
                        Long = true,
                        ReadLimit = long.MaxValue,
                        Deadline = s_now + TimeSpan.FromHours(1),
                        Head = table.Head,
                        TableId = null,
                        Scratch = basis.Scratch,
                        Memory = new JobMemory(managedCap: 1 << 10),
                        Transfer = new TransferAllowance(),
                        Stop = default
                    };
                    break;
            }

            // At the cap passes, above it suspends
            var result = await ComputeAsync(rig, context, change == "over-cap" ? expected.Length - 1 : expected.Length, afterListing);

            Assert.True(result.Action == Enum.Parse<GateAction>(action) && result.Reason == Enum.Parse<MaintenanceReason>(reason), $"{result.Action} {result.Reason}: {result.Detail}");
            if (result.Action == GateAction.Proceed)
            {
                AssertExcluded(result.Set, expected);
            }
            else
            {
                Assert.Null(result.Set);
            }
        }

        [Fact]
        public async Task ExclusionsBeyondTheirByteCapSuspendInsideTheManagedBudget()
        {
            var rig = await PayloadAsync();
            var table = rig.Table;
            var original = rig.Active.Started;
            var fixture = CleanupRecordKit.Fixture(original.TableId, rig.Active.C, original.O, original.A, original.Head, original.FirstJson) with
            {
                Anchor = original.Anchor,
                Destination = rig.Active.Destination
            };
            // Allowed paths of 985 characters, whose strings alone exceed a job's managed cap
            var directory = string.Join('/', Enumerable.Repeat(new string('x', 240), 4));
            var paths = Enumerable.Range(0, 50_000).Select(x => $"/t/{x:D5}/{directory}/file.parquet").ToArray();
            var parts = new List<PlantedRecord>();
            foreach (var chunk in paths.Chunk(1000))
            {
                var part = CleanupRecordKit.Part(fixture, parts.Count, chunk);
                await CleanupRecordKit.PlantAsync(table.Raw, "t", part);
                parts.Add(part);
            }
            var header = CleanupRecordKit.Header(fixture, parts.ToArray());
            await CleanupRecordKit.PlantAsync(table.Raw, "t", header);
            var association = CleanupRecordKit.AssociationOf(header);
            var started = original with { Association = association, Totals = CleanupRecordKit.TotalsOf(header) };
            var active = new ActiveAdvance()
            {
                Association = association,
                Started = started,
                Destination = rig.Active.Destination,
                CutoffTicks = rig.Active.CutoffTicks,
                Evidence = rig.Active.Evidence,
                Phase = AdvancePhase.Payload,
                Cursor = ActiveAdvance.Start(started.Totals)
            };
            await ForeignAsync(table, paths.Select(x => Add(x.Substring(3))).ToArray());
            var context = _kit.AdvanceContext(table.Head);
            var log = await AdvancePreparation.ListAsync(table.Configuration, table.Metered, table.Head, context);

            var result = await AdvanceExclusions.ComputeAsync(table.Configuration, table.Metered, log, active, context);
            await context.SettleAsync();

            AssertReleased(context);
            Assert.True(paths.Sum(x => 2L * x.Length) > JobMemory.DefaultManagedCap);
            Assert.True(result is { Set: null, Action: GateAction.Suspend, Reason: MaintenanceReason.ManagedMemoryCap }, $"{result.Action} {result.Reason}: {result.Detail}, {result.Set?.Count} paths kept");
            // Charged up to the cap while computing
            Assert.InRange(context.Memory.ManagedPeak, AdvanceExclusions.MaxExclusionBytes - ExclusionSet.BytesOf(paths[0]), JobMemory.DefaultManagedCap);
        }

        [Fact]
        public async Task RebaseChargesTheKeptAndTheNewExclusionsWhileBothExist()
        {
            var rig = await PayloadAsync();
            var kept = LongExclusions(10_000);
            rig.Active.Exclusions = kept;
            await ForeignAsync(rig.Table, Add("part-0003.parquet"));
            await RebasingAsync(rig);

            // Room for the rebase's own work, not for the kept set
            AssertDeclined(await RebaseAsync(rig, new JobMemory(managedCap: 16 << 20)), MaintenanceReason.ManagedMemoryCap);
            Assert.Equal(AdvanceState.Rebasing, rig.Active.State);
            Assert.Same(kept, rig.Active.Exclusions);

            var memory = new JobMemory();
            long atRebase = -1;
            rig.Cleanup.AdvanceHooksForTests.Step = step =>
            {
                if (step == AdvanceSteps.Rebase)
                {
                    atRebase = memory.Managed;
                }
                return Task.CompletedTask;
            };
            Assert.Equal(OutcomeKind.Completed, (await RebaseAsync(rig, memory)).Kind);

            AssertExcluded(rig.Active.Exclusions, "/t/part-0003.parquet");
            Assert.Equal(kept.Bytes + ExclusionSet.BytesOf("/t/part-0003.parquet"), atRebase);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RebaseReleasesTheKeptAndTheNewExclusionsWhenItThrowsOrStops(bool stops)
        {
            var rig = await PayloadAsync();
            var kept = LongExclusions(10_000);
            rig.Active.Exclusions = kept;
            await ForeignAsync(rig.Table, Add("part-0003.parquet"));
            await RebasingAsync(rig);
            using var worker = new CancellationTokenSource();
            var memory = new JobMemory();
            long held = -1;
            rig.Cleanup.AdvanceHooksForTests.Step = step =>
            {
                if (step == AdvanceSteps.Rebase)
                {
                    held = memory.Managed;
                    if (stops)
                    {
                        worker.Cancel();
                        throw new OperationCanceledException(worker.Token);
                    }
                    throw new IOException("injected");
                }
                return Task.CompletedTask;
            };

            if (stops)
            {
                var context = _kit.AdvanceContext(rig.Table.Head, rig.Ledger, memory: memory, stop: worker.Token);
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => rig.Cleanup.RunAsync(JobKind.Advance, context));
                await context.SettleAsync();
            }
            else
            {
                AssertDeclined(await RebaseAsync(rig, memory), MaintenanceReason.Transient);
            }

            Assert.Equal((kept.Bytes + ExclusionSet.BytesOf("/t/part-0003.parquet"), 0L), (held, memory.Managed));
        }

        [Fact]
        public async Task RebaseWhoseShortReadsRunOutPromotes()
        {
            var rig = await PayloadAsync();
            await ForeignAsync(rig.Table, Add("part-0003.parquet"));
            await RebasingAsync(rig);
            var demand = rig.Active.Lease!.Demand;

            // The anchor's rehash spends the only read
            var promoted = await AdvanceExecutionKit.JobAsync(rig.Cleanup, ShortAdvanceContext(rig.Table, 1, rig.Ledger));

            Assert.True(promoted.NeedsLongAdmission, $"{promoted.Kind} {promoted.Reason}: {promoted.Detail}");
            Assert.Equal((AdvanceState.Rebasing, CapacityState.Runnable, demand), (rig.Active.State, rig.Active.Lease!.State, rig.Ledger.Current));
            Assert.Equal(0, rig.Logs.Warnings(nameof(MaintenanceReason.Transient)));

            // The long admission rebases
            Assert.Equal(OutcomeKind.Completed, (await RebaseAsync(rig)).Kind);
            AssertExcluded(rig.Active.Exclusions, "/t/part-0003.parquet");
        }

        [Fact]
        public async Task ShortExclusionsWhoseResolutionsOutgrowTheirReadsPromote()
        {
            var rig = await PayloadAsync(local: true);
            await ForeignAsync(rig.Table, Add("part-0002.parquet"));
            IFileStorage Metered(out MaintenanceBuckets buckets)
            {
                buckets = MaintenanceBucketsTests.Unthrottled();
                return MeteredFileStorage.Wrap(rig.Table.Raw, buckets, ListingPaging.LocalDisk);
            }
            // The object reads, counted alone without a meter
            var unmetered = _kit.AdvanceContext(rig.Table.Head);
            AssertExcluded((await ComputeAsync(rig, unmetered, storage: rig.Table.Raw)).Set, "/t/part-0002.parquet");
            var reads = unmetered.ReadsUsed;
            // Long admission charges every native resolution of the commit and the remaining paths
            var full = _kit.AdvanceContext(rig.Table.Head);
            AssertExcluded((await ComputeAsync(rig, full, storage: Metered(out var buckets))).Set, "/t/part-0002.parquet");
            var charges = buckets.NativeRequests;
            Assert.True(charges > 1, $"{charges} charges");

            // One read short promotes before any charge
            var promoted = await ComputeAsync(rig, ShortAdvanceContext(rig.Table, reads + charges - 1, rig.Ledger), storage: Metered(out var promotedBuckets));

            Assert.True(promoted is { Set: null, Action: GateAction.Promote }, $"{promoted.Action} {promoted.Reason}: {promoted.Detail}");
            Assert.True(promotedBuckets.NativeRequests == 0, $"{promotedBuckets.NativeRequests} charged before the promotion");
            // Exactly enough proceeds, and every charge is a counted read
            var fits = ShortAdvanceContext(rig.Table, reads + charges, rig.Ledger);
            AssertExcluded((await ComputeAsync(rig, fits, storage: Metered(out var fitsBuckets))).Set, "/t/part-0002.parquet");
            Assert.Equal((charges, reads + charges, reads + charges), (fitsBuckets.NativeRequests, fits.ReadsUsed, full.ReadsUsed));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DeclinedExclusionsRepayTheirResolutions(bool longAdmission)
        {
            var rig = await PayloadAsync(local: true);
            const int valid = 100;
            var paths = Enumerable.Range(0, valid).Select(x => $"foreign-{x:D4}.parquet").ToArray();
            foreach (var path in paths)
            {
                await File.WriteAllBytesAsync(Path.Combine(rig.Table.Root!, "t", path), new byte[] { 1 });
            }
            // An unusable last reference after a hundred native resolutions
            await ForeignAsync(rig.Table, paths.Select(Add).Append(Add("../outside.parquet")).ToArray());
            var buckets = MaintenanceBucketsTests.Unthrottled();
            var storage = MeteredFileStorage.Wrap(rig.Table.Raw, buckets, ListingPaging.LocalDisk);

            for (var attempt = 0; attempt < 2; attempt++)
            {
                var context = longAdmission
                    ? _kit.AdvanceContext(rig.Table.Head)
                    : ShortAdvanceContext(rig.Table, MaintenanceWorkerSettings.DefaultShortReads, rig.Ledger);
                var result = await ComputeAsync(rig, context, storage: storage);
                Assert.True(result is { Set: null, Action: GateAction.Suspend, Reason: MaintenanceReason.PathRejected }, $"{result.Action} {result.Reason}: {result.Detail}");
                // The planned head and the foreign commit; the repayment counts no reads
                Assert.Equal(2, context.ReadsUsed);
            }

            // Charged in turns, as a computed set's resolutions are
            Assert.True(buckets.NativeRequests >= 2 * valid, $"Two declined exclusions resolved {valid} paths each, but {buckets.NativeRequests} were charged with {buckets.Requests.Debt} debt");
        }

        [Fact]
        public async Task ExclusionChargesYieldAtTheDeadline()
        {
            // Every remaining path in one part
            var rig = await PayloadAsync(local: true, partPaths: 100);
            Assert.Equal(1, rig.Active.Started.Totals.Parts);
            await ForeignAsync(rig.Table, Add("part-0002.parquet"));
            // One request a second on the bucket's own clock
            var bucketClock = new FakeTimeProvider(s_now);
            var buckets = new MaintenanceBuckets(bucketClock, requestsPerSecond: 1, requestBurst: 1, bytesPerSecond: 1L << 30, byteBurst: 1L << 30);
            var clock = new FakeTimeProvider(s_now);
            using var worker = new CancellationTokenSource();
            var context = new CleanupContext(new JobRun(rig.Table.Head, JobKind.Advance) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Advance,
                Long = false,
                ReadLimit = MaintenanceWorkerSettings.DefaultShortReads,
                Deadline = s_now + TimeSpan.FromMinutes(15),
                Head = rig.Table.Head,
                TableId = null,
                Scratch = _kit.AdvanceContext(rig.Table.Head).Scratch,
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = worker.Token
            };

            var computing = ComputeAsync(rig, context, storage: MeteredFileStorage.Wrap(rig.Table.Raw, buckets, ListingPaging.LocalDisk));
            // Granted a turn at a time until the fourth charge waits
            while (true)
            {
                await MaintenanceBucketsTests.Until(() => computing.IsCompleted || buckets.Requests.QueueLength == 1);
                if (computing.IsCompleted || buckets.NativeRequests == 3)
                {
                    break;
                }
                var granted = buckets.Requests.Charged;
                bucketClock.Advance(TimeSpan.FromSeconds(1));
                await MaintenanceBucketsTests.Until(() => computing.IsCompleted || buckets.Requests.Charged > granted);
            }
            Assert.False(computing.IsCompleted, $"Computed with {buckets.NativeRequests} charges");
            clock.Advance(TimeSpan.FromMinutes(16));
            bucketClock.Advance(TimeSpan.FromSeconds(1));
            var yielded = await Task.WhenAny(computing, Task.Delay(TimeSpan.FromSeconds(10))) == computing;
            if (!yielded)
            {
                worker.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => computing);
            }
            Assert.True(yielded, $"Charging went on past the deadline: {buckets.NativeRequests} charges");

            var result = await computing;
            Assert.True(result is { Set: null, Action: GateAction.Retry, Reason: MaintenanceReason.JobDeadline }, $"{result.Action} {result.Reason}: {result.Detail}");
            // The granted charge, and none after it
            Assert.Equal((4L, 0), (buckets.NativeRequests, buckets.Requests.QueueLength));
        }

        [Fact]
        public async Task ForeignChangeWhoseGateConsumesTheShortReadBudgetPromotes()
        {
            var rig = await PayloadAsync();
            for (var commit = 0; commit < 5; commit++)
            {
                await ForeignAsync(rig.Table, Add("part-0003.parquet"));
            }

            // Fresh short admissions whose reads the gate's capability replay outgrows
            var first = await AdvanceExecutionKit.TurnAsync(rig.Cleanup, DeletionAdmission(rig, 10));
            var second = await AdvanceExecutionKit.TurnAsync(rig.Cleanup, DeletionAdmission(rig, 10));

            Assert.Equal((AdvanceState.Runnable, 0L), (rig.Active.State, rig.Active.Processed));
            Assert.All(new[] { first, second }, result => Assert.True(result.NeedsLongAdmission, $"{result.Kind} {result.Reason}: {result.Detail}"));
            // The long admission reaches the rebase
            Assert.Equal(OutcomeKind.NoWork, (await AdvanceExecutionKit.TurnAsync(rig.Cleanup, DeletionAdmission(rig, long.MaxValue))).Kind);
            Assert.Equal(AdvanceState.Rebasing, rig.Active.State);
        }

        // A delete admission over the rig's ledger with the given reads, long when they are unbounded
        private CleanupContext DeletionAdmission(Rig rig, long reads)
        {
            var basis = _kit.DeletionContext(rig.Table.Head, rig.Ledger);
            return new CleanupContext(new JobRun(rig.Table.Head, JobKind.Deletion) { Cancellation = new CancellationTokenSource() }, new FakeTimeProvider(s_now))
            {
                Kind = JobKind.Deletion,
                Long = reads == long.MaxValue,
                ReadLimit = reads,
                Deadline = basis.Deadline,
                Head = basis.Head,
                TableId = basis.TableId,
                Scratch = basis.Scratch,
                Memory = basis.Memory,
                Transfer = basis.Transfer,
                Stop = basis.Stop,
                Capacity = basis.Capacity,
                CapacityOwner = basis.CapacityOwner
            };
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task GapAfterThePlannedHeadLeavesTheDeleteTurnsForAForeignChange(bool logsComplete)
        {
            var rig = await StartedAsync(logsComplete, restart: true, local: false);
            var table = rig.Table;
            var demand = rig.Active.Lease!.Demand;
            await ForeignAsync(table, Add("part-0003.parquet"));
            await ForeignAsync(table, Add("part-0005.parquet"));
            await table.Raw.Rm(table.LogPath(Json(table.Head - 1)));
            var records = await table.SnapshotAsync();

            var turned = await TurnAsync(rig);

            if (logsComplete)
            {
                // The rebase finds the changed history incomplete
                Assert.True(turned.Kind == OutcomeKind.NoWork && rig.Active.State == AdvanceState.Rebasing, $"{turned.Kind} {turned.Reason}: {turned.Detail}, {rig.Active.State}");
                turned = await RebaseAsync(rig);
            }
            AssertDeclined(turned, MaintenanceReason.HistoryChanged);
            Assert.Equal((AdvanceState.Suspended, CapacityState.Suspended), (rig.Active.State, rig.Active.Lease!.State));
            Assert.Equal(demand, rig.Ledger.Total(CapacityState.Suspended));
            Assert.Null(rig.Cleanup.ActiveForTests);
            Assert.Equal(records, await table.SnapshotAsync());
            Assert.Equal(rig.Active.C, await AdvanceExecutionKit.FloorAsync(table));
        }

        [Fact]
        public async Task IntersectionStreamsCandidatesWhoseKeyProtectionHolds()
        {
            var table = await _kit.ChurnedAsync(local: false);
            var context = _kit.AdvanceContext(table.Head);
            var domain = PathCanonicalizer.Create(table.Configuration.Store).Domain;
            var emitted = new List<string>();
            using (var candidates = new PathSet(context.Scratch, context.Memory, domain, PathRole.Candidate))
            using (var protection = new PathSet(context.Scratch, context.Memory, domain, PathRole.Protection))
            using (var other = new PathSet(context.Scratch, context.Memory, PathCanonicalizer.Create(table.Configuration.Store).Domain with { Identity = PathIdentity.Cloud }, PathRole.Protection))
            {
                candidates.Add("A", "/t/a");
                candidates.Add("B", "/t/b");
                candidates.Add("B", "/t/B");
                candidates.Add("C");
                candidates.Add("D", "/t/d");
                protection.Add("B");
                protection.Add("C");
                protection.Add("E");
                candidates.Seal();
                protection.Seal();
                other.Seal();
                Assert.Throws<ArgumentException>(() => PathSet.Intersection(protection, candidates));
                Assert.Throws<ArgumentException>(() => PathSet.Intersection(candidates, other));
                using var both = PathSet.Intersection(candidates, protection);
                while (both.MoveNext())
                {
                    emitted.Add(both.DeletionPathText);
                }
            }

            // Every candidate under a protected key that names a path
            Assert.Equal(new[] { "/t/B", "/t/b" }, emitted);
            AssertReleased(context);
        }
    }
}

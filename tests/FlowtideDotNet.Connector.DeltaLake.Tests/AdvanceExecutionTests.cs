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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class AdvanceExecutionTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = AdvanceExecutionKit.Now;

        private readonly AdvanceExecutionKit _kit = new AdvanceExecutionKit();

        public void Dispose()
        {
            _kit.Dispose();
        }

        // A started advance adopted by a cleanup whose turns run fake phases
        private sealed class Rig
        {
            public required ExecutionTable Table { get; init; }

            public required DeltaTableCleanup Cleanup { get; init; }

            public required ActiveAdvance Active { get; init; }

            public required CapacityLedger Ledger { get; init; }

            public required DeltaMaintenanceWorkerTests.CollectingLogger Logs { get; init; }
        }

        // Published by one cleanup and adopted by another when restarted, with its lease runnable in a ledger of its own
        private async Task<Rig> StartedAsync(bool logsComplete = false, bool restart = false, bool local = true, long requestsPerSecond = MaintenanceBuckets.DefaultRequestsPerSecond)
        {
            var table = await _kit.ChurnedAsync(local);
            var logs = new DeltaMaintenanceWorkerTests.CollectingLogger();
            var cleanup = AdvanceExecutionKit.Cleanup(table, logs);
            await _kit.PublishAsync(cleanup, table, logsComplete: logsComplete);
            if (restart)
            {
                cleanup = AdvanceExecutionKit.Cleanup(table, logs);
            }
            var ledger = new CapacityLedger(() => default, requestsPerSecond);
            var active = await _kit.AdoptAsync(cleanup, table, ledger);
            Assert.Equal(logsComplete ? AdvancePhase.Payload : AdvancePhase.Log, active.Phase);
            Assert.Equal(CapacityState.Runnable, active.Lease!.State);
            return new Rig() { Table = table, Cleanup = cleanup, Active = active, Ledger = ledger, Logs = logs };
        }

        private Task<CleanupResult> TurnAsync(Rig rig) => AdvanceExecutionKit.TurnAsync(rig.Cleanup, _kit.DeletionContext(rig.Table.Head, rig.Ledger));

        // Applies each step to the advance and returns its slice, the last step from then on
        private static AdvancePhaseSlice Scripted(params Func<ActiveAdvance, SliceResult>[] steps)
        {
            var next = 0;
            return (active, turn, context) => Task.FromResult(steps[Math.Min(next++, steps.Length - 1)](active));
        }

        private static SliceResult Slice(SliceStatus status, MaintenanceReason reason = MaintenanceReason.None) => new SliceResult(status, reason, 0) { Detail = $"A scripted {status} slice" };

        private static Func<ActiveAdvance, SliceResult> Still(SliceStatus status, MaintenanceReason reason = MaintenanceReason.None) => _ => Slice(status, reason);

        // One log target fewer, as a settled delete leaves
        private static Func<ActiveAdvance, SliceResult> Moved(SliceStatus status, MaintenanceReason reason = MaintenanceReason.None) => active =>
        {
            active.LogTargetsLeft--;
            return Slice(status, reason);
        };

        private static void AssertDeclined(CleanupResult result, MaintenanceReason reason)
        {
            Assert.True(result.Kind == OutcomeKind.Declined && result.Reason == reason, $"{result.Kind} {result.Reason}: {result.Detail}");
        }

        [Fact]
        public void LogTargetsDeleteCompactionsFirstThenEachVersionDownwardAndNeverC()
        {
            const long c = 10;
            static string V(long version) => $"{version:D20}";
            var names = Enumerable.Range(0, 13).Select(x => $"{V(x)}.json").Concat(new[]
            {
                $"{V(9)}.crc",
                $"{V(10)}.crc",
                $"{V(5)}.checkpoint.parquet",
                $"{V(10)}.checkpoint.parquet",
                $"{V(7)}.checkpoint.0000000002.0000000002.parquet",
                $"{V(7)}.checkpoint.0000000001.0000000002.parquet",
                $"{V(6)}.checkpoint.{Guid.NewGuid():D}.parquet",
                $"{V(3)}.{V(5)}.compacted.json",
                $"{V(8)}.{V(12)}.compacted.json",
                $"{V(10)}.{V(11)}.compacted.json",
                $"{V(11)}.{V(12)}.compacted.json",
                $"{V(4)}.unknown",
                "_last_checkpoint",
                $".{V(5)}.json.stage.tmp",
                $".flowtide-{Guid.NewGuid():N}.checkpoint.tmp",
                $"{DeltaTransactionReader.CleanupControlPrefix}{V(10)}.record",
                ".hidden",
                "unversioned"
            });
            // Listed in an order unlike the deletion order
            var classes = LogClassifier.Classify(names.Reverse().Select(x => new IOEntry($"/t/_delta_log/{x}")));

            var expected = new[]
            {
                // Compactions starting at or below C, one spanning it, the highest start first
                $"{V(10)}.{V(11)}.compacted.json",
                $"{V(8)}.{V(12)}.compacted.json",
                $"{V(3)}.{V(5)}.compacted.json",
                // Then each version below C from the highest: commit, checksum, classic checkpoint, multipart parts in part order
                $"{V(9)}.json",
                $"{V(9)}.crc",
                $"{V(8)}.json",
                $"{V(7)}.json",
                $"{V(7)}.checkpoint.0000000001.0000000002.parquet",
                $"{V(7)}.checkpoint.0000000002.0000000002.parquet",
                $"{V(6)}.json",
                $"{V(5)}.json",
                $"{V(5)}.checkpoint.parquet",
                $"{V(4)}.json",
                $"{V(3)}.json",
                $"{V(2)}.json",
                $"{V(1)}.json",
                $"{V(0)}.json"
            };
            Assert.Equal(expected, LogTargets.Order(classes, c).Select(x => x.Entry.Name));
            Assert.Equal(expected.Length, LogTargets.Count(classes, c));
        }

        [Theory]
        [InlineData(true, 200)]
        [InlineData(false, 200)]
        [InlineData(true, 5000)]
        [InlineData(false, 5000)]
        public async Task PassedSliceReestimatesDemandAtTheMeasuredLatency(bool local, long requestsPerSecond)
        {
            var rig = await StartedAsync(local: local, requestsPerSecond: requestsPerSecond);
            var before = rig.Active.Lease!.Demand;
            // Two serial log deletes in 200 ms, a full slice
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxPaths = 2 };
            rig.Cleanup.LogPhaseForTests = Scripted(active =>
            {
                active.LogTargetsLeft -= 2;
                active.LogEntries -= 2;
                return Slice(SliceStatus.Progressed) with { Issued = 2, Elapsed = TimeSpan.FromMilliseconds(200) };
            });

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);

            // A quarter of the way from 50 ms to 100 ms
            Assert.Equal(TimeSpan.FromTicks(CapacityEstimator.DefaultLatency.Ticks + (TimeSpan.FromMilliseconds(100) - CapacityEstimator.DefaultLatency).Ticks / 4), rig.Cleanup.Latency);
            var demand = CapacityEstimator.Advance(AdvanceDemand.Of(rig.Table.Configuration, rig.Active, rig.Cleanup.Latency), requestsPerSecond);
            Assert.NotEqual(before, demand);
            Assert.Equal(demand, rig.Active.Lease!.Demand);
            Assert.Equal(demand, rig.Ledger.Current);
            Assert.Same(rig.Active, rig.Cleanup.ActiveForTests);
            Assert.True(rig.Cleanup.IsDue(JobKind.Deletion, rig.Table.Head, s_now));
        }

        [Fact]
        public async Task DeadlineLimitedSlicesOnASlowStoreRepriceTheAdvance()
        {
            var rig = await StartedAsync(logsComplete: true);
            var before = rig.Active.Lease!.Demand;
            // Serial deletes, as on pinned S3, each slice spending its whole issuance time at one second per delete
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxOutstanding = 1 };
            var perDelete = TimeSpan.FromSeconds(1);
            var issuance = rig.Cleanup.Settings.IssuanceTime;
            rig.Cleanup.PayloadPhaseForTests = Scripted(_ => Slice(SliceStatus.Progressed) with { Issued = (int)(issuance / perDelete), Elapsed = issuance, IssuanceWindow = issuance - perDelete });

            // Four minutes of deletes
            for (int i = 0; i < 8; i++)
            {
                Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            }

            var demand = rig.Active.Lease!.Demand;
            Assert.True(rig.Cleanup.Latency >= perDelete / 2 && rig.Cleanup.Latency <= perDelete,
                $"After 8 deadline-limited slices measured at {perDelete} per delete the advance is priced at {rig.Cleanup.Latency} per delete ({before.Requests} request equivalents before, {demand.Requests} now)");
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task SliceCountsInFullOnlyWhenItsIssuanceReachedItsTime(bool issuing)
        {
            var rig = await StartedAsync(logsComplete: true);
            rig.Cleanup.Settings = rig.Cleanup.Settings with { MaxOutstanding = 1 };
            var settings = rig.Cleanup.Settings;
            // Ten issuance times from the first issuance to settlement, spent issuing or only waiting in settlement
            var elapsed = settings.IssuanceTime * 10;
            rig.Cleanup.PayloadPhaseForTests = Scripted(_ => Slice(SliceStatus.Progressed) with { Issued = 1, Elapsed = elapsed, IssuanceWindow = issuing ? elapsed : TimeSpan.Zero });

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);

            // A quarter of the way as a full slice, or one delete's share of it
            var counted = issuing ? settings.MaxPaths : 1;
            var expected = CapacityEstimator.DefaultLatency + (elapsed - CapacityEstimator.DefaultLatency) * counted / (4 * settings.MaxPaths);
            Assert.InRange(rig.Cleanup.Latency, expected - TimeSpan.FromMilliseconds(1), expected + TimeSpan.FromMilliseconds(1));
        }

        [Fact]
        public async Task OneStalledDeleteInAShortLastSliceKeepsThePrice()
        {
            var rig = await StartedAsync(logsComplete: true);
            // The last three paths of an advance, issued at once, one of them settling after 35 seconds
            rig.Cleanup.PayloadPhaseForTests = Scripted(_ => Slice(SliceStatus.Progressed) with { Issued = 3, Elapsed = TimeSpan.FromSeconds(35) });

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);

            Assert.True(rig.Cleanup.Latency < TimeSpan.FromSeconds(1), $"One 35 s delete in a 3-path slice priced the advance at {rig.Cleanup.Latency} per delete");
        }

        [Fact]
        public async Task RetriedSliceKeepsEverythingAndWarnsOnce()
        {
            var rig = await StartedAsync();
            var demand = rig.Active.Lease!.Demand;
            rig.Cleanup.LogPhaseForTests = Scripted(Still(SliceStatus.Retry, MaintenanceReason.ListingIneligible), Still(SliceStatus.Retry, MaintenanceReason.ListingIneligible),
                _ => Slice(SliceStatus.Retry) with { Stop = IssuanceStop.Deadline });

            AssertDeclined(await TurnAsync(rig), MaintenanceReason.ListingIneligible);
            AssertDeclined(await TurnAsync(rig), MaintenanceReason.ListingIneligible);
            // A stop retries without a reason of its own
            AssertDeclined(await TurnAsync(rig), MaintenanceReason.Transient);

            Assert.Equal((AdvanceState.Runnable, 0, -1L), (rig.Active.State, rig.Active.Failures, rig.Active.FailedAt));
            Assert.Equal((CapacityState.Runnable, demand), (rig.Active.Lease!.State, rig.Active.Lease.Demand));
            Assert.Same(rig.Active, rig.Cleanup.ActiveForTests);
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.ListingIneligible)));
            Assert.Equal(0, rig.Logs.Warnings(nameof(MaintenanceReason.Transient)));
        }

        [Fact]
        public async Task ThreeFailuresAtOnePositionSuspendWithTheAuthorityKept()
        {
            var rig = await StartedAsync();
            var records = await rig.Table.SnapshotAsync();
            var demand = rig.Active.Lease!.Demand;
            rig.Cleanup.LogPhaseForTests = Scripted(Still(SliceStatus.Failed, MaintenanceReason.DeleteFailed));

            for (int failures = 1; failures < AdvanceExecution.MaxDurableFailures; failures++)
            {
                AssertDeclined(await TurnAsync(rig), MaintenanceReason.DeleteFailed);
                Assert.Equal((AdvanceState.Runnable, failures), (rig.Active.State, rig.Active.Failures));
                Assert.Same(rig.Active, rig.Cleanup.ActiveForTests);
            }
            AssertDeclined(await TurnAsync(rig), MaintenanceReason.DeleteFailed);

            // Suspended once, its demand reported apart, its records and reservation kept
            Assert.Equal(AdvanceState.Suspended, rig.Active.State);
            Assert.Equal(CapacityState.Suspended, rig.Active.Lease!.State);
            Assert.Equal(demand, rig.Ledger.Total(CapacityState.Suspended));
            Assert.Equal(default, rig.Ledger.Current);
            Assert.Null(rig.Cleanup.ActiveForTests);
            Assert.False(rig.Cleanup.IsDue(JobKind.Deletion, rig.Table.Head, s_now));
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.DeleteFailed)));
            Assert.Equal(records, await rig.Table.SnapshotAsync());
            Assert.Equal(rig.Active.C, await AdvanceExecutionKit.FloorAsync(rig.Table));
        }

        [Fact]
        public async Task MovedPositionResetsTheFailureCount()
        {
            var rig = await StartedAsync();
            var failed = Still(SliceStatus.Failed, MaintenanceReason.DeleteFailed);
            rig.Cleanup.LogPhaseForTests = Scripted(failed, failed, Moved(SliceStatus.Failed, MaintenanceReason.DeleteFailed), failed, Moved(SliceStatus.Progressed), failed, failed);

            var counts = new List<int>();
            for (int turn = 0; turn < 7; turn++)
            {
                await TurnAsync(rig);
                counts.Add(rig.Active.Failures);
            }

            // A failure at another target or a slice past the failed one starts the count over
            Assert.Equal(new[] { 1, 2, 1, 2, 0, 1, 2 }, counts);
            Assert.Equal(AdvanceState.Runnable, rig.Active.State);
            Assert.Same(rig.Active, rig.Cleanup.ActiveForTests);
        }

        [Fact]
        public async Task SuspendedSliceRetiresTheAdvanceForTheDay()
        {
            // A restarted cleanup, whose daily advance is still due without one
            var rig = await StartedAsync(restart: true);
            var demand = rig.Active.Lease!.Demand;
            Assert.False(rig.Cleanup.IsDue(JobKind.Advance, rig.Table.Head, s_now));
            rig.Cleanup.LogPhaseForTests = Scripted(Still(SliceStatus.Suspended, MaintenanceReason.HistoryChanged));

            AssertDeclined(await TurnAsync(rig), MaintenanceReason.HistoryChanged);

            Assert.Equal(AdvanceState.Suspended, rig.Active.State);
            Assert.Equal(demand, rig.Ledger.Total(CapacityState.Suspended));
            Assert.Null(rig.Cleanup.ActiveForTests);
            Assert.False(rig.Cleanup.IsDue(JobKind.Deletion, rig.Table.Head, s_now));
            Assert.False(rig.Cleanup.IsDue(JobKind.Advance, rig.Table.Head, s_now));
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.HistoryChanged)));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ForeignChangeSuspendsTheLogPhaseAndRebasesThePayloadPhase(bool logsComplete)
        {
            var rig = await StartedAsync(logsComplete);
            var demand = rig.Active.Lease!.Demand;
            var rebase = Scripted(Still(SliceStatus.Rebase, MaintenanceReason.HistoryChanged));
            rig.Cleanup.LogPhaseForTests = rebase;
            rig.Cleanup.PayloadPhaseForTests = rebase;

            var result = await TurnAsync(rig);

            Assert.False(rig.Cleanup.IsDue(JobKind.Deletion, rig.Table.Head, s_now));
            if (!logsComplete)
            {
                AssertDeclined(result, MaintenanceReason.HistoryChanged);
                Assert.Equal(AdvanceState.Suspended, rig.Active.State);
                Assert.Equal(demand, rig.Ledger.Total(CapacityState.Suspended));
                Assert.Null(rig.Cleanup.ActiveForTests);
                return;
            }
            // An advance job rebases while the demand stays runnable
            Assert.Equal(OutcomeKind.NoWork, result.Kind);
            Assert.Equal(AdvanceState.Rebasing, rig.Active.State);
            Assert.Equal(demand, rig.Ledger.Current);
            Assert.Same(rig.Active, rig.Cleanup.ActiveForTests);
            Assert.True(rig.Cleanup.IsDue(JobKind.Advance, rig.Table.Head, s_now));
        }

        [Fact]
        public async Task DisabledDeletesPauseUntilASlicePasses()
        {
            var rig = await StartedAsync();
            var demand = rig.Active.Lease!.Demand;
            rig.Cleanup.LogPhaseForTests = Scripted(Still(SliceStatus.Disabled, MaintenanceReason.DeletesDisabled), Still(SliceStatus.Retry, MaintenanceReason.Transient),
                Moved(SliceStatus.Progressed));

            AssertDeclined(await TurnAsync(rig), MaintenanceReason.DeletesDisabled);
            Assert.Equal(AdvanceState.Paused, rig.Active.State);
            Assert.Equal((demand, default), (rig.Ledger.Total(CapacityState.Paused), rig.Ledger.Current));
            Assert.True(rig.Cleanup.IsDue(JobKind.Deletion, rig.Table.Head, s_now));

            AssertDeclined(await TurnAsync(rig), MaintenanceReason.Transient);
            Assert.Equal(AdvanceState.Paused, rig.Active.State);

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            Assert.Equal(AdvanceState.Runnable, rig.Active.State);
            Assert.Equal(CapacityState.Runnable, rig.Active.Lease!.State);
            Assert.Equal(default, rig.Ledger.Total(CapacityState.Paused));
            Assert.Equal(rig.Active.Lease.Demand, rig.Ledger.Current);
        }

        [Fact]
        public async Task PromotedAndUnadmittedSlicesKeepTheAdvance()
        {
            var rig = await StartedAsync();
            rig.Cleanup.LogPhaseForTests = Scripted(Still(SliceStatus.Promote), Still(SliceStatus.NotAdmitted, MaintenanceReason.Unexpected));

            Assert.True((await TurnAsync(rig)).NeedsLongAdmission);
            var unadmitted = await TurnAsync(rig);

            Assert.Equal((OutcomeKind.Failed, MaintenanceReason.Unexpected), (unadmitted.Kind, unadmitted.Reason));
            Assert.Equal((AdvanceState.Runnable, 0), (rig.Active.State, rig.Active.Failures));
            Assert.Same(rig.Active, rig.Cleanup.ActiveForTests);
        }

        [Fact]
        public async Task FinishedAdvanceLeavesTheTurnsAndAsksForTheNext()
        {
            var rig = await StartedAsync(logsComplete: true);
            Assert.False(rig.Cleanup.IsDue(JobKind.Advance, rig.Table.Head, s_now));
            rig.Cleanup.PayloadPhaseForTests = Scripted(x =>
            {
                x.Phase = AdvancePhase.Done;
                return Slice(SliceStatus.Completed);
            });

            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);

            Assert.Null(rig.Cleanup.ActiveForTests);
            Assert.False(rig.Cleanup.IsDue(JobKind.Deletion, rig.Table.Head, s_now));
            Assert.True(rig.Cleanup.IsDue(JobKind.Advance, rig.Table.Head, s_now));
        }

        [Theory]
        [InlineData("none", nameof(GateVerdict.Passed))]
        [InlineData("replaced", nameof(GateVerdict.CheckpointReplaced))]
        [InlineData("mtime", nameof(GateVerdict.CheckpointReplaced))]
        [InlineData("started", nameof(GateVerdict.AuthorityChanged))]
        [InlineData("logs-complete", nameof(GateVerdict.AuthorityChanged))]
        [InlineData("reservation", nameof(GateVerdict.AuthorityChanged))]
        [InlineData("same-c-other-id", nameof(GateVerdict.AuthorityChanged))]
        [InlineData("same-c-other-digest", nameof(GateVerdict.AuthorityChanged))]
        [InlineData("uninterpreted", nameof(GateVerdict.ListingFailed))]
        [InlineData("gap-below", nameof(GateVerdict.Passed))]
        [InlineData("gap-from-c", nameof(GateVerdict.ListingFailed))]
        public async Task AdvanceGateReliesOnCItsAuthorityAndTheRetainedRange(string change, string expected)
        {
            var table = await _kit.ChurnedAsync();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            var published = await _kit.PublishAsync(cleanup, table);
            // A newer root than C, which the capability rehashes instead of C
            await AdvanceExecutionKit.CommitAsync(table, 2);
            await _kit.BuildAsync(table.Raw, table.Head);
            var active = await _kit.AdoptAsync(cleanup, table);
            Assert.True(published.C > 5 && published.C + 1 < active.Started.Head);
            Assert.DoesNotContain(active.Evidence.Capability.Sources, x => x.IsCheckpoint && x.Version == active.C);

            switch (change)
            {
                case "replaced":
                    // Other bytes at the size and modification time the header names
                    var path = Path.Combine(table.Root!, "t", "_delta_log", $"{active.C:D20}.checkpoint.parquet");
                    var modified = File.GetLastWriteTimeUtc(path);
                    var bytes = File.ReadAllBytes(path);
                    bytes[bytes.Length / 2] ^= 0xFF;
                    File.WriteAllBytes(path, bytes);
                    File.SetLastWriteTimeUtc(path, modified);
                    var listed = (await DeltaTransactionReader.ListLog(table.Raw, "t")).Checkpoints.Single(x => x.Version == active.C).IOEntry;
                    Assert.Equal((active.Destination.ListedSize, active.Destination.ListedModifiedTicks), (listed.Size ?? -1, listed.LastModificationTime?.UtcTicks ?? -1));
                    break;
                case "mtime":
                    // Unchanged bytes, modified five minutes later
                    var touched = Path.Combine(table.Root!, "t", "_delta_log", $"{active.C:D20}.checkpoint.parquet");
                    File.SetLastWriteTimeUtc(touched, File.GetLastWriteTimeUtc(touched).AddMinutes(5));
                    break;
                case "started":
                    await table.Raw.Rm(table.LogPath((await table.ControlsAsync()).Single(x => x.EndsWith(ControlName.StartedSuffix, StringComparison.Ordinal))));
                    break;
                case "logs-complete":
                    active.Phase = AdvancePhase.Payload;
                    break;
                case "reservation":
                    // Another advance's start, at a lower boundary
                    var other = CleanupRecordKit.Fixture(active.Started.TableId, 8, 0, 0, 14, 0);
                    await CleanupRecordKit.PlantAsync(table.Raw, "t", CleanupRecordKit.Started(other, CleanupRecordKit.Header(other, CleanupRecordKit.Part(other, 0, "/t/f-8.parquet"))));
                    break;
                case "same-c-other-id":
                    // Another advance's start at C
                    var rival = CleanupRecordKit.Fixture(active.Started.TableId, active.C, 0, 0, 14, 0);
                    await CleanupRecordKit.PlantAsync(table.Raw, "t", CleanupRecordKit.Started(rival, CleanupRecordKit.Header(rival, CleanupRecordKit.Part(rival, 0, "/t/f-c.parquet"))));
                    break;
                case "same-c-other-digest":
                    // This advance's start at C under another header
                    var again = CleanupRecordKit.Fixture(active.Started.TableId, active.C, 0, 0, 14, 0) with { AdvanceId = active.Association.AdvanceId };
                    await CleanupRecordKit.PlantAsync(table.Raw, "t", CleanupRecordKit.Started(again, CleanupRecordKit.Header(again, CleanupRecordKit.Part(again, 0, "/t/f-c.parquet"))));
                    break;
                case "uninterpreted":
                    await CleanupRecordKit.PlantAsync(table.Raw, "t", $"{DeltaTransactionReader.CleanupControlPrefix}{active.C:D20}.unknown", new byte[1]);
                    break;
                case "gap-below":
                    await table.Raw.Rm(table.LogPath($"{5:D20}.json"));
                    break;
                case "gap-from-c":
                    await table.Raw.Rm(table.LogPath($"{active.C + 1:D20}.json"));
                    break;
            }
            var decided = 0;
            var gate = ChunkGate.ForAdvance(table.Configuration, table.Metered, active, (log, _) =>
            {
                decided++;
                return Task.FromResult(new GateResult(GateVerdict.Passed) { Head = log.Head });
            });
            var context = _kit.DeletionContext(table.Head);

            var result = await gate.PassAsync(context);

            var verdict = Enum.Parse<GateVerdict>(expected);
            Assert.True(result.Verdict == verdict, $"{result.Verdict} {result.Listing}: {result.Detail}");
            Assert.True(gate.Durable && result.Durable);
            Assert.Equal(verdict switch
            {
                GateVerdict.Passed => GateAction.Proceed,
                GateVerdict.ListingFailed => GateAction.Retry,
                _ => GateAction.Suspend
            }, result.Action);
            // Decided only on a listing whose checks all passed
            Assert.Equal(result.Passed ? 1 : 0, decided);
            Assert.Equal(change switch
            {
                "uninterpreted" => ListingCheck.ControlFenced,
                "gap-from-c" => ListingCheck.RangeGap,
                _ => ListingCheck.Passed
            }, result.Listing);
            Assert.Equal(0, context.Memory.Managed);
        }

        [Fact]
        public async Task SliceTimesItsIssuanceToSettlementApartFromItsGate()
        {
            var table = await _kit.ChurnedAsync(local: false);
            await using (var stream = await table.Raw.OpenWrite("/t/x.parquet"))
            {
                await stream.WriteAsync(new byte[1]);
            }
            var clock = new FakeTimeProvider(s_now);
            // The gate takes two seconds
            var gate = ChunkGate.ForStages(table.Configuration, table.Metered, (_, _) =>
            {
                clock.Advance(TimeSpan.FromSeconds(2));
                return Task.FromResult(new GateResult(GateVerdict.Passed));
            });
            await using (var stream = await table.Raw.OpenWrite("/t/y.parquet"))
            {
                await stream.WriteAsync(new byte[1]);
            }
            var entered = new[] { new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
            var release = new[] { new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
            var deletes = 0;
            var executor = new DeletionExecutor(table.Configuration, table.Metered)
            {
                BeforeDeleteForTests = async _ =>
                {
                    var delete = Interlocked.Increment(ref deletes) - 1;
                    entered[delete].TrySetResult();
                    await release[delete].Task;
                }
            };
            var targets = new[] { executor.TargetOf("/t/x.parquet"), executor.TargetOf("/t/y.parquet") };
            Assert.All(targets, x => Assert.Equal(KeepReason.None, executor.Check(x)));
            // One delete outstanding
            var run = new DeletionRun(executor, gate, new ListDeletionSource(targets), clock, new DeletionRunSettings() { MaxOutstanding = 1 });

            var slice = run.RunSliceAsync(_kit.DeletionContext(table.Head, clock: clock));
            await entered[0].Task;
            // The second issued a second after the first, and settled three seconds after its issuance
            clock.Advance(TimeSpan.FromSeconds(1));
            release[0].SetResult();
            await entered[1].Task;
            clock.Advance(TimeSpan.FromSeconds(3));
            release[1].SetResult();
            var result = await slice;

            Assert.Equal((SliceStatus.Completed, 2), (result.Status, result.Issued));
            Assert.Equal((TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(4)), (result.GateElapsed, result.IssuanceWindow, result.Elapsed));
        }

        [Fact]
        public async Task DurableRunPassesExcludedTargetsAndStillSuspendsOnRejectedPaths()
        {
            var table = await _kit.ChurnedAsync();
            var configuration = table.Configuration;
            var (check, log) = await ListingEvidence.ReadLogAsync(configuration, table.Metered, table.Head, default);
            Assert.Equal(ListingCheck.Passed, check);
            var established = await CleanupEvidence.EstablishAsync(configuration, table.Metered, log!, log!.Head, null, durable: true, _kit.AdvanceContext(table.Head));
            Assert.True(established.Passed, $"{established.Verdict}: {established.Detail}");
            foreach (var name in new[] { "a", "b", "c" })
            {
                await using var stream = await table.Raw.OpenWrite($"/t/x-{name}.parquet");
                await stream.WriteAsync(new byte[1]);
            }
            var executor = new DeletionExecutor(configuration, table.Metered);
            var targets = new[]
            {
                executor.TargetOf("/t/x-a.parquet"),
                executor.TargetOf("/t/x-b.parquet") with { Excluded = true },
                executor.TargetOf("/t/x-c.parquet"),
                // Not this table's path, though excluded
                executor.TargetOf("/elsewhere/x-d.parquet") with { Excluded = true }
            };
            var run = new DeletionRun(executor, ChunkGate.ForEvidence(configuration, table.Metered, established.Evidence!), new ListDeletionSource(targets), new FakeTimeProvider(s_now));
            Assert.True(run.Durable);
            var context = _kit.DeletionContext(table.Head);

            var slice = await run.RunSliceAsync(context);
            await context.SettleAsync();

            // The placed excluded target is passed without a delete, the rejected one holds the cursor
            Assert.Equal((SliceStatus.Suspended, MaintenanceReason.PathRejected, 3L), (slice.Status, slice.Reason, slice.Cursor));
            Assert.Equal((false, true, false), (await table.Raw.Exists("/t/x-a.parquet"), await table.Raw.Exists("/t/x-b.parquet"), await table.Raw.Exists("/t/x-c.parquet")));
            Assert.Equal((2L, 1L, 1L), (executor.Tally.Deleted, executor.Tally.KeptFor(KeepReason.Excluded), executor.Tally.KeptFor(KeepReason.Unqualified)));
        }
    }
}

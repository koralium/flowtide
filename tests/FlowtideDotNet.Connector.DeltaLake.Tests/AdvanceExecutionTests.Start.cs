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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class AdvanceExecutionTests
    {
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);

        // Ends each new advance once its hint is raised, before its manifest
        internal static DeltaTableCleanup PlanOnly(DeltaTableCleanup cleanup)
        {
            cleanup.AdvanceHooksForTests.Step = step => step == AdvanceSteps.Hint
                ? throw new AdvanceEndedException(new CleanupResult(OutcomeKind.Completed, MaintenanceReason.None, "Planned; nothing is published"))
                : Task.CompletedTask;
            return cleanup;
        }

        // A resume starts without exclusions, as one in an uninterrupted epoch does
        internal static DeltaTableCleanup WithoutExclusions(DeltaTableCleanup cleanup)
        {
            cleanup.AdvanceForTests.ResumeExclusionsForTests = static (_, _, _, _, _, _, _) => Task.FromResult(new ExclusionResult(ExclusionSet.Empty));
            return cleanup;
        }

        // Hours of drain on the request axis
        private static CapacityDemand Hours(double hours) => new CapacityDemand((long)Math.Round(hours * 3600 * MaintenanceBuckets.DefaultRequestsPerSecond), 0);

        // A phase that counts its slices and retries each
        private sealed class CountingPhase
        {
            public int Calls;

            public AdvancePhaseSlice Slice => (active, turn, context) =>
            {
                Interlocked.Increment(ref Calls);
                return Task.FromResult(new SliceResult(SliceStatus.Retry, MaintenanceReason.Transient, 0) { Detail = "A counted slice" });
            };
        }

        // One long advance job, which releases everything it held
        private async Task<CleanupResult> AdvanceAsync(DeltaTableCleanup cleanup, ExecutionTable table, CapacityLedger? ledger = null)
        {
            var context = _kit.AdvanceContext(table.Head, ledger);
            CleanupResult result;
            try
            {
                result = await cleanup.RunAsync(JobKind.Advance, context);
            }
            finally
            {
                await context.SettleAsync();
            }
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
            Assert.Empty(context.Scratch.Files);
            return result;
        }

        // A short advance admission with the given reads, as the worker's first gate takes it
        private CleanupContext ShortContext(ExecutionTable table, long reads)
        {
            var basis = _kit.AdvanceContext(table.Head);
            var clock = new FakeTimeProvider(s_now);
            return new CleanupContext(new JobRun(table.Head, JobKind.Advance) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Advance,
                Long = false,
                ReadLimit = reads,
                Deadline = clock.GetUtcNow() + TimeSpan.FromHours(1),
                Head = table.Head,
                TableId = null,
                Scratch = basis.Scratch,
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default
            };
        }

        private static bool IsStarted(string name) => ControlName.TryParse(name, out var parsed) && parsed.Kind == ControlKind.Started;

        [Fact]
        public async Task CapacityAdmissionIsProspectiveAndAtomic()
        {
            var table = await _kit.ChurnedAsync();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            var ledger = new CapacityLedger(() => default);
            var checkedAndAdded = 0;
            ledger.BetweenCheckAndAddForTests = () => checkedAndAdded++;
            CapacityLease? competing = null;
            var manifest = Array.Empty<string>();
            // Admitted before selection; another table reserves all but one request of twelve hours once the parts and header are published
            cleanup.AdvanceHooksForTests.Step = async step =>
            {
                if (step == AdvanceSteps.Header)
                {
                    manifest = await table.ControlsAsync();
                    competing = ledger.TryReserve(default, "other-table", ControlTokens.New(), Hours(12) - new CapacityDemand(1, 0));
                }
            };

            var result = await AdvanceAsync(cleanup, table, ledger);

            AssertDeclined(result, MaintenanceReason.DeleteBacklog);
            // Only the competitor passed a check; the advance's demand was never added
            Assert.NotNull(competing);
            Assert.Equal(1, checkedAndAdded);
            Assert.Equal(competing!.Demand, ledger.Current);
            // No started record after the decline, the manifest left for recovery and the day's advance still due
            Assert.Contains(manifest, x => ControlName.TryParse(x, out var name) && name.Kind == ControlKind.Header);
            Assert.DoesNotContain(manifest, IsStarted);
            Assert.Equal(manifest, await table.ControlsAsync());
            Assert.Null(cleanup.ActiveForTests);
            Assert.True(cleanup.IsDue(JobKind.Advance, table.Head, s_now));

            // The next recovery scavenges the manifest before admission declines again
            cleanup.AdvanceHooksForTests.Step = null;
            AssertDeclined(await AdvanceAsync(cleanup, table, ledger), MaintenanceReason.DeleteBacklog);
            Assert.Empty(await table.ControlsAsync());
        }

        [Fact]
        public async Task FailedStartedPublicationReleasesTheReservation()
        {
            var table = await _kit.ChurnedAsync();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            cleanup.AdvanceHooksForTests.BeforePublish = (kind, name) => kind == ControlKind.Started ? throw new IOException("The started record does not land") : Task.CompletedTask;
            var ledger = new CapacityLedger(() => default);
            var checkedAndAdded = 0;
            ledger.BetweenCheckAndAddForTests = () => checkedAndAdded++;

            AssertDeclined(await AdvanceAsync(cleanup, table, ledger), MaintenanceReason.PublishFailed);

            // Reserved, then disposed with nothing handed off
            Assert.Equal(1, checkedAndAdded);
            Assert.Equal(default, ledger.Current);
            Assert.Null(cleanup.ActiveForTests);
            Assert.DoesNotContain(await table.ControlsAsync(), IsStarted);
        }

        [Fact]
        public async Task PlanAboveTwelveHoursAloneDeclinesBeforeAnyRecord()
        {
            var table = await _kit.ChurnedAsync();
            var logger = new DeltaMaintenanceWorkerTests.CollectingLogger();
            var clock = new FakeTimeProvider(s_now);
            var cleanup = AdvanceExecutionKit.Cleanup(table, logger, clock);
            // Its own deletes take an hour each
            for (int i = 0; i < 10; i++)
            {
                cleanup.ObserveLatency(TimeSpan.FromHours(1), cleanup.Settings.MaxPaths);
            }
            var published = new List<string>();
            cleanup.AdvanceHooksForTests.BeforePublish = (kind, name) =>
            {
                published.Add(name);
                return Task.CompletedTask;
            };
            var ledger = new CapacityLedger(() => default);

            for (int day = 0; day < 3; day++)
            {
                Assert.True(cleanup.IsDue(JobKind.Advance, table.Head, clock.GetUtcNow()));
                AssertDeclined(await AdvanceAsync(cleanup, table, ledger), MaintenanceReason.DeleteBacklog);

                // Nothing published to scavenge, nothing reserved, and no other planning that day
                Assert.Empty(published);
                Assert.Empty(await table.ControlsAsync());
                Assert.Equal(default, ledger.Current);
                Assert.False(cleanup.IsDue(JobKind.Advance, table.Head, clock.GetUtcNow()));
                clock.Advance(TimeSpan.FromDays(1));
            }
            Assert.Equal(1, logger.Warnings(nameof(MaintenanceReason.DeleteBacklog)));
        }

        [Theory]
        // About eight and a half hours alone, then about twelve and a half
        [InlineData(2, true)]
        [InlineData(3, false)]
        public async Task PlanAloneStartsUpToTwelveHours(int observedHours, bool starts)
        {
            var table = await _kit.ChurnedAsync();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            // One full slice whose deletes took the given hours each
            cleanup.ObserveLatency(TimeSpan.FromHours(observedHours), cleanup.Settings.MaxPaths);
            var ledger = new CapacityLedger(() => default);

            var result = await AdvanceAsync(cleanup, table, ledger);

            if (starts)
            {
                Assert.True(result.Kind == OutcomeKind.Completed, $"{result.Kind} {result.Reason}: {result.Detail}");
                Assert.Contains(await table.ControlsAsync(), IsStarted);
                Assert.Equal(cleanup.ActiveForTests!.Lease!.Demand, ledger.Current);
                Assert.InRange(ledger.DrainSeconds(ledger.Current) / 3600, 6, 12);
            }
            else
            {
                AssertDeclined(result, MaintenanceReason.DeleteBacklog);
                Assert.Contains("alone would take 12", result.Detail);
                Assert.Empty(await table.ControlsAsync());
                Assert.Equal(default, ledger.Current);
                Assert.Null(cleanup.ActiveForTests);
            }
        }

        // Guards the estimator and the ledger against pricing latency-bound deletes at different rates
        [Fact]
        public async Task PlanAloneAtARaisedRateStillDeclinesLatencyBoundDeletes()
        {
            var table = await _kit.ChurnedAsync();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            // One full slice whose deletes took three hours each
            cleanup.ObserveLatency(TimeSpan.FromHours(3), cleanup.Settings.MaxPaths);
            var ledger = new CapacityLedger(() => default, 5000);

            var result = await AdvanceAsync(cleanup, table, ledger);

            AssertDeclined(result, MaintenanceReason.DeleteBacklog);
            Assert.Contains("alone would take 12", result.Detail);
            Assert.Empty(await table.ControlsAsync());
            Assert.Equal(default, ledger.Current);
            Assert.Null(cleanup.ActiveForTests);
        }

        [Fact]
        public async Task OneSlowMeasuredDeleteDoesNotDeclineTheNextAdvance()
        {
            // An earlier advance's slice issued one delete that took three hours, scaled to this plan's size
            var rig = await StartedAsync(logsComplete: true);
            var slow = TimeSpan.FromHours(3);
            rig.Cleanup.PayloadPhaseForTests = Scripted(_ => Slice(SliceStatus.Progressed) with { Issued = 1, Elapsed = slow });
            Assert.Equal(OutcomeKind.Completed, (await TurnAsync(rig)).Kind);
            // The deletes that slice counted for
            var step = (slow.Ticks - CapacityEstimator.DefaultLatency.Ticks) / (4L * rig.Cleanup.Settings.MaxPaths);
            var counted = (rig.Cleanup.Latency.Ticks - CapacityEstimator.DefaultLatency.Ticks) / step;

            var table = await _kit.ChurnedAsync();
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            cleanup.ObserveLatency(slow, (int)counted);
            var ledger = new CapacityLedger(() => default);

            var result = await AdvanceAsync(cleanup, table, ledger);

            Assert.True(result.Kind == OutcomeKind.Completed, $"{result.Kind} {result.Reason}: {result.Detail}; the slice counted for {counted} deletes, priced at {cleanup.Latency} per delete");
        }

        [Theory]
        [InlineData("foreign-head")]
        [InlineData("reservation")]
        [InlineData("checkpoint")]
        public async Task RecheckDeclinesAChangedTableBeforeItsReservation(string change)
        {
            var table = await _kit.ChurnedAsync();
            // Two own commits after the cutoff and a root at the second, which the capability reads instead of C
            for (long version = 15; version <= 16; version++)
            {
                await DeltaTestKit.WriteOwnCommit(table.Raw, "t", version, 0, (s_now - TimeSpan.FromHours(1)).ToUnixTimeMilliseconds() + version, DeltaLakeCheckpointBuildTests.Add($"young-{version}.parquet"));
            }
            table.Head = 16;
            await _kit.BuildAsync(table.Raw, 16);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            AdvanceBoundary? boundary = null;
            cleanup.AdvanceForTests.SelectedHookForTests = x => boundary = x;
            cleanup.AdvanceHooksForTests.Step = async step =>
            {
                if (step != AdvanceSteps.Header)
                {
                    return;
                }
                switch (change)
                {
                    case "foreign-head":
                        await DeltaTestKit.WriteCommit(table.Raw, "t", table.Head + 1, DeltaLakeCheckpointBuildTests.Add("foreign.parquet"));
                        break;
                    case "reservation":
                        // Another advance's start at a lower boundary
                        var other = CleanupRecordKit.Fixture("table-id", 8, 0, 0, 14, 0);
                        await CleanupRecordKit.PlantAsync(table.Raw, "t", CleanupRecordKit.Started(other, CleanupRecordKit.Header(other, CleanupRecordKit.Part(other, 0, "/t/f-8.parquet"))));
                        break;
                    default:
                        // C listed with another modification time
                        var path = Path.Combine(table.Root!, "t", "_delta_log", $"{boundary!.C:D20}.checkpoint.parquet");
                        File.SetLastWriteTimeUtc(path, File.GetLastWriteTimeUtc(path).AddMinutes(5));
                        break;
                }
            };
            var ledger = new CapacityLedger(() => default);
            var checkedAndAdded = 0;
            ledger.BetweenCheckAndAddForTests = () => checkedAndAdded++;

            var result = await AdvanceAsync(cleanup, table, ledger);

            Assert.Equal(12, boundary!.C);
            Assert.True(result is { Kind: OutcomeKind.Declined, Reason: MaintenanceReason.Transient, ForeignHead: false }, $"{result.Kind} {result.Reason}: {result.Detail}");
            Assert.Equal((0, default(CapacityDemand)), (checkedAndAdded, ledger.Current));
            Assert.Null(cleanup.ActiveForTests);
            Assert.DoesNotContain(await table.ControlsAsync(), x => IsStarted(x) && x.Contains(boundary.AdvanceId, StringComparison.Ordinal));
            Assert.True(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
        }

        [Fact]
        public async Task StartDeclinesWhenACapabilitySourceIsBelowC()
        {
            var table = await _kit.ChurnedAsync(local: false);
            var hooks = table.Hooks!;
            var name = $"{12:D20}.checkpoint.parquet";
            var whole = await DeltaTestKit.ReadBytes(hooks.Inner, table.LogPath(name));
            var torn = whole.ToArray();
            torn[0] ^= 0xFF;
            // The capability's root read finds C unreadable and roots at 8; every later read finds C whole
            var reads = 0;
            hooks.Before = async (verb, path) =>
            {
                if (reads == 1)
                {
                    reads++;
                    await CleanupRecordKit.PlantAsync(hooks.Inner, "t", name, whole);
                }
                else if (reads == 0 && verb == "OpenRead" && path.Full == table.LogPath(name))
                {
                    reads++;
                    await CleanupRecordKit.PlantAsync(hooks.Inner, "t", name, torn);
                }
            };
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            AdvanceBoundary? boundary = null;
            cleanup.AdvanceForTests.SelectedHookForTests = x => boundary = x;
            var ledger = new CapacityLedger(() => default);
            var checkedAndAdded = 0;
            ledger.BetweenCheckAndAddForTests = () => checkedAndAdded++;

            var result = await AdvanceAsync(cleanup, table, ledger);

            Assert.Equal(2, reads);
            Assert.Equal(12, boundary!.C);
            AssertDeclined(result, MaintenanceReason.CleanupGate);
            Assert.Contains("version 8,", result.Detail);
            Assert.Equal((0, default(CapacityDemand)), (checkedAndAdded, ledger.Current));
            Assert.Null(cleanup.ActiveForTests);
            Assert.DoesNotContain(await table.ControlsAsync(), IsStarted);
        }

        [Fact]
        public async Task OIsCorrectMidAdvance()
        {
            var table = await _kit.ChurnedAsync();
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            // Checkpoints at 4 and 8: the advance goes to 8
            await table.Raw.Rm(table.LogPath($"{12:D20}.checkpoint.parquet"));
            var first = AdvanceExecutionKit.Cleanup(table);
            first.AdvanceHooksForTests.Step = async step =>
            {
                if (step == AdvanceSteps.Started)
                {
                    // Its log deletion stops after 7, 6 and 5, a checkpoint lands at 12, and the process ends
                    for (long version = 5; version <= 7; version++)
                    {
                        await table.Raw.Rm(table.LogPath($"{version:D20}.json"));
                    }
                    await _kit.BuildAsync(table.Raw, 12);
                    throw new IOException("The process ended");
                }
            };
            var ended = await AdvanceAsync(first, table);
            Assert.Equal((OutcomeKind.Failed, MaintenanceReason.Transient), (ended.Kind, ended.Reason));
            Assert.Null(first.ActiveForTests);

            // A fresh selection would find O at 8 and take 12; the restart resumes from the records instead
            var restarted = AdvanceExecutionKit.Cleanup(table);
            var selected = 0;
            restarted.AdvanceForTests.SelectedHookForTests = _ => selected++;

            var result = await AdvanceAsync(restarted, table);

            Assert.True(result.Kind == OutcomeKind.Completed, $"{result.Kind} {result.Reason}: {result.Detail}");
            Assert.Equal(0, selected);
            var active = restarted.ActiveForTests!;
            Assert.Equal((8L, 0L, 0L, AdvancePhase.Log), (active.C, active.Started.O, active.Started.FirstJson, active.Phase));
            Assert.True(await table.Raw.Exists(table.LogPath($"{0:D20}.json")));

            // Only what lies below 8 goes, and the advance ends at its done
            await _kit.ExecuteAsync(restarted, table);
            await AdvanceExecutionKit.AssertAdvancedAsync(table, 8);
            Assert.True(await table.Raw.Exists(table.LogPath($"{12:D20}.checkpoint.parquet")));
        }

        [Fact]
        public async Task ResumeIsExemptFromAdmission()
        {
            var table = await _kit.ChurnedAsync();
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table);
            // A hint below C, which the resume raises to it
            await new StowageCheckpointTransport(table.Raw, "t").WriteHintAsync(new LastCheckpointInfo() { Version = 4, Size = 1 }, default);
            // Another table's thirteen hours close admission
            var ledger = new CapacityLedger(() => default);
            var other = ledger.Restore(default, "other-table", ControlTokens.New(), Hours(13), CapacityState.Runnable);
            Assert.False(ledger.Admits());
            var restarted = WithoutExclusions(AdvanceExecutionKit.Cleanup(table));

            var result = await AdvanceAsync(restarted, table, ledger);

            Assert.True(result.Kind == OutcomeKind.Completed, $"{result.Kind} {result.Reason}: {result.Detail}");
            var active = restarted.ActiveForTests!;
            Assert.Equal(published.Association, active.Association);
            Assert.Equal(CapacityState.Runnable, active.Lease!.State);
            Assert.Equal(other.Demand + active.Lease.Demand, ledger.Current);
            Assert.Equal(published.C, (await DeltaMaintenanceWorkerTests.Hint(table.Raw))!.Version);
        }

        [Fact]
        public void DemandSaturatesInsteadOfWrapping()
        {
            var inputs = new AdvanceDemandInputs(LogTargets: long.MaxValue / 2, PayloadPaths: long.MaxValue / 2, Parts: 16_384, PartBytes: long.MaxValue / 2, CheckpointBytes: long.MaxValue / 2,
                AnchorBytes: long.MaxValue / 2, CapabilityBytes: long.MaxValue / 2, CapabilityObjects: 2, LogEntries: long.MaxValue / 2, Paging: ListingPaging.S3, Outstanding: 4,
                Latency: TimeSpan.FromHours(1), KeyExtra: 1 << 20);

            Assert.Equal(new CapacityDemand(long.MaxValue, long.MaxValue), CapacityEstimator.Advance(inputs, MaintenanceBuckets.DefaultRequestsPerSecond));
        }

        [Theory]
        [InlineData("range")]
        [InlineData("unknown")]
        [InlineData("retention")]
        [InlineData("checkpoint")]
        [InlineData("exclusions")]
        public async Task FailedResumeCheckKeepsTheRecordsAndHoldsTheLease(string failure)
        {
            var table = await _kit.ChurnedAsync();
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table);
            var configuration = table.Configuration;
            switch (failure)
            {
                case "checkpoint":
                    // A root above C, then other bytes in C at the size and modification time its header names
                    await _kit.BuildAsync(table.Raw, 14);
                    var path =Path.Combine(table.Root!, "t", "_delta_log", $"{published.C:D20}.checkpoint.parquet");
                    var modified = File.GetLastWriteTimeUtc(path);
                    var bytes = File.ReadAllBytes(path);
                    bytes[bytes.Length / 2] ^= 0xFF;
                    File.WriteAllBytes(path, bytes);
                    File.SetLastWriteTimeUtc(path, modified);
                    break;
                case "range":
                    // A root above C, then a commit between them gone
                    await _kit.BuildAsync(table.Raw, 14);
                    await table.Raw.Rm(table.LogPath($"{13:D20}.json"));
                    break;
                case "unknown":
                    await CleanupRecordKit.PlantAsync(table.Raw, "t", $"{5:D20}.unknown", new byte[1]);
                    break;
                case "retention":
                    // A sink with a retention the table does not publish
                    configuration = CleanupConfiguration.Validate(table.Raw, "t", false, new CleanupOptIns(TimeSpan.FromDays(8), null));
                    break;
                case "exclusions":
                    // The planned head replaced in place
                    await table.Raw.Rm(table.LogPath($"{published.Started.Head:D20}.json"));
                    await DeltaTestKit.WriteOwnCommit(table.Raw, "t", published.Started.Head, 0, BoundarySelectorTests.Old(published.Started.Head),
                        DeltaLakeCheckpointBuildTests.Add("part-0014.parquet"), DeltaLakeCheckpointBuildTests.Add("part-0002.parquet"));
                    break;
            }
            var cleanup = new DeltaTableCleanup(configuration, table.Metered, new FakeTimeProvider(s_now), "t", null, () => table.Raw, new DiscardCounters(), null, "t");
            var records = await table.SnapshotAsync();
            var ledger = new CapacityLedger(() => default);

            var result = await AdvanceAsync(cleanup, table, ledger);

            var (reason, state) = failure switch
            {
                "range" => (MaintenanceReason.MissingHistory, CapacityState.Suspended),
                "unknown" => (MaintenanceReason.UnclassifiedLogFile, CapacityState.Suspended),
                "retention" => (MaintenanceReason.RetentionNotPublished, CapacityState.Paused),
                "checkpoint" => (MaintenanceReason.HistoryChanged, CapacityState.Suspended),
                _ => (MaintenanceReason.ManifestSuspended, CapacityState.Suspended)
            };
            AssertDeclined(result, reason);
            Assert.False(result.ForeignHead);
            Assert.Null(cleanup.ActiveForTests);
            Assert.Equal(records, await table.SnapshotAsync());
            Assert.Equal(published.C, await AdvanceExecutionKit.FloorAsync(table));
            // Its demand held apart from the runnable demand
            Assert.Equal(default, ledger.Current);
            Assert.True(ledger.Total(state).Requests > 0);
        }

        [Fact]
        public async Task ResumeSuspendsWhenCIsNotItsHeadersContent()
        {
            var table = await _kit.ChurnedAsync();
            // C listed and readable as its header names it, with another digest
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
            var ledger = new CapacityLedger(() => default);
            var cleanup = WithoutExclusions(AdvanceExecutionKit.Cleanup(table));

            AssertDeclined(await AdvanceAsync(cleanup, table, ledger), MaintenanceReason.HistoryChanged);

            Assert.Null(cleanup.ActiveForTests);
            Assert.Equal(default, ledger.Current);
            Assert.True(ledger.Total(CapacityState.Suspended).Requests > 0);
        }

        [Fact]
        public async Task ResumeSuspendsWhenACapabilitySourceIsBelowC()
        {
            var table = await _kit.ChurnedAsync();
            // The only root is at 4, below the advance's boundary
            await table.Raw.Rm(table.LogPath($"{8:D20}.checkpoint.parquet"));
            await table.Raw.Rm(table.LogPath($"{12:D20}.checkpoint.parquet"));
            var fixture = CleanupRecordKit.Fixture("table-id", 8, 0, 0, table.Head, 0);
            var part = CleanupRecordKit.Part(fixture, 0, "/t/part-0003.parquet");
            var header = CleanupRecordKit.Header(fixture, part);
            foreach (var record in new[] { part, header, CleanupRecordKit.Started(fixture, header) })
            {
                await CleanupRecordKit.PlantAsync(table.Raw, "t", record);
            }
            var ledger = new CapacityLedger(() => default);

            AssertDeclined(await AdvanceAsync(WithoutExclusions(AdvanceExecutionKit.Cleanup(table)), table, ledger), MaintenanceReason.CleanupGate);

            Assert.Equal(default, ledger.Current);
            Assert.True(ledger.Total(CapacityState.Suspended).Requests > 0);
        }

        [Fact]
        public async Task UnsetPausesAndKeepsReservation()
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
            var phases = new CountingPhase();
            try
            {
                // Sink A starts an advance and is disposed
                var (sinkA, cleanupA) = Sink(budget, clock, table, table.Configuration, phases);
                sinkA.Published(table.Head);
                for (var tick = 0; tick < 8 && cleanupA.ActiveForTests == null; tick++)
                {
                    await TickAsync(budget, sinkA);
                }
                Assert.NotNull(cleanupA.ActiveForTests);
                var active = cleanupA.ActiveForTests;
                var demand = active.Lease!.Demand;
                await sinkA.StopAsync(s_wait);
                Assert.Equal(demand, budget.Capacity.Total(CapacityState.Paused));
                var calls = phases.Calls;
                var records = await table.SnapshotAsync();

                // Sink B over the same table and budget without retention, for two days
                var (sinkB, cleanupB) = Sink(budget, clock, table, CleanupConfiguration.Validate(table.Raw, "t", false, CleanupOptIns.Off), phases);
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

                // Nothing issued, the records and reservation kept, the demand reported paused
                Assert.Equal(calls, phases.Calls);
                Assert.Null(cleanupB.ActiveForTests);
                Assert.Equal(records, await table.SnapshotAsync());
                Assert.Equal(active.C, await AdvanceExecutionKit.FloorAsync(table));
                var below = await TryBuildAsync(owners, directory, table.Raw, active.C - 1);
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

        // A worker on the shared budget whose cleanup's delete turns run the counted phases
        private static (DeltaMaintenanceWorker Worker, DeltaTableCleanup Cleanup) Sink(DeltaMaintenanceBudget budget, FakeTimeProvider clock, ExecutionTable table, CleanupConfiguration configuration,
            CountingPhase phases)
        {
            var cleanup = WithoutExclusions(new DeltaTableCleanup(configuration, table.Metered, clock, "t", null, () => table.Raw, new DiscardCounters(), null, "t")
            {
                LogPhaseForTests = phases.Slice,
                PayloadPhaseForTests = phases.Slice
            });
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

        private static async Task TickAsync(DeltaMaintenanceBudget budget, DeltaMaintenanceWorker worker)
        {
            budget.TickNow();
            await worker.CurrentJob.WaitAsync(s_wait);
        }

        private static async Task<BuildResult> TryBuildAsync(ScratchOwners owners, string directory, IFileStorage storage, long version)
        {
            var owner = owners.Add(new ScratchDirectory(Path.Combine(directory, ScratchOwners.ShortId())));
            BuildResult result;
            using (var scratch = owner.CreateJob())
            {
                result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = "t",
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Clock = new FakeTimeProvider(s_now)
                }, new BuildRequest() { Version = version, Root = RootPolicy.Auto, KnownUnsupported = new HashSet<ObjectIdentity>() });
            }
            owners.Release(owner);
            return result;
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RaisingRRechecksBothPhases(bool logsComplete)
        {
            var table = await _kit.ChurnedAsync();
            var published = await _kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table, logsComplete: logsComplete);
            // The sink is rebuilt with thirty days, which the table publishes from its next commit
            var raised = TimeSpan.FromDays(30);
            var formatted = RetentionProperties.Format(raised);
            await DeltaTestKit.WriteOwnCommit(table.Raw, "t", table.Head + 1, 0, BoundarySelectorTests.Old(table.Head + 1),
                DeltaTestKit.Metadata(DeltaTestKit.UserSchema, new Dictionary<string, string>() { [RetentionProperties.LogKey] = formatted, [RetentionProperties.DeletedFileKey] = formatted }));
            table.Head++;
            var configuration = CleanupConfiguration.Validate(table.Raw, "t", false, new CleanupOptIns(raised, null));
            var clock = new FakeTimeProvider(s_now);
            var phases = new CountingPhase();
            var cleanup = WithoutExclusions(new DeltaTableCleanup(configuration, table.Metered, clock, "t", null, () => table.Raw, new DiscardCounters(), null, "t")
            {
                LogPhaseForTests = phases.Slice,
                PayloadPhaseForTests = phases.Slice
            });
            var ledger = new CapacityLedger(() => default);
            var records = await table.SnapshotAsync();

            var waiting = await AdvanceAsync(cleanup, table, ledger);

            // C is after the new cutoff: the advance waits with its reservation and its demand paused, never ending coverage
            Assert.True(waiting is { Kind: OutcomeKind.NoWork, Reason: MaintenanceReason.NoEligibleCheckpoint, ForeignHead: false }, $"{waiting.Kind} {waiting.Reason}: {waiting.Detail}");
            Assert.Null(cleanup.ActiveForTests);
            Assert.False(cleanup.IsDue(JobKind.Deletion, table.Head, s_now));
            Assert.Equal(OutcomeKind.NoWork, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
            Assert.Equal(0, phases.Calls);
            Assert.Equal(records, await table.SnapshotAsync());
            Assert.Equal(published.C, await AdvanceExecutionKit.FloorAsync(table));
            Assert.True(ledger.Total(CapacityState.Paused).Requests > 0);
            Assert.Equal(default, ledger.Current);

            // Past the new cutoff the advance hands off in the phase its records reached
            clock.Advance(raised);
            var resumed = await AdvanceAsync(cleanup, table, ledger);
            Assert.True(resumed.Kind == OutcomeKind.Completed, $"{resumed.Kind} {resumed.Reason}: {resumed.Detail}");
            var active = cleanup.ActiveForTests!;
            Assert.Equal((published.C, logsComplete ? AdvancePhase.Payload : AdvancePhase.Log), (active.C, active.Phase));
            Assert.Equal((CapacityState.Runnable, default(CapacityDemand)), (active.Lease!.State, ledger.Total(CapacityState.Paused)));
        }

        [Fact]
        public async Task CatchUpRunsBackToBack()
        {
            var table = await _kit.ChurnedAsync();
            // Checkpoints at 4 and 8, both eligible
            await table.Raw.Rm(table.LogPath($"{12:D20}.checkpoint.parquet"));
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            // The log below C as the log phase leaves it
            cleanup.LogPhaseForTests = async (active, turn, context) =>
            {
                for (long version = 0; version < active.C; version++)
                {
                    foreach (var name in new[] { $"{version:D20}.json", $"{version:D20}.checkpoint.parquet" })
                    {
                        if (await table.Raw.Exists(table.LogPath(name)))
                        {
                            await table.Raw.Rm(table.LogPath(name));
                        }
                    }
                }
                active.Phase = AdvancePhase.Payload;
                return new SliceResult(SliceStatus.Completed, MaintenanceReason.None, 0);
            };
            // A valid done, as the payload phase publishes it
            cleanup.PayloadPhaseForTests = async (active, turn, context) =>
            {
                var started = active.Started;
                var done = new DoneRecord(active.Association, started.TableId, started.O, started.A, started.Head, started.Anchor, new RecordCursor(started.Totals.Parts, 0, 0), started.Totals,
                    started.Totals.Emitted);
                await new ControlPublisher(turn.Storage, turn.Configuration.Store.Table, context).PublishAsync(ControlKind.Done, active.Association, -1, () => ControlRecords.Encode(done));
                active.Phase = AdvancePhase.Done;
                return new SliceResult(SliceStatus.Completed, MaintenanceReason.None, 0);
            };
            var ledger = new CapacityLedger(() => default);
            var reservations = 0;
            ledger.BetweenCheckAndAddForTests = () => reservations++;
            // A short admission whose reads leave each step capped at the lowest eligible checkpoint
            var promoted = await cleanup.RunAsync(JobKind.Advance, ShortContext(table, 1));
            Assert.True(promoted.NeedsLongAdmission);

            var advances = new List<ActiveAdvance>();
            for (int advance = 0; advance < 2; advance++)
            {
                Assert.True(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
                var started = await AdvanceAsync(cleanup, table, ledger);
                Assert.True(started.Kind == OutcomeKind.Completed, $"{started.Kind} {started.Reason}: {started.Detail}");
                advances.Add(cleanup.ActiveForTests!);
                Assert.False(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
                // Its log turn, then its payload turn
                for (int turn = 0; turn < 2; turn++)
                {
                    Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.TurnAsync(cleanup, _kit.DeletionContext(table.Head, ledger))).Kind);
                }
                Assert.Null(cleanup.ActiveForTests);
            }

            // Two advances the same day, each with its own selection and reservation
            Assert.Equal(new long[] { 4, 8 }, advances.Select(x => x.C));
            Assert.NotEqual(advances[0].Association.AdvanceId, advances[1].Association.AdvanceId);
            Assert.Equal(2, reservations);

            // The third finds no eligible checkpoint and concludes the day
            Assert.True(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
            var none = await AdvanceAsync(cleanup, table, ledger);
            Assert.True(none is { Kind: OutcomeKind.NoWork, Reason: MaintenanceReason.NoEligibleCheckpoint }, $"{none.Kind} {none.Reason}: {none.Detail}");
            Assert.Equal(2, reservations);
            Assert.False(cleanup.IsDue(JobKind.Advance, table.Head, s_now));
        }

        [Fact]
        public async Task CappedAdvanceCatchesUpImmediatelyAfterARestart()
        {
            var table = await _kit.ChurnedAsync(local: false);
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            // Checkpoints at 4 and 8, both eligible
            await table.Raw.Rm(table.LogPath($"{12:D20}.checkpoint.parquet"));
            var first = AdvanceExecutionKit.Cleanup(table);
            // A short admission whose reads cap the step at the lowest eligible checkpoint
            Assert.True((await AdvanceExecutionKit.JobAsync(first, ShortContext(table, 1))).NeedsLongAdmission);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(first, _kit.AdvanceContext(table.Head, new CapacityLedger(() => default)))).Kind);
            Assert.Equal(4, first.ActiveForTests!.C);

            // A restarted cleanup resumes and finishes it
            var restarted = AdvanceExecutionKit.Cleanup(table);
            var ledger = new CapacityLedger(() => default);
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(restarted, _kit.AdvanceContext(table.Head, ledger))).Kind);
            Assert.Equal(4, restarted.ActiveForTests!.C);
            await _kit.ExecuteAsync(restarted, table, ledger);

            // The next advance is due at once and selects the remaining checkpoint
            Assert.True(restarted.IsDue(JobKind.Advance, table.Head, s_now), "Eligible checkpoint 8 waits for the next day after resumed checkpoint 4 completed");
            Assert.Equal(OutcomeKind.Completed, (await AdvanceExecutionKit.JobAsync(restarted, _kit.AdvanceContext(table.Head, ledger))).Kind);
            Assert.Equal(8, restarted.ActiveForTests!.C);
        }

        [Fact]
        public async Task ClosedLedgerWithoutRecordsListsOnceAndEstablishesNothing()
        {
            var table = await _kit.ChurnedAsync(local: false);
            var ledger = new CapacityLedger(() => default);
            ledger.Restore(default, "other-table", ControlTokens.New(), Hours(13), CapacityState.Runnable);
            // A lease of this table that no listed record names
            ledger.Restore(default, TableLeases.KeyOf(table.Raw, "t"), ControlTokens.New(), Hours(1), CapacityState.Suspended);
            var cleanup = AdvanceExecutionKit.Cleanup(table);

            for (int retry = 0; retry < 2; retry++)
            {
                table.Hooks!.ClearRequests();

                AssertDeclined(await AdvanceAsync(cleanup, table, ledger), MaintenanceReason.DeleteBacklog);

                var requests = table.Hooks.Requests;
                Assert.Equal(1, requests.Count(x => x.StartsWith("Ls ", StringComparison.Ordinal)));
                Assert.DoesNotContain(requests, x => x.StartsWith("OpenRead ", StringComparison.Ordinal));
            }
            Assert.Equal(default, ledger.Total(CapacityState.Suspended));
        }

        [Fact]
        public async Task RestoredDemandPricesTheAnchorItProvedNotTheRecordsLength()
        {
            var restored = new List<CapacityDemand>();
            foreach (var length in new[] { 512L, long.MaxValue / 2 })
            {
                var table = await _kit.ChurnedAsync();
                // A compaction starting at C is one of its log targets
                await AdvanceExecutionKit.PlantCompactionAsync(table, 8, 9);
                var fixture = CleanupRecordKit.Fixture("table-id", 8, 0, 0, table.Head, 0);
                fixture = fixture with { Anchor = fixture.Anchor with { Length = length } };
                var part = CleanupRecordKit.Part(fixture, 0, "/t/part-0003.parquet");
                var header = CleanupRecordKit.Header(fixture, part);
                foreach (var record in new[] { part, header, CleanupRecordKit.Started(fixture, header) })
                {
                    await CleanupRecordKit.PlantAsync(table.Raw, "t", record);
                }
                var ledger = new CapacityLedger(() => default);

                // Resumed and suspended at the fixture's C, which is not its header's content
                AssertDeclined(await AdvanceAsync(WithoutExclusions(AdvanceExecutionKit.Cleanup(table)), table, ledger), MaintenanceReason.HistoryChanged);

                // The helper's value at the same listing with the evidence established at its head
                var context = _kit.AdvanceContext(table.Head);
                var log = await AdvancePreparation.ListAsync(table.Configuration, table.Metered, table.Head, context);
                var established = await CleanupEvidence.EstablishAsync(table.Configuration, table.Metered, log, log.Head, null, durable: true, context);
                await context.SettleAsync();
                var totals = CleanupRecordKit.TotalsOf(header);
                var expected = CapacityEstimator.Advance(AdvanceDemand.Of(table.Configuration, log, established.Evidence!, 8, LogTargets.Count(LogClassifier.Classify(log), 8), totals.Emitted,
                    totals.Parts, totals.PartBytes, CapacityEstimator.DefaultLatency), MaintenanceBuckets.DefaultRequestsPerSecond);
                Assert.Equal(expected, ledger.Total(CapacityState.Suspended));
                restored.Add(expected);
            }
            Assert.Equal(restored[0], restored[1]);
        }

        [Fact]
        public async Task PricedPayloadSlicesCoverLongNativeKeys()
        {
            // The extra characters are the native root a local key holds beyond the path its part names
            var table = await _kit.ChurnedAsync();
            var context = _kit.AdvanceContext(table.Head);
            var log = await AdvancePreparation.ListAsync(table.Configuration, table.Metered, table.Head, context);
            var established = await CleanupEvidence.EstablishAsync(table.Configuration, table.Metered, log, log.Head, null, durable: true, context);
            await context.SettleAsync();
            const string path = "/t/part-0003.parquet";
            var key = new DeletionExecutor(table.Configuration, table.Metered).TargetOf(path).Key!;
            var inputs = AdvanceDemand.Of(table.Configuration, log, established.Evidence!, 12, 0, 1, 1, ControlRecords.PartLength(path.Length, 1), TimeSpan.Zero);
            Assert.True(inputs.KeyExtra > 0);
            Assert.Equal(key.Length - path.Length, inputs.KeyExtra);

            // Fifty thousand 100-byte paths under a 600-character root fill each slice's buffer before its path count
            const int paths = 50_000;
            const int pathBytes = 100;
            const int extra = 600;
            var priced = inputs with { PayloadPaths = paths, Parts = 5, PartBytes = 5L * ControlRecords.PartLength(paths / 5 * pathBytes, paths / 5), KeyExtra = extra };
            long Gates(AdvanceDemandInputs x) => CapacityEstimator.Advance(x with { CheckpointBytes = x.CheckpointBytes + 1 }, MaintenanceBuckets.DefaultRequestsPerSecond).Bytes - CapacityEstimator.Advance(x, MaintenanceBuckets.DefaultRequestsPerSecond).Bytes;
            var pricedSlices = Gates(priced) - Gates(priced with { PayloadPaths = 0 });
            // As a slice buffers them: each target charged its overhead and its key's characters twice
            var perSlice = Math.Min(DeletionRunSettings.DefaultMaxPaths, DeletionRunSettings.DefaultMaxBufferBytes / (DeletionRun.TargetOverheadBytes + 2L * (pathBytes + extra)));
            var actual = (paths + perSlice - 1) / perSlice;
            Assert.True(pricedSlices >= actual, $"Priced {pricedSlices} payload slices, but {actual} buffered ones remain");
        }
    }
}

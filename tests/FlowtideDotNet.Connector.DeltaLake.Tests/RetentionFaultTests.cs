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
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Durable advance twins of runtime faults, each kept to the matrix invariants
    public abstract class RetentionFaultCells : RetentionCells
    {
        private protected RetentionFaultCells(MatrixTemplate template) : base(template)
        {
        }

        // Started, then past its log phase and first progress when asked
        private protected async Task<Process> AdvancedAsync(Cell cell, bool payload, Process? process = null)
        {
            process ??= Start(cell);
            var progressed = false;
            process.Hooks.Step = step =>
            {
                progressed |= step == AdvanceSteps.Progress;
                return Task.CompletedTask;
            };
            await RunAsync(cell, process, () => !payload || progressed);
            process.Hooks.Step = null;
            Assert.Equal(payload ? AdvancePhase.Payload : AdvancePhase.Log, process.Active!.Phase);
            return process;
        }

        private protected static int Deleted(Cell cell) => cell.Outcome.Settled.Count(x => x.Outcome.Status == DeleteStatus.Deleted);

        // A durable decline or rebase that keeps the advance's authority, never a stop
        private protected static void AssertHeld(CleanupResult? result, ActiveAdvance active)
        {
            Assert.True(result is { Kind: OutcomeKind.Declined or OutcomeKind.NoWork }, $"{result?.Kind} {result?.Reason}: {result?.Detail}");
            Assert.True(active.State is AdvanceState.Suspended or AdvanceState.Rebasing or AdvanceState.Runnable or AdvanceState.Paused, $"{active.State}");
            Assert.NotEqual(AdvancePhase.Done, active.Phase);
        }

        // Turns, and the jobs that rebase, until the advance leaves its turns
        private protected async Task FinishAsync(Cell cell, Process process)
        {
            for (int turn = 0; turn < 40 && process.Active != null; turn++)
            {
                await NextAsync(cell, process);
            }
            Assert.Null(process.Active);
        }

        // Other bytes at the same length and modification time
        private protected static async Task ReplaceKeepingIdentityAsync(ExecutionTable table, string name, byte[] bytes)
        {
            var path = table.LogPath(name);
            if (table.Root != null)
            {
                var native = Path.Combine(Invariants.NativeTable(table), "_delta_log", name);
                var modified = File.GetLastWriteTimeUtc(native);
                Assert.Equal(new FileInfo(native).Length, bytes.Length);
                File.WriteAllBytes(native, bytes);
                File.SetLastWriteTimeUtc(native, modified);
                return;
            }
            Assert.Equal((await DeltaTestKit.ReadBytes(table.Raw, path)).Length, bytes.Length);
            // The test's own requests, not the advance's
            using (table.Hooks!.Unrecorded())
            {
                await table.Raw.Rm(path);
                await CleanupRecordKit.PlantAsync(table.Raw, "t", name, bytes);
            }
        }

        // Control record names as stored, whatever a listing conceals
        private protected static async Task<string[]> StoredAsync(ExecutionTable table) =>
            (await DeltaTransactionReader.ListLog(Invariants.View(table), "t")).Controls.Select(x => x.Name).Order(StringComparer.Ordinal).ToArray();
    }

    public class RetentionFaultTests : RetentionFaultCells, IClassFixture<MatrixTemplate>
    {
        public RetentionFaultTests(MatrixTemplate template) : base(template)
        {
        }

        private static string Json(long version) => $"{version:D20}.json";

        [Theory]
        [InlineData("missing")]
        [InlineData("replaced")]
        public async Task BoundaryCheckpointChangeNeverDeletes(string change)
        {
            var cell = await CellAsync(local: true);
            var first = await AdvancedAsync(cell, payload: false);
            // Own commits in its epoch and a newer root, which a restarted process's capability rehashes instead of C
            var epoch = first.Active!.Evidence.Anchor.AdoptedAt;
            await AppendAsync(cell, "part-0098.parquet", own: true, epoch);
            await AppendAsync(cell, "part-0099.parquet", own: true, epoch);
            await _kit.BuildAsync(cell.Table.Raw, cell.Table.Head);
            var process = Start(cell);
            AssertHandedOff(await JobAsync(cell, process), resumed: true);
            var active = process.Active!;
            Assert.DoesNotContain(active.Evidence.Capability.Sources, x => x.IsCheckpoint && x.Version == C);
            var checkpoint = Path.Combine(Invariants.NativeTable(cell.Table), "_delta_log", $"{C:D20}.checkpoint.parquet");
            if (change == "missing")
            {
                File.Delete(checkpoint);
            }
            else
            {
                // Its writer's name changed in its footer: a readable checkpoint at the size and modification time the header names
                var bytes = File.ReadAllBytes(checkpoint);
                var at = bytes.AsSpan().IndexOf("parquet-cpp"u8);
                Assert.True(at >= 0, "The checkpoint names no writer");
                bytes[at] = (byte)'P';
                await ReplaceKeepingIdentityAsync(cell.Table, $"{C:D20}.checkpoint.parquet", bytes);
            }
            await InvalidateAsync(cell);

            AssertHeld(await NextAsync(cell, process), active);
            Assert.Equal(AdvanceState.Suspended, active.State);
            // A restart suspends it again
            AssertResult(await RestartAsync(cell), OutcomeKind.Declined, MaintenanceReason.HistoryChanged);

            await AssertUnresolvedAsync(cell);
        }

        [Fact]
        public async Task MissingAnchorJsonNeverDeletes()
        {
            var cell = await CellAsync(local: true);
            var process = await AdvancedAsync(cell, payload: true);
            var active = process.Active!;
            var anchor = active.Evidence.Anchor.Version;
            Assert.Equal(cell.Table.Head, anchor);
            // Own commits after it in its epoch, then its JSON gone with a checkpoint that keeps its version readable
            await AppendAsync(cell, "part-0098.parquet", own: true, epoch: active.Evidence.Anchor.AdoptedAt);
            await AppendAsync(cell, "part-0099.parquet", own: true, epoch: active.Evidence.Anchor.AdoptedAt);
            await _kit.BuildAsync(cell.Table.Raw, anchor);
            File.Delete(Path.Combine(Invariants.NativeTable(cell.Table), "_delta_log", Json(anchor)));
            await InvalidateAsync(cell);

            AssertHeld(await NextAsync(cell, process), active);
            Assert.Equal(AdvanceState.Suspended, active.State);
            AssertResult(await RestartAsync(cell), OutcomeKind.Declined, MaintenanceReason.MissingHistory);

            await AssertUnresolvedAsync(cell);
        }

        [Fact]
        public async Task EqualSizeMtimeAnchorReplacementNeverDeletes()
        {
            var cell = await CellAsync(local: true);
            var process = await AdvancedAsync(cell, payload: true);
            var active = process.Active!;
            var anchor = Json(active.Evidence.Anchor.Version);
            var listed = (await DeltaTransactionReader.ListLog(cell.Table.Raw, "t")).Commits[active.Evidence.Anchor.Version].IOEntry;
            Assert.True(listed.Size > 0 && listed.LastModificationTime != null, $"The listing names size {listed.Size} and time {listed.LastModificationTime}");
            // Its first line and length kept, another file added
            var original = await DeltaTestKit.ReadBytes(cell.Table.Raw, cell.Table.LogPath(anchor));
            var text = Encoding.UTF8.GetString(original);
            Assert.Contains("part-0014.parquet", text);
            await ReplaceKeepingIdentityAsync(cell.Table, anchor, Encoding.UTF8.GetBytes(text.Replace("part-0014.parquet", "part-9999.parquet")));
            var relisted = (await DeltaTransactionReader.ListLog(cell.Table.Raw, "t")).Commits[active.Evidence.Anchor.Version].IOEntry;
            Assert.Equal((listed.Size, listed.LastModificationTime), (relisted.Size, relisted.LastModificationTime));
            await InvalidateAsync(cell);

            AssertHeld(await NextAsync(cell, process), active);
            Assert.Equal(AdvanceState.Suspended, active.State);
            AssertResult(await RestartAsync(cell), OutcomeKind.Declined, MaintenanceReason.ManifestSuspended);

            // The anchor's own bytes back for the invariants
            await ReplaceKeepingIdentityAsync(cell.Table, anchor, original);
            await AssertUnresolvedAsync(cell);
        }

        [Fact]
        public async Task SameIdRollbackNeverDeletes()
        {
            var cell = await CellAsync(local: false);
            var process = await AdvancedAsync(cell, payload: true);
            var active = process.Active!;
            var head = Json(cell.Table.Head);
            var original = await DeltaTestKit.ReadBytes(cell.Table.Raw, cell.Table.LogPath(head));
            // The table rolls back below the validated head under the same id, and the worker's published version with it
            using (cell.Table.Hooks!.Unrecorded())
            {
                await cell.Table.Raw.Rm(cell.Table.LogPath(head));
            }
            cell.Table.Head--;
            await InvalidateAsync(cell);

            AssertHeld(await NextAsync(cell, process), active);
            Assert.Equal(AdvanceState.Suspended, active.State);
            AssertResult(await RestartAsync(cell), OutcomeKind.Declined, MaintenanceReason.ManifestSuspended);

            // Regrown to the old head, the suspended advance stays suspended
            await CleanupRecordKit.PlantAsync(cell.Table.Raw, "t", head, original);
            cell.Table.Head++;
            Assert.Equal(OutcomeKind.NoWork, (await NextAsync(cell, process))!.Kind);
            await AssertUnresolvedAsync(cell);
        }

        [Fact]
        public async Task StaleAdvisoryIdNeverDeletes()
        {
            var cell = await CellAsync(local: false);
            var process = await AdvancedAsync(cell, payload: true);
            var active = process.Active!;
            var stale = active.Evidence.TableId;
            // Another writer replaces the table's identity, publishing the same retention
            var retention = RetentionProperties.Format(AdvanceExecutionKit.Retention);
            var metadata = DeltaTestKit.Metadata(DeltaTestKit.UserSchema, new Dictionary<string, string>() { [RetentionProperties.LogKey] = retention, [RetentionProperties.DeletedFileKey] = retention });
            metadata.MetaData!.Id = "another-table";
            cell.Table.Head++;
            await DeltaTestKit.WriteCommit(cell.Table.Raw, "t", cell.Table.Head, metadata);
            await InvalidateAsync(cell);

            // The worker's advisory id still names the old table
            var basis = _kit.DeletionContext(cell.Table.Head, process.Ledger);
            var clock = new FakeTimeProvider(AdvanceExecutionKit.Now);
            var advised = new CleanupContext(new JobRun(cell.Table.Head, JobKind.Deletion) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Deletion,
                Long = false,
                ReadLimit = basis.ReadLimit,
                Deadline = basis.Deadline,
                Head = cell.Table.Head,
                TableId = stale,
                Scratch = basis.Scratch,
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default,
                Capacity = process.Ledger
            };
            AssertHeld(await cell.Outcome.RunAsync(() => AdvanceExecutionKit.TurnAsync(process.Cleanup, advised)), active);
            // Any rebase it asks for finds the table changed
            for (int turn = 0; turn < 3 && process.Active != null; turn++)
            {
                AssertHeld(await NextAsync(cell, process), active);
            }

            Assert.Equal(AdvanceState.Suspended, active.State);
            await AssertUnresolvedAsync(cell);
        }

        [Fact]
        public async Task ProtocolUpgradeBetweenSlicesNeverDeletes()
        {
            var cell = await CellAsync(local: true);
            var process = await AdvancedAsync(cell, payload: true);
            var active = process.Active!;
            // Another writer adds a writer feature cleanup does not support
            cell.Table.Head++;
            await DeltaTestKit.WriteCommit(cell.Table.Raw, "t", cell.Table.Head, DeltaTestKit.ProtocolWith("futureWriterFeature"));
            await InvalidateAsync(cell);

            for (int turn = 0; turn < 3 && process.Active != null; turn++)
            {
                AssertHeld(await NextAsync(cell, process), active);
            }

            Assert.Equal(AdvanceState.Suspended, active.State);
            await AssertUnresolvedAsync(cell);
        }

        [Fact]
        public async Task RevokedListingAssertionNeverDeletes()
        {
            var cell = await CellAsync(local: false);
            var process = await AdvancedAsync(cell, payload: true);
            // The worker stops, pausing its advance's demand
            process.Ledger.Pause(default);
            var raw = cell.Table.Raw;

            // A replacement sink that no longer asserts the listing cannot keep retention on
            Assert.Throws<ArgumentException>(() => CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, null)));
            var off = CleanupConfiguration.Validate(raw, "t", false, CleanupOptIns.Off);
            await InvalidateAsync(cell);
            var replacement = new DeltaTableCleanup(off, cell.Table.Metered, new FakeTimeProvider(AdvanceExecutionKit.Now), "t", null, () => raw, new DiscardCounters(), null, "t");
            cell.Outcome.Attach(replacement);
            Assert.False(replacement.IsDue(JobKind.Advance, cell.Table.Head, AdvanceExecutionKit.Now));
            Assert.Equal(OutcomeKind.NoWork, (await cell.Outcome.RunAsync(() => AdvanceExecutionKit.JobAsync(replacement, _kit.AdvanceContext(cell.Table.Head, process.Ledger))))!.Kind);
            Assert.Equal(OutcomeKind.NoWork, (await cell.Outcome.RunAsync(() => AdvanceExecutionKit.TurnAsync(replacement, _kit.DeletionContext(cell.Table.Head, process.Ledger))))!.Kind);

            // Its authority kept and its demand reported paused
            Assert.NotEqual(default, process.Ledger.Total(CapacityState.Paused));
            await AssertUnresolvedAsync(cell);
        }

        [Fact]
        public async Task RetentionOffThenOnCompletesTheHeldAdvance()
        {
            var cell = await CellAsync(local: true);
            var process = await AdvancedAsync(cell, payload: true);
            // The worker stops, pausing its advance's demand
            process.Ledger.Pause(default);
            var raw = cell.Table.Raw;
            var deleted = Deleted(cell);

            // A replacement sink without retention leaves the advance and its demand held
            var off = cell.Outcome.Attach(new DeltaTableCleanup(CleanupConfiguration.Validate(raw, "t", false, CleanupOptIns.Off), cell.Table.Metered, new FakeTimeProvider(AdvanceExecutionKit.Now), "t", null,
                () => raw, new DiscardCounters(), null, "t"));
            Assert.False(off.IsDue(JobKind.Advance, cell.Table.Head, AdvanceExecutionKit.Now));
            Assert.Equal(OutcomeKind.NoWork, (await cell.Outcome.RunAsync(() => AdvanceExecutionKit.JobAsync(off, _kit.AdvanceContext(cell.Table.Head, process.Ledger))))!.Kind);
            Assert.Equal(OutcomeKind.NoWork, (await cell.Outcome.RunAsync(() => AdvanceExecutionKit.TurnAsync(off, _kit.DeletionContext(cell.Table.Head, process.Ledger))))!.Kind);
            Assert.Equal(deleted, Deleted(cell));
            Assert.NotEqual(default, process.Ledger.Total(CapacityState.Paused));

            // A third sink with retention set again resumes it to its done and releases its demand
            AssertHandedOff(await RestartAsync(cell, process.Ledger), resumed: true);
            Assert.Equal((default(CapacityDemand), default(CapacityDemand)), (process.Ledger.Current, process.Ledger.Total(CapacityState.Paused)));
            await AssertDoneAsync(cell);
        }

        public enum ConcealedSite
        {
            PayloadGate,
            Recovery,
            Rebase
        }

        [Theory]
        [InlineData(ConcealedSite.PayloadGate)]
        [InlineData(ConcealedSite.Recovery)]
        [InlineData(ConcealedSite.Rebase)]
        public async Task ConcealedSuccessorFoundByStat(ConcealedSite site)
        {
            var cell = await CellAsync(local: false);
            var hooks = cell.Table.Hooks!;
            var process = await AdvancedAsync(cell, payload: true);
            var kept = new List<string>() { "/t/part-0009.parquet" };
            if (site == ConcealedSite.Rebase)
            {
                // Rebasing onto another writer's commit
                await AppendAsync(cell, "part-0009.parquet", own: false);
                Assert.Equal(OutcomeKind.NoWork, (await NextAsync(cell, process))!.Kind);
                Assert.Equal(AdvanceState.Rebasing, process.Active!.State);
                kept.Add("/t/part-0010.parquet");
            }
            // Another writer's next commit re-adds a planned path, concealed by a complete-looking listing
            var readded = kept[^1].Substring(3);
            var concealed = cell.Table.LogPath(Json(cell.Table.Head + 1));
            hooks.Hidden = path => path.Full == concealed;
            await WriteAppendAsync(cell.Table.Raw, cell.Table.Head + 1, readded, own: false);
            var (check, _) = await ListingEvidence.ReadLogAsync(cell.Table.Configuration, cell.Table.Metered, cell.Table.Head, default);
            Assert.Equal(ListingCheck.SuccessorPresent, check);
            var deleted = Deleted(cell);

            if (site == ConcealedSite.Recovery)
            {
                // A restart while it is concealed lists nothing it could resume from
                AssertResult(await RestartAsync(cell), OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            }
            else
            {
                for (int turn = 0; turn < 2; turn++)
                {
                    AssertResult(await NextAsync(cell, process), OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
                }
                Assert.Equal(site == ConcealedSite.Rebase ? AdvanceState.Rebasing : AdvanceState.Runnable, process.Active!.State);
            }
            Assert.Equal(deleted, Deleted(cell));

            // Listed again, it is kept and the advance completes
            hooks.Hidden = null;
            await RetainAppendedAsync(cell, readded);
            if (site == ConcealedSite.Recovery)
            {
                // A restarted sink anchors above the foreign commit
                await AppendAsync(cell, "part-0099.parquet", own: true);
                AssertHandedOff(await RestartAsync(cell), resumed: true);
            }
            else
            {
                await FinishAsync(cell, process);
            }
            await AssertDoneAsync(cell, kept.ToArray());
        }

        [Theory]
        [InlineData("duplicate")]
        [InlineData("regress")]
        public async Task RegressingOrDuplicateNamesAtPayloadGate(string fault)
        {
            var clone = await CellAsync(local: true);
            var relisted = new RelistedStorage(clone.Table.Raw);
            var table = clone.Table;
            // Listed through a wrapper below the meter
            var cell = new Cell()
            {
                Table = new ExecutionTable()
                {
                    Raw = table.Raw,
                    Metered = MeteredFileStorage.Wrap(relisted, MaintenanceBucketsTests.Unthrottled(), table.Configuration.Store.Store.Paging),
                    Configuration = table.Configuration,
                    Root = table.Root,
                    Head = table.Head
                },
                Baseline = clone.Baseline,
                Outcome = clone.Outcome
            };
            var process = await AdvancedAsync(cell, payload: true);
            var active = process.Active!;
            var head = Json(cell.Table.Head);
            relisted.Relist = fault == "duplicate"
                ? entries => entries.Append(entries.Single(x => x.Name == head)).ToList()
                : entries => entries.Reverse().ToList();
            var deleted = Deleted(cell);

            var result = await NextAsync(cell, process);

            if (fault == "duplicate")
            {
                // A repeated name fails the listing's checks, and the slice retries with nothing deleted
                AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
                Assert.Equal((AdvanceState.Runnable, deleted), (active.State, Deleted(cell)));
            }
            else
            {
                // A native listing has no order and is sorted before its checks
                Assert.Equal(OutcomeKind.Completed, result!.Kind);
                Assert.True(Deleted(cell) > deleted);
            }
            relisted.Relist = null;
            await FinishAsync(cell, process);
            await AssertDoneAsync(cell);
        }

        [Fact]
        public async Task CommitLandsBeforeStatRetries()
        {
            var cell = await CellAsync(local: false);
            var hooks = cell.Table.Hooks!;
            var process = await AdvancedAsync(cell, payload: true);
            var active = process.Active!;
            // Another writer's commit lands between the gate's listing and its successor probe
            var successor = cell.Table.LogPath(Json(cell.Table.Head + 1));
            var landed = false;
            hooks.Before = async (verb, path) =>
            {
                if (verb == "Stat" && path.Full == successor && !landed)
                {
                    landed = true;
                    await WriteAppendAsync(hooks.Inner, cell.Table.Head + 1, "foreign-0001.parquet", own: false);
                }
            };
            var deleted = Deleted(cell);

            var result = await NextAsync(cell, process);

            Assert.True(landed);
            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            Assert.Equal((AdvanceState.Runnable, deleted), (active.State, Deleted(cell)));
            hooks.Before = null;
            await RetainAppendedAsync(cell, "foreign-0001.parquet");

            // The landed commit is then a foreign head the advance rebases onto
            await FinishAsync(cell, process);
            await AssertDoneAsync(cell);
        }

        [Fact]
        public async Task StorageInstanceSwapKeepsAuthority()
        {
            var cell = await CellAsync(local: false);
            var table = cell.Table;
            // The sink's storage setting, which a replacement instance can take
            IFileStorage current = table.Raw;
            var configuration = CleanupConfiguration.Validate(() => current, "t", true, new CleanupOptIns(AdvanceExecutionKit.Retention, null));
            using var other = Files.Of.InternalMemory($"./swapped-{Guid.NewGuid():N}");
            var process = Start(cell, cleanupOf: x => new DeltaTableCleanup(configuration, x.Metered, new FakeTimeProvider(AdvanceExecutionKit.Now), "t", null, () => current, new DiscardCounters(), null, "t"));
            // Swapped while the second progress is published, before its predecessor's delete
            process.Hooks.BeforePublish = async (kind, name) =>
            {
                if (kind == ControlKind.Progress && ControlName.TryParse(name, out var progress) && progress.Ordinal == 2 && current == table.Raw)
                {
                    await InvalidateAsync(cell);
                    current = other;
                }
            };
            await AdvancedAsync(cell, payload: true, process);
            var active = process.Active!;

            Assert.Equal(OutcomeKind.Completed, (await NextAsync(cell, process))!.Kind);
            Assert.True(current == other);
            // The predecessor's delete was kept for the changed storage, and the next slice pauses
            Assert.Contains(cell.Outcome.Settled, x => x.Target.Kind == TargetKind.Control && x.Outcome.Reason == KeepReason.StorageChanged);
            AssertHeld(await NextAsync(cell, process), active);
            Assert.Equal(AdvanceState.Paused, active.State);
            Assert.NotEqual(default, process.Ledger.Total(CapacityState.Paused));

            await AssertUnresolvedAsync(cell);
        }

        [Fact]
        public async Task DotPrefixedOmissionIsAResidual()
        {
            var cell = await CellAsync(local: false);
            var hooks = cell.Table.Hooks!;
            await AdvancedAsync(cell, payload: true);
            var logsComplete = (await cell.Table.ControlsAsync()).Single(x => Is(x, ControlKind.LogsComplete));
            // A listing that omits a control record, which the listing's own checks do not see
            hooks.Hidden = path => path.Name == logsComplete;
            var (check, _) = await ListingEvidence.ReadLogAsync(cell.Table.Configuration, cell.Table.Metered, cell.Table.Head, default);
            Assert.Equal(ListingCheck.Passed, check);

            // A restart resumes before its logs-complete, republishes it, completes and leaves the omitted one behind
            AssertHandedOff(await RestartAsync(cell), resumed: true);
            var stored = await StoredAsync(cell.Table);
            Assert.Equal(2, stored.Length);
            Assert.Contains(logsComplete, stored);

            // Listed again, a later recovery removes it
            hooks.Hidden = null;
            AssertResult(await RestartAsync(cell), OutcomeKind.NoWork, MaintenanceReason.NoEligibleCheckpoint);
            await AssertDoneAsync(cell);
        }
    }

    [Collection(nameof(MaintenanceSwitchCollection))]
    public class RetentionSwitchFaultTests : RetentionFaultCells, IClassFixture<MatrixTemplate>
    {
        public RetentionSwitchFaultTests(MatrixTemplate template) : base(template)
        {
            Switch(false);
        }

        private static void Switch(bool on) => AppContext.SetSwitch(MaintenanceSwitches.DisableMaintenanceDeletes, on);

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task KillSwitchAcrossRestartPauses(bool payload)
        {
            var cell = await CellAsync(local: true);
            try
            {
                await AdvancedAsync(cell, payload);
                var records = await cell.Table.ControlsAsync();
                var deleted = Deleted(cell);

                // Switched on while the process is down, the restart resumes and pauses
                Switch(true);
                var process = Start(cell);
                AssertResult(await JobAsync(cell, process), OutcomeKind.Completed, MaintenanceReason.None);
                var active = process.Active!;
                AssertResult(await NextAsync(cell, process), OutcomeKind.Declined, MaintenanceReason.DeletesDisabled);
                Assert.Equal(AdvanceState.Paused, active.State);
                Assert.NotEqual(default, process.Ledger.Total(CapacityState.Paused));
                Assert.Equal(records, await cell.Table.ControlsAsync());
                Assert.Equal(C, await AdvanceExecutionKit.FloorAsync(cell.Table));
                Assert.Equal(deleted, Deleted(cell));

                // Switched off, the same process completes
                Switch(false);
                await FinishAsync(cell, process);
                await AssertDoneAsync(cell);
            }
            finally
            {
                Switch(false);
            }
        }
    }
}

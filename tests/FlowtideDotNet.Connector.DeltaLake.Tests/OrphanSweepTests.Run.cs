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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // A local table with own commits 0 to 2, its volatile evidence and orphan runs over old planted files
    internal sealed class OrphanRunKit : IDisposable
    {
        public const long Head = 2;

        // Older than any cutoff
        public static readonly DateTimeOffset Old = AdvanceExecutionKit.Now - TimeSpan.FromDays(30);

        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public OrphanRunKit()
        {
            Owner = _owners.Add(new ScratchDirectory(Path.Combine(Contexts.NewRoot(), "s")));
        }

        public AdvanceExecutionKit Contexts { get; } = new AdvanceExecutionKit();

        // Where finished targets files are charged
        public ScratchDirectory Owner { get; }

        // One target a slice, retried at once
        public static DeletionRunSettings Settings => new DeletionRunSettings() { MaxPaths = 1, RetryDelay = TimeSpan.Zero };

        public void Dispose()
        {
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            _owners.Release();
            Contexts.Dispose();
        }

        // The table at /t on local disk
        public async Task<ExecutionTable> TableAsync()
        {
            var table = Contexts.Open(Contexts.NewRoot(), Head);
            await OrphanSweepTests.RetainedTableAsync(table.Raw, "t", Head);
            return table;
        }

        // An inline run's cleanup with the kit's slice settings
        public static DeltaTableCleanup Cleanup(ExecutionTable table)
        {
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            cleanup.Settings = cleanup.Settings with { MaxPaths = 1, RetryDelay = TimeSpan.Zero };
            return cleanup;
        }

        public static string PathOf(ExecutionTable table, string name) => Path.Combine(table.Root!, "t", name);

        public static bool Exists(ExecutionTable table, string name) => File.Exists(PathOf(table, name));

        // Three bytes below the table, written long ago
        public static void Plant(ExecutionTable table, params string[] names)
        {
            foreach (var name in names)
            {
                var path = PathOf(table, name);
                File.WriteAllBytes(path, new byte[3]);
                File.SetLastWriteTimeUtc(path, Old.UtcDateTime);
            }
        }

        // Volatile evidence anchored at the listed head as the sweep establishes it, and that listing's entries
        public async Task<(CleanupEvidence Evidence, int LogEntries)> EvidenceAsync(CleanupConfiguration configuration, IFileStorage metered, long head)
        {
            var context = Contexts.AdvanceContext(head);
            try
            {
                var (check, log) = await ListingEvidence.ReadLogAsync(configuration, metered, head, context.Stop, context.Memory);
                Assert.Equal(ListingCheck.Passed, check);
                var established = await CleanupEvidence.EstablishAsync(configuration, metered, log!, log!.Head, null, durable: false, context);
                Assert.True(established.Passed, $"{established.Verdict}: {established.Detail}");
                return (established.Evidence!, log.Entries.Count);
            }
            finally
            {
                await context.SettleAsync();
            }
        }

        // The names below the table in a finished targets file, each frozen at three bytes and the old time
        public OrphanTargets Targets(CleanupConfiguration configuration, params string[] names)
        {
            using var scratch = Owner.CreateJob();
            using var writer = new OrphanTargetsWriter(scratch, new JobMemory());
            var payload = new byte[OrphanCandidatePayload.MaxLength];
            foreach (var name in names)
            {
                var length = OrphanCandidatePayload.Encode(payload, 3, Old, null);
                writer.Add(configuration.Store.Table.Full + "/" + name, payload.AsSpan(0, length));
            }
            var targets = writer.Finish();
            _disposables.Add(targets);
            return targets;
        }

        // A run over the meter on the kit's clock
        public OrphanRun Run(CleanupConfiguration configuration, IFileStorage metered, CleanupEvidence evidence, OrphanTargets targets, DeletionRunSettings settings, RetentionCounters counters,
            CapacityLease? lease = null)
        {
            var run = new OrphanRun(configuration, metered, new FakeTimeProvider(AdvanceExecutionKit.Now), settings, evidence, targets, lease, counters);
            _disposables.Add(run);
            return run;
        }

        // One slice under a short delete admission, which releases everything
        public async Task<CleanupResult> SliceAsync(OrphanRun run, long head)
        {
            var context = Contexts.DeletionContext(head);
            CleanupResult result;
            try
            {
                result = await run.RunSliceAsync(context);
            }
            finally
            {
                await context.SettleAsync();
            }
            Assert.Equal(0, context.Memory.Managed);
            Assert.Empty(context.Scratch.Files);
            return result;
        }

        // One delete turn of the cleanup as a worker runs it
        public Task<CleanupResult> TurnAsync(DeltaTableCleanup cleanup, long head) => AdvanceExecutionKit.TurnAsync(cleanup, Contexts.DeletionContext(head));
    }

    public partial class OrphanSweepTests
    {
        private static readonly string[] s_runTargets = { "old-a.parquet", "old-b.parquet", "old-c.parquet" };

        private static string Describe(CleanupResult result) => $"{result.Kind} {result.Reason}: {result.Detail}";

        [Fact]
        public async Task ForeignChangeStopsOrphanRun()
        {
            using var kit = new OrphanRunKit();
            var table = await kit.TableAsync();
            OrphanRunKit.Plant(table, s_runTargets);
            var cleanup = OrphanRunKit.Cleanup(table);
            var (evidence, _) = await kit.EvidenceAsync(table.Configuration, table.Metered, table.Head);
            var targets = kit.Targets(table.Configuration, s_runTargets);
            Assert.Equal(new FileInfo(targets.Path).Length, kit.Owner.Ledger.Reserved);
            var run = kit.Run(table.Configuration, table.Metered, evidence, targets, cleanup.Settings, cleanup.Counters);
            cleanup.RegisterOrphan(run);

            var first = await kit.TurnAsync(cleanup, table.Head);
            Assert.True(first.Kind == OutcomeKind.Completed, Describe(first));
            Assert.False(OrphanRunKit.Exists(table, "old-a.parquet"));

            // Not an own commit of the anchor's epoch
            await DeltaTestKit.WriteCommit(table.Raw, "t", table.Head + 1, DeltaLakeCheckpointBuildTests.Add("foreign.parquet"));
            var second = await kit.TurnAsync(cleanup, table.Head + 1);

            Assert.True(second.Kind == OutcomeKind.Declined && second.Reason == MaintenanceReason.HistoryChanged, Describe(second));
            Assert.Equal(GateVerdict.ForeignHead, evidence.StoppedBy);
            Assert.True(run.Ended);
            Assert.True(OrphanRunKit.Exists(table, "old-b.parquet"));
            Assert.True(OrphanRunKit.Exists(table, "old-c.parquet"));
            Assert.Equal(1, cleanup.Counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted));
            Assert.False(File.Exists(targets.Path));
            Assert.Equal(0, kit.Owner.Ledger.Reserved);
            Assert.False(cleanup.IsDue(JobKind.Deletion, table.Head + 1, Now));
        }

        [Fact]
        public async Task OrphanRunDeletesExactlyItsTargetsInSlices()
        {
            using var kit = new OrphanRunKit();
            var table = await kit.TableAsync();
            // The head's live file
            OrphanRunKit.Plant(table, "part-0002.parquet");
            OrphanRunKit.Plant(table, s_runTargets);
            var (evidence, _) = await kit.EvidenceAsync(table.Configuration, table.Metered, table.Head);
            var targets = kit.Targets(table.Configuration, s_runTargets);
            var counters = new RetentionCounters();
            var run = kit.Run(table.Configuration, table.Metered, evidence, targets, OrphanRunKit.Settings, counters);

            for (int slice = 1; slice <= s_runTargets.Length; slice++)
            {
                var result = await kit.SliceAsync(run, table.Head);
                Assert.True(result.Kind == OutcomeKind.Completed, Describe(result));
                Assert.Equal(slice, run.Cursor);
                for (int i = 0; i < s_runTargets.Length; i++)
                {
                    Assert.True(OrphanRunKit.Exists(table, s_runTargets[i]) == i >= slice, s_runTargets[i]);
                }
                // Retired by the slice that finished it
                Assert.Equal(slice == s_runTargets.Length, run.Ended);
            }

            Assert.True(OrphanRunKit.Exists(table, "part-0002.parquet"));
            Assert.Equal(3, counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted));
            Assert.Equal(0, counters.Deletes(TargetKind.Payload, DeleteStatus.Kept));
            Assert.False(File.Exists(targets.Path));
            Assert.Equal(0, kit.Owner.Ledger.Reserved);
            Assert.Equal(OutcomeKind.NoWork, (await kit.SliceAsync(run, table.Head)).Kind);
        }

        [Fact]
        public async Task LocalDirectoryTargetIsKeptWithoutHaltingTheRun()
        {
            using var kit = new OrphanRunKit();
            var table = await kit.TableAsync();
            OrphanRunKit.Plant(table, "old-a.parquet", "old-c.parquet");
            Directory.CreateDirectory(OrphanRunKit.PathOf(table, "old-b.parquet"));
            var (evidence, _) = await kit.EvidenceAsync(table.Configuration, table.Metered, table.Head);
            var targets = kit.Targets(table.Configuration, s_runTargets);
            var counters = new RetentionCounters();
            var run = kit.Run(table.Configuration, table.Metered, evidence, targets, OrphanRunKit.Settings, counters);

            for (int slice = 1; slice <= s_runTargets.Length; slice++)
            {
                var result = await kit.SliceAsync(run, table.Head);
                Assert.True(result.Kind == OutcomeKind.Completed, Describe(result));
            }

            Assert.True(run.Ended);
            Assert.True(Directory.Exists(OrphanRunKit.PathOf(table, "old-b.parquet")));
            Assert.False(OrphanRunKit.Exists(table, "old-a.parquet"));
            Assert.False(OrphanRunKit.Exists(table, "old-c.parquet"));
            Assert.Equal(1, counters.OrphanKept(KeepReason.Directory));
            Assert.Equal(1, counters.Deletes(TargetKind.Payload, DeleteStatus.Kept));
            Assert.Equal(2, counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted));
        }

        [Fact]
        public async Task OrphanRunMakesDeleteTurnsDueAndHoldsTheSweep()
        {
            using var kit = new OrphanRunKit();
            var table = await kit.TableAsync();
            OrphanRunKit.Plant(table, s_runTargets);
            var cleanup = OrphanRunKit.Cleanup(table);
            var (evidence, entries) = await kit.EvidenceAsync(table.Configuration, table.Metered, table.Head);
            Assert.Equal(OrphanSweepRequestResult.Scheduled, cleanup.RequestOrphanSweep());
            Assert.True(cleanup.IsDue(JobKind.Orphan, table.Head, Now));
            Assert.False(cleanup.IsDue(JobKind.Deletion, table.Head, Now));

            // A latency average away from the default, which the lease is priced at
            cleanup.ObserveLatency(TimeSpan.FromSeconds(1), cleanup.Settings.MaxPaths);
            Assert.NotEqual(CapacityEstimator.DefaultLatency, cleanup.Latency);
            var ledger = new CapacityLedger(() => default);
            var key = OrphanRun.LeaseKeyOf(TableLeases.KeyOf(RawStorage.Unwrap(table.Raw), table.Configuration.Store.Table));
            var targets = kit.Targets(table.Configuration, s_runTargets);
            CapacityDemand Remaining(long count, TimeSpan latency) => OrphanRun.Demand(table.Configuration, evidence, entries, count, targets.PathBytesLeft, latency, ledger.RequestsPerSecond);
            var lease = ledger.TryReserve(default, key, OrphanRun.LeaseId, Remaining(3, cleanup.Latency));
            Assert.NotNull(lease);
            var run = kit.Run(table.Configuration, table.Metered, evidence, targets, cleanup.Settings, cleanup.Counters, lease);

            cleanup.RegisterOrphan(run);
            Assert.True(cleanup.IsDue(JobKind.Deletion, table.Head, Now));
            Assert.False(cleanup.IsDue(JobKind.Orphan, table.Head, Now));

            var first = await kit.TurnAsync(cleanup, table.Head);
            Assert.True(first.Kind == OutcomeKind.Completed, Describe(first));
            // The remaining targets at the cleanup's latency
            Assert.NotEqual(Remaining(2, CapacityEstimator.DefaultLatency), Remaining(2, cleanup.Latency));
            Assert.Equal(Remaining(2, cleanup.Latency), lease.Demand);
            Assert.Equal(lease.Demand, ledger.Total(CapacityState.Runnable));
            Assert.True(cleanup.IsDue(JobKind.Deletion, table.Head, Now));
            Assert.False(cleanup.IsDue(JobKind.Orphan, table.Head, Now));

            for (int turn = 2; turn <= s_runTargets.Length; turn++)
            {
                var result = await kit.TurnAsync(cleanup, table.Head);
                Assert.True(result.Kind == OutcomeKind.Completed, Describe(result));
            }

            Assert.True(run.Ended);
            Assert.All(s_runTargets, x => Assert.False(OrphanRunKit.Exists(table, x), x));
            Assert.False(cleanup.IsDue(JobKind.Deletion, table.Head, Now));
            // The request the run held
            Assert.True(cleanup.IsDue(JobKind.Orphan, table.Head, Now));
            Assert.Null(ledger.StateOf(default, key));
            Assert.Equal(default, ledger.Total(CapacityState.Runnable));
        }

        [Fact]
        public async Task AdvanceAuthorityStopsOrphanRun()
        {
            using var kit = new OrphanRunKit();
            var table = await kit.TableAsync();
            OrphanRunKit.Plant(table, s_runTargets);
            var cleanup = OrphanRunKit.Cleanup(table);
            var (evidence, _) = await kit.EvidenceAsync(table.Configuration, table.Metered, table.Head);
            var targets = kit.Targets(table.Configuration, s_runTargets);
            var run = kit.Run(table.Configuration, table.Metered, evidence, targets, cleanup.Settings, cleanup.Counters);
            cleanup.RegisterOrphan(run);

            var first = await kit.TurnAsync(cleanup, table.Head);
            Assert.True(first.Kind == OutcomeKind.Completed, Describe(first));

            // An advance's started record, listed between the turns
            var started = ControlName.Group(ControlKind.Started, 1, "advance", new byte[32], "attempt");
            await File.WriteAllTextAsync(OrphanRunKit.PathOf(table, Path.Combine("_delta_log", started)), "{}");
            var second = await kit.TurnAsync(cleanup, table.Head);

            Assert.True(second.Kind == OutcomeKind.Declined && second.Reason == MaintenanceReason.CleanupGate, Describe(second));
            Assert.True(run.Ended);
            Assert.False(OrphanRunKit.Exists(table, "old-a.parquet"));
            Assert.True(OrphanRunKit.Exists(table, "old-b.parquet"));
            Assert.True(OrphanRunKit.Exists(table, "old-c.parquet"));
            Assert.False(File.Exists(targets.Path));
            Assert.False(cleanup.IsDue(JobKind.Deletion, table.Head, Now));
        }

        [Fact]
        public async Task UninterpretedControlRecordStopsOrphanRun()
        {
            using var kit = new OrphanRunKit();
            var table = await kit.TableAsync();
            OrphanRunKit.Plant(table, s_runTargets);
            var cleanup = OrphanRunKit.Cleanup(table);
            var (evidence, _) = await kit.EvidenceAsync(table.Configuration, table.Metered, table.Head);
            var targets = kit.Targets(table.Configuration, s_runTargets);
            var run = kit.Run(table.Configuration, table.Metered, evidence, targets, cleanup.Settings, cleanup.Counters);
            cleanup.RegisterOrphan(run);

            var first = await kit.TurnAsync(cleanup, table.Head);
            Assert.True(first.Kind == OutcomeKind.Completed, Describe(first));

            // A record of a later version's form, listed between the turns
            await File.WriteAllTextAsync(OrphanRunKit.PathOf(table, Path.Combine("_delta_log", ".flowtide-cleanup.future")), "{}");
            var second = await kit.TurnAsync(cleanup, table.Head);

            Assert.True(second.Kind == OutcomeKind.Declined && second.Reason == MaintenanceReason.CleanupGate, Describe(second));
            Assert.True(run.Ended);
            Assert.False(OrphanRunKit.Exists(table, "old-a.parquet"));
            Assert.True(OrphanRunKit.Exists(table, "old-b.parquet"));
            Assert.True(OrphanRunKit.Exists(table, "old-c.parquet"));
            Assert.False(File.Exists(targets.Path));
            Assert.Equal(0, kit.Owner.Ledger.Reserved);
            Assert.False(cleanup.IsDue(JobKind.Deletion, table.Head, Now));
        }

        [Fact]
        public async Task StartedAdvanceTakesTheDeleteTurnFirst()
        {
            using var kit = new OrphanRunKit();
            var table = await kit.Contexts.ChurnedAsync();
            await AdvanceExecutionKit.PayloadFilesAsync(table);
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            await kit.Contexts.PublishAsync(cleanup, table);
            var active = await kit.Contexts.AdoptAsync(cleanup, table);
            var advanced = 0;
            cleanup.AdvanceHooksForTests.Settled = (_, _) => Interlocked.Increment(ref advanced);
            OrphanRunKit.Plant(table, s_runTargets);
            var (evidence, _) = await kit.EvidenceAsync(table.Configuration, table.Metered, table.Head);
            var run = kit.Run(table.Configuration, table.Metered, evidence, kit.Targets(table.Configuration, s_runTargets), OrphanRunKit.Settings, cleanup.Counters);
            cleanup.RegisterOrphan(run);

            // Each turn is the advance's until its done
            for (int turn = 1; cleanup.ActiveForTests != null; turn++)
            {
                Assert.True(turn <= 40, "The advance never published its done");
                var result = await kit.TurnAsync(cleanup, table.Head);
                Assert.True(result.Kind == OutcomeKind.Completed, Describe(result));
                Assert.True(Volatile.Read(ref advanced) > 0, $"Turn {turn} followed no advance delete");
                Assert.False(run.Ended);
                Assert.Equal(0, run.Cursor);
                Assert.All(s_runTargets, x => Assert.True(OrphanRunKit.Exists(table, x), x));
            }
            Assert.Equal(AdvancePhase.Done, active.Phase);

            // Then the orphan run's
            var next = await kit.TurnAsync(cleanup, table.Head);
            Assert.True(next.Kind == OutcomeKind.Completed, Describe(next));
            Assert.Equal(1, run.Cursor);
            Assert.False(OrphanRunKit.Exists(table, "old-a.parquet"));
        }

        [Fact]
        public async Task AzureOrphanRunCountsItsSubmissionProbes()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            using var kit = new OrphanRunKit();
            server.Now = OrphanRunKit.Old;
            var table = LoopbackTable(server);
            await RetainedTableAsync(table.Raw, table.Name, OrphanRunKit.Head);
            // The head's live file, and two of the three targets
            server.Put("tables/t/part-0002.parquet", new byte[3], OrphanRunKit.Old);
            server.Put("tables/t/old-a.parquet", new byte[3], OrphanRunKit.Old);
            server.Put("tables/t/old-c.parquet", new byte[3], OrphanRunKit.Old);
            var metered = Metered(table);
            var (evidence, _) = await kit.EvidenceAsync(table.Configuration, metered, OrphanRunKit.Head);
            var counters = new RetentionCounters();
            var run = kit.Run(table.Configuration, metered, evidence, kit.Targets(table.Configuration, s_runTargets), new DeletionRunSettings() { RetryDelay = TimeSpan.Zero }, counters);
            server.Requests.Clear();

            var result = await kit.SliceAsync(run, OrphanRunKit.Head);

            Assert.True(result.Kind == OutcomeKind.Completed, Describe(result));
            Assert.True(run.Ended);
            Assert.Equal(1, counters.CandidateProbes(ProbeResult.Absent));
            Assert.Equal(2, counters.CandidateProbes(ProbeResult.Present));
            Assert.Equal(0, counters.CandidateProbes(ProbeResult.Error));
            var requests = server.Requests.ToList();
            foreach (var name in s_runTargets)
            {
                Assert.Contains($"HEAD /c/tables/t/{name} HTTP/1.1", requests);
            }
            Assert.Equal(new[] { "DELETE /c/tables/t/old-a.parquet HTTP/1.1", "DELETE /c/tables/t/old-c.parquet HTTP/1.1" }, requests.Where(x => x.StartsWith("DELETE ", StringComparison.Ordinal)).Order(StringComparer.Ordinal));
            Assert.False(server.Contains("tables/t/old-a.parquet"));
            Assert.False(server.Contains("tables/t/old-c.parquet"));
            Assert.True(server.Contains("tables/t/part-0002.parquet"));
            Assert.Equal(2, counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted));
            Assert.Equal(1, counters.Deletes(TargetKind.Payload, DeleteStatus.Absent));
        }

        [Fact]
        public async Task StoppedWorkerReleasesItsOrphanRun()
        {
            using var kit = new AdvanceExecutionKit();
            var rig = await RequestRigAsync(kit, null);
            var configuration = CleanupConfiguration.Validate(rig.Raw, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, null));
            var metered = MeteredFileStorage.Wrap(rig.Raw, rig.Budget.Buckets, configuration.Store.Store.Paging);
            var key = OrphanRun.LeaseKeyOf(TableLeases.KeyOf(RawStorage.Unwrap(rig.Raw), configuration.Store.Table));
            var payload = new byte[OrphanCandidatePayload.MaxLength];
            var payloadLength = OrphanCandidatePayload.Encode(payload, 3, OrphanRunKit.Old, null);
            OrphanRun? run = null;
            CleanupEvidence? evidence = null;
            ScratchDirectory? scratch = null;
            var owner = CancellationToken.None;
            long baseline = -1;
            try
            {
                await QuiesceRequestRigAsync(rig);
                await using (var stream = await rig.Raw.OpenWrite("/t/old.parquet"))
                {
                    await stream.WriteAsync(new byte[3]);
                }
                // Registered from the orphan job, with its lease under the worker's owner
                rig.Cleanup.OrphanForTests.CoreForTests = async context =>
                {
                    var (check, log) = await ListingEvidence.ReadLogAsync(configuration, metered, context.Head, context.Stop, context.Memory);
                    var established = await CleanupEvidence.EstablishAsync(configuration, metered, log!, log!.Head, null, durable: false, context);
                    evidence = established.Evidence!;
                    scratch = context.Scratch.Owner;
                    baseline = scratch.Ledger.Reserved;
                    owner = context.CapacityOwner;
                    OrphanTargets targets;
                    using (var writer = new OrphanTargetsWriter(context.Scratch, context.Memory))
                    {
                        writer.Add(configuration.Store.Table.Full + "/old.parquet", payload.AsSpan(0, payloadLength));
                        targets = writer.Finish();
                    }
                    var ledger = context.Capacity!;
                    var lease = ledger.TryReserve(owner, key, OrphanRun.LeaseId, OrphanRun.Demand(configuration, evidence, log.Entries.Count, targets.Count, targets.PathBytes, rig.Cleanup.Latency, ledger.RequestsPerSecond));
                    run = new OrphanRun(configuration, metered, rig.Clock, rig.Cleanup.Settings, evidence, targets, lease, rig.Cleanup.Counters);
                    rig.Cleanup.RegisterOrphan(run);
                    return new CleanupResult(OutcomeKind.Completed);
                };
                Assert.Equal(OrphanSweepRequestResult.Scheduled, rig.Request());
                rig.Budget.TickNow();
                await rig.Worker.CurrentJob.WaitAsync(s_requestWait);

                Assert.Equal(1, rig.Count(JobKind.Orphan));
                Assert.NotNull(run);
                Assert.Equal(0, rig.Count(JobKind.Deletion));
                // Runnable before the stop: a pause alone would leave it paused
                Assert.Equal(CapacityState.Runnable, rig.Budget.Capacity.StateOf(owner, key));
                Assert.True(File.Exists(run.Targets.Path));
                Assert.Equal(baseline + new FileInfo(run.Targets.Path).Length, scratch!.Ledger.Reserved);
            }
            finally
            {
                await StopRequestRigAsync(rig);
            }

            Assert.True(run!.Ended);
            Assert.Null(rig.Budget.Capacity.StateOf(owner, key));
            Assert.Equal(default, rig.Budget.Capacity.Total(CapacityState.Paused));
            Assert.Equal(default, rig.Budget.Capacity.Total(CapacityState.Runnable));
            Assert.False(File.Exists(run.Targets.Path));
            Assert.Equal(baseline, scratch!.Ledger.Reserved);

            // A run a detached orphan job registers after the stop
            var lateScratch = Scratch(DiskLedger.DefaultCap);
            OrphanTargets lateTargets;
            using (var writer = new OrphanTargetsWriter(lateScratch, new JobMemory()))
            {
                writer.Add(configuration.Store.Table.Full + "/old.parquet", payload.AsSpan(0, payloadLength));
                lateTargets = writer.Finish();
            }
            var late = new OrphanRun(configuration, metered, rig.Clock, rig.Cleanup.Settings, evidence!, lateTargets, null, new RetentionCounters());
            _disposables.Add(late);
            rig.Cleanup.RegisterOrphan(late);
            Assert.True(late.Ended);
            Assert.False(File.Exists(lateTargets.Path));
            Assert.Equal(0, lateScratch.Owner.Ledger.Reserved);
            Assert.False(rig.Cleanup.IsDue(JobKind.Deletion, 0, rig.Clock.GetUtcNow()));
        }
    }
}

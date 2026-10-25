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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Stowage;
using System.Globalization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// One registered orphan run: its targets file, volatile evidence and capacity lease, executed by the table's delete turns.
    /// </summary>
    internal sealed class OrphanRun : IDisposable
    {
        // The orphan namespace of its capacity lease key and advance ids
        public const string LeaseId = "orphan";

        // The last lease number handed out
        private static long s_leases;

        private readonly CleanupConfiguration _configuration;
        private readonly IFileStorage _storage;
        private readonly CleanupEvidence _evidence;
        private readonly CapacityLease? _lease;
        private readonly RetentionCounters _counters;
        private readonly DeletionExecutor _executor;
        private readonly DeletionRunSettings _settings;
        private readonly DeletionRun _run;
        // The memory of the slice that reads the targets file now
        private JobMemory? _memory;
        // Entries of the latest passed gate's log listing
        private long _logEntries;
        // Set by a gate of this run that stops it, replacing the slice's reason
        private MaintenanceReason? _stopReason;
        // 1 once a pinned Azure submission found a directory
        private int _halted;
        // 1 once retired
        private int _disposed;

        // Storage is the run's meter; the lease is null without a capacity ledger
        public OrphanRun(CleanupConfiguration configuration, IFileStorage storage, TimeProvider clock, DeletionRunSettings settings, CleanupEvidence evidence, OrphanTargets targets,
            CapacityLease? lease, RetentionCounters counters)
        {
            _configuration = configuration;
            _storage = storage;
            _evidence = evidence;
            Targets = targets;
            _lease = lease;
            _counters = counters;
            _executor = new DeletionExecutor(configuration, storage) { Settled = OnSettled, Withheld = OnSettled };
            var outer = settings.DeletesDisabled;
            _settings = settings with { Orphan = true, DeletesDisabled = () => Volatile.Read(ref _halted) != 0 || outer?.Invoke() == true };
            _run = new DeletionRun(_executor, ChunkGate.ForLog(configuration, storage, DecideAsync), targets.Source(_executor, () => _memory!), clock, _settings);
        }

        // The table's warn-once log
        internal Action<MaintenanceReason, string>? Warn { get; set; }

        // Runs before each delete is submitted, after its checks passed
        internal Func<DeletionTarget, Task>? BeforeDeleteForTests
        {
            get => _executor.BeforeDeleteForTests;
            set => _executor.BeforeDeleteForTests = value;
        }

        // The per-delete latency its remaining demand is priced at
        internal Func<TimeSpan>? Latency { get; set; }

        // Takes each slice's measured per-delete latency and the deletes it counts for
        internal Action<TimeSpan, int>? ObserveLatency { get; set; }

        public OrphanTargets Targets { get; }

        // Targets the run has passed
        public long Cursor { get; private set; }

        // True once retired
        public bool Ended => Volatile.Read(ref _disposed) != 0;

        /// <summary>
        /// One slice under a delete admission, mapped to the turn's result; a halt, a finished run or a stop retires the run.
        /// </summary>
        public async Task<CleanupResult> RunSliceAsync(CleanupContext context)
        {
            if (Ended)
            {
                return new CleanupResult(OutcomeKind.NoWork);
            }
            _memory = context.Memory;
            var before = Cursor;
            var slice = await _run.RunSliceAsync(context);
            Cursor = _run.Cursor;
            // Before the remaining demand is priced, as an advance's slice
            if ((slice.Status is SliceStatus.Progressed or SliceStatus.Completed) && slice.Issued > 0)
            {
                var outstanding = Math.Clamp(_settings.MaxOutstanding, 1, DeletionRun.OutstandingLimit(_configuration.Store.Store));
                ObserveLatency?.Invoke(AdvanceExecution.PerDelete(slice, outstanding), AdvanceExecution.Measured(slice, _settings));
            }
            var detail = slice.Detail ?? slice.Gate?.Detail;
            // Before the status, which a kept directory leaves Completed or Progressed
            if (Volatile.Read(ref _halted) != 0)
            {
                var message = $"The store reported a directory for an orphan target at submission: the orphan run stops with {Targets.Count - Cursor} of {Targets.Count} targets left";
                Warn?.Invoke(MaintenanceReason.InvalidMaintenanceOptions, message);
                Dispose();
                return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.InvalidMaintenanceOptions, message);
            }
            var (result, retire) = slice.Status switch
            {
                SliceStatus.Progressed => (new CleanupResult(OutcomeKind.Completed, MaintenanceReason.None, detail), false),
                SliceStatus.Completed => (new CleanupResult(OutcomeKind.Completed, MaintenanceReason.None, detail), true),
                // A record this version cannot interpret does not clear by itself
                SliceStatus.Retry when slice.Gate is { Listing: ListingCheck.ControlFenced } => (new CleanupResult(OutcomeKind.Declined, MaintenanceReason.CleanupGate, $"The orphan run stops: {detail}"), true),
                SliceStatus.Retry => (new CleanupResult(OutcomeKind.Declined, slice.Reason == MaintenanceReason.None ? MaintenanceReason.Transient : slice.Reason, detail), false),
                SliceStatus.Failed => (new CleanupResult(OutcomeKind.Declined, MaintenanceReason.DeleteFailed, detail), false),
                SliceStatus.Promote => (CleanupResult.Promote, false),
                SliceStatus.Stopped => (new CleanupResult(OutcomeKind.Declined, _stopReason ?? slice.Reason, detail), true),
                SliceStatus.Disabled => (new CleanupResult(OutcomeKind.Declined, MaintenanceReason.DeletesDisabled, detail), true),
                _ => (new CleanupResult(OutcomeKind.Failed, MaintenanceReason.Unexpected, $"A {slice.Status} orphan slice: {detail}"), true)
            };
            if (retire)
            {
                Dispose();
            }
            else if (Cursor > before && _lease is { } lease)
            {
                lease.Update(Demand(_configuration, _evidence, Interlocked.Read(ref _logEntries), Targets.Count - Cursor, Targets.PathBytesLeft, Latency?.Invoke() ?? CapacityEstimator.DefaultLatency,
                    lease.RequestsPerSecond, Targets.Md5BytesLeft));
            }
            return result;
        }

        // The evidence from its anchor, then a listing without any reservation or uninterpreted record
        private async Task<GateResult> DecideAsync(LogEvidence log, CleanupContext context)
        {
            var validated = await _evidence.ValidateAsync(_configuration, _storage, log, context);
            if (!validated.Passed)
            {
                return validated;
            }
            Interlocked.Exchange(ref _logEntries, log.Entries.Count);
            if (!log.Fence.Clear)
            {
                _stopReason = MaintenanceReason.CleanupGate;
                return new GateResult(GateVerdict.AuthorityChanged, $"The orphan run stops: {log.Fence.Detail}") { Head = log.Head };
            }
            return validated;
        }

        // Each settled or withheld target's counts; a directory at a pinned Azure submission halts issuance
        private void OnSettled(DeletionTarget target, DeleteOutcome outcome)
        {
            _counters.Add(target, outcome);
            var kept = outcome.Status == DeleteStatus.Kept;
            if (kept && outcome.Reason is KeepReason.UnknownSize or KeepReason.IdentityChanged or KeepReason.ProbeError or KeepReason.Directory)
            {
                _counters.AddOrphanKept(outcome.Reason);
            }
            if (_configuration.Store.Store.Kind != StoreKind.Azure)
            {
                return;
            }
            // Only outcomes reached after the submission's Stat
            ProbeResult? probe = outcome switch
            {
                { Status: DeleteStatus.Absent } => ProbeResult.Absent,
                { Status: DeleteStatus.Kept, Reason: KeepReason.ProbeError } => ProbeResult.Error,
                { Status: DeleteStatus.Deleted or DeleteStatus.Failed } => ProbeResult.Present,
                { Status: DeleteStatus.Kept, Reason: KeepReason.IdentityChanged or KeepReason.Directory } => ProbeResult.Present,
                { Status: DeleteStatus.Kept, Reason: KeepReason.UnknownSize } when target.Size > 0 => ProbeResult.Present,
                _ => null
            };
            if (probe is { } result)
            {
                _counters.AddCandidateProbe(result);
            }
            if (kept && outcome.Reason == KeepReason.Directory)
            {
                Volatile.Write(ref _halted, 1);
            }
        }

        /// <summary>
        /// The capacity lease key of a table's orphan run, apart from its advances' key.
        /// </summary>
        public static string LeaseKeyOf(string tableKey) => tableKey + "\0" + LeaseId;

        /// <summary>
        /// An advance id no other orphan run's lease holds, for independent runs that share a lease key.
        /// </summary>
        public static string NextLeaseId() => LeaseId + "/" + Interlocked.Increment(ref s_leases).ToString(CultureInfo.InvariantCulture);

        /// <summary>
        /// Deleting the targets, whose paths take the given UTF-8 bytes and whose frozen MD5s the given buffer bytes, priced as an advance with payload paths only, each probed first on pinned Azure.
        /// </summary>
        public static CapacityDemand Demand(CleanupConfiguration configuration, CleanupEvidence evidence, long logEntries, long targets, long pathBytes, TimeSpan latency, long requestsPerSecond, long md5Bytes = 0)
        {
            var store = configuration.Store.Store;
            var sources = evidence.Capability.Sources.ToList();
            var keyExtra = Math.Max(0, DeletionExecutor.KeyPrefixLength(configuration) - (configuration.Store.Table.Full.Length + 1));
            var inputs = new AdvanceDemandInputs(0, targets, 0, 0, 0, evidence.Anchor.Proof.Length, sources.Sum(x => x.Length), sources.Count, logEntries, store.Paging,
                DeletionRun.OutstandingLimit(store), latency, OrphanCandidates: store.Kind == StoreKind.Azure ? targets : 0, UnknownStore: store.Kind == StoreKind.Unknown, KeyExtra: keyExtra,
                PathBytes: pathBytes, Md5Bytes: md5Bytes);
            return CapacityEstimator.Advance(inputs, requestsPerSecond);
        }

        /// <summary>
        /// Deletes the targets file and releases its charge and the lease, then warns of targets left behind, once.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }
            Targets.Dispose();
            _lease?.Dispose();
            if (_executor.Tally.Leaked is > 0 and var leaked)
            {
                Warn?.Invoke(MaintenanceReason.DeleteFailed, $"{leaked} orphan files are left behind, their deletes failed in {DeletionRun.MaxFailedSlices} slices");
            }
        }
    }
}

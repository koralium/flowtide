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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// What preparation established at one checked listing: V, the oldest retained version O and its root, adoption A and the table's capability.
    /// </summary>
    internal sealed record PreparedAdvance(LogEvidence Log, long V, long O, LogTransactionFile? OldestRoot, bool OldestRootMarked, long A, string TableId, TableCapability Capability)
    {
        public CommitAnchor? Anchor { get; init; }

        // The lowest commit listed from O: O, or O + 1 above a root at O without its own commit
        public long FirstJson { get; init; }

        // Established at the listing's head
        public CleanupEvidence? Evidence { get; init; }

        // Recovery's one resumable advance, resumed instead of selecting
        public ControlGroup? Resumable { get; init; }
    }

    /// <summary>
    /// The exclusions a resumed advance starts with among its paths from the cursor on; without a set it retries, promotes or suspends as the result's action says.
    /// </summary>
    internal delegate Task<ExclusionResult> ResumeExclusions(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, CleanupEvidence evidence, StartedRecord started,
        RecordCursor from, CleanupContext context);

    /// <summary>
    /// The sanity checks before an advance selects its boundary: listing, capability, adoption, recovery of earlier advances, published retention and the oldest retained root.
    /// </summary>
    internal static class AdvancePreparation
    {
        private static readonly IReadOnlySet<string> s_noAdvances = new HashSet<string>();

        /// <summary>
        /// Prepares at a listing taken after V, or finds the one advance to resume; anything that fails ends the job with its result.
        /// </summary>
        public static async Task<PreparedAdvance> PrepareAsync(CleanupConfiguration configuration, IFileStorage storage, string retention, CleanupContext context, RetentionCounters? counters = null,
            Action<RecoveryResult>? onRecovered = null)
        {
            // V, captured at admission before the listing
            var published = context.Head;
            if (published < 0)
            {
                throw AdvanceJob.NoWork(MaintenanceReason.None, "Nothing is published yet");
            }
            var log = await ListAsync(configuration, storage, published, context);
            // Without listed records a closed ledger declines before establishment
            if (log.Listing.Controls.Count == 0 && context.Capacity is { } capacity)
            {
                capacity.Retain(context.CapacityOwner, TableLeases.KeyOf(RawStorage.Unwrap(storage), configuration.Store.Table), s_noAdvances);
                if (!capacity.Admits())
                {
                    throw AdvanceJob.Backlog(capacity);
                }
            }
            var head = log.Head;
            var established = await CleanupEvidence.EstablishAsync(configuration, storage, log, head, null, durable: true, context);
            if (!established.Passed)
            {
                throw Ended(established, context);
            }
            var evidence = established.Evidence!;
            // Released advances' records are gone from the listing the rest of the job sees
            var recovered = await AdvanceRecovery.RecoverAsync(configuration, storage, log, evidence, context, counters);
            onRecovered?.Invoke(recovered);
            log = recovered.Log;
            if (Resumable(log, recovered.Unresolved) is { } group)
            {
                var started = group.Started!;
                return new PreparedAdvance(log, published, started.O, null, false, started.A, evidence.TableId, evidence.Capability)
                {
                    Anchor = evidence.Anchor,
                    FirstJson = started.FirstJson,
                    Evidence = evidence,
                    Resumable = group
                };
            }
            // No unresolved advance's started, logs-complete or progress record
            if (!log.Fence.Clear)
            {
                throw recovered.Unresolved.Any(x => x.State == GroupState.Suspended)
                    ? AdvanceJob.Declined(MaintenanceReason.ManifestSuspended, $"A started advance has no evidence that releases it: {log.Fence.Detail}")
                    : AdvanceJob.Declined(MaintenanceReason.CleanupGate, $"Another advance's authority is listed: {log.Fence.Detail}");
            }
            if (!RetentionProperties.Matches(evidence.Capability.Configuration, retention))
            {
                throw AdvanceJob.Declined(MaintenanceReason.RetentionNotPublished, $"The table does not publish {RetentionProperties.LogKey} and {RetentionProperties.DeletedFileKey} as '{retention}'");
            }
            var (oldest, root, marked) = await OldestAsync(configuration.Store.Table, storage, log, context);
            // The lowest commit listed from O: O, or O + 1 above a root at O without its own commit
            var from = root != null && !log.Listing.Commits.ContainsKey(oldest) ? oldest + 1 : oldest;
            if (log.CheckRange(from, head) != ListingCheck.Passed)
            {
                throw AdvanceJob.Declined(MaintenanceReason.MissingHistory, $"Commits from {from} to {head} are not all listed");
            }
            return new PreparedAdvance(log, published, oldest, root, marked, evidence.Anchor.AdoptedAt, evidence.TableId, evidence.Capability)
            {
                Anchor = evidence.Anchor,
                FirstJson = from,
                Evidence = evidence
            };
        }

        // The only unresolved advance when it is resumable and every listed reservation is its own
        private static ControlGroup? Resumable(LogEvidence log, IReadOnlyList<ControlGroup> unresolved)
        {
            if (unresolved.Count != 1 || unresolved[0] is not { State: GroupState.Resumable } group || log.Fence.ReservedFloor != group.C || log.Fence.Fenced)
            {
                return null;
            }
            foreach (var entry in log.Listing.Controls)
            {
                if (ControlName.TryParse(entry.Name, out var name) && name.Kind is ControlKind.Started or ControlKind.LogsComplete or ControlKind.Progress
                    && (name.Boundary != group.C || !string.Equals(name.AdvanceId, group.AdvanceId, StringComparison.Ordinal)))
                {
                    return null;
                }
            }
            return group;
        }

        /// <summary>
        /// Resumes the resumable advance from its records; a failed check ends the job with its lease paused for a policy and suspended otherwise.
        /// </summary>
        public static async Task<ActiveAdvance> ResumeAsync(CleanupConfiguration configuration, IFileStorage storage, string formatted, TimeSpan retention, DateTimeOffset now, TimeSpan latency,
            PreparedAdvance prepared, ResumeExclusions exclusions, CleanupContext context)
        {
            var log = prepared.Log;
            var evidence = prepared.Evidence!;
            var group = prepared.Resumable!;
            var started = group.Started!;
            var c = group.C;
            var table = configuration.Store.Table;
            var classes = LogClassifier.Classify(log);
            var key = TableLeases.KeyOf(RawStorage.Unwrap(storage), table);
            var remaining = AdvanceRecovery.Remaining(configuration, log, classes, evidence, group, latency);
            // Priced at the ledger's own request rate
            CapacityLease? Restore(CapacityState state) => context.Capacity is { } ledger
                ? ledger.Restore(context.CapacityOwner, key, group.AdvanceId, CapacityEstimator.Advance(remaining, ledger.RequestsPerSecond), state)
                : null;
            AdvanceEndedException Hold(CapacityState state, AdvanceEndedException ended)
            {
                Restore(state);
                return ended;
            }
            AdvanceEndedException Suspend(MaintenanceReason reason, string? detail) =>
                Hold(CapacityState.Suspended, AdvanceJob.Declined(reason, $"The advance to checkpoint {c} is suspended with its authority kept: {detail}"));

            if (log.CheckRange(c, log.Head) != ListingCheck.Passed)
            {
                throw Suspend(MaintenanceReason.MissingHistory, $"Commits from {c} to {log.Head} are not all listed");
            }
            if (classes.V2OrSidecar)
            {
                throw Suspend(MaintenanceReason.SidecarRoot, "The log lists a V2 checkpoint or a sidecar folder");
            }
            if (classes.LowestUnknownVersioned < c)
            {
                throw Suspend(MaintenanceReason.UnclassifiedLogFile, $"The log lists an unknown name at version {classes.LowestUnknownVersioned}");
            }
            // The log phase would delete a source below C
            if (evidence.Capability.Sources.FirstOrDefault(x => x.Version < c) is { } source)
            {
                throw Suspend(MaintenanceReason.CleanupGate, $"The table's protocol or metadata is read from version {source.Version}");
            }
            if (!RetentionProperties.Matches(evidence.Capability.Configuration, formatted))
            {
                throw Hold(CapacityState.Paused, AdvanceJob.Declined(MaintenanceReason.RetentionNotPublished,
                    $"The table does not publish {RetentionProperties.LogKey} and {RetentionProperties.DeletedFileKey} as '{formatted}'; the advance to checkpoint {c} waits with its authority kept"));
            }
            long timestamp;
            try
            {
                timestamp = await BoundarySelector.ReadTimestampAsync(storage, log, c, started.A, now.ToUnixTimeMilliseconds(), context);
            }
            catch (AdvanceEndedException e) when (e.Result.Kind == OutcomeKind.Declined && e.Result.Reason != MaintenanceReason.Transient)
            {
                throw Suspend(e.Result.Reason, e.Result.Detail);
            }
            // A raised retention waits until C is old enough again
            if (!BoundarySelector.Eligible(timestamp, BoundarySelector.Midnight(now, retention)))
            {
                throw Hold(CapacityState.Paused, AdvanceJob.NoWork(MaintenanceReason.NoEligibleCheckpoint, $"Checkpoint {c} is after the retention's cutoff; the advance waits with its authority kept"));
            }
            var read = await ReadDestinationAsync(table, storage, log, group, context);
            if (read == null)
            {
                throw Suspend(MaintenanceReason.HistoryChanged, $"Checkpoint {c} is not the content its header names");
            }
            var active = ActiveAdvance.From(group, evidence);
            var excluded = await exclusions(configuration, storage, log, evidence, started, active.Cursor, context);
            if (excluded.Set == null)
            {
                // A retry or promotion keeps recovery's runnable lease
                throw excluded.Action switch
                {
                    GateAction.Retry => AdvanceJob.Declined(excluded.Reason == MaintenanceReason.None ? MaintenanceReason.Transient : excluded.Reason,
                        $"The advance to checkpoint {c} retries its resume: {excluded.Detail}"),
                    GateAction.Promote => AdvanceJob.Promote(context, $"The advance to checkpoint {c} resumes under long admission: {excluded.Detail}"),
                    _ => Suspend(MaintenanceReason.ManifestSuspended, excluded.Detail)
                };
            }
            await AdvanceJob.RaiseHintAsync(storage, table, log, c, read, context);
            active.LogEntries = log.Entries.Count;
            active.LogTargetsLeft = active.Phase == AdvancePhase.Log ? LogTargets.Count(classes, c) : 0;
            active.Exclusions = excluded.Set;
            active.Lease = Restore(CapacityState.Runnable);
            return active;
        }

        // C listed at the size and modification time its header names, read whole to that content; null when it is not
        private static async Task<DestinationRead?> ReadDestinationAsync(IOPath table, IFileStorage storage, LogEvidence log, ControlGroup group, CleanupContext context)
        {
            var destination = group.Header!.Destination;
            var file = ContentProof.Find(log.Listing, group.C, true);
            if (file == null || (file.IOEntry.Size ?? -1) != destination.ListedSize || (file.IOEntry.LastModificationTime?.UtcTicks ?? -1) != destination.ListedModifiedTicks)
            {
                return null;
            }
            var read = await DestinationCheckpoint.ReadAsync(table, storage, file, context.Scratch, context.Memory, context.Transfer, context, null, context.Stop);
            switch (read.Status)
            {
                case DestinationStatus.Preempted:
                    AdvanceJob.StopIfIssuanceStopped(context);
                    throw AdvanceJob.Declined(MaintenanceReason.Transient, read.Detail ?? $"Reading checkpoint {group.C} stopped");
                case DestinationStatus.ReadsSpent:
                    throw AdvanceJob.Promote(context, read.Detail ?? $"Reading checkpoint {group.C} needs a read this admission has spent");
                case DestinationStatus.Read when read.Proof!.SameContent(destination.Length, destination.Sha256):
                    return read;
                default:
                    return null;
            }
        }

        // A checked log listing, or the decline its failure ends the job with
        internal static async Task<LogEvidence> ListAsync(CleanupConfiguration configuration, IFileStorage storage, long published, CleanupContext context)
        {
            ListingCheck check;
            LogEvidence? log;
            try
            {
                (check, log) = await ListingEvidence.ReadLogAsync(configuration, storage, published, context.Stop, context.Memory);
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e) when (e is not CheckpointDeclinedException)
            {
                throw AdvanceJob.Declined(MaintenanceReason.Transient, $"The advance's log listing failed: {e.GetType().Name}: {e.Message}");
            }
            if (check != ListingCheck.Passed)
            {
                throw AdvanceJob.Declined(MaintenanceReason.ListingIneligible, $"The advance's log listing failed its {check} check");
            }
            return log!;
        }

        // O from the contiguous suffix to the head: zero with every commit from zero, otherwise the oldest root that probes usable
        private static async Task<(long O, LogTransactionFile? Root, bool Marked)> OldestAsync(IOPath table, IFileStorage storage, LogEvidence log, CleanupContext context)
        {
            var head = log.Head;
            var segment = new DeltaLogSegment(storage, table, log.Listing);
            if (segment.HasHistoryFromZero(head))
            {
                return (0, null, false);
            }
            var transport = new StowageCheckpointTransport(storage, table);
            // Oldest candidate first
            var candidates = segment.RootCandidates(head, head).ToList();
            for (int i = candidates.Count - 1; i >= 0; i--)
            {
                var probed = await AdvanceJob.ProbeAsync(transport, candidates[i], context);
                if (probed.Class == ProbeClass.Usable)
                {
                    return (candidates[i].Version, candidates[i], probed.Marker != null);
                }
            }
            throw AdvanceJob.Declined(MaintenanceReason.MissingHistory, $"Commits before {segment.SuffixStart(head)} are missing and no listed checkpoint usably roots the head");
        }

        // A gate that did not pass: a stop, a promotion before the long admission, or a decline
        internal static AdvanceEndedException Ended(GateResult result, CleanupContext context)
        {
            var detail = $"{result.Verdict}: {result.Detail}";
            return result.Verdict switch
            {
                GateVerdict.Preempted => new AdvanceEndedException(new CleanupResult(OutcomeKind.Preempted, result.Reason, detail)),
                GateVerdict.NeedsLongAdmission or GateVerdict.ReadsSpent => AdvanceJob.Promote(context, detail),
                _ when result.Action == GateAction.Retry => AdvanceJob.Declined(result.Reason == MaintenanceReason.None ? MaintenanceReason.Transient : result.Reason, detail),
                GateVerdict.NotOwn or GateVerdict.StageMismatch => new AdvanceEndedException(new CleanupResult(OutcomeKind.Declined, result.Reason, detail) { ForeignHead = true }),
                _ => AdvanceJob.Declined(result.Reason == MaintenanceReason.None ? MaintenanceReason.CleanupGate : result.Reason, detail)
            };
        }
    }
}

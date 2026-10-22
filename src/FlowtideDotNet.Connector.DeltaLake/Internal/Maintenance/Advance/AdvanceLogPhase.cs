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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// A started advance's log phase: one gated slice of ordered log deletions per turn, then the verification that publishes logs-complete.
    /// </summary>
    internal static class AdvanceLogPhase
    {
        /// <summary>
        /// Deletes in order, one at a time, what the slice's own gate listing leaves below C; a gate that leaves nothing publishes logs-complete instead.
        /// </summary>
        public static async Task<SliceResult> RunSliceAsync(ActiveAdvance active, AdvanceTurn turn, CleanupContext context)
        {
            await turn.Hooks.StepAsync(AdvanceSteps.LogSlice);
            var executor = turn.NewExecutor();
            var remainder = new Remainder();
            var gate = ChunkGate.ForAdvance(turn.Configuration, turn.Storage, active, (log, gated) => DecideAsync(log, gated, active, turn, executor, remainder));
            var slice = await new DeletionRun(executor, gate, remainder, turn.Clock, turn.Settings).RunSliceAsync(context);
            if (remainder.Targets is not { } targets)
            {
                return slice;
            }
            // What the slice's settled deletes leave
            active.LogTargetsLeft = targets.Count - slice.Cursor;
            active.LogEntries = remainder.Entries - slice.Cursor;
            if (slice.Status != SliceStatus.Completed)
            {
                return slice;
            }
            if (targets.Count > 0)
            {
                // Verified only by a later gate's listing
                return slice with { Status = SliceStatus.Progressed };
            }
            return await CompleteAsync(active, turn, context, slice);
        }

        // The gate's last check: a classifiable log, a usable hint and the names below C the listing's fence leaves to this advance
        private static async Task<GateResult> DecideAsync(LogEvidence log, CleanupContext context, ActiveAdvance active, AdvanceTurn turn, DeletionExecutor executor, Remainder remainder)
        {
            var c = active.C;
            var classes = LogClassifier.Classify(log);
            if (classes.V2OrSidecar)
            {
                return new GateResult(GateVerdict.Unclassified, "The log lists a V2 checkpoint or a sidecar folder, which the advance does not handle") { Head = log.Head };
            }
            if (classes.LowestUnknownVersioned < c)
            {
                return new GateResult(GateVerdict.Unclassified, $"The log lists an unknown name at version {classes.LowestUnknownVersioned}, below checkpoint {c}") { Head = log.Head };
            }
            var hint = await new StowageCheckpointTransport(turn.Storage, turn.Configuration.Store.Table).ReadHintAsync(context.Stop);
            if (hint is not { Parts: null or 1 } || hint.Version < c || !log.Listing.Checkpoints.Any(x => x.Version >= hint.Version && x.IOEntry.Size is not 0))
            {
                return new GateResult(GateVerdict.HintUnusable, $"The hint is not a single-part hint at or above checkpoint {c} backed by a listed checkpoint: {(hint == null ? "none" : $"version {hint.Version}")}")
                {
                    Head = log.Head
                };
            }
            var names = LogTargets.Order(classes, c);
            var targets = new List<DeletionTarget>(names.Count);
            foreach (var name in names)
            {
                // A compaction is no version's own file
                if (!log.Fence.AllowsOwnerDelete(name.Kind == LogNameKind.Compaction ? -1 : name.Version, c))
                {
                    return new GateResult(GateVerdict.AuthorityChanged, $"The advance to checkpoint {c} may not delete {name.Entry.Name}: {log.Fence.Detail}") { Head = log.Head };
                }
                targets.Add(executor.TargetOf(name.Entry.Path.Full, TargetKind.Log));
            }
            remainder.Set(targets, log.Entries.Count);
            return new GateResult(GateVerdict.Passed) { Head = log.Head };
        }

        // Logs-complete read back, then the payload phase
        private static async Task<SliceResult> CompleteAsync(ActiveAdvance active, AdvanceTurn turn, CleanupContext context, SliceResult verified)
        {
            await turn.Hooks.StepAsync(AdvanceSteps.LogVerified);
            var started = active.Started;
            var publisher = new ControlPublisher(turn.Storage, turn.Configuration.Store.Table, context, beforePublish: turn.Hooks.BeforePublish);
            try
            {
                await publisher.PublishAsync(ControlKind.LogsComplete, active.Association, -1, () => ControlRecords.Encode(new LogsCompleteRecord(active.Association, started.TableId, started.O)));
            }
            catch (CleanupPreemptedException e)
            {
                return verified with { Status = SliceStatus.Retry, Reason = e.Stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.None, Stop = e.Stop, Detail = e.Message };
            }
            catch (CheckpointDeclinedException e)
            {
                return verified with { Status = SliceStatus.Retry, Reason = e.Reason, Detail = e.Message };
            }
            catch (Exception e) when (e is not OperationCanceledException || !context.Stop.IsCancellationRequested)
            {
                return verified with { Status = SliceStatus.Retry, Reason = MaintenanceReason.Transient, Detail = $"{e.GetType().Name}: {e.Message}" };
            }
            active.Phase = AdvancePhase.Payload;
            await turn.Hooks.StepAsync(AdvanceSteps.LogsComplete);
            return verified;
        }

        // The targets one gate's listing leaves, read by the slice that gate passed
        private sealed class Remainder : IDeletionSource
        {
            public IReadOnlyList<DeletionTarget>? Targets { get; private set; }

            // Entries of that listing
            public int Entries { get; private set; }

            public void Set(IReadOnlyList<DeletionTarget> targets, int entries)
            {
                Targets = targets;
                Entries = entries;
            }

            public IAsyncEnumerable<DeletionTarget> ReadFrom(long position, CancellationToken cancellationToken) =>
                new ListDeletionSource(Targets ?? Array.Empty<DeletionTarget>()).ReadFrom(position, cancellationToken);
        }
    }
}

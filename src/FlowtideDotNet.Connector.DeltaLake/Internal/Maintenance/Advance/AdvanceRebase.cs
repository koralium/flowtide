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

using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// A payload phase's foreign change, handled in an advance job: the changed history revalidated and the exclusions recomputed before its delete turns go on.
    /// </summary>
    internal static class AdvanceRebase
    {
        /// <summary>
        /// Rebases the advance onto the listed head, or leaves it rebasing, paused or suspended; throws only an OperationCanceledException from the context's stop.
        /// </summary>
        public static async Task<CleanupResult> RunAsync(ActiveAdvance active, AdvanceTurn turn, CleanupContext context)
        {
            // The kept exclusions are charged for the rebase
            var held = active.Exclusions.Bytes;
            try
            {
                context.Memory.Reserve(held);
            }
            catch (CheckpointDeclinedException e)
            {
                return Retry(active, turn, e.Reason, IssuanceStop.None, e.Message);
            }
            try
            {
                return await RunCoreAsync(active, turn, context);
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e)
            {
                return Retry(active, turn, MaintenanceReason.Transient, IssuanceStop.None, $"{e.GetType().Name}: {e.Message}");
            }
            finally
            {
                context.Memory.Release(held);
            }
        }

        private static async Task<CleanupResult> RunCoreAsync(ActiveAdvance active, AdvanceTurn turn, CleanupContext context)
        {
            var configuration = turn.Configuration;
            var storage = turn.Storage;
            var (check, listed) = await ListingEvidence.ReadLogAsync(configuration, storage, context.Head, context.Stop, context.Memory);
            if (check != ListingCheck.Passed)
            {
                return Apply(active, turn, context, GateResult.OfListing(check, true));
            }
            var log = listed!;
            var validated = await active.Evidence.ValidateAsync(configuration, storage, log, context, active.C);
            // A gap in the changed history suspends, one in the advance's own range retries
            if (validated.Verdict == GateVerdict.ListingFailed && log.CheckRange(active.Started.Head + 1, log.Head) != ListingCheck.Passed)
            {
                return Suspend(active, turn, context, MaintenanceReason.HistoryChanged, $"Commits after the planned head {active.Started.Head} up to {log.Head} are not all listed");
            }
            if (validated.Passed)
            {
                return await RunnableAsync(active, turn, context, $"Head {validated.Head} continues the advance's epoch");
            }
            if (validated.Action != GateAction.Rebase)
            {
                return Apply(active, turn, context, validated);
            }
            var current = validated.Current!;
            if (configuration.OptIns.Retention is not { } retention || !RetentionProperties.Matches(current.Configuration, RetentionProperties.Format(retention)))
            {
                AdvanceExecution.MoveLease(active, context, CapacityState.Paused);
                return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.RetentionNotPublished,
                    $"The advance to checkpoint {active.C} waits for the table to publish its retention at head {log.Head}");
            }
            if (await CheckpointAsync(storage, log, active, context) is { } replaced)
            {
                return Apply(active, turn, context, replaced);
            }
            var exclusions = await AdvanceExclusions.ComputeAsync(configuration, storage, log, active, context);
            if (exclusions.Set is not { } set)
            {
                return Apply(active, turn, context, exclusions.Action, exclusions.Reason, IssuanceStop.None, exclusions.Detail);
            }
            // Charged beside the kept set until the rebase ends
            context.Memory.Reserve(set.Bytes);
            try
            {
                if (validated.Verdict == GateVerdict.NewEpoch)
                {
                    // Anchored at the own head, whose epoch later own commits continue
                    var established = await CleanupEvidence.EstablishAsync(configuration, storage, log, log.Head, null, durable: true, context, current);
                    if (!established.Passed)
                    {
                        return Apply(active, turn, context, established);
                    }
                    active.Evidence = established.Evidence!;
                }
                else if (!active.Evidence.AcceptHead(validated.HeadProof!, current))
                {
                    return Suspend(active, turn, context, MaintenanceReason.Unexpected, $"Head {validated.Head} could not be accepted");
                }
                // Rehashed at every later gate though the anchor moved
                active.Evidence.KeepPlannedHead(exclusions.PlannedHead!);
                active.Exclusions = set;
                return await RunnableAsync(active, turn, context, $"Rebased onto head {validated.Head} with {set.Count} planned paths referenced again");
            }
            finally
            {
                context.Memory.Release(set.Bytes);
            }
        }

        // C listed at the size and modification time its header names, then rehashed whole to its content; null when it is
        private static async Task<GateResult?> CheckpointAsync(IFileStorage storage, LogEvidence log, ActiveAdvance active, CleanupContext context)
        {
            var destination = active.Destination;
            var file = ContentProof.Find(log.Listing, active.C, true);
            if (file == null || (file.IOEntry.Size ?? -1) != destination.ListedSize || (file.IOEntry.LastModificationTime?.UtcTicks ?? -1) != destination.ListedModifiedTicks)
            {
                return new GateResult(GateVerdict.CheckpointReplaced, $"Checkpoint {active.C} is not listed as its header names it") { Head = log.Head, Durable = true };
            }
            var read = await ProofReader.ReadAsync(storage, file.IOEntry.Path, active.C, file.IOEntry.Size, false, context);
            return read.Status switch
            {
                ProofStatus.Read when read.Length == destination.Length && read.Sha256.AsSpan().SequenceEqual(destination.Sha256) => null,
                ProofStatus.Preempted => new GateResult(GateVerdict.Preempted, $"Rehashing checkpoint {active.C} stopped") { Stop = read.Stop, Head = log.Head, Durable = true },
                ProofStatus.ReadsSpent => new GateResult(GateVerdict.ReadsSpent, $"Rehashing checkpoint {active.C} needs a read this admission has spent") { Head = log.Head, Durable = true },
                _ => new GateResult(GateVerdict.CheckpointReplaced, $"Checkpoint {active.C} is gone or has other content than its header names") { Head = log.Head, Durable = true }
            };
        }

        // Issuance goes on with the rebased evidence and exclusions
        private static async Task<CleanupResult> RunnableAsync(ActiveAdvance active, AdvanceTurn turn, CleanupContext context, string detail)
        {
            active.State = AdvanceState.Runnable;
            AdvanceExecution.MoveLease(active, context, CapacityState.Runnable);
            await turn.Hooks.StepAsync(AdvanceSteps.Rebase);
            return new CleanupResult(OutcomeKind.Completed, MaintenanceReason.None, detail);
        }

        // A fresh short admission spends its reads the same way
        private static CleanupResult Apply(ActiveAdvance active, AdvanceTurn turn, CleanupContext context, GateResult gate) =>
            Apply(active, turn, context, gate.Verdict == GateVerdict.ReadsSpent ? GateAction.Promote : gate.Action, gate.Reason, gate.Stop, $"{gate.Verdict}: {gate.Detail}");

        // As the delete turns map a slice: a retry keeps rebasing, a promotion asks for long admission, anything else suspends
        private static CleanupResult Apply(ActiveAdvance active, AdvanceTurn turn, CleanupContext context, GateAction action, MaintenanceReason reason, IssuanceStop stop, string? detail) => action switch
        {
            GateAction.Retry => Retry(active, turn, reason, stop, detail),
            GateAction.Promote => CleanupResult.Promote,
            _ => Suspend(active, turn, context, reason, detail)
        };

        private static CleanupResult Retry(ActiveAdvance active, AdvanceTurn turn, MaintenanceReason reason, IssuanceStop stop, string? detail)
        {
            var retry = reason == MaintenanceReason.None ? MaintenanceReason.Transient : reason;
            if (stop == IssuanceStop.None)
            {
                turn.Warn(retry, $"The advance to checkpoint {active.C} keeps its authority and retries its rebase: {detail}");
            }
            return new CleanupResult(OutcomeKind.Declined, retry, detail);
        }

        // Issuance stays stopped with the authority and records kept until a later resume
        private static CleanupResult Suspend(ActiveAdvance active, AdvanceTurn turn, CleanupContext context, MaintenanceReason reason, string? detail)
        {
            if (reason == MaintenanceReason.None)
            {
                reason = MaintenanceReason.ManifestSuspended;
            }
            active.State = AdvanceState.Suspended;
            AdvanceExecution.MoveLease(active, context, CapacityState.Suspended);
            var message = $"The advance to checkpoint {active.C} is suspended with its authority kept: {detail}";
            turn.Warn(reason, message);
            return new CleanupResult(OutcomeKind.Declined, reason, message);
        }
    }
}

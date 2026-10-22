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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// What one delete turn of a started advance runs with; the settings are the cleanup's own.
    /// </summary>
    internal sealed record AdvanceTurn(CleanupConfiguration Configuration, IFileStorage Storage, TimeProvider Clock, DeletionRunSettings Settings, AdvanceHooks Hooks,
        Action<MaintenanceReason, string> Warn)
    {
        // The per-delete latency demand is priced at
        public Func<TimeSpan> Latency { get; init; } = static () => CapacityEstimator.DefaultLatency;

        // Takes each slice's measured per-delete latency and the deletes it counts for
        public Action<TimeSpan, int>? ObserveLatency { get; init; }

        // The sink's counts of settled and withheld deletes and slice timings
        public RetentionCounters? Counters { get; init; }

        /// <summary>
        /// An executor over the turn's storage with the advance's delete hooks, its settled deletes and its keeps before submission counted first.
        /// </summary>
        public DeletionExecutor NewExecutor() => new DeletionExecutor(Configuration, Storage) { BeforeDeleteForTests = Hooks.BeforeDelete, Settled = Counted + Hooks.Settled, Withheld = Counted };

        private Action<DeletionTarget, DeleteOutcome>? Counted => Counters is { } counters ? counters.Add : null;
    }

    /// <summary>
    /// One slice of a started advance's phase under a delete admission; never throws for runtime failures.
    /// </summary>
    internal delegate Task<SliceResult> AdvancePhaseSlice(ActiveAdvance active, AdvanceTurn turn, CleanupContext context);

    /// <summary>
    /// Runs one slice of a started advance per delete turn and applies its outcome to the advance's state and lease.
    /// </summary>
    internal static class AdvanceExecution
    {
        // Consecutive durable failures at one position before the advance suspends
        public const int MaxDurableFailures = 3;

        /// <summary>
        /// The phase's slice, then its status mapped to the advance and the turn's result; throws only an OperationCanceledException from the context's stop.
        /// </summary>
        public static async Task<CleanupResult> RunTurnAsync(ActiveAdvance active, AdvanceTurn turn, AdvancePhaseSlice log, AdvancePhaseSlice payload, CleanupContext context)
        {
            if (active.Phase == AdvancePhase.Done || active.State is AdvanceState.Rebasing or AdvanceState.Suspended)
            {
                return new CleanupResult(OutcomeKind.NoWork);
            }
            var phase = active.Phase;
            // The kept exclusions are charged for the slice
            var held = active.Exclusions.Bytes;
            try
            {
                context.Memory.Reserve(held);
            }
            catch (CheckpointDeclinedException e)
            {
                return Retry(active, turn, e.Reason, IssuanceStop.None, e.Message);
            }
            SliceResult slice;
            try
            {
                slice = await (phase == AdvancePhase.Log ? log : payload)(active, turn, context);
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e)
            {
                return new CleanupResult(OutcomeKind.Failed, MaintenanceReason.Unexpected, $"{e.GetType().Name}: {e.Message}");
            }
            finally
            {
                context.Memory.Release(held);
            }
            // A slice disabled before its gate leaves the latest timings
            if (slice.Status != SliceStatus.Disabled || slice.Gate != null)
            {
                turn.Counters?.AddSlice(slice.GateElapsed, slice.Elapsed);
            }
            // The gate's own listing: an absent successor once it passed, or the check it failed
            if (slice.Gate is { } gate && (gate.Passed || gate.Listing != ListingCheck.Passed))
            {
                turn.Counters?.AddListing(gate.Listing);
            }
            var detail = slice.Detail ?? slice.Gate?.Detail;
            switch (slice.Status)
            {
                case SliceStatus.Completed:
                case SliceStatus.Progressed:
                    if (Position(active) != active.FailedAt)
                    {
                        active.Failures = 0;
                        active.FailedAt = -1;
                    }
                    if (slice.Issued > 0)
                    {
                        turn.ObserveLatency?.Invoke(PerDelete(slice, phase == AdvancePhase.Log ? DeletionRun.LogOutstanding : Outstanding(turn)), Measured(slice, turn.Settings));
                    }
                    // The done released its lease
                    if (active.Phase == AdvancePhase.Done)
                    {
                        return new CleanupResult(OutcomeKind.Completed, MaintenanceReason.None, detail);
                    }
                    if (active.State == AdvanceState.Paused)
                    {
                        active.State = AdvanceState.Runnable;
                        MoveLease(active, context, CapacityState.Runnable);
                    }
                    if (active.Lease is { } lease)
                    {
                        lease.Update(CapacityEstimator.Advance(AdvanceDemand.Of(turn.Configuration, active, turn.Latency()), lease.RequestsPerSecond));
                    }
                    return new CleanupResult(OutcomeKind.Completed, MaintenanceReason.None, detail);
                case SliceStatus.Retry:
                    return Retry(active, turn, slice.Reason, slice.Stop, detail);
                case SliceStatus.Failed:
                    var position = Position(active);
                    active.Failures = active.FailedAt == position ? active.Failures + 1 : 1;
                    active.FailedAt = position;
                    if (active.Failures >= MaxDurableFailures)
                    {
                        return Suspend(active, turn, context, MaintenanceReason.DeleteFailed, $"{active.Failures} consecutive slices failed at one target: {detail}");
                    }
                    return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.DeleteFailed, detail);
                case SliceStatus.Suspended:
                case SliceStatus.Rebase when phase == AdvancePhase.Log:
                    return Suspend(active, turn, context, slice.Reason, detail);
                case SliceStatus.Rebase:
                    // The lease stays as it is while an advance job rebases
                    active.State = AdvanceState.Rebasing;
                    return new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.None, detail);
                case SliceStatus.Disabled:
                    active.State = AdvanceState.Paused;
                    MoveLease(active, context, CapacityState.Paused);
                    return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.DeletesDisabled, detail);
                case SliceStatus.Promote:
                    return CleanupResult.Promote;
                default:
                    return new CleanupResult(OutcomeKind.Failed, MaintenanceReason.Unexpected, $"A {slice.Status} slice: {detail}");
            }
        }

        /// <summary>
        /// Moves the advance's demand to a state as an upsert of its lease, and keeps the lease that replaced it.
        /// </summary>
        public static void MoveLease(ActiveAdvance active, CleanupContext context, CapacityState state)
        {
            if (active.Lease is { } lease && context.Capacity is { } capacity)
            {
                active.Lease = capacity.Restore(context.CapacityOwner, lease.LeaseKey, lease.AdvanceId, lease.Demand, state);
            }
        }

        /// <summary>
        /// A slice's time per delete with at most the given number outstanding; zero when it issued none.
        /// </summary>
        public static TimeSpan PerDelete(SliceResult slice, int outstanding)
        {
            if (slice.Issued <= 0)
            {
                return TimeSpan.Zero;
            }
            return TimeSpan.FromTicks(slice.Elapsed.Ticks * Math.Min(Math.Max(1, outstanding), slice.Issued) / slice.Issued);
        }

        // The deletes a slice counts for, by the share of its path limit or of its issuance time it spent issuing
        private static int Measured(SliceResult slice, DeletionRunSettings settings)
        {
            return (int)Math.Min(Math.Max(slice.Issued, settings.MaxPaths * slice.IssuanceWindow.Ticks / settings.IssuanceTime.Ticks), settings.MaxPaths);
        }

        private static CleanupResult Retry(ActiveAdvance active, AdvanceTurn turn, MaintenanceReason reason, IssuanceStop stop, string? detail)
        {
            var retry = reason == MaintenanceReason.None ? MaintenanceReason.Transient : reason;
            if (stop == IssuanceStop.None)
            {
                turn.Warn(retry, $"The advance to checkpoint {active.C} keeps its authority and retries its slice: {detail}");
            }
            return new CleanupResult(OutcomeKind.Declined, retry, detail);
        }

        // Stops issuance with the authority and records kept until a later resume
        private static CleanupResult Suspend(ActiveAdvance active, AdvanceTurn turn, CleanupContext context, MaintenanceReason reason, string? detail)
        {
            if (reason == MaintenanceReason.None)
            {
                reason = MaintenanceReason.ManifestSuspended;
            }
            active.State = AdvanceState.Suspended;
            MoveLease(active, context, CapacityState.Suspended);
            var message = $"The advance to checkpoint {active.C} is suspended with its authority kept: {detail}";
            turn.Warn(reason, message);
            return new CleanupResult(OutcomeKind.Declined, reason, message);
        }

        // Payload positions count processed paths, log positions the targets left below -1
        private static long Position(ActiveAdvance active) => active.Phase == AdvancePhase.Log ? -2 - active.LogTargetsLeft : active.Processed;

        private static int Outstanding(AdvanceTurn turn) => Math.Clamp(turn.Settings.MaxOutstanding, 1, DeletionRun.OutstandingLimit(turn.Configuration.Store.Store));
    }
}

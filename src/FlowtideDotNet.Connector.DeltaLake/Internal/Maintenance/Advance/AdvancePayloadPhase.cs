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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// A started advance's payload phase: one gated slice of a validated part per turn with its progress, then done and the teardown.
    /// </summary>
    internal static class AdvancePayloadPhase
    {
        public static async Task<SliceResult> RunSliceAsync(ActiveAdvance active, AdvanceTurn turn, CleanupContext context)
        {
            await turn.Hooks.StepAsync(AdvanceSteps.PayloadSlice);
            var configuration = turn.Configuration;
            var executor = turn.NewExecutor();
            var publisher = new ControlPublisher(turn.Storage, configuration.Store.Table, context, beforePublish: turn.Hooks.BeforePublish);
            var processed = active.Processed;
            SliceResult slice;
            using (var source = new PartSource(active, executor, turn.Storage))
            {
                var settings = turn.Settings with { SettleProgress = (position, admitted) => SettleProgressAsync(active, source, executor, publisher, position, admitted) };
                var run = new DeletionRun(executor, ChunkGate.ForAdvance(configuration, turn.Storage, active, source.ValidateAsync), source, turn.Clock, settings, processed);
                slice = await run.RunSliceAsync(context);
            }
            if (active.Processed != processed)
            {
                await turn.Hooks.StepAsync(AdvanceSteps.Progress);
            }
            var totals = active.Started.Totals;
            if (slice.Status is not (SliceStatus.Completed or SliceStatus.Progressed) || active.Cursor != new RecordCursor(totals.Parts, 0, 0) || active.Processed != totals.Emitted)
            {
                return slice;
            }
            return await DoneAsync(active, turn, publisher, slice, context);
        }

        // The canonical cursor under the next sequence, read back; then its predecessor removed and the summary moved
        private static async Task<bool> SettleProgressAsync(ActiveAdvance active, PartSource source, DeletionExecutor executor, ControlPublisher publisher, long position, CleanupContext context)
        {
            var next = source.CursorAt(position);
            var association = active.Association;
            // Consumed even by a publication that fails
            var sequence = ++active.Sequence;
            var published = await publisher.PublishAsync(ControlKind.Progress, association, sequence, () => ControlRecords.Encode(new ProgressRecord(association, sequence, next, position)));
            if (active.ProgressName is { } previous)
            {
                // A predecessor left behind goes with the teardown
                var target = executor.TargetOf(executor.Configuration.Store.Table.Combine(DeltaTransactionReader.DeltaLogDirName).Combine(previous).Full, TargetKind.Control);
                Task<DeleteOutcome>? delete = null;
                if (context.TryIssueSettled(stop => delete = executor.DeleteAsync(target, false, stop)) != null)
                {
                    await delete!;
                }
            }
            var part = active.Part!;
            if (next.Part != part.Ordinal)
            {
                active.PartStart += part.Paths;
                active.PartBytesLeft -= part.Bytes;
                active.Part = null;
            }
            active.Cursor = next;
            active.Processed = position;
            active.ProgressName = published.Name;
            return true;
        }

        // Done from the started record after a passed gate at the terminal position, then the teardown that keeps it
        private static async Task<SliceResult> DoneAsync(ActiveAdvance active, AdvanceTurn turn, ControlPublisher publisher, SliceResult slice, CleanupContext context)
        {
            await turn.Hooks.StepAsync(AdvanceSteps.Done);
            var started = active.Started;
            var done = new DoneRecord(active.Association, started.TableId, started.O, started.A, started.Head, started.Anchor, new RecordCursor(started.Totals.Parts, 0, 0), started.Totals,
                started.Totals.Emitted);
            try
            {
                await publisher.PublishAsync(ControlKind.Done, active.Association, -1, () => ControlRecords.Encode(done));
            }
            catch (CheckpointDeclinedException e)
            {
                return slice with { Status = SliceStatus.Retry, Reason = e.Reason, Detail = e.Message };
            }
            catch (CleanupPreemptedException e)
            {
                return slice with { Status = SliceStatus.Retry, Reason = e.Stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.None, Stop = e.Stop, Detail = e.Message };
            }
            catch (Exception e) when (e is not OperationCanceledException || !context.Stop.IsCancellationRequested)
            {
                return slice with { Status = SliceStatus.Retry, Reason = MaintenanceReason.Transient, Detail = $"{e.GetType().Name}: {e.Message}" };
            }
            active.Phase = AdvancePhase.Done;
            // The durable done ends the advance's demand
            active.Lease?.Dispose();
            try
            {
                await turn.Hooks.StepAsync(AdvanceSteps.Teardown);
                var log = await AdvancePreparation.ListAsync(turn.Configuration, turn.Storage, context.Head, context);
                var recovered = await AdvanceRecovery.RecoverAsync(turn.Configuration, turn.Storage, log, active.Evidence, context, turn.Counters);
                return slice with { Detail = $"The advance to checkpoint {active.C} is done; its teardown removed {recovered.Deleted} records" };
            }
            catch (Exception e) when (e is not OperationCanceledException || !context.Stop.IsCancellationRequested)
            {
                // A later recovery's release by the done finishes it
                return slice with { Detail = $"The advance to checkpoint {active.C} is done; its teardown waits for a later recovery: {e.GetType().Name}: {e.Message}" };
            }
        }
    }
}

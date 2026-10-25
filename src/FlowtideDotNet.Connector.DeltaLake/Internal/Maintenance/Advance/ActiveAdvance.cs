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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// The work a started advance does next.
    /// </summary>
    internal enum AdvancePhase : byte
    {
        Log,
        Payload,
        Done
    }

    /// <summary>
    /// Whether a started advance's delete turns issue.
    /// </summary>
    internal enum AdvanceState : byte
    {
        Runnable,
        // Deletes are switched off; the next slice that passes makes it runnable
        Paused,
        // A foreign change waits for an advance job's rebase
        Rebasing,
        // Issuance stopped with the authority kept for a later resume
        Suspended
    }

    /// <summary>
    /// A started advance's bounded summary between its admissions; never durable, rebuilt from records after a restart.
    /// </summary>
    internal sealed class ActiveAdvance
    {
        public required Association Association { get; init; }

        // O, A, the planned head, the first JSON, the anchor and the totals
        public required StartedRecord Started { get; init; }

        // C as its header names it
        public required CheckpointSummary Destination { get; init; }

        public required long CutoffTicks { get; init; }

        public long C => Association.C;

        // Changed under the table lease, only by the code that validated the change
        public required CleanupEvidence Evidence { get; set; }

        public CapacityLease? Lease { get; set; }

        public AdvancePhase Phase { get; set; }

        public AdvanceState State { get; set; }

        // The next position in canonical form
        public RecordCursor Cursor { get; set; }

        public long Processed { get; set; }

        // The highest progress sequence published or recovered
        public long Sequence { get; set; }

        // The newest validated progress, deleted only after its successor validates
        public string? ProgressName { get; set; }

        // The cursor's part as the header read when the cursor entered it describes it
        public PartDescriptor? Part { get; set; }

        // Paths in the parts before the cursor's
        public long PartStart { get; set; }

        // Whole-object bytes of the parts from the cursor's on
        public long PartBytesLeft { get; set; }

        // From the latest log gate's classification
        public long LogTargetsLeft { get; set; }

        // Entries of the latest passed gate's listing
        public long LogEntries { get; set; }

        // Consecutive durable failures at FailedAt
        public int Failures { get; set; }

        public long FailedAt { get; set; } = -1;

        public ExclusionSet Exclusions { get; set; } = ExclusionSet.Empty;

        /// <summary>
        /// The first part's first record, or the terminal position of an advance without parts.
        /// </summary>
        public static RecordCursor Start(RecordTotals totals) => totals.Parts == 0 ? new RecordCursor(0, 0, 0) : new RecordCursor(0, 0, ControlRecords.FirstPathOffset);

        /// <summary>
        /// The summary of a resumable group's validated records with the evidence established for it; its payload phase once its logs-complete validated.
        /// </summary>
        public static ActiveAdvance From(ControlGroup resumable, CleanupEvidence evidence)
        {
            if (resumable.State != GroupState.Resumable || resumable.Started is not { } started || resumable.Header is not { } header)
            {
                throw new ArgumentException($"The group at {resumable.C} is not resumable", nameof(resumable));
            }
            var progress = resumable.Progress;
            return new ActiveAdvance()
            {
                Association = started.Association,
                Started = started,
                Destination = header.Destination,
                CutoffTicks = header.CutoffTicks,
                Evidence = evidence,
                Phase = resumable.LogsComplete ? AdvancePhase.Payload : AdvancePhase.Log,
                Cursor = progress?.Next ?? Start(started.Totals),
                Processed = progress?.Processed ?? 0,
                Sequence = progress?.Sequence ?? 0,
                ProgressName = resumable.ProgressName,
                PartBytesLeft = resumable.PartBytesLeft ?? started.Totals.PartBytes
            };
        }
    }
}

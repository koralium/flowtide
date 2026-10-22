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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;
using System.Collections.Frozen;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    internal enum AdvanceStep : byte
    {
        // Within the effective short cap
        Short,
        // Windowed planning under long admission
        Long
    }

    /// <summary>
    /// One selected advance: the oldest retained version O, the boundary checkpoint C, adoption A, the published V, the listed head, the cutoff and the sink's checkpoint interval.
    /// </summary>
    internal sealed record AdvanceBoundary(string TableId, long O, LogTransactionFile? OldestRoot, bool OldestRootMarked,
        long C, LogTransactionFile Checkpoint, long A, long V, long Head, DateTimeOffset Cutoff, LogEvidence Log, AdvanceStep Step,
        long EffectiveShortCap, TimeSpan Retention, int Interval)
    {
        // Versions of listed checkpoints the worker's builds found unsupported
        public IReadOnlySet<long> Unsupported { get; init; } = FrozenSet<long>.Empty;

        // Fresh for every selection
        public string AdvanceId { get; init; } = ControlTokens.New();

        // The complete anchor preparation established
        public CommitAnchor? Anchor { get; init; }

        // The lowest commit listed from O: O, or O + 1 above a root at O without its own commit
        public long FirstJson { get; init; }

        // The native path resolutions of the table's last plan
        public long Resolutions { get; init; }
    }

    internal enum LogNameKind : byte
    {
        Commit,
        Crc,
        ClassicCheckpoint,
        MultipartCheckpoint,
        UuidCheckpoint,
        Compaction,
        Stage,
        Temporary,
        Control,
        Hint,
        ProtectedDirectory,
        HiddenUnversioned,
        UnknownVersioned,
        UnknownUnversioned
    }

    /// <summary>
    /// A listed log entry with its kind, its version and the end version of a range such as a compaction.
    /// </summary>
    internal readonly record struct ClassifiedName(IOEntry Entry, LogNameKind Kind, long Version, long EndVersion);

    /// <summary>
    /// Every entry of one log listing by kind, with the stages and what declines cleanup.
    /// </summary>
    internal sealed class LogClassification
    {
        public required IReadOnlyList<ClassifiedName> Names { get; init; }

        public required IReadOnlyList<ClassifiedName> Stages { get; init; }

        // V2 checkpoint or sidecar evidence
        public bool V2OrSidecar { get; init; }

        public long LowestUnknownVersioned { get; init; } = long.MaxValue;
    }

    /// <summary>
    /// The counted reads an advance needs beside its candidate commits, and the short cap they leave.
    /// </summary>
    internal static class AdvanceBudget
    {
        public static long Ancillary(int stages, long tailCommits, bool newestAboveC, bool unmarkedOldestRoot)
            => checked(2 /* C and C.json */ + stages + tailCommits + (newestAboveC ? 1 : 0) + (unmarkedOldestRoot ? 1 : 0));

        public static long EffectiveShortCap(long readsRemainingAfterPreparation, long ancillary)
            => Math.Max(0, readsRemainingAfterPreparation - ancillary);
    }

    internal enum AdvancePlanStatus : byte
    {
        Planned,
        Declined,
        NeedsLongAdmission,
        NotBuilt
    }

    /// <summary>
    /// One emitted part record in job scratch: sorted UTF-8 deletion paths with its count, whole length and whole SHA-256.
    /// </summary>
    internal sealed record AdvancePart(int Window, int Index, string ScratchPath, long Paths, long Bytes, byte[] Sha256)
    {
        // Advance-wide, from zero
        public int Ordinal { get; init; }
    }

    /// <summary>
    /// A planned advance: its status, the destination read of C, the parts it emitted and the totals over every window.
    /// </summary>
    internal sealed class AdvancePlan : IDisposable
    {
        private bool _disposed;

        public required AdvancePlanStatus Status { get; init; }

        public MaintenanceReason Reason { get; init; }

        public string? Detail { get; init; }

        public DestinationRead? Destination { get; init; }

        // The newest checkpoint above C read for protection, null without one
        public DestinationRead? Newest { get; init; }

        public IReadOnlyList<AdvancePart> Parts { get; init; } = Array.Empty<AdvancePart>();

        // Totals over every window, summed with checked arithmetic
        public long Candidates { get; init; }

        public long Protected { get; init; }

        public long Kept { get; init; }

        public long Emitted { get; init; }

        // The native path resolutions of a plan read whole or promoted for them
        public long? Resolutions { get; init; }

        // The folder holding the parts, which disposal deletes them from
        public JobScratch? Scratch { get; init; }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            if (Scratch != null)
            {
                foreach (var part in Parts)
                {
                    Scratch.Delete(part.ScratchPath);
                }
            }
        }
    }
}

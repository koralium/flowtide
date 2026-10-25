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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    internal enum DiscoveryKind : byte
    {
        // A marked gap-free lineage reaches the adoption epoch
        Covered,
        // A new own commit is needed: the head has no adoption provenance, or a listed checkpoint is at or above it
        Anchor,
        // An exact build at the handed-off version
        Repair,
        // No repair can run
        Declined,
        // A read could not decide
        Undecided
    }

    /// <summary>
    /// What one discovery found at the handed-off version V.
    /// </summary>
    internal sealed record Discovery(DiscoveryKind Kind, long V, BuildDemand? Demand = null, MaintenanceReason Reason = MaintenanceReason.None, string? Detail = null);

    /// <summary>
    /// The reads a discovery makes, which the worker performs and tests script.
    /// </summary>
    internal interface ICoverageReads
    {
        // A listed commit's provenance from its first line, unknown when not strictly valid, null when it is gone
        Task<CommitProvenance?> ProvenanceAsync(LogTransactionFile commit);

        // The root an Auto walk over the candidates takes
        Task<RootPrediction> PredictAsync(IReadOnlyList<LogTransactionFile> candidates);

        // True when the destination's checkpoint is gap-free, false when it is not, null when it is gone or changed
        Task<bool?> ConfirmAsync(LogTransactionFile checkpoint);
    }

    /// <summary>
    /// Decides whether a marked gap-free lineage reaches the adoption epoch, and otherwise which anchor or exact repair build it needs.
    /// </summary>
    internal static class CoverageDiscovery
    {
        /// <summary>
        /// One discovery over a listing at the handed-off version; with release, coverage is confirmed from the destination.
        /// </summary>
        public static async Task<Discovery> DiscoverAsync(DeltaLogSegment segment, long v, bool release, bool rebuildLegacy, ICoverageReads reads)
        {
            var listing = segment.Listing;
            if (!listing.Commits.TryGetValue(v, out var head))
            {
                return new Discovery(DiscoveryKind.Undecided, v, Reason: MaintenanceReason.TargetNotInLog, Detail: $"Version {v} is not in the log, whose head is {segment.Head}");
            }
            if (await reads.ProvenanceAsync(head) is not { } provenance)
            {
                return new Discovery(DiscoveryKind.Undecided, v, Reason: MaintenanceReason.Transient, Detail: $"Commit {v} was removed after the listing");
            }
            if (!provenance.IsOwn)
            {
                return new Discovery(DiscoveryKind.Anchor, v, Detail: $"Version {v} has no adoption provenance");
            }
            // A newer listed head without provenance: the anchor lands above it
            if (segment.Head > v && listing.Commits.TryGetValue(segment.Head, out var newer) && await reads.ProvenanceAsync(newer) is { IsOwn: false })
            {
                return new Discovery(DiscoveryKind.Anchor, v, Detail: $"Version {segment.Head} above {v} has no adoption provenance");
            }
            var adoptedAt = provenance.AdoptedAt!.Value;
            // A listed checkpoint above V
            var newestListed = listing.Checkpoints.Count > 0 ? listing.Checkpoints[0].Version : -1;
            if (newestListed > v)
            {
                return new Discovery(DiscoveryKind.Anchor, v, Detail: $"Checkpoint {newestListed} is above version {v}");
            }

            // The lineage at V; with nothing checkpointed at V it is also the root a build at V walks
            var walk = await reads.PredictAsync(segment.RootCandidates(v, v).ToList());
            if (walk.Unknown != null)
            {
                return new Discovery(DiscoveryKind.Undecided, v, Reason: MaintenanceReason.UnknownCheckpoint, Detail: walk.Unknown);
            }
            var refuted = false;
            if (walk.Root is { MarkedGapFree: true } newest && newest.Identity.Version >= adoptedAt)
            {
                if (!release)
                {
                    return new Discovery(DiscoveryKind.Covered, v);
                }
                var confirmed = await reads.ConfirmAsync(listing.Checkpoints.First(x => x.Version == newest.Identity.Version));
                if (confirmed == null)
                {
                    return new Discovery(DiscoveryKind.Undecided, v, Reason: MaintenanceReason.UnknownCheckpoint, Detail: $"Checkpoint {newest.Identity.Version} changed after the listing");
                }
                if (confirmed.Value)
                {
                    return new Discovery(DiscoveryKind.Covered, v);
                }
                refuted = true;
            }

            // The exact target is V, above every listed checkpoint
            if (newestListed >= v)
            {
                return new Discovery(DiscoveryKind.Anchor, v, Detail: $"Checkpoint {newestListed} is at or above version {v}");
            }
            var history = segment.HasHistoryFromZero(v);
            if (!refuted && walk.Used(rebuildLegacy, history) is { MarkedGapFree: true })
            {
                return new Discovery(DiscoveryKind.Repair, v, new BuildDemand(v, RootPolicy.Auto));
            }
            if (history)
            {
                return new Discovery(DiscoveryKind.Repair, v, new BuildDemand(v, RootPolicy.None));
            }
            var reason = walk.Blocked ? MaintenanceReason.NoAdmissibleRoot : MaintenanceReason.MissingHistory;
            var skipped = walk.Skipped.Count > 0 ? $"; {string.Join("; ", walk.Skipped)}" : "";
            return new Discovery(DiscoveryKind.Declined, v, Reason: reason,
                Detail: $"No marked gap-free root reaches version {v} and commits before {segment.SuffixStart(v)} are missing{skipped}");
        }
    }
}

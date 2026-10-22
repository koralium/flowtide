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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    internal enum ProbeClass : byte
    {
        // Scanned within the envelope: admissible as a root
        Usable,
        // No valid Parquet frame
        Torn,
        // A valid file the bounded scan does not admit
        OutOfEnvelope,
        // Removed or changed after the listing
        Unknown
    }

    /// <summary>
    /// A listed checkpoint as a root choice sees it: its class, its footer's coverage marker and row count.
    /// </summary>
    internal sealed record ProbedCheckpoint(ProbeClass Class, ObjectIdentity Identity, CoverageMarker? Marker, long Rows, long Length, string? Detail)
    {
        public bool MarkedGapFree => Class == ProbeClass.Usable && Marker is { Inherited: null } m && m.Version == Identity.Version;
    }

    /// <summary>
    /// A usable root a walk keeps for its caller: the file, its admission scan and marker, which the caller releases.
    /// </summary>
    internal sealed class HeldRoot
    {
        public required LocalCheckpoint File { get; init; }

        public required ScanResult Scan { get; init; }

        public CoverageMarker? Marker { get; init; }
    }

    /// <summary>
    /// What an Auto root walk saw: skipped and stale candidates, downloaded bytes, the newest valid root and whether one was blocked.
    /// </summary>
    internal sealed class RootWalk
    {
        public RootWalk(IReadOnlySet<ObjectIdentity> knownUnsupported)
        {
            KnownUnsupported = knownUnsupported;
        }

        // Skipped while their identity holds
        public IReadOnlySet<ObjectIdentity> KnownUnsupported { get; }

        public List<string> Skipped { get; } = new List<string>();

        // Known unsupported roots whose listing identity changed
        public List<ObjectIdentity> Stale { get; } = new List<ObjectIdentity>();

        public long TransferBytes { get; set; }

        // The newest candidate that was valid, admitted or out of the envelope
        public long? NewestValidRoot { get; set; }

        // A candidate was skipped as unsupported or out of the envelope
        public bool Blocked { get; set; }

        // Why a candidate's state is unknown; the walk never falls back past it
        public string? Unknown { get; set; }
    }

    /// <summary>
    /// The root an Auto walk took, with its file released, and what the walk saw.
    /// </summary>
    internal sealed record RootPrediction(ProbedCheckpoint? Root, bool Blocked, string? Unknown, IReadOnlyList<string> Skipped)
    {
        // The root a build uses: none when the legacy rebuild drops an unmarked or gapped root for a replay from zero
        public ProbedCheckpoint? Used(bool rebuildLegacy, bool historyFromZero) =>
            Root != null && rebuildLegacy && historyFromZero && (Root.Marker == null || Root.Marker.Inherited != null) ? null : Root;
    }

    /// <summary>
    /// A build's root choice: the Auto walk over candidates, its prediction, and the probe of one listed checkpoint.
    /// </summary>
    internal static class CheckpointProbe
    {
        /// <summary>
        /// Walks root candidates newest first as an Auto build does: skips known unsupported, torn and out-of-envelope ones and stops at one whose state is unknown.
        /// </summary>
        public static async Task<HeldRoot?> WalkAsync(ICheckpointTransport transport, IOPath table, JobScratch scratch, JobMemory memory, TransferAllowance? transfer,
            IEnumerable<LogTransactionFile> candidates, RootCacheScope? cache, RootWalk walk, Action? beforeFetch, CancellationToken cancellationToken)
        {
            foreach (var candidate in candidates)
            {
                var identity = ObjectIdentity.Of(table, candidate);
                walk.Stale.AddRange(walk.KnownUnsupported.Where(x => x.Version == candidate.Version && x != identity));
                if (walk.KnownUnsupported.Contains(identity))
                {
                    walk.Blocked = true;
                    walk.Skipped.Add($"checkpoint {candidate.Version}: unsupported");
                    continue;
                }
                // Each fallback download is new work
                beforeFetch?.Invoke();
                var (fetched, unknown) = await CheckpointInspector.FetchAsync(transport, scratch, memory, transfer, candidate, cancellationToken, cache);
                if (fetched == null)
                {
                    walk.Unknown = unknown;
                    return null;
                }
                if (fetched.Owned)
                {
                    walk.TransferBytes += fetched.Length;
                }
                ScanResult? scan = null;
                try
                {
                    if (CheckpointPageScan.IsTorn(fetched.Path))
                    {
                        fetched.Dispose(scratch);
                        continue;
                    }
                    try
                    {
                        scan = CheckpointPageScan.Scan(fetched.Path, memory);
                    }
                    catch (CheckpointDeclinedException e)
                    {
                        // Out of the envelope is still valid; it only cannot be a root
                        walk.Blocked = true;
                        walk.NewestValidRoot ??= candidate.Version;
                        walk.Skipped.Add($"checkpoint {candidate.Version}: {e.Message}");
                        fetched.Dispose(scratch);
                        continue;
                    }
                    walk.NewestValidRoot ??= candidate.Version;
                    var marker = CoverageMarker.Parse(scan.Footer.Marker);
                    // Admitted by the scan, so the download is kept under its identity
                    fetched = CheckpointInspector.Admit(fetched, cache, scratch);
                    return new HeldRoot() { File = fetched, Scan = scan, Marker = marker };
                }
                catch
                {
                    // Nobody else holds the root yet, so its file, pin or scan charge goes here
                    if (scan != null)
                    {
                        memory.Release(scan.ModelBytes);
                    }
                    fetched.Dispose(scratch);
                    throw;
                }
            }
            return null;
        }

        /// <summary>
        /// The root an Auto build would take from the candidates, classified as its walk classifies them; the root's file and scan are released.
        /// </summary>
        public static async Task<RootPrediction> PredictAutoRootAsync(ICheckpointTransport transport, JobScratch scratch, JobMemory memory, TransferAllowance? transfer,
            IEnumerable<LogTransactionFile> candidates, IReadOnlySet<ObjectIdentity> knownUnsupported, RootCacheScope? cache, Action? beforeFetch, CancellationToken cancellationToken)
        {
            var walk = new RootWalk(knownUnsupported);
            var held = await WalkAsync(transport, transport.Table, scratch, memory, transfer, candidates, cache, walk, beforeFetch, cancellationToken);
            ProbedCheckpoint? root = null;
            if (held != null)
            {
                try
                {
                    root = new ProbedCheckpoint(ProbeClass.Usable, held.File.Identity, held.Marker, held.Scan.Footer.RowGroupRows.Sum(), held.File.Length, null);
                }
                finally
                {
                    memory.Release(held.Scan.ModelBytes);
                    held.File.Dispose(scratch);
                }
            }
            return new RootPrediction(root, walk.Blocked, walk.Unknown, walk.Skipped);
        }

        /// <summary>
        /// Classifies one listed checkpoint and reads its marker as a build's root choice would; releases its file and scan before returning.
        /// </summary>
        public static async Task<ProbedCheckpoint> ProbeAsync(ICheckpointTransport transport, JobScratch scratch, JobMemory memory, TransferAllowance? transfer,
            LogTransactionFile file, RootCacheScope? cache, CancellationToken cancellationToken)
        {
            var identity = ObjectIdentity.Of(transport.Table, file);
            var (fetched, unknown) = await CheckpointInspector.FetchAsync(transport, scratch, memory, transfer, file, cancellationToken, cache);
            if (fetched == null)
            {
                return new ProbedCheckpoint(ProbeClass.Unknown, identity, null, 0, 0, unknown);
            }
            ScanResult? scan = null;
            try
            {
                if (CheckpointPageScan.IsTorn(fetched.Path))
                {
                    return new ProbedCheckpoint(ProbeClass.Torn, identity, null, 0, fetched.Length, "No valid Parquet frame");
                }
                try
                {
                    scan = CheckpointPageScan.Scan(fetched.Path, memory);
                }
                catch (CheckpointDeclinedException e)
                {
                    return new ProbedCheckpoint(ProbeClass.OutOfEnvelope, identity, null, 0, fetched.Length, e.Message);
                }
                // Admitted by the scan: the download is kept under its identity
                fetched = CheckpointInspector.Admit(fetched, cache, scratch);
                return new ProbedCheckpoint(ProbeClass.Usable, identity, CoverageMarker.Parse(scan.Footer.Marker), scan.Footer.RowGroupRows.Sum(), fetched.Length, null);
            }
            finally
            {
                if (scan != null)
                {
                    memory.Release(scan.ModelBytes);
                }
                fetched.Dispose(scratch);
            }
        }
    }
}

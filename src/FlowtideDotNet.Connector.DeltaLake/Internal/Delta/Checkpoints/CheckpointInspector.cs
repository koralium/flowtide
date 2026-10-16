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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    internal enum CheckpointClass : byte
    {
        Valid,
        Unsupported,
        Torn,
        Unknown
    }

    /// <summary>
    /// A listed checkpoint's bytes as a local file: the native path on local disk, a cached root, or a download to job scratch.
    /// </summary>
    internal sealed class LocalCheckpoint
    {
        public required string Path { get; init; }

        public required bool Owned { get; init; }

        // A cached root, pinned while the build reads it
        public RootCache.Lease? Lease { get; init; }

        public required ObjectIdentity Identity { get; init; }

        public required long Version { get; init; }

        public required long Length { get; init; }

        private bool _disposed;

        // Idempotent: a second delete of the same path would find its charge already moved to the pending list
        public void Dispose(JobScratch scratch)
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            if (Owned)
            {
                scratch.Delete(Path);
            }
            Lease?.Dispose();
        }
    }

    /// <summary>
    /// A classified checkpoint: the class, whether it is admissible as a root, and its footer's row count.
    /// </summary>
    internal sealed record InspectedCheckpoint(CheckpointClass Class, bool Admissible, long Rows, long Length, string? Detail)
    {
        // Read from the storage rather than from local disk or the cache
        public bool Downloaded { get; init; }
    }

    /// <summary>
    /// Classifies listed checkpoints and decides when the hint needs a write or is dangling.
    /// </summary>
    internal static class CheckpointInspector
    {
        /// <summary>
        /// The checkpoint as a local file, or why its state is unknown: removed or changed after the listing.
        /// </summary>
        public static async Task<(LocalCheckpoint? Checkpoint, string? Unknown)> FetchAsync(ICheckpointTransport transport, JobScratch scratch, JobMemory memory,
            TransferAllowance? transfer, LogTransactionFile file, CancellationToken cancellationToken, RootCacheScope? cache = null)
        {
            var identity = ObjectIdentity.Of(transport.Table, file);
            var listed = file.IOEntry.Size;
            if (transport.NativePath(file.IOEntry.Path) is { } native)
            {
                // The probe, then the bytes read in place, charged once as a download would be
                await transport.ChargeNativeAsync(1, 0, cancellationToken);
                var info = new FileInfo(native);
                if (!info.Exists || (listed != null && info.Length != listed))
                {
                    return (null, $"Checkpoint {file.Version} changed after the listing");
                }
                await transport.ChargeNativeAsync(0, info.Length, cancellationToken);
                return (new LocalCheckpoint() { Path = native, Owned = false, Identity = identity, Version = file.Version, Length = info.Length }, null);
            }
            // A cached copy only under the identity the listing shows now
            if (cache is { } scope && scope.Cache.TryGet(scope, identity) is { } lease)
            {
                long cachedLength;
                try
                {
                    cachedLength = new FileInfo(lease.Path).Length;
                }
                catch
                {
                    lease.Dispose();
                    throw;
                }
                if (listed == null || cachedLength == listed)
                {
                    return (new LocalCheckpoint() { Path = lease.Path, Owned = false, Lease = lease, Identity = identity, Version = file.Version, Length = cachedLength }, null);
                }
                // A copy whose length is not the listed size is not this object
                lease.Dispose();
                scope.Cache.Drop(scope, identity);
            }

            // Declined before the download, whose chunked reservations would each fit and evict every cached root first
            if (listed > scratch.Ledger.Cap)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.DiskCap, DeclineDependency.None, $"Checkpoint {file.Version} has {listed} bytes, over the {scratch.Ledger.Cap} byte disk cap");
            }
            var download = scratch.NewPath(".download.parquet");
            long length;
            // Stowage reads whole bodies, so a download is admitted by its listed size
            using (transfer != null ? await transfer.AdmitAsync(listed ?? 0, cancellationToken) : null)
            {
                await using var target = scratch.Create(download);
                length = await transport.DownloadAsync(file.IOEntry.Path, target, memory, cancellationToken);
            }
            if (length < 0 || (listed != null && length != listed))
            {
                scratch.Delete(download);
                return (null, length < 0 ? $"Checkpoint {file.Version} was removed after the listing" : $"Checkpoint {file.Version} has {length} bytes, the listing said {listed}");
            }
            return (new LocalCheckpoint() { Path = download, Owned = true, Identity = identity, Version = file.Version, Length = length }, null);
        }

        /// <summary>
        /// Classifies a fetched checkpoint; only the sidecar leaf is ever decoded, so a file out of the envelope is still classified.
        /// </summary>
        public static InspectedCheckpoint Classify(LocalCheckpoint checkpoint, JobMemory memory)
        {
            if (CheckpointPageScan.IsTorn(checkpoint.Path))
            {
                return new InspectedCheckpoint(CheckpointClass.Torn, false, 0, checkpoint.Length, "No valid Parquet frame");
            }
            var admissible = false;
            try
            {
                var full = CheckpointPageScan.Scan(checkpoint.Path, memory);
                memory.Release(full.ModelBytes);
                admissible = true;
            }
            catch (CheckpointDeclinedException)
            {
            }

            ScanResult sidecar;
            try
            {
                sidecar = CheckpointPageScan.Scan(checkpoint.Path, memory, select: projection =>
                    projection.WithLeaves(projection.Leaves.Where(x => x.Role == ProjectedRole.SidecarPath).ToList()));
            }
            catch (CheckpointDeclinedException e)
            {
                // Not even the sidecar leaf can be read within the limits: undecidable, so never used and never rebuilt
                return new InspectedCheckpoint(CheckpointClass.Unsupported, false, 0, checkpoint.Length, e.Message);
            }
            try
            {
                var rows = sidecar.Footer.RowGroupRows.Sum();
                if (sidecar.Projection.Leaves.Count == 0)
                {
                    return new InspectedCheckpoint(CheckpointClass.Valid, admissible, rows, checkpoint.Length, null);
                }
                using var reader = new BoundedCheckpointReader(checkpoint.Path, sidecar, memory);
                reader.Read(_ => { });
                return new InspectedCheckpoint(CheckpointClass.Valid, admissible, rows, checkpoint.Length, null);
            }
            catch (CheckpointDeclinedException e)
            {
                return new InspectedCheckpoint(CheckpointClass.Unsupported, false, 0, checkpoint.Length, e.Message);
            }
            finally
            {
                memory.Release(sidecar.ModelBytes);
            }
        }

        /// <summary>
        /// Moves a downloaded checkpoint into the cache and returns it pinned there; anything else, or a failed move, is returned as it is.
        /// </summary>
        public static LocalCheckpoint Admit(LocalCheckpoint checkpoint, RootCacheScope? cache, JobScratch scratch)
        {
            if (!checkpoint.Owned || cache?.Cache.Put(cache, checkpoint.Identity, scratch, checkpoint.Path) is not { } lease)
            {
                return checkpoint;
            }
            return new LocalCheckpoint() { Path = lease.Path, Owned = false, Lease = lease, Identity = checkpoint.Identity, Version = checkpoint.Version, Length = checkpoint.Length };
        }

        public static async Task<InspectedCheckpoint> InspectAsync(ICheckpointTransport transport, JobScratch scratch, JobMemory memory, TransferAllowance? transfer,
            LogTransactionFile file, CancellationToken cancellationToken, RootCacheScope? cache = null)
        {
            var (checkpoint, unknown) = await FetchAsync(transport, scratch, memory, transfer, file, cancellationToken, cache);
            if (checkpoint == null)
            {
                return new InspectedCheckpoint(CheckpointClass.Unknown, false, 0, 0, unknown);
            }
            try
            {
                var inspected = Classify(checkpoint, memory) with { Downloaded = checkpoint.Owned };
                // An admitted download is kept for a later build that uses it as its root
                if (inspected.Class == CheckpointClass.Valid && inspected.Admissible)
                {
                    checkpoint = Admit(checkpoint, cache, scratch);
                }
                return inspected;
            }
            finally
            {
                checkpoint.Dispose(scratch);
            }
        }

        /// <summary>
        /// The hint is written when absent, older, or naming this version with another size; it is never lowered.
        /// </summary>
        public static bool HintNeedsWrite(LastCheckpointInfo? hint, long version, long actions, long sizeInBytes)
        {
            return hint == null || hint.Version < version || (hint.Version == version && (hint.SizeInBytes != sizeInBytes || hint.Size != actions));
        }

        /// <summary>
        /// A hint at or below the target with no non-empty classic checkpoint from its version to the target, which kernel fails on.
        /// </summary>
        public static bool IsDangling(LastCheckpointInfo? hint, DeltaLogSegment segment, long target)
        {
            return hint != null && hint.Version <= target
                && !segment.Listing.Checkpoints.Any(x => x.Version >= hint.Version && x.Version <= target && x.IOEntry.Size is not 0);
        }
    }
}

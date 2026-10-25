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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    internal enum DestinationStatus : byte
    {
        Read,
        // Gone before the read
        Missing,
        // Its length is not the listed size
        Changed,
        // Torn, out of the envelope, or not exactly one protocol and one metaData row
        Invalid,
        // The admission's counted reads were spent; nothing was read
        ReadsSpent,
        // The context stopped issuance before the read completed
        Preempted
    }

    /// <summary>
    /// One checkpoint read from the destination: the proof of its complete file, its coverage marker, rows and adds.
    /// </summary>
    internal sealed record DestinationRead(DestinationStatus Status, string? Detail = null)
    {
        // The complete file's SHA-256, never a build's record digest
        public ContentProof? Proof { get; init; }

        public CoverageMarker? Marker { get; init; }

        public long Rows { get; init; }

        public long Adds { get; init; }

        public bool GapFree => Status == DestinationStatus.Read && Marker is { Inherited: null } m && m.Version == Proof!.Version;
    }

    /// <summary>
    /// Reads a listed checkpoint from the destination into scratch while hashing it, then validates it in one full bounded pass; never the root cache or an in-place read.
    /// </summary>
    internal static class DestinationCheckpoint
    {
        /// <summary>
        /// The validated read; records go to the sink during the pass and count only when the status is Read; caps and storage errors propagate.
        /// </summary>
        public static async Task<DestinationRead> ReadAsync(IOPath table, IFileStorage storage, LogTransactionFile checkpoint, JobScratch scratch, JobMemory memory,
            TransferAllowance transfer, CleanupContext? context, RecordSink? records, CancellationToken cancellationToken)
        {
            if (context?.CheckStop() is { } stopped && stopped != IssuanceStop.None)
            {
                return new DestinationRead(DestinationStatus.Preempted, $"Issuance stopped: {stopped}");
            }
            var listed = checkpoint.IOEntry.Size;
            // Declined before any download
            if (listed > scratch.Ledger.Cap)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.DiskCap, DeclineDependency.None, $"Checkpoint {checkpoint.Version} has {listed} bytes, over the {scratch.Ledger.Cap} byte disk cap");
            }
            if (context != null && !context.TryCountRead())
            {
                return new DestinationRead(DestinationStatus.ReadsSpent, $"This admission has none of its {context.ReadLimit} reads left");
            }
            var download = scratch.NewPath(".destination.parquet");
            var buffer = Array.Empty<byte>();
            try
            {
                ProofRead read;
                // Admitted by its listed size
                using (await transfer.AdmitAsync(listed ?? 0, cancellationToken))
                {
                    buffer = memory.Allocate(ProofReader.BufferSize);
                    await using var target = scratch.Create(download);
                    var source = await storage.OpenRead(checkpoint.IOEntry.Path, cancellationToken);
                    if (source == null)
                    {
                        return new DestinationRead(DestinationStatus.Missing, $"Checkpoint {checkpoint.Version} was removed after the listing");
                    }
                    await using var proof = new ProofStream(source, context, proveFirstLine: false);
                    int count;
                    while ((count = await proof.ReadAsync(buffer, cancellationToken)) > 0)
                    {
                        await target.WriteAsync(buffer.AsMemory(0, count), cancellationToken);
                    }
                    read = proof.Complete(checkpoint.Version);
                }
                if (listed != null && read.Length != listed)
                {
                    return new DestinationRead(DestinationStatus.Changed, $"Checkpoint {checkpoint.Version} has {read.Length} bytes, the listing said {listed}");
                }
                var proved = new ContentProof(checkpoint.IOEntry.Path, checkpoint.Version, true, ObjectIdentity.Of(table, checkpoint), read.Length, read.Sha256!);
                return Validate(download, proved, memory, records, cancellationToken);
            }
            catch (CleanupPreemptedException e)
            {
                return new DestinationRead(DestinationStatus.Preempted, e.Message);
            }
            catch (OperationCanceledException) when (context != null && context.Stop.IsCancellationRequested)
            {
                return new DestinationRead(DestinationStatus.Preempted, "The worker stopped");
            }
            finally
            {
                if (buffer.Length > 0)
                {
                    memory.Free(buffer);
                }
                scratch.Delete(download);
            }
        }

        // A resource limit, never a fact about the checkpoint
        private static bool IsCap(MaintenanceReason reason) => reason is MaintenanceReason.ManagedMemoryCap or MaintenanceReason.NativeMemoryCap or MaintenanceReason.DiskCap;

        // The full projection in one pass: row total, protocol and metaData counts, adds and the marker
        private static DestinationRead Validate(string path, ContentProof proof, JobMemory memory, RecordSink? records, CancellationToken cancellationToken)
        {
            ScanResult scan;
            try
            {
                scan = CheckpointPageScan.Scan(path, memory);
            }
            catch (CheckpointDeclinedException e) when (!IsCap(e.Reason))
            {
                return new DestinationRead(DestinationStatus.Invalid, e.Message);
            }
            var sinkThrew = false;
            try
            {
                long protocols = 0;
                long metaData = 0;
                long adds = 0;
                long rows;
                using (var reader = new BoundedCheckpointReader(path, scan, memory))
                {
                    rows = reader.Read(record =>
                    {
                        switch (CheckpointRecord.Kind(record))
                        {
                            case RecordKind.Protocol:
                                protocols++;
                                break;
                            case RecordKind.MetaData:
                                metaData++;
                                break;
                            case RecordKind.Add:
                                adds++;
                                break;
                        }
                        if (records != null)
                        {
                            try
                            {
                                records(record);
                            }
                            catch
                            {
                                sinkThrew = true;
                                throw;
                            }
                        }
                    }, cancellationToken);
                }
                var footerRows = scan.Footer.RowGroupRows.Sum();
                if (rows != footerRows)
                {
                    return new DestinationRead(DestinationStatus.Invalid, $"The pass read {rows} rows, the footer's row groups hold {footerRows}");
                }
                if (protocols != 1 || metaData != 1)
                {
                    return new DestinationRead(DestinationStatus.Invalid, $"Checkpoint {proof.Version} has {protocols} protocol and {metaData} metaData rows");
                }
                return new DestinationRead(DestinationStatus.Read)
                {
                    Proof = proof,
                    Marker = CoverageMarker.Parse(scan.Footer.Marker),
                    Rows = rows,
                    Adds = adds
                };
            }
            catch (CheckpointDeclinedException e) when (!sinkThrew && !IsCap(e.Reason))
            {
                return new DestinationRead(DestinationStatus.Invalid, e.Message);
            }
            finally
            {
                memory.Release(scan.ModelBytes);
            }
        }
    }
}

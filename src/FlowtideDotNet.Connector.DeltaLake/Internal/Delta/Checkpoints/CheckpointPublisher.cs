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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    internal sealed record PublishResult(bool Published, string? Failure);

    /// <summary>
    /// Publishes a validated checkpoint at its canonical name, checks its size, then writes the hint.
    /// </summary>
    internal static class CheckpointPublisher
    {
        // Room the destination volume keeps beyond the checkpoint
        public const long FreeSpaceMargin = 64L << 20;

        public static async Task<PublishResult> PublishAsync(ICheckpointTransport transport, JobScratch scratch, JobMemory memory,
            CheckpointDescriptor checkpoint, Func<string, long>? freeSpace, CancellationToken cancellationToken, Action<string>? faultForTests = null, RootCacheScope? cache = null)
        {
            var destination = transport.Table.Combine(DeltaTransactionReader.DeltaLogDirName).Combine($"{checkpoint.Version:D20}.checkpoint.parquet");
            if (transport.NativePath(destination) is { } native)
            {
                // The move, the rename, the size check and the bytes published, all before the first of them
                await transport.ChargeNativeAsync(3, checkpoint.SizeInBytes, cancellationToken);
                PublishLocal(scratch, checkpoint, native, freeSpace, faultForTests);
                var length = new FileInfo(native).Length;
                if (length != checkpoint.SizeInBytes)
                {
                    return new PublishResult(false, $"The published checkpoint has {length} bytes, {checkpoint.SizeInBytes} were written");
                }
            }
            else
            {
                await transport.UploadAsync(checkpoint.LocalPath, destination, memory, cancellationToken);
                // The cached identity is listed before the size check, or by it when the listing has sizes; a failed listing only skips the cache
                ObjectIdentity? identity = null;
                if (cache != null)
                {
                    try
                    {
                        identity = await transport.IdentityAsync(destination, checkpoint.Version, cancellationToken);
                    }
                    catch (Exception e) when (e is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                    {
                    }
                }
                var size = identity is { Size: >= 0 } ? identity.Size : await transport.SizeAsync(destination, cancellationToken);
                if (size != checkpoint.SizeInBytes)
                {
                    return new PublishResult(false, size == null ? "The published checkpoint's size is unknown" : $"The published checkpoint has {size} bytes, {checkpoint.SizeInBytes} were written");
                }
                // The output stays as a local root; best effort, the checkpoint is published
                RootCache.Lease? lease = null;
                if (identity != null)
                {
                    try
                    {
                        lease = cache!.Cache.Put(cache, identity, scratch, checkpoint.LocalPath, supersede: true);
                    }
                    catch (Exception e) when (e is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                    {
                    }
                }
                if (lease != null)
                {
                    lease.Dispose();
                }
                else
                {
                    scratch.Delete(checkpoint.LocalPath);
                }
            }

            // The hint is never lowered; the size check above came first
            var hint = await transport.ReadHintAsync(cancellationToken);
            if (CheckpointInspector.HintNeedsWrite(hint, checkpoint.Version, checkpoint.Actions, checkpoint.SizeInBytes))
            {
                faultForTests?.Invoke("hint");
                await transport.WriteHintAsync(new LastCheckpointInfo()
                {
                    Version = checkpoint.Version,
                    Size = checkpoint.Actions,
                    SizeInBytes = checkpoint.SizeInBytes,
                    NumOfAddFiles = checkpoint.AddFiles
                }, cancellationToken);
            }
            return new PublishResult(true, null);
        }

        // Moves the output next to the destination, then renames it over the canonical name
        private static void PublishLocal(JobScratch scratch, CheckpointDescriptor checkpoint, string native, Func<string, long>? freeSpace, Action<string>? faultForTests)
        {
            var ledger = scratch.Ledger;
            var size = checkpoint.SizeInBytes;
            // The adjacent copy holds its own charge while both files exist
            ledger.Reserve(size);
            var logDirectory = Path.GetDirectoryName(native)!;
            long? free;
            try
            {
                free = freeSpace != null ? freeSpace(logDirectory) : FreeSpaceOf(logDirectory);
            }
            catch
            {
                ledger.Release(size);
                throw;
            }
            if (free < size + FreeSpaceMargin)
            {
                ledger.Release(size);
                throw new CheckpointDeclinedException(MaintenanceReason.DiskCap, DeclineDependency.None, $"The table's volume has {free} bytes free for a {size} byte checkpoint");
            }
            var temp = Path.Combine(logDirectory, $".flowtide-{Guid.NewGuid():N}.checkpoint.tmp");
            try
            {
                faultForTests?.Invoke("move");
                File.Move(checkpoint.LocalPath, temp);
                // Across file systems the move copied, which no path comparison detects on Unix, so the temp is always made durable
                using (var file = new FileStream(temp, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete))
                {
                    file.Flush(true);
                }
                // The scratch file is gone, so its charge goes; a copy that could not delete its source keeps that charge until the source is deleted
                if (File.Exists(checkpoint.LocalPath))
                {
                    scratch.Delete(checkpoint.LocalPath);
                }
                else
                {
                    ledger.Release(scratch.Release(checkpoint.LocalPath));
                }
                faultForTests?.Invoke("rename");
                File.Move(temp, native, overwrite: true);
                ledger.Release(size);
            }
            catch
            {
                // The temp keeps its charge until it is gone
                scratch.Adopt(temp, size);
                scratch.Delete(temp);
                throw;
            }
        }

        // Null when the volume cannot be queried, such as a UNC share: the check is skipped, while the reservation and the failure cleanup still hold
        private static long? FreeSpaceOf(string directory)
        {
            try
            {
                // On Unix a drive is any path on the mounted file system, so the table's own mount is measured
                var drive = OperatingSystem.IsWindows() ? Path.GetPathRoot(Path.GetFullPath(directory))! : Path.GetFullPath(directory);
                return new DriveInfo(drive).AvailableFreeSpace;
            }
            catch (Exception e) when (e is ArgumentException or IOException or UnauthorizedAccessException)
            {
                return null;
            }
        }
    }
}

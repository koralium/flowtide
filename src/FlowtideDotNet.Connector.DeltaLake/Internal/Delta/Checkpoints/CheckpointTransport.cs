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
using Stowage;
using Stowage.Impl;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// What a checkpoint build and its publication do against the table's storage.
    /// </summary>
    internal interface ICheckpointTransport
    {
        IFileStorage Storage { get; }

        IOPath Table { get; }

        Task<DeltaLogSegment> ListAsync(CancellationToken cancellationToken);

        // The hint, null when it is absent, torn or unreadable
        Task<LastCheckpointInfo?> ReadHintAsync(CancellationToken cancellationToken);

        Task WriteHintAsync(LastCheckpointInfo hint, CancellationToken cancellationToken);

        // The file on local disk behind a table path, null for other storages
        string? NativePath(IOPath path);

        // Copies the object into a charged scratch stream and returns its length; -1 when it is gone
        Task<long> DownloadAsync(IOPath path, Stream target, JobMemory memory, CancellationToken cancellationToken);

        // Uploads in chunks; on failure or cancellation the upload is abandoned, so nothing partial is committed
        Task UploadAsync(string local, IOPath destination, JobMemory memory, CancellationToken cancellationToken);

        // The object's length: Stat, then the listing, then its bytes counted for a storage that reports no size; null when it is gone
        Task<long?> SizeAsync(IOPath path, CancellationToken cancellationToken);

        // The identity a listing of the log gives the object, null when it is not listed
        Task<ObjectIdentity?> IdentityAsync(IOPath path, long version, CancellationToken cancellationToken);

        // Charges a native operation that bypasses the storage to the maintenance buckets before it runs; free when the storage is not metered
        ValueTask ChargeNativeAsync(long requests, long bytes, CancellationToken cancellationToken);
    }

    internal sealed class StowageCheckpointTransport : ICheckpointTransport
    {
        public const int DefaultChunkSize = 4 << 20;
        private const int HintCap = 64 << 10;

        public StowageCheckpointTransport(IFileStorage storage, IOPath table)
        {
            Storage = storage;
            Table = table;
        }

        public IFileStorage Storage { get; }

        public IOPath Table { get; }

        public int ChunkSize { get; init; } = DefaultChunkSize;

        // Runs after each uploaded chunk, so tests can cancel between chunks
        internal Action<int>? AfterChunkForTests { get; init; }

        public IOPath LogPath => Table.Combine(DeltaTransactionReader.DeltaLogDirName);

        public IOPath HintPath => LogPath.Combine("_last_checkpoint");

        public Task<DeltaLogSegment> ListAsync(CancellationToken cancellationToken) => DeltaLogSegment.ListAsync(Storage, Table, cancellationToken);

        public async Task<LastCheckpointInfo?> ReadHintAsync(CancellationToken cancellationToken)
        {
            try
            {
                await using var stream = await Storage.OpenRead(HintPath, cancellationToken);
                if (stream == null)
                {
                    return null;
                }
                var buffer = new byte[HintCap];
                var length = 0;
                int read;
                while (length < buffer.Length && (read = await stream.ReadAsync(buffer.AsMemory(length), cancellationToken)) > 0)
                {
                    length += read;
                }
                if (length == buffer.Length)
                {
                    return null;
                }
                return JsonSerializer.Deserialize<LastCheckpointInfo>(buffer.AsSpan(0, length));
            }
            catch (JsonException)
            {
                return null;
            }
        }

        public async Task WriteHintAsync(LastCheckpointInfo hint, CancellationToken cancellationToken)
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(hint);
            if (NativePath(HintPath) is { } native)
            {
                // The temp write and the rename
                await ChargeNativeAsync(2, bytes.Length, cancellationToken);
                // A reader never meets a hint half written or held open
                var temp = Path.Combine(Path.GetDirectoryName(native)!, $".flowtide-{Guid.NewGuid():N}.last_checkpoint.tmp");
                try
                {
                    await File.WriteAllBytesAsync(temp, bytes, cancellationToken);
                    File.Move(temp, native, overwrite: true);
                }
                catch
                {
                    TryDelete(temp);
                    throw;
                }
                return;
            }
            // Disposing commits on object stores, so a failed write is abandoned rather than disposed
            var stream = await Storage.OpenWrite(HintPath, cancellationToken);
            await stream.WriteAsync(bytes, cancellationToken);
            await stream.DisposeAsync();
        }

        private static void TryDelete(string path)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        public string? NativePath(IOPath path) => Storage is ILocalDiskFileStorage local ? local.ToNativeLocalPath(path) : null;

        public ValueTask ChargeNativeAsync(long requests, long bytes, CancellationToken cancellationToken) => MeteredFileStorage.ChargeNativeAsync(Storage, requests, bytes, cancellationToken);

        public async Task<long> DownloadAsync(IOPath path, Stream target, JobMemory memory, CancellationToken cancellationToken)
        {
            await using var source = await Storage.OpenRead(path, cancellationToken);
            if (source == null)
            {
                return -1;
            }
            var buffer = memory.Allocate(256 << 10);
            try
            {
                long total = 0;
                int read;
                while ((read = await source.ReadAsync(buffer, cancellationToken)) > 0)
                {
                    await target.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
                    total += read;
                }
                return total;
            }
            finally
            {
                memory.Free(buffer);
            }
        }

        public async Task UploadAsync(string local, IOPath destination, JobMemory memory, CancellationToken cancellationToken)
        {
            var buffer = memory.Allocate(ChunkSize);
            try
            {
                await using var source = new FileStream(local, FileMode.Open, FileAccess.Read, FileShare.Read, 1, FileOptions.SequentialScan | FileOptions.Asynchronous);
                var target = await Storage.OpenWrite(destination, cancellationToken);
                // Disposing commits on object stores, so the stream is disposed only after every chunk is written
                int read;
                var chunks = 0;
                while ((read = await source.ReadAsync(buffer.AsMemory(0, ChunkSize), cancellationToken)) > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await target.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
                    AfterChunkForTests?.Invoke(++chunks);
                }
                cancellationToken.ThrowIfCancellationRequested();
                await target.DisposeAsync();
            }
            finally
            {
                memory.Free(buffer);
            }
        }

        public async Task<ObjectIdentity?> IdentityAsync(IOPath path, long version, CancellationToken cancellationToken)
        {
            var listed = await Storage.Ls(path.Parent, cancellationToken: cancellationToken);
            return listed.FirstOrDefault(x => x.Path.Full == path.Full) is { } entry
                ? new ObjectIdentity(Table.Full, version, entry.Size ?? -1, entry.LastModificationTime, entry.MD5)
                : null;
        }

        public async Task<long?> SizeAsync(IOPath path, CancellationToken cancellationToken)
        {
            var entry = await Storage.Stat(path, cancellationToken);
            if (entry?.Size != null)
            {
                return entry.Size;
            }
            var listed = await Storage.Ls(path.Parent, cancellationToken: cancellationToken);
            if (listed.FirstOrDefault(x => x.Path.Full == path.Full) is { Size: { } size })
            {
                return size;
            }
            // Only stores without size metadata get here, such as the in-memory one
            await using var stream = await Storage.OpenRead(path, cancellationToken);
            if (stream == null)
            {
                return null;
            }
            var buffer = new byte[64 << 10];
            long length = 0;
            int read;
            while ((read = await stream.ReadAsync(buffer, cancellationToken)) > 0)
            {
                length += read;
            }
            return length;
        }
    }
}

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

using Stowage;
using Stowage.Impl;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// Maintenance's view of the table storage: every call is charged a request before it is issued, and stream bytes as they move.
    /// </summary>
    internal class MeteredFileStorage : IFileStorage, IConnectorOwnedStorage
    {
        private static readonly JsonSerializerOptions s_indented = new JsonSerializerOptions() { WriteIndented = true };

        protected MeteredFileStorage(IFileStorage inner, MaintenanceBuckets buckets, ListingPaging paging)
        {
            Inner = inner;
            Buckets = buckets;
            Paging = paging;
        }

        /// <summary>
        /// Meters the storage; local disk stays visible as local disk, so native paths keep working.
        /// </summary>
        public static MeteredFileStorage Wrap(IFileStorage storage, MaintenanceBuckets buckets, ListingPaging paging)
        {
            // Metered once, so no call is charged twice
            if (storage is MeteredFileStorage metered)
            {
                storage = metered.Inner;
            }
            return storage is ILocalDiskFileStorage ? new LocalDiskMeteredFileStorage(storage, buckets, paging) : new MeteredFileStorage(storage, buckets, paging);
        }

        /// <summary>
        /// The meter among the connector's own wrappers, null for storage maintenance does not meter.
        /// </summary>
        public static MeteredFileStorage? Of(IFileStorage storage)
        {
            while (true)
            {
                if (storage is MeteredFileStorage metered)
                {
                    return metered;
                }
                if (storage is not IConnectorOwnedStorage owned)
                {
                    return null;
                }
                storage = owned.Inner;
            }
        }

        /// <summary>
        /// Charges a native operation on the storage's table, such as a rename or an in-place read; free when the storage is not metered.
        /// </summary>
        public static ValueTask ChargeNativeAsync(IFileStorage storage, long requests, long bytes, CancellationToken cancellationToken)
        {
            return Of(storage) is { } metered ? metered.Buckets.ChargeNativeAsync(requests, bytes, cancellationToken) : ValueTask.CompletedTask;
        }

        public IFileStorage Inner { get; }

        public MaintenanceBuckets Buckets { get; }

        public ListingPaging Paging { get; }

        private ValueTask Request(CancellationToken cancellationToken) => Buckets.Requests.AcquireAsync(1, cancellationToken);

        public async Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
        {
            // An empty listing costs its one request; later pages are estimated from the entries and repaid after
            await Request(cancellationToken);
            var entries = await Inner.Ls(path, recurse, cancellationToken);
            await Buckets.RepayRequestsAsync(MaintenanceBuckets.ExtraPages(Paging, entries.Count), cancellationToken);
            return entries;
        }

        public async Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
        {
            await Request(cancellationToken);
            var stream = await Inner.OpenRead(path, cancellationToken);
            return stream == null ? null : new MeteredReadStream(stream, Buckets);
        }

        public async Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default)
        {
            await Request(cancellationToken);
            return new MeteredWriteStream(await Inner.OpenWrite(path, cancellationToken), Buckets, WritesInPlace);
        }

        // Writes reach the final name as they happen
        protected virtual bool WritesInPlace => false;

        public async Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default)
        {
            await Request(cancellationToken);
            await Inner.Ren(name, newName, cancellationToken);
        }

        public async Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default)
        {
            await Request(cancellationToken);
            var text = await Inner.ReadText(path, encoding, cancellationToken);
            if (text != null)
            {
                // The bytes the text was decoded from
                await Buckets.RepayBytesAsync((encoding ?? Encoding.UTF8).GetByteCount(text), cancellationToken);
            }
            return text;
        }

        public async Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default)
        {
            await Request(cancellationToken);
            await Buckets.ChargeBytesAsync((encoding ?? Encoding.UTF8).GetByteCount(contents), cancellationToken);
            await Inner.WriteText(path, contents, encoding, cancellationToken);
        }

        // Through the metered text calls, as Stowage's own implementation reads and writes JSON
        public async Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default)
        {
            var text = await ReadText(path, null, cancellationToken);
            return text == null ? default : JsonSerializer.Deserialize<T>(text);
        }

        public Task WriteAsJson(IOPath path, object value, bool pretty = false, CancellationToken cancellationToken = default)
        {
            return WriteText(path, JsonSerializer.Serialize(value, pretty ? s_indented : null), null, cancellationToken);
        }

        public async Task Rm(IOPath path, CancellationToken cancellationToken = default)
        {
            await Request(cancellationToken);
            await Inner.Rm(path, cancellationToken);
        }

        public async Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default)
        {
            await Request(cancellationToken);
            return await Inner.Exists(path, cancellationToken);
        }

        public async Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default)
        {
            await Request(cancellationToken);
            return await Inner.Stat(path, cancellationToken);
        }

        // The inner storage is the sink's, which outlives every worker
        public void Dispose()
        {
        }
    }

    /// <summary>
    /// The meter over local disk, which keeps its native paths reachable.
    /// </summary>
    internal sealed class LocalDiskMeteredFileStorage : MeteredFileStorage, ILocalDiskFileStorage
    {
        public LocalDiskMeteredFileStorage(IFileStorage inner, MaintenanceBuckets buckets, ListingPaging paging)
            : base(inner, buckets, paging)
        {
            WritesInPlace = IsPinnedLocalDisk(inner);
        }

        // Only the pinned local disk is known to write in place
        protected override bool WritesInPlace { get; }

        public string ToNativeLocalPath(IOPath path) => ((ILocalDiskFileStorage)Inner).ToNativeLocalPath(path);

        private static bool IsPinnedLocalDisk(IFileStorage inner)
        {
            try
            {
                return StoreDescriptor.Of(RawStorage.Unwrap(inner)).Kind == StoreKind.LocalDisk;
            }
            catch
            {
                return false;
            }
        }
    }

    /// <summary>
    /// Charges each read's bytes after it returns; a read never asks for more than one chunk.
    /// </summary>
    internal sealed class MeteredReadStream : Stream
    {
        private readonly Stream _inner;
        private readonly MaintenanceBuckets _buckets;

        public MeteredReadStream(Stream inner, MaintenanceBuckets buckets)
        {
            _inner = inner;
            _buckets = buckets;
        }

        public override bool CanRead => _inner.CanRead;

        public override bool CanSeek => _inner.CanSeek;

        public override bool CanWrite => false;

        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        private int Clamp(int count) => (int)Math.Min(count, _buckets.ByteChunk);

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            var read = _inner.Read(buffer.Slice(0, Clamp(buffer.Length)));
            if (read > 0)
            {
                _buckets.RepayBytes(read);
            }
            return read;
        }

        public override int ReadByte()
        {
            var value = _inner.ReadByte();
            if (value >= 0)
            {
                _buckets.RepayBytes(1);
            }
            return value;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var read = await _inner.ReadAsync(buffer.Slice(0, Clamp(buffer.Length)), cancellationToken);
            if (read > 0)
            {
                await _buckets.RepayBytesAsync(read, cancellationToken);
            }
            return read;
        }

        public override void Flush() => _inner.Flush();

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
            }
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await _inner.DisposeAsync();
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// Charges each chunk before it is written; after a failed or cancelled write, disposing abandons an upload rather than publishing it and only closes a write in place.
    /// </summary>
    internal sealed class MeteredWriteStream : Stream
    {
        private readonly Stream _inner;
        private readonly MaintenanceBuckets _buckets;
        // Nothing to hold back, so a failed write is still closed
        private readonly bool _writesInPlace;
        private int _disposed;
        private volatile bool _abandoned;

        public MeteredWriteStream(Stream inner, MaintenanceBuckets buckets, bool writesInPlace = false)
        {
            _inner = inner;
            _buckets = buckets;
            _writesInPlace = writesInPlace;
        }

        // A write or flush failed, so the inner stream is never disposed into publication
        public bool Abandoned => _abandoned;

        public override bool CanRead => false;

        public override bool CanSeek => _inner.CanSeek;

        public override bool CanWrite => _inner.CanWrite;

        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override void Write(byte[] buffer, int offset, int count) => Write(new ReadOnlySpan<byte>(buffer, offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            try
            {
                while (!buffer.IsEmpty)
                {
                    var chunk = (int)Math.Min(buffer.Length, _buckets.ByteChunk);
                    _buckets.Bytes.Acquire(chunk);
                    _inner.Write(buffer.Slice(0, chunk));
                    buffer = buffer.Slice(chunk);
                }
            }
            catch
            {
                _abandoned = true;
                throw;
            }
        }

        public override void WriteByte(byte value)
        {
            try
            {
                _buckets.Bytes.Acquire(1);
                _inner.WriteByte(value);
            }
            catch
            {
                _abandoned = true;
                throw;
            }
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            try
            {
                while (!buffer.IsEmpty)
                {
                    var chunk = (int)Math.Min(buffer.Length, _buckets.ByteChunk);
                    await _buckets.Bytes.AcquireAsync(chunk, cancellationToken);
                    await _inner.WriteAsync(buffer.Slice(0, chunk), cancellationToken);
                    buffer = buffer.Slice(chunk);
                }
            }
            catch
            {
                _abandoned = true;
                throw;
            }
        }

        public override void Flush()
        {
            try
            {
                _inner.Flush();
            }
            catch
            {
                _abandoned = true;
                throw;
            }
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            try
            {
                await _inner.FlushAsync(cancellationToken);
            }
            catch
            {
                _abandoned = true;
                throw;
            }
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

        public override void SetLength(long value) => _inner.SetLength(value);

        protected override void Dispose(bool disposing)
        {
            if (disposing && Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                if (!_abandoned)
                {
                    _inner.Dispose();
                }
                else if (_writesInPlace)
                {
                    try
                    {
                        _inner.Dispose();
                    }
                    catch
                    {
                        // The failed write already reported its error
                    }
                }
            }
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                if (!_abandoned)
                {
                    await _inner.DisposeAsync();
                }
                else if (_writesInPlace)
                {
                    try
                    {
                        await _inner.DisposeAsync();
                    }
                    catch
                    {
                        // The failed write already reported its error
                    }
                }
            }
            GC.SuppressFinalize(this);
        }
    }
}

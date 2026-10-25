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

using FlowtideDotNet.AcceptanceTests.Internal;
using Stowage;
using Stowage.Impl;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// Counts requests and injects torn writes or actions before a request.
    /// Stowage's in memory storage hands every reader of a file the same stream, so reads, listings, removes and write disposals are serialized per store.
    /// </summary>
    internal class HookFileStorage : IFileStorage
    {
        private readonly IFileStorage _inner;
        private readonly ConcurrentQueue<string> _requests = new ConcurrentQueue<string>();
        // One lock per wrapped store, two wrappers over one in-memory store would get the same stream
        private static readonly ConditionalWeakTable<IFileStorage, SemaphoreSlim> s_locks = new ConditionalWeakTable<IFileStorage, SemaphoreSlim>();
        private static readonly AsyncLocal<bool> s_unrecorded = new AsyncLocal<bool>();
        private readonly SemaphoreSlim _lock;

        public HookFileStorage(IFileStorage inner)
        {
            _inner = inner;
            _lock = s_locks.GetValue(inner, static _ => new SemaphoreSlim(1, 1));
        }

        // Keeps the local disk interface visible through the wrapper
        public static HookFileStorage Wrap(IFileStorage inner)
        {
            return inner is ILocalDiskFileStorage ? new LocalDiskHookFileStorage(inner) : new HookFileStorage(inner);
        }

        private async Task<T> Locked<T>(Func<Task<T>> action, CancellationToken cancellationToken)
        {
            await _lock.WaitAsync(cancellationToken);
            try
            {
                return await action();
            }
            finally
            {
                _lock.Release();
            }
        }

        private async Task Locked(Func<Task> action, CancellationToken cancellationToken)
        {
            await _lock.WaitAsync(cancellationToken);
            try
            {
                await action();
            }
            finally
            {
                _lock.Release();
            }
        }

        public IFileStorage Inner => _inner;

        // Requests as "Verb path", in order
        public IReadOnlyList<string> Requests => _requests.ToArray();

        public void ClearRequests()
        {
            _requests.Clear();
        }

        // The test's own requests inside the scope are not recorded, only storage calls belong in it
        public IDisposable Unrecorded()
        {
            s_unrecorded.Value = true;
            return new UnrecordedScope();
        }

        private sealed class UnrecordedScope : IDisposable
        {
            public void Dispose()
            {
                s_unrecorded.Value = false;
            }
        }

        // The next write of a path ending with this keeps only TearAfterBytes bytes and crashes
        public string? TearWriteOf { get; set; }

        public int TearAfterBytes { get; set; }

        public int Tears;

        // Writes and flushes of a path ending with this, as "W<count>" and "F"
        public string? RecordWritesOf { get; set; }

        public ConcurrentQueue<string> WriteEvents { get; } = new ConcurrentQueue<string>();

        // Runs before a request, with the verb and the path
        public Func<string, IOPath, Task>? Before { get; set; }

        // Paths this wrapper does not show, as if they were not written yet
        public Func<IOPath, bool>? Hidden { get; set; }

        // Listed entries carry a modification time counted per path at each publication, a strong identity for in-memory stores
        public bool StampIdentity { get; set; }

        // Counted per wrapped store, so two wrappers of one store never stamp different bytes alike
        private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<IFileStorage, ConcurrentDictionary<string, long>> s_writes = new();

        private ConcurrentDictionary<string, long> _writes => s_writes.GetValue(_inner, _ => new ConcurrentDictionary<string, long>());

        private void CountWrite(IOPath path)
        {
            _writes.AddOrUpdate(path.Full, 1, static (_, count) => count + 1);
        }

        private IOEntry Stamp(IOEntry entry)
        {
            var count = _writes.TryGetValue(entry.Path.Full, out var writes) ? writes : 0;
            var stamped = new IOEntry(entry.Path.Full)
            {
                Size = entry.Size,
                MD5 = entry.MD5,
                CreatedTime = entry.CreatedTime,
                LastModificationTime = DateTimeOffset.UnixEpoch.AddTicks(count),
                Tag = entry.Tag
            };
            foreach (var property in entry.Properties)
            {
                stamped.Properties[property.Key] = property.Value;
            }
            foreach (var metadata in entry.Metadata)
            {
                stamped.Metadata[metadata.Key] = metadata.Value;
            }
            return stamped;
        }

        // Holds a write's publication until the returned task completes, so a write can land after a later request
        public Func<IOPath, Task?>? DeferPublication { get; set; }

        // Called when a deferred write's dispose starts waiting
        public Action<IOPath>? PublicationParked { get; set; }

        private async Task Record(string verb, IOPath? path)
        {
            if (!s_unrecorded.Value)
            {
                _requests.Enqueue($"{verb} {path?.Full}");
            }
            // Read once, a test may clear the hook while a request is in flight
            var before = Before;
            if (before != null && path != null)
            {
                await before(verb, path);
            }
        }

        public async Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default)
        {
            await Record("OpenWrite", path);
            // The bytes become visible when the stream is disposed, which is when the identity changes
            var deferred = DeferPublication?.Invoke(path);
            var parked = PublicationParked;
            Stream stream = new SerializedDisposeStream(await _inner.OpenWrite(path, cancellationToken), _lock, () => CountWrite(path), deferred, deferred != null && parked != null ? () => parked(path) : null);
            var record = RecordWritesOf;
            if (record != null && path.Full.EndsWith(record))
            {
                stream = new RecordingStream(stream, WriteEvents);
            }
            var tear = TearWriteOf;
            if (tear != null && path.Full.EndsWith(tear))
            {
                TearWriteOf = null;
                Interlocked.Increment(ref Tears);
                return new TornStream(stream, TearAfterBytes);
            }
            return stream;
        }

        public async Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default)
        {
            await Record("Ren", name);
            await Locked(async () =>
            {
                await _inner.Ren(name, newName, cancellationToken);
                CountWrite(newName);
            }, cancellationToken);
        }

        public async Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default)
        {
            await Record("ReadText", path);
            using var copy = await ReadCopy(path, cancellationToken);
            return copy == null ? null : (encoding ?? Encoding.UTF8).GetString(copy.ToArray());
        }

        public async Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default)
        {
            await Record("WriteText", path);
            await Locked(async () =>
            {
                await _inner.WriteText(path, contents, encoding, cancellationToken);
                CountWrite(path);
            }, cancellationToken);
        }

        public async Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
        {
            await Record("Ls", path);
            // Stamped under the lock that publishes, so an entry and its identity describe the same bytes
            var entries = await Locked(async () =>
            {
                var listed = await _inner.Ls(path, recurse, cancellationToken);
                return StampIdentity ? listed.Select(Stamp).ToList() : listed.ToList();
            }, cancellationToken);
            var hidden = Hidden;
            return hidden == null ? entries : entries.Where(x => !hidden(x.Path)).ToList();
        }

        public async Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
        {
            await Record("OpenRead", path);
            if (Hidden?.Invoke(path) == true)
            {
                return null;
            }
            return await ReadCopy(path, cancellationToken);
        }

        private async Task<MemoryStream?> ReadCopy(IOPath path, CancellationToken cancellationToken)
        {
            await _lock.WaitAsync(cancellationToken);
            try
            {
                var stream = await _inner.OpenRead(path, cancellationToken);
                if (stream == null)
                {
                    return null;
                }
                try
                {
                    var copy = new MemoryStream();
                    await stream.CopyToAsync(copy, cancellationToken);
                    copy.Position = 0;
                    return copy;
                }
                finally
                {
                    // Disposing the in-memory store's stream adds its file back, undoing a remove made during the read
                    if (stream is not MemoryStream)
                    {
                        await stream.DisposeAsync();
                    }
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default)
        {
            using var copy = await ReadCopy(path, cancellationToken);
            return copy == null ? default : JsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(copy.ToArray()));
        }

        public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default)
        {
            return Locked(async () =>
            {
                await _inner.WriteAsJson(path, value, writeIndented, cancellationToken);
                CountWrite(path);
            }, cancellationToken);
        }

        public async Task Rm(IOPath path, CancellationToken cancellationToken = default)
        {
            await Record("Rm", path);
            await Locked(async () =>
            {
                await _inner.Rm(path, cancellationToken);
                CountWrite(path);
            }, cancellationToken);
        }

        public async Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default)
        {
            await Record("Exists", path);
            if (Hidden?.Invoke(path) == true)
            {
                return false;
            }
            return await Locked(() => _inner.Exists(path, cancellationToken), cancellationToken);
        }

        public async Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default)
        {
            await Record("Stat", path);
            return await Locked(async () =>
            {
                var entry = await _inner.Stat(path, cancellationToken);
                return entry != null && StampIdentity ? Stamp(entry) : entry;
            }, cancellationToken);
        }

        public void Dispose()
        {
            _inner.Dispose();
        }

        // Disposing a write stream publishes the file, so it takes the store's lock
        private sealed class SerializedDisposeStream : Stream
        {
            private readonly Stream _inner;
            private readonly SemaphoreSlim _lock;
            private readonly Action _published;
            private readonly Task? _deferred;
            private readonly Action? _parked;
            private int _disposed;

            public SerializedDisposeStream(Stream inner, SemaphoreSlim @lock, Action published, Task? deferred, Action? parked)
            {
                _inner = inner;
                _lock = @lock;
                _published = published;
                _deferred = deferred;
                _parked = parked;
            }

            public override bool CanRead => _inner.CanRead;
            public override bool CanSeek => _inner.CanSeek;
            public override bool CanWrite => _inner.CanWrite;
            public override long Length => _inner.Length;
            public override long Position { get => _inner.Position; set => _inner.Position = value; }

            public override void Flush() => _inner.Flush();

            public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

            public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

            public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

            public override void SetLength(long value) => _inner.SetLength(value);

            public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

            public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.WriteAsync(buffer, offset, count, cancellationToken);

            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => _inner.WriteAsync(buffer, cancellationToken);

            protected override void Dispose(bool disposing)
            {
                if (disposing && Interlocked.Exchange(ref _disposed, 1) == 0)
                {
                    _parked?.Invoke();
                    _deferred?.GetAwaiter().GetResult();
                    _lock.Wait();
                    try
                    {
                        _inner.Dispose();
                        _published();
                    }
                    finally
                    {
                        _lock.Release();
                    }
                }
            }

            public override async ValueTask DisposeAsync()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                {
                    if (_deferred != null)
                    {
                        _parked?.Invoke();
                        await _deferred;
                    }
                    await _lock.WaitAsync();
                    try
                    {
                        await _inner.DisposeAsync();
                        _published();
                    }
                    finally
                    {
                        _lock.Release();
                    }
                }
                GC.SuppressFinalize(this);
            }
        }

        private sealed class RecordingStream : Stream
        {
            private readonly Stream _inner;
            private readonly ConcurrentQueue<string> _events;

            public RecordingStream(Stream inner, ConcurrentQueue<string> events)
            {
                _inner = inner;
                _events = events;
            }

            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => throw new NotSupportedException();
            public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

            public override void Flush()
            {
                _events.Enqueue("F");
                _inner.Flush();
            }

            public override async Task FlushAsync(CancellationToken cancellationToken)
            {
                _events.Enqueue("F");
                await _inner.FlushAsync(cancellationToken);
            }

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count)
            {
                _events.Enqueue($"W{count}");
                _inner.Write(buffer, offset, count);
            }

            public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                _events.Enqueue($"W{buffer.Length}");
                await _inner.WriteAsync(buffer, cancellationToken);
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _inner.Dispose();
                }
            }

            public override async ValueTask DisposeAsync()
            {
                await _inner.DisposeAsync();
                GC.SuppressFinalize(this);
            }
        }

        // Keeps the first bytes, then the process "dies"
        private sealed class TornStream : Stream
        {
            private readonly Stream _inner;
            private readonly int _keep;
            private int _written;
            private bool _torn;

            public TornStream(Stream inner, int keep)
            {
                _inner = inner;
                _keep = keep;
            }

            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => throw new NotSupportedException();
            public override long Position { get => _written; set => throw new NotSupportedException(); }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count)
            {
                var room = _keep - _written;
                if (count <= room)
                {
                    _inner.Write(buffer, offset, count);
                    _written += count;
                    return;
                }
                if (room > 0)
                {
                    _inner.Write(buffer, offset, room);
                    _written += room;
                }
                _torn = true;
                _inner.Dispose();
                throw new CrashException();
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                Write(buffer, offset, count);
                return Task.CompletedTask;
            }

            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                Write(buffer.ToArray(), 0, buffer.Length);
                return ValueTask.CompletedTask;
            }

            public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            protected override void Dispose(bool disposing)
            {
                if (disposing && !_torn)
                {
                    _inner.Dispose();
                }
            }

            public override async ValueTask DisposeAsync()
            {
                if (!_torn)
                {
                    await _inner.DisposeAsync();
                }
                GC.SuppressFinalize(this);
            }
        }
    }

    // The wrapper over local disk keeps its native paths reachable
    internal sealed class LocalDiskHookFileStorage : HookFileStorage, ILocalDiskFileStorage
    {
        public LocalDiskHookFileStorage(IFileStorage inner) : base(inner)
        {
        }

        public string ToNativeLocalPath(IOPath path) => ((ILocalDiskFileStorage)Inner).ToNativeLocalPath(path);
    }

    // A hook wrapper the connector would own, so cleanup classifies the storage below it
    internal sealed class OwnedHookFileStorage : HookFileStorage, Internal.Maintenance.IConnectorOwnedStorage
    {
        public OwnedHookFileStorage(IFileStorage inner) : base(inner)
        {
        }

        public static HookFileStorage Over(IFileStorage inner)
        {
            return inner is ILocalDiskFileStorage ? new OwnedLocalDiskHookFileStorage(inner) : new OwnedHookFileStorage(inner);
        }
    }

    // The owned wrapper over local disk, which keeps native paths reachable
    internal sealed class OwnedLocalDiskHookFileStorage : HookFileStorage, Internal.Maintenance.IConnectorOwnedStorage, ILocalDiskFileStorage
    {
        public OwnedLocalDiskHookFileStorage(IFileStorage inner) : base(inner)
        {
        }

        public string ToNativeLocalPath(IOPath path) => ((ILocalDiskFileStorage)Inner).ToNativeLocalPath(path);
    }
}

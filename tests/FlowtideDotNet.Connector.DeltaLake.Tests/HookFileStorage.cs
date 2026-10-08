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
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// Counts requests and injects torn writes or actions before a request.
    /// Stowage's in memory storage hands every reader of a file the same stream, so reads are copied one at a time.
    /// </summary>
    internal class HookFileStorage : IFileStorage
    {
        private readonly IFileStorage _inner;
        private readonly ConcurrentQueue<string> _requests = new ConcurrentQueue<string>();
        // Shared by every wrapper, two wrappers over one in-memory store would get the same stream
        private static readonly SemaphoreSlim s_readLock = new SemaphoreSlim(1, 1);
        private static readonly AsyncLocal<bool> s_unrecorded = new AsyncLocal<bool>();

        public HookFileStorage(IFileStorage inner)
        {
            _inner = inner;
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

        private async Task Record(string verb, IOPath? path)
        {
            if (!s_unrecorded.Value)
            {
                _requests.Enqueue($"{verb} {path?.Full}");
            }
            if (Before != null && path != null)
            {
                await Before(verb, path);
            }
        }

        public async Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default)
        {
            await Record("OpenWrite", path);
            var stream = await _inner.OpenWrite(path, cancellationToken);
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
            await _inner.Ren(name, newName, cancellationToken);
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
            await _inner.WriteText(path, contents, encoding, cancellationToken);
        }

        public async Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
        {
            await Record("Ls", path);
            var entries = await _inner.Ls(path, recurse, cancellationToken);
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
            await s_readLock.WaitAsync(cancellationToken);
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
                s_readLock.Release();
            }
        }

        public async Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default)
        {
            using var copy = await ReadCopy(path, cancellationToken);
            return copy == null ? default : JsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(copy.ToArray()));
        }

        public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default)
        {
            return _inner.WriteAsJson(path, value, writeIndented, cancellationToken);
        }

        public async Task Rm(IOPath path, CancellationToken cancellationToken = default)
        {
            await Record("Rm", path);
            await _inner.Rm(path, cancellationToken);
        }

        public async Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default)
        {
            await Record("Exists", path);
            if (Hidden?.Invoke(path) == true)
            {
                return false;
            }
            return await _inner.Exists(path, cancellationToken);
        }

        public async Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default)
        {
            await Record("Stat", path);
            return await _inner.Stat(path, cancellationToken);
        }

        public void Dispose()
        {
            _inner.Dispose();
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
        }
    }
}

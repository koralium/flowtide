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
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// Models a cloud store's write stream: the object appears only when the stream is disposed, holding whatever was written,
    /// even after a fault. A stream that is never disposed commits nothing.
    /// </summary>
    internal sealed class CommitOnDisposeStorage : IFileStorage
    {
        private readonly IFileStorage _inner;

        public CommitOnDisposeStorage(IFileStorage inner)
        {
            _inner = inner;
        }

        public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default)
        {
            return Task.FromResult<Stream>(new CommitOnDisposeStream(_inner, path));
        }

        public Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default) => _inner.OpenRead(path, cancellationToken);

        public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => _inner.Ren(name, newName, cancellationToken);

        public Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default) => _inner.ReadText(path, encoding, cancellationToken);

        public async Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default)
        {
            await using var stream = await OpenWrite(path, cancellationToken);
            var bytes = (encoding ?? Encoding.UTF8).GetBytes(contents);
            await stream.WriteAsync(bytes, cancellationToken);
        }

        public Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default) => _inner.Ls(path, recurse, cancellationToken);

        public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default) => _inner.ReadAsJson<T>(path, cancellationToken);

        public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => _inner.WriteAsJson(path, value, writeIndented, cancellationToken);

        public Task Rm(IOPath path, CancellationToken cancellationToken = default) => _inner.Rm(path, cancellationToken);

        public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => _inner.Exists(path, cancellationToken);

        public Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default) => _inner.Stat(path, cancellationToken);

        public void Dispose() => _inner.Dispose();

        // Buffers the bytes and commits them on dispose; it has no finalizer, so an abandoned stream commits nothing
        private sealed class CommitOnDisposeStream : Stream
        {
            private readonly IFileStorage _inner;
            private readonly IOPath _path;
            private readonly MemoryStream _buffer = new MemoryStream();
            private bool _committed;

            public CommitOnDisposeStream(IFileStorage inner, IOPath path)
            {
                _inner = inner;
                _path = path;
            }

            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => _buffer.Length;
            public override long Position { get => _buffer.Position; set => throw new NotSupportedException(); }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count) => _buffer.Write(buffer, offset, count);

            public override void Write(ReadOnlySpan<byte> buffer) => _buffer.Write(buffer);

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                _buffer.Write(buffer, offset, count);
                return Task.CompletedTask;
            }

            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                _buffer.Write(buffer.Span);
                return ValueTask.CompletedTask;
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    CommitAsync().AsTask().GetAwaiter().GetResult();
                }
            }

            public override async ValueTask DisposeAsync()
            {
                await CommitAsync();
                GC.SuppressFinalize(this);
            }

            private async ValueTask CommitAsync()
            {
                if (_committed)
                {
                    return;
                }
                _committed = true;
                await using var target = await _inner.OpenWrite(_path);
                await target.WriteAsync(_buffer.GetBuffer().AsMemory(0, (int)_buffer.Length));
            }
        }
    }
}

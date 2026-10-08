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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    /// <summary>
    /// The bytes of a commit file in fixed size chunks, a commit is neither limited to one array nor copied while it grows.
    /// </summary>
    internal sealed class CommitBytes
    {
        internal const int DefaultChunkSize = 1024 * 1024;
        private const int DefaultInitialSize = 4096;

        private readonly int _chunkSize;
        private readonly int _initialSize;
        private readonly List<byte[]> _chunks = new List<byte[]>();
        private long _length;

        public CommitBytes(int chunkSize = DefaultChunkSize, int initialSize = DefaultInitialSize)
        {
            _chunkSize = chunkSize;
            _initialSize = Math.Min(initialSize, chunkSize);
        }

        public long Length => _length;

        // The allocated bytes
        internal long Capacity => _chunks.Sum(x => (long)x.Length);

        // The first chunk starts small and doubles, a commit that needs a second chunk gets full ones
        public void Append(ReadOnlySpan<byte> bytes)
        {
            while (!bytes.IsEmpty)
            {
                var index = (int)(_length / _chunkSize);
                if (index == _chunks.Count)
                {
                    _chunks.Add(new byte[index == 0 ? _initialSize : _chunkSize]);
                }
                var chunk = _chunks[index];
                var offset = (int)(_length % _chunkSize);
                if (offset == chunk.Length)
                {
                    Array.Resize(ref chunk, Math.Min(chunk.Length * 2, _chunkSize));
                    _chunks[index] = chunk;
                }
                var count = Math.Min(bytes.Length, chunk.Length - offset);
                bytes.Slice(0, count).CopyTo(chunk.AsSpan(offset));
                _length += count;
                bytes = bytes.Slice(count);
            }
        }

        public static async Task<CommitBytes?> Read(IFileStorage storage, IOPath path, int chunkSize = DefaultChunkSize)
        {
            using var stream = await storage.OpenRead(path);
            if (stream == null)
            {
                return null;
            }
            var bytes = new CommitBytes(chunkSize);
            var buffer = new byte[Math.Min(chunkSize, 81920)];
            int read;
            while ((read = await stream.ReadAsync(buffer)) > 0)
            {
                bytes.Append(buffer.AsSpan(0, read));
            }
            return bytes;
        }

        // The used part of each chunk in order
        private IEnumerable<ReadOnlyMemory<byte>> Segments()
        {
            for (int i = 0; i < _chunks.Count; i++)
            {
                var used = i < _chunks.Count - 1 ? _chunkSize : (int)(_length - (long)i * _chunkSize);
                yield return _chunks[i].AsMemory(0, used);
            }
        }

        public bool StartsWith(ReadOnlySpan<byte> prefix)
        {
            if (prefix.Length > _length)
            {
                return false;
            }
            foreach (var segment in Segments())
            {
                var count = Math.Min(prefix.Length, segment.Length);
                if (!segment.Span.Slice(0, count).SequenceEqual(prefix.Slice(0, count)))
                {
                    return false;
                }
                prefix = prefix.Slice(count);
                if (prefix.IsEmpty)
                {
                    return true;
                }
            }
            return prefix.IsEmpty;
        }

        // Every byte of the other commit is this commit's at the same position
        public bool StartsWith(CommitBytes other)
        {
            if (other._length > _length)
            {
                return false;
            }
            long position = 0;
            foreach (var segment in other.Segments())
            {
                if (!StartsWithAt(segment.Span, position))
                {
                    return false;
                }
                position += segment.Length;
            }
            return true;
        }

        public bool ContentEquals(CommitBytes other)
        {
            return _length == other._length && StartsWith(other);
        }

        private bool StartsWithAt(ReadOnlySpan<byte> bytes, long position)
        {
            while (!bytes.IsEmpty)
            {
                var chunk = _chunks[(int)(position / _chunkSize)];
                var offset = (int)(position % _chunkSize);
                var count = Math.Min(bytes.Length, _chunkSize - offset);
                if (!chunk.AsSpan(offset, count).SequenceEqual(bytes.Slice(0, count)))
                {
                    return false;
                }
                position += count;
                bytes = bytes.Slice(count);
            }
            return true;
        }

        /// <summary>
        /// The bytes before the first newline, null when there is none.
        /// </summary>
        public byte[]? FirstLine()
        {
            long end = -1;
            long position = 0;
            foreach (var segment in Segments())
            {
                var index = segment.Span.IndexOf((byte)'\n');
                if (index >= 0)
                {
                    end = position + index;
                    break;
                }
                position += segment.Length;
            }
            if (end < 0)
            {
                return null;
            }
            var line = new byte[end];
            long copied = 0;
            foreach (var segment in Segments())
            {
                var count = (int)Math.Min(segment.Length, end - copied);
                segment.Span.Slice(0, count).CopyTo(line.AsSpan((int)copied));
                copied += count;
                if (copied == end)
                {
                    break;
                }
            }
            return line;
        }

        // The first bytes are flushed on their own, a torn copy then still names its stage
        public async Task WriteTo(Stream stream, int flushAfter)
        {
            long written = 0;
            foreach (var segment in Segments())
            {
                var data = segment;
                if (written < flushAfter && written + data.Length >= flushAfter)
                {
                    var head = (int)(flushAfter - written);
                    await stream.WriteAsync(data.Slice(0, head));
                    await stream.FlushAsync();
                    data = data.Slice(head);
                    written += head;
                }
                await stream.WriteAsync(data);
                written += data.Length;
            }
        }

        // Small commits only, an array holds at most 2 GB
        public byte[] ToArray()
        {
            var array = new byte[_length];
            long position = 0;
            foreach (var segment in Segments())
            {
                segment.Span.CopyTo(array.AsSpan((int)position));
                position += segment.Length;
            }
            return array;
        }

        /// <summary>
        /// A write-only stream that appends to the bytes.
        /// </summary>
        public Stream AsAppendStream()
        {
            return new AppendStream(this);
        }

        private sealed class AppendStream : Stream
        {
            private readonly CommitBytes _bytes;

            public AppendStream(CommitBytes bytes)
            {
                _bytes = bytes;
            }

            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => _bytes.Length;
            public override long Position { get => _bytes.Length; set => throw new NotSupportedException(); }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count)
            {
                _bytes.Append(buffer.AsSpan(offset, count));
            }

            public override void Write(ReadOnlySpan<byte> buffer)
            {
                _bytes.Append(buffer);
            }

            public override void WriteByte(byte value)
            {
                _bytes.Append(new ReadOnlySpan<byte>(in value));
            }
        }
    }
}

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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(PinnedClientCollection))]
    public class DeletionVectorReaderTests
    {
        private const int Mib = 1 << 20;

        private static readonly long[] s_first = new long[] { 1, 5, 9 };
        private static readonly long[] s_second = new long[] { 2, 6, 70_000 };

        public enum StreamFault
        {
            None,
            Short,
            Read,
            Dispose,
            FirstLength,
            SecondLength
        }

        private static TrackedStorage NewStorage(string name)
        {
            return new TrackedStorage(Files.Of.InternalMemory($"./{name}"));
        }

        // Padding before each entry, as foreign files may have
        private static (byte[] File, DeletionVector[] Vectors) Build(string name, int padding, long size, params long[][] entries)
        {
            using var file = new MemoryStream();
            file.WriteByte(1);
            var vectors = new List<DeletionVector>();
            foreach (var positions in entries)
            {
                file.Write(new byte[padding]);
                var frame = DeletionVectorWriter.Frame(RoaringBitmapArray.Create(positions));
                vectors.Add(new DeletionVector()
                {
                    StorageType = "p",
                    PathOrInlineDv = name,
                    Offset = file.Position,
                    SizeInBytes = frame.Length - DeletionVectorWriter.FrameOverhead,
                    Cardinality = positions.Length
                });
                file.Write(frame);
            }
            if (file.Length < size)
            {
                file.Write(new byte[size - file.Length]);
            }
            return (file.ToArray(), vectors.ToArray());
        }

        private static Task<DeletionVector[]> WriteFile(IFileStorage storage, string name, int padding, params long[][] entries)
        {
            return WriteSized(storage, name, padding, 0, entries);
        }

        private static async Task<DeletionVector[]> WriteSized(IFileStorage storage, string name, int padding, long size, params long[][] entries)
        {
            var (file, vectors) = Build(name, padding, size, entries);
            await using var stream = await storage.OpenWrite($"/t/{name}");
            await stream.WriteAsync(file);
            return vectors;
        }

        [Fact]
        public async Task APackedFileIsFetchedOnce()
        {
            var storage = NewStorage(nameof(APackedFileIsFetchedOnce));
            var vectors = await WriteFile(storage, "a.bin", 0, s_first, s_second);

            var reader = new DeletionVectorReader(storage, "/t");

            Assert.Equal(s_second, await reader.Read(vectors[1]));
            Assert.Equal(s_first, await reader.Read(vectors[0]));
            Assert.Equal(s_second, await reader.Read(vectors[1]));
            Assert.Single(storage.Opened);
            Assert.True(storage.Opened[0].Disposed);
        }

        [Fact]
        public async Task EachFileIsFetchedOnce()
        {
            var storage = NewStorage(nameof(EachFileIsFetchedOnce));
            var a = await WriteFile(storage, "a.bin", 0, s_first, s_second);
            var b = await WriteFile(storage, "b.bin", 0, s_second, s_first);

            var reader = new DeletionVectorReader(storage, "/t");

            Assert.Equal(s_first, await reader.Read(a[0]));
            Assert.Equal(s_second, await reader.Read(b[0]));
            Assert.Equal(s_second, await reader.Read(a[1]));
            Assert.Equal(s_first, await reader.Read(b[1]));
            Assert.Equal(2, storage.Opened.Count);
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.S3)]
        public async Task TheObjectStoreClientsFetchAPackedFileOnce(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var (file, vectors) = Build("a.bin", 0, 0, s_first, s_second);
            server.Put("t/a.bin", file);
            using var storage = server.CreateStorage();

            var reader = new DeletionVectorReader(storage, "/c/t");

            Assert.Equal(s_second, await reader.Read(vectors[1]));
            Assert.Equal(s_first, await reader.Read(vectors[0]));
            Assert.Equal(new[] { "GET /c/t/a.bin HTTP/1.1" }, server.Requests);
        }

        [Theory]
        [InlineData(16 * Mib, 1)]
        [InlineData(16 * Mib + 1, 2)]
        public async Task AFileUpToTheLimitIsFetchedOnce(long size, int fetches)
        {
            var storage = NewStorage(nameof(AFileUpToTheLimitIsFetchedOnce) + size);
            var vectors = await WriteSized(storage, "a.bin", 0, size, s_first, s_second);

            var reader = new DeletionVectorReader(storage, "/t");

            Assert.Equal(s_first, await reader.Read(vectors[0]));
            Assert.Equal(s_second, await reader.Read(vectors[1]));
            Assert.Equal(fetches, storage.Opened.Count);
            Assert.All(storage.Opened, x => Assert.True(x.Disposed));
        }

        [Fact]
        public async Task AFileOverTheLimitIsFetchedPerVector()
        {
            var storage = NewStorage(nameof(AFileOverTheLimitIsFetchedPerVector));
            var vectors = await WriteFile(storage, "large.bin", 9 * Mib, s_first, s_second);

            var reader = new DeletionVectorReader(storage, "/t");

            Assert.Equal(s_first, await reader.Read(vectors[0]));
            Assert.Equal(s_second, await reader.Read(vectors[1]));
            Assert.Equal(2, storage.Opened.Count);
            Assert.All(storage.Opened, x => Assert.True(x.Disposed));
        }

        [Fact]
        public async Task AFileOverTheLimitKeepsWhatWasKept()
        {
            var storage = NewStorage(nameof(AFileOverTheLimitKeepsWhatWasKept));
            var a = await WriteFile(storage, "a.bin", 0, s_first);
            var large = await WriteSized(storage, "large.bin", 0, 16 * Mib + 1, s_second);

            var reader = new DeletionVectorReader(storage, "/t");

            Assert.Equal(s_first, await reader.Read(a[0]));
            Assert.Equal(s_second, await reader.Read(large[0]));
            Assert.Equal(s_first, await reader.Read(a[0]));
            Assert.Equal(2, storage.Opened.Count);
        }

        [Fact]
        public async Task TwoFilesThatFillTheLimitAreBothKept()
        {
            var storage = NewStorage(nameof(TwoFilesThatFillTheLimitAreBothKept));
            var a = await WriteSized(storage, "a.bin", 0, 8 * Mib, s_first);
            var b = await WriteSized(storage, "b.bin", 0, 8 * Mib, s_second);

            var reader = new DeletionVectorReader(storage, "/t");

            Assert.Equal(s_first, await reader.Read(a[0]));
            Assert.Equal(s_second, await reader.Read(b[0]));
            Assert.Equal(s_first, await reader.Read(a[0]));
            Assert.Equal(s_second, await reader.Read(b[0]));
            Assert.Equal(2, storage.Opened.Count);
        }

        [Fact]
        public async Task KeepingPastTheLimitDropsWhatWasKept()
        {
            var storage = NewStorage(nameof(KeepingPastTheLimitDropsWhatWasKept));
            var a = await WriteFile(storage, "a.bin", 9 * Mib, s_first);
            var b = await WriteFile(storage, "b.bin", 9 * Mib, s_second);

            var reader = new DeletionVectorReader(storage, "/t");

            Assert.Equal(s_first, await reader.Read(a[0]));
            Assert.Equal(s_second, await reader.Read(b[0]));
            Assert.Equal(2, storage.Opened.Count);
            Assert.Equal(s_second, await reader.Read(b[0]));
            Assert.Equal(2, storage.Opened.Count);
            Assert.Equal(s_first, await reader.Read(a[0]));
            Assert.Equal(3, storage.Opened.Count);

            var c = await WriteFile(storage, "c.bin", 0, s_second);
            storage.Opened.Clear();
            Assert.Equal(s_second, await reader.Read(c[0]));
            Assert.Equal(s_first, await reader.Read(a[0]));
            Assert.Single(storage.Opened);
        }

        [Fact]
        public async Task AFileReadInSmallPiecesIsKeptWhole()
        {
            var storage = NewStorage(nameof(AFileReadInSmallPiecesIsKeptWhole));
            var vectors = await WriteFile(storage, "a.bin", 4_097, s_first, s_second);
            storage.Chunk = 7;

            var reader = new DeletionVectorReader(storage, "/t");

            Assert.Equal(s_second, await reader.Read(vectors[1]));
            Assert.Equal(s_first, await reader.Read(vectors[0]));
            Assert.Single(storage.Opened);
            Assert.True(storage.Opened[0].Disposed);
        }

        [Theory]
        [InlineData(StreamFault.Short)]
        [InlineData(StreamFault.Read)]
        [InlineData(StreamFault.Dispose)]
        public async Task AFailedFetchKeepsNothing(StreamFault fault)
        {
            var storage = NewStorage(nameof(AFailedFetchKeepsNothing) + fault);
            var vectors = await WriteFile(storage, "a.bin", 4_097, s_first, s_second);
            storage.Fault = fault;

            var reader = new DeletionVectorReader(storage, "/t");

            await Assert.ThrowsAnyAsync<IOException>(() => reader.Read(vectors[1]));
            storage.Fault = StreamFault.None;
            Assert.Equal(s_first, await reader.Read(vectors[0]));
            Assert.Equal(s_second, await reader.Read(vectors[1]));
            Assert.Equal(2, storage.Opened.Count);
            Assert.All(storage.Opened, x => Assert.True(x.Disposed));
        }

        [Theory]
        [InlineData(0, false)]
        [InlineData(4_097, false)]
        [InlineData(4_097, true)]
        public async Task AnUnseekableFileFindsItsVector(int padding, bool second)
        {
            var storage = NewStorage(nameof(AnUnseekableFileFindsItsVector) + padding + second);
            var vectors = await WriteFile(storage, "a.bin", padding, s_first, s_second);
            storage.Seekable = false;

            var reader = new DeletionVectorReader(storage, "/t");

            Assert.Equal(second ? s_second : s_first, await reader.Read(vectors[second ? 1 : 0]));
            Assert.True(storage.Opened.Single().Disposed);
        }

        // Absent, zero and one all mean the first entry
        [Theory]
        [InlineData(null)]
        [InlineData(1L)]
        [InlineData(0L)]
        public async Task AVectorAtTheStartOfTheFile(long? offset)
        {
            var storage = NewStorage(nameof(AVectorAtTheStartOfTheFile) + offset);
            var vectors = await WriteFile(storage, "a.bin", 0, s_first, s_second);
            vectors[0].Offset = offset;

            Assert.Equal(s_first, await new DeletionVectorReader(storage, "/t").Read(vectors[0]));
        }

        [Fact]
        public async Task AMissingFileIsFetchedAgain()
        {
            var storage = NewStorage(nameof(AMissingFileIsFetchedAgain));
            var vector = new DeletionVector() { StorageType = "p", PathOrInlineDv = "a.bin", Offset = 1 };
            var reader = new DeletionVectorReader(storage, "/t");

            await Assert.ThrowsAsync<DeltaFileNotFoundException>(() => reader.Read(vector));

            var written = await WriteFile(storage, "a.bin", 0, s_first);
            Assert.Equal(s_first, await reader.Read(written[0]));
        }

        [Fact]
        public async Task AVectorPastTheEndOfTheFileKeepsNothing()
        {
            var storage = NewStorage(nameof(AVectorPastTheEndOfTheFileKeepsNothing));
            var vectors = await WriteFile(storage, "a.bin", 0, s_first);
            var bad = new DeletionVector() { StorageType = "p", PathOrInlineDv = "a.bin", Offset = 1_000 };

            var reader = new DeletionVectorReader(storage, "/t");

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reader.Read(bad));
            Assert.Equal(s_first, await reader.Read(vectors[0]));
            Assert.Equal(2, storage.Opened.Count);
        }

        [Theory]
        [InlineData(StreamFault.FirstLength)]
        [InlineData(StreamFault.SecondLength)]
        public async Task AFailedLengthCheckDisposesTheFile(StreamFault fault)
        {
            var storage = NewStorage(nameof(AFailedLengthCheckDisposesTheFile) + fault);
            var vectors = await WriteFile(storage, "a.bin", 0, s_first);
            storage.Fault = fault;
            var reader = new DeletionVectorReader(storage, "/t");

            await Assert.ThrowsAsync<IOException>(() => reader.Read(vectors[0]));

            Assert.True(storage.Opened.Single().Disposed);
        }

        private sealed class TrackedStorage : ForwardingStorage, IFileStorage
        {
            public TrackedStorage(IFileStorage inner) : base(inner)
            {
            }

            public List<TrackedStream> Opened { get; } = new List<TrackedStream>();

            public int Chunk { get; set; } = int.MaxValue;

            public bool Seekable { get; set; } = true;

            public StreamFault Fault { get; set; }

            public new async Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
            {
                var stream = await Inner.OpenRead(path, cancellationToken);
                if (stream == null)
                {
                    return null;
                }
                var tracked = new TrackedStream(stream, Chunk, Seekable, Fault);
                Opened.Add(tracked);
                return tracked;
            }
        }

        private sealed class TrackedStream : Stream
        {
            private readonly Stream _inner;
            private readonly int _chunk;
            private readonly bool _seekable;
            private readonly StreamFault _fault;
            private readonly long _end;
            private int _lengthReads;

            public TrackedStream(Stream inner, int chunk, bool seekable, StreamFault fault)
            {
                _inner = inner;
                _chunk = chunk;
                _seekable = seekable;
                _fault = fault;
                _end = fault is StreamFault.Short or StreamFault.Read ? inner.Length / 2 : long.MaxValue;
            }

            public bool Disposed { get; private set; }

            public override bool CanRead => true;

            public override bool CanSeek => _seekable;

            public override bool CanWrite => false;

            public override long Length
            {
                get
                {
                    _lengthReads++;
                    if ((_fault == StreamFault.FirstLength && _lengthReads == 1) || (_fault == StreamFault.SecondLength && _lengthReads == 2))
                    {
                        throw new IOException("The length check failed");
                    }
                    return _seekable ? _inner.Length : throw new NotSupportedException();
                }
            }

            public override long Position
            {
                get => _seekable ? _inner.Position : throw new NotSupportedException();
                set => _inner.Position = _seekable ? value : throw new NotSupportedException();
            }

            public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

            public override int Read(Span<byte> buffer)
            {
                if (_inner.Position >= _end)
                {
                    return _fault == StreamFault.Read ? throw new IOException("The read failed") : 0;
                }
                var count = (int)Math.Min(Math.Min(buffer.Length, _chunk), _end - _inner.Position);
                return _inner.Read(buffer.Slice(0, count));
            }

            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                return new ValueTask<int>(Read(buffer.Span));
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return Task.FromResult(Read(buffer.AsSpan(offset, count)));
            }

            public override void Flush()
            {
            }

            public override long Seek(long offset, SeekOrigin origin) => _seekable ? _inner.Seek(offset, origin) : throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            protected override void Dispose(bool disposing)
            {
                if (disposing && !Disposed)
                {
                    Disposed = true;
                    _inner.Dispose();
                    if (_fault == StreamFault.Dispose)
                    {
                        throw new IOException("The close failed");
                    }
                }
                base.Dispose(disposing);
            }
        }
    }
}

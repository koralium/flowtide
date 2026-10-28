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

using Apache.Arrow;
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using ParquetSharp;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class ParquetDataFileTests
    {
        [Fact]
        public async Task AMissingFileIsNotFound()
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());

            await Assert.ThrowsAsync<DeltaFileNotFoundException>(() => ParquetSharpReader.OpenDataFile(storage, "/t", "gone.parquet", forDeletes: true));
        }

        [Fact]
        public async Task ACorruptFileClosesItsStream()
        {
            var stream = new WatchedStream(new byte[100]);

            await Assert.ThrowsAsync<ParquetException>(() => ParquetSharpReader.OpenDataFile(new StreamStorage(stream), "/t", "data.parquet", forDeletes: true));
            Assert.True(stream.Disposed);
        }

        [Fact]
        public async Task RowGroupsStartWhereThePreviousEnds()
        {
            using var file = await Open(Write(5, maxRowGroupLength: 3));

            Assert.Equal(5, file.NumRows);
            Assert.Equal(new long[] { 0, 3 }, file.RowGroupStarts);
        }

        [Fact]
        public async Task AFooterCountingOtherRowsIsADecodeError()
        {
            // 1000 rows is the varint D0 0F after an i64 field header
            var bytes = Write(1000);
            var footerLength = BitConverter.ToInt32(bytes, bytes.Length - 8);
            var footer = bytes.Length - 8 - footerLength;
            var at = bytes.AsSpan(footer, footerLength).IndexOf(new byte[] { 0x16, 0xD0, 0x0F });
            Assert.True(at >= 0, "the footer row count was not found");
            // The first match is the file's count, then the row group's
            bytes[footer + at + 1] = 0xD2;
            var stream = new WatchedStream(bytes);

            var error = await Assert.ThrowsAsync<InvalidDataException>(() => ParquetSharpReader.OpenDataFile(new StreamStorage(stream), "/t", "data.parquet", forDeletes: true));

            Assert.Equal("Delta file data.parquet cannot be decoded: the footer has 1001 rows and its row groups 1000", error.Message);
            Assert.True(stream.Disposed);
        }

        private static byte[] Write(int rows, long maxRowGroupLength = 1024 * 1024)
        {
            var schema = new Schema.Builder().Field(new Field("a", Int32Type.Default, true)).Build();
            using var batch = new RecordBatch(schema, new[] { new Int32Array.Builder().AppendRange(new int[rows]).Build() }, rows);
            using var stream = new MemoryStream();
            using var properties = new WriterPropertiesBuilder().MaxRowGroupLength(maxRowGroupLength).Build();
            using (var writer = new ParquetSharp.Arrow.FileWriter(stream, schema, properties, leaveOpen: true))
            {
                writer.WriteRecordBatch(batch);
                writer.Close();
            }
            return stream.ToArray();
        }

        private static async Task<ParquetDataFile> Open(byte[] bytes)
        {
            var storage = Files.Of.InternalMemory("./" + ScratchOwners.ShortId());
            using (var write = await storage.OpenWrite("/t/data.parquet"))
            {
                await write.WriteAsync(bytes);
            }
            return await ParquetSharpReader.OpenDataFile(storage, "/t", "data.parquet", forDeletes: true);
        }

        private sealed class WatchedStream : MemoryStream
        {
            public WatchedStream(byte[] bytes) : base(bytes)
            {
            }

            public bool Disposed { get; private set; }

            protected override void Dispose(bool disposing)
            {
                Disposed = true;
                base.Dispose(disposing);
            }
        }

        // Opens the one given stream for every read
        private sealed class StreamStorage : ForwardingStorage, IFileStorage
        {
            private readonly Stream _stream;

            public StreamStorage(Stream stream) : base(Files.Of.InternalMemory($"./stream-{Guid.NewGuid():N}"))
            {
                _stream = stream;
            }

            Task<Stream?> IFileStorage.OpenRead(IOPath path, CancellationToken cancellationToken) => Task.FromResult<Stream?>(_stream);
        }
    }
}

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

using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using System.Buffers;

namespace FlowtideDotNet.Storage.Tests.Reservoir
{
    public class ReadOnlySequenceStreamTests
    {
        private static ReadOnlySequence<byte> CreateSingleSegmentSequence(byte[] data)
        {
            return new ReadOnlySequence<byte>(data);
        }

        private static ReadOnlySequence<byte> CreateMultiSegmentSequence(params byte[][] segments)
        {
            var first = new BufferSegment(new ReadOnlyMemory<byte>(segments[0]));
            var current = first;
            for (int i = 1; i < segments.Length; i++)
            {
                var next = new BufferSegment(new ReadOnlyMemory<byte>(segments[i]));
                current.SetNext(next);
                current = next;
            }
            return new ReadOnlySequence<byte>(first, 0, current, current.Memory.Length);
        }

        [Fact]
        public void Properties_ReturnsExpectedValues()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            Assert.True(stream.CanRead);
            Assert.True(stream.CanSeek);
            Assert.False(stream.CanWrite);
            Assert.Equal(5, stream.Length);
            Assert.Equal(0, stream.Position);
        }

        [Fact]
        public void Read_SingleSegment_ReadsAllData()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var buffer = new byte[5];
            var bytesRead = stream.Read(buffer, 0, 5);

            Assert.Equal(5, bytesRead);
            Assert.Equal(data, buffer);
            Assert.Equal(5, stream.Position);
        }

        [Fact]
        public void Read_PartialReads_ReadsSequentially()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var buffer = new byte[3];
            var bytesRead1 = stream.Read(buffer, 0, 3);
            Assert.Equal(3, bytesRead1);
            Assert.Equal(new byte[] { 1, 2, 3 }, buffer);

            var buffer2 = new byte[3];
            var bytesRead2 = stream.Read(buffer2, 0, 3);
            Assert.Equal(2, bytesRead2);
            Assert.Equal(new byte[] { 4, 5, 0 }, buffer2);
        }

        [Fact]
        public void Read_AtEnd_ReturnsZero()
        {
            var data = new byte[] { 1, 2 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var buffer = new byte[2];
            stream.ReadExactly(buffer, 0, 2);

            var bytesRead = stream.Read(buffer, 0, 2);
            Assert.Equal(0, bytesRead);
        }

        [Fact]
        public void Read_EmptySequence_ReturnsZero()
        {
            using var stream = new ReadOnlySequenceStream(ReadOnlySequence<byte>.Empty);

            var buffer = new byte[5];
            var bytesRead = stream.Read(buffer, 0, 5);
            Assert.Equal(0, bytesRead);
        }

        [Fact]
        public void Read_WithOffset_WritesToCorrectPosition()
        {
            var data = new byte[] { 10, 20, 30 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var buffer = new byte[5];
            var bytesRead = stream.Read(buffer, 2, 3);

            Assert.Equal(3, bytesRead);
            Assert.Equal(new byte[] { 0, 0, 10, 20, 30 }, buffer);
        }

        [Fact]
        public void Read_MultiSegment_ReadsAcrossSegments()
        {
            using var stream = new ReadOnlySequenceStream(
                CreateMultiSegmentSequence(
                    new byte[] { 1, 2, 3 },
                    new byte[] { 4, 5, 6 },
                    new byte[] { 7, 8 }
                ));

            var buffer = new byte[8];
            var bytesRead = stream.Read(buffer, 0, 8);

            Assert.Equal(8, bytesRead);
            Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, buffer);
        }

        [Fact]
        public void Read_MultiSegment_PartialReadsAcrossBoundary()
        {
            using var stream = new ReadOnlySequenceStream(
                CreateMultiSegmentSequence(
                    new byte[] { 1, 2 },
                    new byte[] { 3, 4 }
                ));

            var buffer1 = new byte[3];
            var bytesRead1 = stream.Read(buffer1, 0, 3);
            Assert.Equal(3, bytesRead1);
            Assert.Equal(new byte[] { 1, 2, 3 }, buffer1);

            var buffer2 = new byte[3];
            var bytesRead2 = stream.Read(buffer2, 0, 3);
            Assert.Equal(1, bytesRead2);
            Assert.Equal(4, buffer2[0]);
        }

        [Fact]
        public void Seek_Begin_SetsPositionFromStart()
        {
            var data = new byte[] { 10, 20, 30, 40, 50 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var result = stream.Seek(3, SeekOrigin.Begin);

            Assert.Equal(3, result);
            Assert.Equal(3, stream.Position);

            var buffer = new byte[2];
            stream.ReadExactly(buffer, 0, 2);
            Assert.Equal(new byte[] { 40, 50 }, buffer);
        }

        [Fact]
        public void Seek_Current_SetsPositionRelativeToCurrent()
        {
            var data = new byte[] { 10, 20, 30, 40, 50 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            stream.ReadExactly(new byte[2], 0, 2);
            var result = stream.Seek(1, SeekOrigin.Current);

            Assert.Equal(3, result);
            Assert.Equal(3, stream.Position);

            var buffer = new byte[1];
            stream.ReadExactly(buffer, 0, 1);
            Assert.Equal(40, buffer[0]);
        }

        [Fact]
        public void Seek_End_SetsPositionFromEnd()
        {
            var data = new byte[] { 10, 20, 30, 40, 50 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var result = stream.Seek(-2, SeekOrigin.End);

            Assert.Equal(3, result);
            Assert.Equal(3, stream.Position);

            var buffer = new byte[2];
            stream.ReadExactly(buffer, 0, 2);
            Assert.Equal(new byte[] { 40, 50 }, buffer);
        }

        [Fact]
        public void Seek_ToEnd_ReturnsZeroOnRead()
        {
            var data = new byte[] { 1, 2, 3 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            stream.Seek(0, SeekOrigin.End);

            var buffer = new byte[1];
            var bytesRead = stream.Read(buffer, 0, 1);
            Assert.Equal(0, bytesRead);
        }

        [Fact]
        public void Seek_NegativePosition_Throws()
        {
            var data = new byte[] { 1, 2, 3 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            Assert.Throws<ArgumentOutOfRangeException>(() => stream.Seek(-1, SeekOrigin.Begin));
        }

        [Fact]
        public void Seek_BeyondEnd_Throws()
        {
            var data = new byte[] { 1, 2, 3 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            Assert.Throws<ArgumentOutOfRangeException>(() => stream.Seek(4, SeekOrigin.Begin));
        }

        [Fact]
        public void Position_Set_SeeksToPosition()
        {
            var data = new byte[] { 10, 20, 30 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            stream.Position = 2;

            Assert.Equal(2, stream.Position);
            var buffer = new byte[1];
            stream.ReadExactly(buffer, 0, 1);
            Assert.Equal(30, buffer[0]);
        }

        [Fact]
        public void Seek_ThenRead_MultiSegment()
        {
            using var stream = new ReadOnlySequenceStream(
                CreateMultiSegmentSequence(
                    new byte[] { 1, 2, 3 },
                    new byte[] { 4, 5, 6 },
                    new byte[] { 7, 8, 9 }
                ));

            stream.Seek(4, SeekOrigin.Begin);

            var buffer = new byte[3];
            var bytesRead = stream.Read(buffer, 0, 3);
            Assert.Equal(3, bytesRead);
            Assert.Equal(new byte[] { 5, 6, 7 }, buffer);
        }

        [Fact]
        public void Seek_BackwardThenRead()
        {
            var data = new byte[] { 10, 20, 30, 40, 50 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            stream.ReadExactly(new byte[4], 0, 4);
            stream.Seek(1, SeekOrigin.Begin);

            var buffer = new byte[2];
            stream.ReadExactly(buffer, 0, 2);
            Assert.Equal(new byte[] { 20, 30 }, buffer);
        }

        [Fact]
        public void Write_ThrowsNotSupported()
        {
            var data = new byte[] { 1 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            Assert.Throws<NotSupportedException>(() => stream.Write(new byte[] { 1 }, 0, 1));
        }

        [Fact]
        public void SetLength_ThrowsNotSupported()
        {
            var data = new byte[] { 1 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            Assert.Throws<NotSupportedException>(() => stream.SetLength(10));
        }

        [Fact]
        public void Flush_DoesNotThrow()
        {
            var data = new byte[] { 1 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            stream.Flush();
        }

        [Fact]
        public void Read_LargerBufferThanData_ReturnsActualCount()
        {
            var data = new byte[] { 1, 2, 3 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var buffer = new byte[10];
            var bytesRead = stream.Read(buffer, 0, 10);

            Assert.Equal(3, bytesRead);
            Assert.Equal(new byte[] { 1, 2, 3 }, buffer[..3]);
        }

        [Fact]
        public void ReadSpan_SingleSegment_ReadsAllData()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            Span<byte> buffer = stackalloc byte[5];
            var bytesRead = stream.Read(buffer);

            Assert.Equal(5, bytesRead);
            Assert.Equal(data, buffer.ToArray());
            Assert.Equal(5, stream.Position);
        }

        [Fact]
        public void ReadSpan_PartialReads_ReadsSequentially()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            Span<byte> buffer1 = stackalloc byte[3];
            var bytesRead1 = stream.Read(buffer1);
            Assert.Equal(3, bytesRead1);
            Assert.Equal(new byte[] { 1, 2, 3 }, buffer1.ToArray());

            Span<byte> buffer2 = stackalloc byte[3];
            var bytesRead2 = stream.Read(buffer2);
            Assert.Equal(2, bytesRead2);
            Assert.Equal(4, buffer2[0]);
            Assert.Equal(5, buffer2[1]);
        }

        [Fact]
        public void ReadSpan_AtEnd_ReturnsZero()
        {
            var data = new byte[] { 1, 2 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            stream.ReadExactly(new byte[2], 0, 2);

            Span<byte> buffer = stackalloc byte[2];
            var bytesRead = stream.Read(buffer);
            Assert.Equal(0, bytesRead);
        }

        [Fact]
        public void ReadSpan_EmptySequence_ReturnsZero()
        {
            using var stream = new ReadOnlySequenceStream(ReadOnlySequence<byte>.Empty);

            Span<byte> buffer = stackalloc byte[5];
            var bytesRead = stream.Read(buffer);
            Assert.Equal(0, bytesRead);
        }

        [Fact]
        public void ReadSpan_MultiSegment_ReadsAcrossSegments()
        {
            using var stream = new ReadOnlySequenceStream(
                CreateMultiSegmentSequence(
                    new byte[] { 1, 2, 3 },
                    new byte[] { 4, 5, 6 },
                    new byte[] { 7, 8 }
                ));

            Span<byte> buffer = stackalloc byte[8];
            var bytesRead = stream.Read(buffer);

            Assert.Equal(8, bytesRead);
            Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, buffer.ToArray());
        }

        [Fact]
        public void ReadSpan_LargerBufferThanData_ReturnsActualCount()
        {
            var data = new byte[] { 1, 2, 3 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            Span<byte> buffer = stackalloc byte[10];
            var bytesRead = stream.Read(buffer);

            Assert.Equal(3, bytesRead);
            Assert.Equal(new byte[] { 1, 2, 3 }, buffer[..3].ToArray());
        }

        [Fact]
        public async Task ReadAsyncByteArray_SingleSegment_ReadsAllData()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var buffer = new byte[5];
            var bytesRead = await stream.ReadAsync(buffer, 0, 5, CancellationToken.None);

            Assert.Equal(5, bytesRead);
            Assert.Equal(data, buffer);
            Assert.Equal(5, stream.Position);
        }

        [Fact]
        public async Task ReadAsyncByteArray_PartialReads_ReadsSequentially()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var buffer1 = new byte[3];
            var bytesRead1 = await stream.ReadAsync(buffer1, 0, 3, CancellationToken.None);
            Assert.Equal(3, bytesRead1);
            Assert.Equal(new byte[] { 1, 2, 3 }, buffer1);

            var buffer2 = new byte[3];
            var bytesRead2 = await stream.ReadAsync(buffer2, 0, 3, CancellationToken.None);
            Assert.Equal(2, bytesRead2);
            Assert.Equal(new byte[] { 4, 5, 0 }, buffer2);
        }

        [Fact]
        public async Task ReadAsyncByteArray_AtEnd_ReturnsZero()
        {
            var data = new byte[] { 1, 2 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            stream.ReadExactly(new byte[2], 0, 2);

            var buffer = new byte[2];
            var bytesRead = await stream.ReadAsync(buffer, 0, 2, CancellationToken.None);
            Assert.Equal(0, bytesRead);
        }

        [Fact]
        public async Task ReadAsyncByteArray_MultiSegment_ReadsAcrossSegments()
        {
            using var stream = new ReadOnlySequenceStream(
                CreateMultiSegmentSequence(
                    new byte[] { 1, 2, 3 },
                    new byte[] { 4, 5, 6 }
                ));

            var buffer = new byte[6];
            var bytesRead = await stream.ReadAsync(buffer, 0, 6, CancellationToken.None);

            Assert.Equal(6, bytesRead);
            Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, buffer);
        }

        [Fact]
        public async Task ReadAsyncByteArray_CancelledToken_ThrowsOperationCanceled()
        {
            var data = new byte[] { 1, 2, 3 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var buffer = new byte[3];
            await Assert.ThrowsAsync<OperationCanceledException>(
                () => stream.ReadAsync(buffer, 0, 3, cts.Token));
        }

        [Fact]
        public async Task ReadAsyncMemory_SingleSegment_ReadsAllData()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var buffer = new byte[5];
            var bytesRead = await stream.ReadAsync(buffer.AsMemory(), CancellationToken.None);

            Assert.Equal(5, bytesRead);
            Assert.Equal(data, buffer);
            Assert.Equal(5, stream.Position);
        }

        [Fact]
        public async Task ReadAsyncMemory_PartialReads_ReadsSequentially()
        {
            var data = new byte[] { 1, 2, 3, 4, 5 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            var buffer1 = new byte[3];
            var bytesRead1 = await stream.ReadAsync(buffer1.AsMemory(), CancellationToken.None);
            Assert.Equal(3, bytesRead1);
            Assert.Equal(new byte[] { 1, 2, 3 }, buffer1);

            var buffer2 = new byte[3];
            var bytesRead2 = await stream.ReadAsync(buffer2.AsMemory(), CancellationToken.None);
            Assert.Equal(2, bytesRead2);
            Assert.Equal(new byte[] { 4, 5, 0 }, buffer2);
        }

        [Fact]
        public async Task ReadAsyncMemory_AtEnd_ReturnsZero()
        {
            var data = new byte[] { 1, 2 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));

            stream.ReadExactly(new byte[2], 0, 2);

            var buffer = new byte[2];
            var bytesRead = await stream.ReadAsync(buffer.AsMemory(), CancellationToken.None);
            Assert.Equal(0, bytesRead);
        }

        [Fact]
        public async Task ReadAsyncMemory_MultiSegment_ReadsAcrossSegments()
        {
            using var stream = new ReadOnlySequenceStream(
                CreateMultiSegmentSequence(
                    new byte[] { 1, 2, 3 },
                    new byte[] { 4, 5, 6 }
                ));

            var buffer = new byte[6];
            var bytesRead = await stream.ReadAsync(buffer.AsMemory(), CancellationToken.None);

            Assert.Equal(6, bytesRead);
            Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6 }, buffer);
        }

        [Fact]
        public async Task ReadAsyncMemory_CancelledToken_ThrowsOperationCanceled()
        {
            var data = new byte[] { 1, 2, 3 };
            using var stream = new ReadOnlySequenceStream(CreateSingleSegmentSequence(data));
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var buffer = new byte[3];
            await Assert.ThrowsAsync<OperationCanceledException>(
                async () => await stream.ReadExactlyAsync(buffer.AsMemory(), cts.Token));
        }
    }
}

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
using System.Buffers.Binary;

namespace FlowtideDotNet.Core.ColumnStore.Serialization.Serializer
{
    /// <summary>
    /// Writer that ensures that the data is padded to 8 bytes.
    /// 
    /// </summary>
    internal ref struct ArrowDataWriter
    {
        private readonly Span<byte> destinationSpan;
        private readonly Span<byte> bufferSpan;
        private readonly IBatchCompressor? compressor;
        private int m_bodyLength;
        private int m_bufferIndex;

        public ArrowDataWriter(Span<byte> destinationSpan, Span<byte> bufferSpan, IBatchCompressor? compressor)
        {
            this.destinationSpan = destinationSpan;
            this.bufferSpan = bufferSpan;
            this.compressor = compressor;
        }

        public int BodyLength => m_bodyLength;

        public void WriteArrowBuffer(ReadOnlySpan<byte> buffer)
        {
            int paddedLength;
            int written;
            if (buffer.Length == 0)
            {
                written = 0;
                paddedLength = 0;
            }
            else if (compressor != null)
            {
                written = compressor.Wrap(buffer, destinationSpan.Slice(m_bodyLength + 8));

                if (written > buffer.Length)
                {
                    // Compression did not help, write uncompressed
                    written = buffer.Length;
                    buffer.CopyTo(destinationSpan.Slice(m_bodyLength + 8));
                    // Write -1 to indicate that the buffer is not compressed
                    BinaryPrimitives.WriteInt64LittleEndian(destinationSpan.Slice(m_bodyLength), -1);
                }
                else
                {
                    // Write the uncompressed length so that the reader knows how much to decompress
                    BinaryPrimitives.WriteInt64LittleEndian(destinationSpan.Slice(m_bodyLength), buffer.Length);
                }
                written += 8;
                paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(written));
                
            }
            else
            {
                paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(buffer.Length));
                buffer.CopyTo(destinationSpan.Slice(m_bodyLength));
                written = buffer.Length;
            }

            // Write buffer length and offset
            var bufferSlice = bufferSpan.Slice(m_bufferIndex * 16);
            BinaryPrimitives.WriteInt64LittleEndian(bufferSlice, m_bodyLength);
            BinaryPrimitives.WriteInt64LittleEndian(bufferSlice.Slice(8), written);
            m_bufferIndex++;

            m_bodyLength += paddedLength;
        }
    }
}

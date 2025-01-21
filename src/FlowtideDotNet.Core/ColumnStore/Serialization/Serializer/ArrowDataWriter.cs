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
using System.Buffers;

namespace FlowtideDotNet.Core.ColumnStore.Serialization.Serializer
{
    /// <summary>
    /// Writer that ensures that the data is padded to 8 bytes.
    /// 
    /// </summary>
    internal struct ArrowDataWriter
    {
        private readonly IBufferWriter<byte> bufferWriter;
        private int m_bodyLength;

        public ArrowDataWriter(IBufferWriter<byte> bufferWriter)
        {
            this.bufferWriter = bufferWriter;
        }

        public void WriteArrowBuffer(ReadOnlySpan<byte> buffer)
        {
            int paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(buffer.Length));

            var destination = bufferWriter.GetSpan(paddedLength);

            if (paddedLength <= destination.Length)
            {
                buffer.CopyTo(destination);
                bufferWriter.Advance(paddedLength);
            }
            else
            {
                WriteArrowBufferMultiSegment(buffer, destination);
                int padding = paddedLength - buffer.Length;
                bufferWriter.GetSpan(padding).Fill(0);
                bufferWriter.Advance(padding);
            }

            m_bodyLength += paddedLength;
        }

        private void WriteArrowBufferMultiSegment(ReadOnlySpan<byte> buffer, Span<byte> destination)
        {
            ReadOnlySpan<byte> input = buffer;
            while (true)
            {
                int writeSize = Math.Min(destination.Length, input.Length);
                input.Slice(0, writeSize).CopyTo(destination);
                bufferWriter.Advance(writeSize);
                input = input.Slice(writeSize);
                if (input.Length > 0)
                {
                    destination = bufferWriter.GetSpan(input.Length);

                    if (destination.IsEmpty)
                    {
                        throw new InvalidOperationException("Buffer writer returned an empty buffer");
                    }

                    continue;
                }
                return;
            }
        }
    }
}

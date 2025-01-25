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
using FlowtideDotNet.Storage.Memory;
using System.Buffers;

namespace FlowtideDotNet.Core.ColumnStore.Serialization.Serializer
{
    /// <summary>
    /// Writer that ensures that the data is padded to 8 bytes.
    /// 
    /// </summary>
    internal struct ArrowDataWriter<TBufferWriter>
        where TBufferWriter : IFlowtideBufferWriter
    {
        private readonly TBufferWriter bufferWriter;
        private int m_bodyLength;

        public ArrowDataWriter(TBufferWriter bufferWriter)
        {
            this.bufferWriter = bufferWriter;
        }

        public void WriteArrowBuffer(ReadOnlySpan<byte> buffer)
        {
            int paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(buffer.Length));
            bufferWriter.Write(buffer);

            if (paddedLength > buffer.Length)
            {
                var padding = paddedLength - buffer.Length;
                bufferWriter.Advance(padding);
            }

            m_bodyLength += paddedLength;
        }
    }
}

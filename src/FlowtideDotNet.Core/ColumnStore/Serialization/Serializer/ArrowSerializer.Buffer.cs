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

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
#pragma warning disable CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    internal ref partial struct ArrowSerializer
#pragma warning restore CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    {
        private int m_bufferBodyLength = 0;
        private int forwardBufferPosition = 0;
        

        public void AddBufferForward(long bufferLength)
        {
            Span<byte> span = m_destination.Slice(m_space + (forwardBufferPosition * 16));
            BinaryPrimitives.WriteInt64LittleEndian(span, m_bufferBodyLength);
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(8), bufferLength);

            int paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(bufferLength));
            m_bufferBodyLength += paddedLength;
            forwardBufferPosition++;
        }

        public void RecordBatchStartBuffersVectorForward(int numElems)
        {
            StartVector(16, numElems, 8);
            m_space -= 16 * numElems;
        }

        public int BufferBodyLength => m_bufferBodyLength;

        public int CreateBuffer(long offset, long length)
        {
            Prep(8, 16);
            PutLong(length);
            PutLong(offset);
            return Offset;
        }
    }
}

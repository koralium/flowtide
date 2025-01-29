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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
#pragma warning disable CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    internal ref partial struct ArrowSerializer
#pragma warning restore CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    {
        private int m_bodyLength;
        public (int offset, int length) WriteBufferData(ReadOnlySpan<byte> buffer)
        {
            var offset = m_bodyLength;
            int paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(buffer.Length));

            buffer.CopyTo(m_destination);

            int padding = paddedLength - buffer.Length;
            if (padding > 0)
            {
                // Set bytes to zero in m_destination
                m_destination.Slice(buffer.Length, padding).Fill(0);
            }
            m_destination = m_destination.Slice(paddedLength);

            m_bodyLength += paddedLength;

            return (offset, buffer.Length);
        }

        public int FinishWritingBufferData()
        {
            int bodyPaddingLength = CalculatePadding(m_bodyLength);

            m_bodyLength += bodyPaddingLength;
            m_destination.Slice(0, bodyPaddingLength).Fill(0);
            m_position += m_bodyLength;
            return m_bodyLength;
        }
    }
}

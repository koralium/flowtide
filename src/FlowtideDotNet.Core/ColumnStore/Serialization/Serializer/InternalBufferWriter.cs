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

using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization.Serializer
{
    internal struct InternalBufferWriter : IFlowtideBufferWriter
    {
        private readonly IBufferWriter<byte> bufferWriter;

        public InternalBufferWriter(IBufferWriter<byte> bufferWriter)
        {
            this.bufferWriter = bufferWriter;
        }

        public void Advance(int count)
        {
            bufferWriter.Advance(count);
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            return bufferWriter.GetMemory(sizeHint);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            return bufferWriter.GetSpan(sizeHint);
        }

        public void Write(ReadOnlySpan<byte> span)
        {
            bufferWriter.Write(span);
        }
    }
}

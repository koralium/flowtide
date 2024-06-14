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

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Memory
{
    public unsafe class NativeMemoryOwner : MemoryManager<byte>, IFlowtideMemoryOwner
    {
        private void* ptr;
        private readonly int length;
        private readonly int alignment;

        public NativeMemoryOwner(void* ptr, int length, int alignment)
        {
            this.ptr = ptr;
            this.length = length;
            this.alignment = alignment;
        }

        public override Span<byte> GetSpan()
        {
            return new Span<byte>(ptr, length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            return new MemoryHandle(ptr, default, default);
        }

        public void Reallocate(int newSize)
        {
            ptr = NativeMemory.AlignedRealloc(ptr, (nuint)newSize, (nuint)alignment);
        }

        public override void Unpin()
        {
            return;
        }

        protected override void Dispose(bool disposing)
        {
            if (ptr != null)
            {
                NativeMemory.Free(ptr);
                ptr = null;
            }
            
        }
    }
}

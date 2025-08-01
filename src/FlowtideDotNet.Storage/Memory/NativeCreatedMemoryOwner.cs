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

using mimalloc;
using System.Buffers;

namespace FlowtideDotNet.Storage.Memory
{
    internal unsafe class NativeCreatedMemoryOwner : MemoryManager<byte>
    {
        internal void* ptr;
        internal int length;
        private nuint alignment;
        private IMemoryAllocator? memoryManager;

        public NativeCreatedMemoryOwner()
        {
        }

        public void Assign(void* ptr, int length, nuint alignment, IMemoryAllocator memoryManager)
        {
            this.ptr = ptr;
            this.length = length;
            this.memoryManager = memoryManager;
            this.alignment = alignment;
        }

        public override Span<byte> GetSpan()
        {
            return new Span<byte>(ptr, length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            return new MemoryHandle(((byte*)ptr) + elementIndex, default, default);
        }

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
            if (ptr != null)
            {
                MiMalloc.mi_free_aligned(ptr, alignment);
                memoryManager!.RegisterFreeToMetrics(length);
                ptr = null;
                length = 0;
            }
        }
    }
}

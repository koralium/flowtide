﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using System.Buffers;

namespace FlowtideDotNet.Storage.Memory
{
    public unsafe class PreAllocatedMemoryOwner : MemoryManager<byte>
    {
        private readonly PreAllocatedMemoryManager _manager;
        private readonly void* ptr;
        private readonly int length;

        public PreAllocatedMemoryOwner(PreAllocatedMemoryManager memoryManager, void* ptr, int length)
        {
            _manager = memoryManager;
            this.ptr = ptr;
            this.length = length;
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
            _manager.Free();
        }
    }
}

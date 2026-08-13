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

using System.Buffers;

namespace FlowtideDotNet.Storage.Memory
{
    public unsafe interface IMemoryAllocator
    {
        /// <summary>
        /// Alignment used for all <see cref="FlowtideMemory"/> allocations.
        /// </summary>
        public const int FlowtideMemoryAlignment = 64;

        IMemoryOwner<byte> Allocate(int size, int alignment);

        IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment);

        /// <summary>
        /// Allocates a 64 byte aligned block.
        /// </summary>
        FlowtideMemory AllocateMemory(int size)
        {
            var allocated = FlowtideMemoryAllocation.AllocateAligned(size, FlowtideMemoryAlignment);
            RegisterAllocationToMetrics(allocated.length);
            return new FlowtideMemory(allocated.ptr, allocated.length);
        }

        /// <summary>
        /// Grows or shrinks a block in place.
        /// </summary>
        void Realloc(ref FlowtideMemory memory, int size)
        {
            if (memory.IsNull)
            {
                memory = AllocateMemory(size);
                return;
            }
            var previousLength = memory.Length;
            var allocated = FlowtideMemoryAllocation.ReallocAligned(memory.Pointer, previousLength, size, FlowtideMemoryAlignment);

            // If length is same and ptr is same, nothing happened
            if (allocated.length == previousLength && allocated.ptr == memory.Pointer)
            {
                return;
            }
            var diff = allocated.length - previousLength;

            if (diff > 0)
            {
                RegisterAllocationToMetrics(diff);
            }
            else if (diff < 0)
            {
                RegisterFreeToMetrics(-diff);
            }
            memory = new FlowtideMemory(allocated.ptr, allocated.length);
        }

        /// <summary>
        /// Frees a block and resets it to default.
        /// </summary>
        void Free(ref FlowtideMemory memory)
        {
            if (memory.IsNull)
            {
                return;
            }
            FlowtideMemoryAllocation.FreeAligned(memory.Pointer, FlowtideMemoryAlignment);
            RegisterFreeToMetrics(memory.Length);
            memory = default;
        }

        void RegisterAllocationToMetrics(int size);

        void RegisterFreeToMetrics(int size);
    }
}

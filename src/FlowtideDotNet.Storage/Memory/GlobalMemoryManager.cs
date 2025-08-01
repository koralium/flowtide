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
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.Memory
{
    /// <summary>
    /// Memory manager that is global in a process, should not be used as much as possible since
    /// the memory allocated with this does not contribute to metric gathering from a stream.
    /// </summary>
    public unsafe class GlobalMemoryManager : IMemoryAllocator
    {
        internal static readonly void* NullPtr = (void*)0;
        public static readonly GlobalMemoryManager Instance = new GlobalMemoryManager();
        private long _allocatedMemory;
        private long _freedMemory;
        private long _allocationCount;
        private long _freeCount;

        private GlobalMemoryManager()
        {
            var meter = new Meter("flowtide.GlobalMemoryManager");
            meter.CreateObservableGauge("flowtide.global.memory.allocated_bytes", () =>
            {
                return new Measurement<long>(_allocatedMemory);
            }, "bytes");
            meter.CreateObservableGauge("flowtide.global.memory.allocation_count", () =>
            {
                return new Measurement<long>(_allocationCount);
            });
            meter.CreateObservableGauge("flowtide.global.memory.freed_bytes", () =>
            {
                return new Measurement<long>(_freedMemory);
            }, "bytes");
            meter.CreateObservableGauge("flowtide.global.memory.free_count", () =>
            {
                return new Measurement<long>(_freeCount);
            });
        }
        public IMemoryOwner<byte> Allocate(int size, int alignment)
        {
            var alignedsize = (size + alignment - 1) & ~(alignment - 1);
            var goodSize = (int)MiMalloc.mi_good_size((nuint)alignedsize);

            var ptr = MiMalloc.mi_aligned_alloc((nuint)alignment, (nuint)goodSize);
            if (ptr == NullPtr)
            {
                throw new InvalidOperationException("Could not allocate memory");
            }
            RegisterAllocationToMetrics(goodSize);
            return NativeCreatedMemoryOwnerFactory.Get(ptr, goodSize, (nuint)alignment, this);
        }

        public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
        {
            var alignedsize = (size + alignment - 1) & ~(alignment - 1);
            alignedsize = (int)MiMalloc.mi_good_size((nuint)alignedsize);

            if (memory is NativeCreatedMemoryOwner native)
            {
                var previousLength = native.length;
                if (alignedsize == previousLength)
                {
                    return memory;
                }
                var newPtr = MiMalloc.mi_realloc_aligned(native.ptr, (nuint)alignedsize, (nuint)alignment);
                if (newPtr == NullPtr)
                {
                    throw new InvalidOperationException("Could not reallocate memory");
                }
                if (newPtr == native.ptr)
                {
                    var diff = alignedsize - previousLength;
                    RegisterAllocationToMetrics(diff);
                }
                else
                {
                    RegisterAllocationToMetrics(alignedsize);
                    RegisterFreeToMetrics(previousLength);
                }
                native.ptr = newPtr;
                native.length = alignedsize;
                return native;
            }
            else
            {
                var ptr = MiMalloc.mi_aligned_alloc((nuint)alignment, (nuint)alignedsize);
                if (ptr == NullPtr)
                {
                    throw new InvalidOperationException("Could not allocate memory");
                }
                RegisterAllocationToMetrics(alignedsize);
                RegisterFreeToMetrics(memory.Memory.Length);
                // Copy the memory
                var existingMemory = memory.Memory;
                NativeMemory.Copy(existingMemory.Pin().Pointer, ptr, (nuint)Math.Min(existingMemory.Length, alignedsize));
                memory.Dispose();
                return NativeCreatedMemoryOwnerFactory.Get(ptr, alignedsize, (nuint)alignment, this);
            }
        }

        public void RegisterAllocationToMetrics(int size)
        {
            Interlocked.Add(ref _allocatedMemory, size);
            Interlocked.Increment(ref _allocationCount);
        }

        public void RegisterFreeToMetrics(int size)
        {
            Interlocked.Add(ref _freedMemory, size);
            Interlocked.Increment(ref _freeCount);
        }
    }
}

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
using System.Diagnostics.Metrics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Memory
{
    /// <summary>
    /// Memory manager that is global in a process, should not be used as much as possible since
    /// the memory allocated with this does not contribute to metric gathering from a stream.
    /// </summary>
    public unsafe class GlobalMemoryManager : IMemoryAllocator
    {
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
            var ptr = NativeMemory.AlignedAlloc((nuint)size, (nuint)alignment);
            RegisterAllocationToMetrics(size);
            return NativeCreatedMemoryOwnerFactory.Get(ptr, size, this);
        }

        public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
        {
            if (memory is NativeCreatedMemoryOwner native)
            {
                var previousLength = native.length;
                var newPtr = NativeMemory.AlignedRealloc(native.ptr, (nuint)size, (nuint)alignment);
                if (newPtr == native.ptr)
                {
                    var diff = size - previousLength;
                    RegisterAllocationToMetrics(diff);
                }
                else
                {
                    RegisterAllocationToMetrics(size);
                    RegisterFreeToMetrics(previousLength);
                }
                native.ptr = newPtr;
                native.length = size;
                return native;
            }
            else
            {
                var ptr = NativeMemory.AlignedAlloc((nuint)size, (nuint)alignment);
                RegisterAllocationToMetrics(size);
                RegisterFreeToMetrics(memory.Memory.Length);
                // Copy the memory
                var existingMemory = memory.Memory;
                NativeMemory.Copy(existingMemory.Pin().Pointer, ptr, (nuint)Math.Min(existingMemory.Length, size));
                memory.Dispose();
                return NativeCreatedMemoryOwnerFactory.Get(ptr, size, this);
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

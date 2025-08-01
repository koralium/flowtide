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
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.Memory
{
    internal unsafe class OperatorMemoryManager : IOperatorMemoryManager
    {
        

        private readonly string m_streamName;
        private readonly string m_operatorName;
        private long _allocatedMemory;
        private long _freedMemory;
        private long _allocationCount;
        private long _freeCount;

        public OperatorMemoryManager(string streamName, string operatorName, Meter meter)
        {
            this.m_streamName = streamName;
            this.m_operatorName = operatorName;

            meter.CreateObservableGauge("flowtide.memory.allocated_bytes", () =>
            {
                return new Measurement<long>(_allocatedMemory, new KeyValuePair<string, object?>("stream", m_streamName), new KeyValuePair<string, object?>("operator", m_operatorName));
            }, "bytes");
            meter.CreateObservableGauge("flowtide.memory.allocation_count", () =>
            {
                return new Measurement<long>(_allocationCount, new KeyValuePair<string, object?>("stream", m_streamName), new KeyValuePair<string, object?>("operator", m_operatorName));
            });
            meter.CreateObservableGauge("flowtide.memory.freed_bytes", () =>
            {
                return new Measurement<long>(_freedMemory, new KeyValuePair<string, object?>("stream", m_streamName), new KeyValuePair<string, object?>("operator", m_operatorName));
            }, "bytes");
            meter.CreateObservableGauge("flowtide.memory.free_count", () =>
            {
                return new Measurement<long>(_freeCount, new KeyValuePair<string, object?>("stream", m_streamName), new KeyValuePair<string, object?>("operator", m_operatorName));
            });
        }

        public IMemoryOwner<byte> Allocate(int size, int alignment)
        {
            var alignedsize = (size + alignment - 1) & ~(alignment - 1);
            var goodSize = (int)MiMalloc.mi_good_size((nuint)alignedsize);

            var ptr = MiMalloc.mi_aligned_alloc((nuint)alignment, (nuint)goodSize);
            if (ptr == GlobalMemoryManager.NullPtr)
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

                if (newPtr == GlobalMemoryManager.NullPtr)
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
                if (ptr == GlobalMemoryManager.NullPtr)
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

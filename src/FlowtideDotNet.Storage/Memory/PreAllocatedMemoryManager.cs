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
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.Memory
{
    public unsafe class PreAllocatedMemoryManager : IMemoryAllocator, IDisposable
    {
        private IMemoryOwner<byte>? _memoryOwner;
        private int _usageCount;
        private bool disposedValue;
        private readonly IMemoryAllocator _operatorMemoryManager;
        private readonly MemoryHandle _pin;

        public PreAllocatedMemoryManager(IMemoryAllocator operatorMemoryManager, MemoryHandle pin)
        {
            this._operatorMemoryManager = operatorMemoryManager;
            this._pin = pin;
        }

        public void Initialize(IMemoryOwner<byte> memoryOwner, int usageCount)
        {
            _memoryOwner = memoryOwner;
            _usageCount = usageCount;
            _operatorMemoryManager.RegisterAllocationToMetrics(memoryOwner.Memory.Length);
        }

        public IMemoryOwner<byte> Allocate(int size, int alignment)
        {
            var allocated = FlowtideMemoryAllocation.AllocateAligned(size, alignment);
            _operatorMemoryManager.RegisterAllocationToMetrics(allocated.length);
            return NativeCreatedMemoryOwnerFactory.Get(allocated.ptr, allocated.length, (nuint)alignment, _operatorMemoryManager);
        }

        public void Free()
        {
            Debug.Assert(_memoryOwner != null);
            var result = Interlocked.Decrement(ref _usageCount);
            if (result == 0)
            {
                Dispose();
            }
        }

        public void RegisterAllocationToMetrics(int size)
        {
            _operatorMemoryManager.RegisterAllocationToMetrics(size);
        }

        public void RegisterFreeToMetrics(int size)
        {
            _operatorMemoryManager.RegisterFreeToMetrics(size);
        }

        public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
        {
            if (memory is NativeCreatedMemoryOwner native)
            {
                var previousLength = native.length;
                var oldPtr = native.ptr;
                FlowtideAllocatedMemory allocated = FlowtideMemoryAllocation.ReallocAligned(oldPtr, previousLength, size, alignment);

                // If length is same and ptr is same, nothing happened
                if (allocated.length == previousLength && allocated.ptr == oldPtr)
                {
                    return memory;
                }

                if (allocated.ptr == oldPtr)
                {
                    var diff = allocated.length - previousLength;
                    RegisterAllocationToMetrics(diff);
                }
                else
                {
                    RegisterAllocationToMetrics(allocated.length);
                    RegisterFreeToMetrics(previousLength);
                }

                native.ptr = allocated.ptr;
                native.length = allocated.length;
                return native;
            }
            else
            {
                var allocated = FlowtideMemoryAllocation.AllocateAligned(size, alignment);
                RegisterAllocationToMetrics(allocated.length);
                RegisterFreeToMetrics(memory.Memory.Length);
                // Copy the memory
                var existingMemory = memory.Memory;
                var copyLength = (nuint)Math.Min(existingMemory.Length, allocated.length);
                fixed (byte* srcPtr = existingMemory.Span)
                {
                    NativeMemory.Copy(srcPtr, allocated.ptr, copyLength);
                }
                memory.Dispose();
                return NativeCreatedMemoryOwnerFactory.Get(allocated.ptr, allocated.length, (nuint)alignment, this);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                var memoryOwner = _memoryOwner;
                _memoryOwner = null;
                if (memoryOwner != null)
                {
                    _pin.Dispose();
                    _operatorMemoryManager.RegisterFreeToMetrics(memoryOwner.Memory.Length);
                    memoryOwner.Dispose();
                }
                disposedValue = true;
            }
        }

        ~PreAllocatedMemoryManager()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

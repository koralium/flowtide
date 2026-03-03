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
    public unsafe class PreAllocatedMemoryManager : IMemoryAllocator
    {
        private IMemoryOwner<byte>? _memoryOwner;
        private int _usageCount;
        private readonly IMemoryAllocator _operatorMemoryManager;

        public PreAllocatedMemoryManager(IMemoryAllocator operatorMemoryManager)
        {
            this._operatorMemoryManager = operatorMemoryManager;
        }

        public void Initialize(IMemoryOwner<byte> memoryOwner, int usageCount)
        {
            _memoryOwner = memoryOwner;
            _usageCount = usageCount;
            _operatorMemoryManager.RegisterAllocationToMetrics(memoryOwner.Memory.Length);
        }

        public IMemoryOwner<byte> Allocate(int size, int alignment)
        {
            var ptr = NativeMemory.AlignedAlloc((nuint)size, (nuint)alignment);
            _operatorMemoryManager.RegisterAllocationToMetrics(size);
            return NativeCreatedMemoryOwnerFactory.Get(ptr, size, (nuint)alignment, _operatorMemoryManager);
        }

        public void Free()
        {
            Debug.Assert(_memoryOwner != null);
            var result = Interlocked.Decrement(ref _usageCount);
            if (result <= 0)
            {
                _operatorMemoryManager.RegisterFreeToMetrics(_memoryOwner.Memory.Length);
                _memoryOwner.Dispose();
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
                // Copy the memory
                var existingMemory = memory.Memory;
                NativeMemory.Copy(existingMemory.Pin().Pointer, ptr, (nuint)Math.Min(existingMemory.Length, size));
                memory.Dispose();
                return NativeCreatedMemoryOwnerFactory.Get(ptr, size, (nuint)alignment, this);
            }
        }
    }
}

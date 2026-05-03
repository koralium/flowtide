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

using FlowtideDotNet.Storage.Mimalloc;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Memory
{
    internal unsafe struct FlowtideAllocatedMemory
    {
        public void* ptr;
        public int length;
    }
    internal unsafe static class FlowtideMemoryAllocation
    {
        private static readonly bool _isMimallocAvailable = NativeLibrary.TryLoad(MiMalloc.NATIVE_LIBRARY,
            typeof(FlowtideMemoryAllocation).Assembly,
            DllImportSearchPath.AssemblyDirectory | DllImportSearchPath.ApplicationDirectory,
            out _);

        static FlowtideMemoryAllocation()
        {
            if (!_isMimallocAvailable)
            {
                Console.WriteLine("[Flowtide Warning] 'mimalloc' native library could not be loaded. Falling back to standard NativeMemory. Performance may be degraded.");
            }
        }

        public static FlowtideAllocatedMemory AllocateAligned(int size, int alignment)
        {
            Debug.Assert(BitOperations.IsPow2(alignment), "Alignment must be a power of 2");
            if (_isMimallocAvailable)
            {
                return AllocateMimalloc(size, alignment);
            }
            return AllocateNativeMemory(size, alignment);
        }

        public static void FreeAligned(void* ptr, nuint alignment)
        {
            if (_isMimallocAvailable)
            {
                FreeAlignedMimalloc(ptr, alignment);
            }
            else
            {
                FreeAlignedNativeMemory(ptr);
            }
        }

        private static void FreeAlignedNativeMemory(void* ptr)
        {
            NativeMemory.AlignedFree(ptr);
        }

        private static void FreeAlignedMimalloc(void* ptr, nuint alignment)
        {
            if (ptr != null)
            {
                MiMalloc.mi_free_aligned(ptr, alignment);
            }
        }

        public static FlowtideAllocatedMemory ReallocAligned(void* ptr, int oldSize, int newSize, int alignment)
        {
            if (_isMimallocAvailable)
            {
                return ReallocMimalloc(ptr, oldSize, newSize, alignment);
            }
            return ReallocNativeMemory(ptr, newSize, alignment);
        }

        private static FlowtideAllocatedMemory ReallocNativeMemory(void* ptr, int newSize, int alignment)
        {
            var newPtr = NativeMemory.AlignedRealloc(ptr, (nuint)newSize, (nuint)alignment);
            if (newPtr == GlobalMemoryManager.NullPtr)
            {
                throw new InvalidOperationException("Could not reallocate memory");
            }
            return new FlowtideAllocatedMemory() { ptr = newPtr, length = newSize };
        }

        private static FlowtideAllocatedMemory ReallocMimalloc(void* ptr, int oldSize, int newSize, int alignment)
        {
            Debug.Assert(BitOperations.IsPow2(alignment), "Alignment must be a power of 2");

            var alignedsize = (newSize + alignment - 1) & ~(alignment - 1);
            alignedsize = (int)MiMalloc.mi_good_size((nuint)alignedsize);
            if (alignedsize == oldSize)
            {
                return new FlowtideAllocatedMemory() { ptr = ptr, length = oldSize };
            }
            var newPtr = MiMalloc.mi_realloc_aligned(ptr, (nuint)alignedsize, (nuint)alignment);
            if (newPtr == GlobalMemoryManager.NullPtr)
            {
                throw new InvalidOperationException("Could not reallocate memory");
            }
            return new FlowtideAllocatedMemory() { ptr = newPtr, length = alignedsize };
        }

        private static FlowtideAllocatedMemory AllocateNativeMemory(int size, int alignment)
        {
            var ptr = NativeMemory.AlignedAlloc((nuint)size, (nuint)alignment);
            if (ptr == GlobalMemoryManager.NullPtr)
            {
                throw new InvalidOperationException("Could not allocate memory");
            }
            return new FlowtideAllocatedMemory { ptr = ptr, length = size };
        }

        private static FlowtideAllocatedMemory AllocateMimalloc(int size, int alignment)
        {
            var alignedsize = (size + alignment - 1) & ~(alignment - 1);
            var goodSize = (int)MiMalloc.mi_good_size((nuint)alignedsize);

            var ptr = MiMalloc.mi_aligned_alloc((nuint)alignment, (nuint)goodSize);
            if (ptr == GlobalMemoryManager.NullPtr)
            {
                throw new InvalidOperationException("Could not allocate memory");
            }
            return new FlowtideAllocatedMemory { ptr = ptr, length = goodSize };
        }
    }
}

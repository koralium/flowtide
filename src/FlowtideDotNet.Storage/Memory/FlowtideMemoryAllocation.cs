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
        private static readonly bool _isMimallocAvailable;

        static FlowtideMemoryAllocation()
        {
            _isMimallocAvailable = NativeLibrary.TryLoad(MiMalloc.NATIVE_LIBRARY,
                typeof(FlowtideMemoryAllocation).Assembly,
                DllImportSearchPath.AssemblyDirectory | DllImportSearchPath.ApplicationDirectory,
                out _);

            if (!_isMimallocAvailable)
            {
                // If it failed to load try a simple load again
                _isMimallocAvailable = NativeLibrary.TryLoad(MiMalloc.NATIVE_LIBRARY,
                out _);
            }

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

            void* newPtr = MiMalloc.mi_realloc_aligned(ptr, (nuint)alignedsize, (nuint)alignment);

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
            if (size < 0) throw new ArgumentOutOfRangeException(nameof(size), size, "Allocation size cannot be negative.");
            if (alignment <= 0) throw new ArgumentOutOfRangeException(nameof(alignment), alignment, "Alignment must be strictly positive.");
            if ((alignment & (alignment - 1)) != 0) throw new ArgumentException("Alignment must be a power of two.", nameof(alignment));

            long maximumRequiredCapacity = (long)size + alignment;
            if (maximumRequiredCapacity > int.MaxValue)
            {
                throw new OutOfMemoryException($"Requested size plus alignment exceeds 32-bit capacity.");
            }

            // --- 2. Safe Native Interop ---
            nuint nSize = (nuint)size;
            nuint nAlign = (nuint)alignment;

            // Base aligned size
            nuint alignedSize = (nSize + nAlign - 1) & ~(nAlign - 1);

            // Get mimalloc's preferred block size
            nuint goodSize = MiMalloc.mi_good_size(alignedSize);

            // --- 3. The Alignment Enforcement ---
            // If mi_good_size rounded us to a value that is no longer a multiple of the alignment,
            // we must re-align the goodSize upwards to prevent native assertion crashes.
            if (goodSize % nAlign != 0)
            {
                Console.WriteLine("Missaligned goodsize");
                goodSize = (goodSize + nAlign - 1) & ~(nAlign - 1);
            }

            // --- 4. Allocation ---
            void* ptr = MiMalloc.mi_aligned_alloc(nAlign, goodSize);

            if (ptr == GlobalMemoryManager.NullPtr)
            {
                throw new OutOfMemoryException($"mimalloc failed to allocate {goodSize} bytes.");
            }

            if (goodSize > int.MaxValue)
            {
                MiMalloc.mi_free(ptr);
                throw new OutOfMemoryException($"mimalloc allocated {goodSize} bytes, which exceeds .NET's 32-bit length constraints.");
            }

            return new FlowtideAllocatedMemory { ptr = ptr, length = (int)goodSize };
        }

        public static void Collect()
        {
            if (_isMimallocAvailable)
            {
                MiMalloc.mi_collect(true);
            }
        }
    }
}

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
using System.Diagnostics;
using System.Numerics;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.Memory
{
    // note: alias inside the namespace, the FlowtideDotNet.MiMalloc NAMESPACE otherwise
    // shadows the type name when resolving from FlowtideDotNet.* namespaces
    using MiMalloc = FlowtideDotNet.MiMalloc.MiMalloc;

    internal unsafe struct FlowtideAllocatedMemory
    {
        public void* ptr;
        public int length;
    }
    internal unsafe static class FlowtideMemoryAllocation
    {
        // The managed mimalloc port requires a 64-bit process; on 32-bit fall back to NativeMemory.
        private static readonly bool _useMimalloc = IntPtr.Size == 8;

        /// <summary>
        /// Whether <see cref="GetAllocationSize"/> can recover a block's size from its pointer alone.
        /// Only mimalloc can do this; the <see cref="NativeMemory"/> fallback has no equivalent, so a
        /// caller that needs the size on a free path must remember it itself when this is false.
        /// </summary>
        public static bool CanQueryAllocationSize => _useMimalloc;

        /// <summary>
        /// The size of a block previously returned by <see cref="AllocateAligned"/>. Only valid when
        /// <see cref="CanQueryAllocationSize"/> is true -- asking mimalloc about a pointer it did not
        /// hand out reads through a page map that has no entry for it.
        /// </summary>
        public static nuint GetAllocationSize(void* ptr)
        {
            Debug.Assert(_useMimalloc, "GetAllocationSize is only valid when mimalloc is in use");
            return MiMalloc.mi_usable_size(ptr);
        }

        static FlowtideMemoryAllocation()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                // mimalloc reads these on first use
                if (Environment.GetEnvironmentVariable("MIMALLOC_ALLOW_THP") == null)
                {
                    Environment.SetEnvironmentVariable("MIMALLOC_ALLOW_THP", "0");
                }

                if (Environment.GetEnvironmentVariable("MIMALLOC_PURGE_DECOMMITS") == null)
                {
                    Environment.SetEnvironmentVariable("MIMALLOC_PURGE_DECOMMITS", "1");
                }
            }
        }

        public static FlowtideAllocatedMemory AllocateAligned(int size, int alignment)
        {
            Debug.Assert(BitOperations.IsPow2(alignment), "Alignment must be a power of 2");
            if (_useMimalloc)
            {
                return AllocateMimalloc(size, alignment);
            }
            return AllocateNativeMemory(size, alignment);
        }

        public static void FreeAligned(void* ptr, nuint alignment)
        {
            if (_useMimalloc)
            {
                if (ptr != null)
                {
                    MiMalloc.mi_free_aligned(ptr, alignment);
                }
            }
            else
            {
                NativeMemory.AlignedFree(ptr);
            }
        }

        public static FlowtideAllocatedMemory ReallocAligned(void* ptr, int oldSize, int newSize, int alignment)
        {
            if (_useMimalloc)
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
            var alignedsize = (size + alignment - 1) & ~(alignment - 1);
            var goodSize = (int)MiMalloc.mi_good_size((nuint)alignedsize);

            void* ptr = MiMalloc.mi_aligned_alloc((nuint)alignment, (nuint)goodSize);

            if (ptr == GlobalMemoryManager.NullPtr)
            {
                throw new InvalidOperationException("Could not allocate memory");
            }
            return new FlowtideAllocatedMemory { ptr = ptr, length = goodSize };
        }

        public static void Collect()
        {
            if (_useMimalloc)
            {
                // Overallocate a bunch of work items to try and hit all threads in the thread pool to run the collection.
                for (int i = 0; i < Environment.ProcessorCount * 8; i++)
                {
                    ThreadPool.QueueUserWorkItem<object?>((_) =>
                    {
                        MiMalloc.mi_collect(true);
                    }, default, false);
                }
            }
        }
    }
}

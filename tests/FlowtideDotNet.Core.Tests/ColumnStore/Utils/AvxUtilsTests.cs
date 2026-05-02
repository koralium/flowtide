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

using FlowtideDotNet.Core.ColumnStore.Utils;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Utils
{
    public class AvxUtilsTests
    {
        #region Windows Native APIs
        const uint MEM_COMMIT = 0x00001000;
        const uint MEM_RESERVE = 0x00002000;
        const uint MEM_RELEASE = 0x00008000;
        const uint PAGE_READWRITE = 0x04;
        const uint PAGE_NOACCESS = 0x01;

        [DllImport("kernel32.dll", SetLastError = true)]
        static extern IntPtr VirtualAlloc(IntPtr lpAddress, UIntPtr dwSize, uint flAllocationType, uint flProtect);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool VirtualProtect(IntPtr lpAddress, UIntPtr dwSize, uint flNewProtect, out uint lpflOldProtect);
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern bool VirtualFree(IntPtr lpAddress, UIntPtr dwSize, uint dwFreeType);
        #endregion

        #region Linux Native APIs
        const int PROT_NONE = 0x0;
        const int PROT_READ = 0x1;
        const int PROT_WRITE = 0x2;
        const int MAP_PRIVATE = 0x02;
        const int MAP_ANONYMOUS = 0x20;

        [DllImport("libc", SetLastError = true)]
        static extern IntPtr mmap(IntPtr addr, UIntPtr length, int prot, int flags, int fd, IntPtr offset);
        [DllImport("libc", SetLastError = true)]
        static extern int mprotect(IntPtr addr, UIntPtr len, int prot);
        [DllImport("libc", SetLastError = true)]
        static extern int munmap(IntPtr addr, UIntPtr length);
        #endregion

        /// <summary>
        /// Tests the InPlaceMemCopyConditionalAddition method when the destination index is at the boundary of a memory page, 
        /// ensuring that it does not read or write beyond the allocated memory and correctly handles the conditional addition without causing access violations.
        /// </summary>
        /// <exception cref="OutOfMemoryException"></exception>
        [Fact]
        public unsafe void TestInPlaceMemCopyConditionalAdditionAtPageBoundary()
        {
            int pageSize = Environment.SystemPageSize;
            IntPtr memory = IntPtr.Zero;

            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    memory = VirtualAlloc(IntPtr.Zero, (UIntPtr)(pageSize * 2), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
                    if (memory == IntPtr.Zero) throw new OutOfMemoryException("VirtualAlloc failed");

                    VirtualProtect(memory + pageSize, (UIntPtr)pageSize, PAGE_NOACCESS, out _);
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                {
                    memory = mmap(IntPtr.Zero, (UIntPtr)(pageSize * 2), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, IntPtr.Zero);
                    if (memory == new IntPtr(-1)) throw new OutOfMemoryException("mmap failed");

                    mprotect(memory + pageSize, (UIntPtr)pageSize, PROT_NONE);
                }
                else
                {
                    Assert.Fail("OS not supported for guard page testing.");
                    return;
                }

                int arrayLength = 121;
                sbyte* pCond = (sbyte*)(memory + pageSize - arrayLength);

                Span<sbyte> conditionalValues = new Span<sbyte>(pCond, arrayLength);
                conditionalValues.Fill(1);

                int[] array = new int[130];
                Array.Fill(array, 10);

                int sourceIndex = 27;
                int destIndex = 28;
                int length = 94;

                AvxUtils.InPlaceMemCopyConditionalAddition(
                    array: array,
                    conditionalValues: conditionalValues,
                    sourceIndex: sourceIndex,
                    destIndex: destIndex,
                    length: length,
                    valueToAdd: 5,
                    conditionalValue: 1
                );

                Assert.Equal(15, array[destIndex]);
            }
            finally
            {
                if (memory != IntPtr.Zero && memory != new IntPtr(-1))
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        VirtualFree(memory, UIntPtr.Zero, MEM_RELEASE);
                    }
                    else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    {
                        munmap(memory, (UIntPtr)(pageSize * 2));
                    }
                }
            }
        }

        /// <summary>
        /// Tests the InPlaceMemCopyConditionalAddition forward (non-overlap) path when the
        /// conditional values array is near a page boundary. LoadVector128 reads 16 bytes but
        /// only 8 are needed, which can cause an access violation.
        /// </summary>
        [Fact]
        public unsafe void TestInPlaceMemCopyConditionalAdditionForwardAtPageBoundary()
        {
            int pageSize = Environment.SystemPageSize;
            IntPtr memory = IntPtr.Zero;

            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    memory = VirtualAlloc(IntPtr.Zero, (UIntPtr)(pageSize * 2), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
                    if (memory == IntPtr.Zero) throw new OutOfMemoryException("VirtualAlloc failed");

                    VirtualProtect(memory + pageSize, (UIntPtr)pageSize, PAGE_NOACCESS, out _);
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                {
                    memory = mmap(IntPtr.Zero, (UIntPtr)(pageSize * 2), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, IntPtr.Zero);
                    if (memory == new IntPtr(-1)) throw new OutOfMemoryException("mmap failed");

                    mprotect(memory + pageSize, (UIntPtr)pageSize, PROT_NONE);
                }
                else
                {
                    Assert.Fail("OS not supported for guard page testing.");
                    return;
                }

                // Place 10 sbytes right before the guard page.
                // The forward path processes 8 elements at a time via AVX.
                // With sourceIndex=0, length=10, and only 10 bytes before the guard page,
                // a LoadVector128 at offset 0 reads 16 bytes, crossing into the guard page.
                int arrayLength = 10;
                sbyte* pCond = (sbyte*)(memory + pageSize - arrayLength);

                Span<sbyte> conditionalValues = new Span<sbyte>(pCond, arrayLength);
                conditionalValues.Fill(1);

                int[] array = new int[arrayLength];
                Array.Fill(array, 10);

                // sourceIndex=0, destIndex=0 means no overlap, takes forward path
                AvxUtils.InPlaceMemCopyConditionalAddition(
                    array: array,
                    conditionalValues: conditionalValues,
                    sourceIndex: 0,
                    destIndex: 0,
                    length: arrayLength,
                    valueToAdd: 5,
                    conditionalValue: 1
                );

                for (int i = 0; i < arrayLength; i++)
                {
                    Assert.Equal(15, array[i]);
                }
            }
            finally
            {
                if (memory != IntPtr.Zero && memory != new IntPtr(-1))
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        VirtualFree(memory, UIntPtr.Zero, MEM_RELEASE);
                    }
                    else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    {
                        munmap(memory, (UIntPtr)(pageSize * 2));
                    }
                }
            }
        }

        /// <summary>
        /// Tests the MemCopyAdditionByType method when the typeIds array is near a page boundary.
        /// LoadVector128 reads 16 bytes for typeIds but only 8 are needed per iteration.
        /// </summary>
        [Fact]
        public unsafe void TestMemCopyAdditionByTypeAtPageBoundary()
        {
            int pageSize = Environment.SystemPageSize;
            IntPtr memory = IntPtr.Zero;

            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    memory = VirtualAlloc(IntPtr.Zero, (UIntPtr)(pageSize * 2), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
                    if (memory == IntPtr.Zero) throw new OutOfMemoryException("VirtualAlloc failed");

                    VirtualProtect(memory + pageSize, (UIntPtr)pageSize, PAGE_NOACCESS, out _);
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                {
                    memory = mmap(IntPtr.Zero, (UIntPtr)(pageSize * 2), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, IntPtr.Zero);
                    if (memory == new IntPtr(-1)) throw new OutOfMemoryException("mmap failed");

                    mprotect(memory + pageSize, (UIntPtr)pageSize, PROT_NONE);
                }
                else
                {
                    Assert.Fail("OS not supported for guard page testing.");
                    return;
                }

                // Place exactly 10 typeId bytes before the guard page.
                // The vectorized path processes 8 at a time; LoadVector128 at offset 0
                // reads 16 bytes, crossing into the guard page.
                int arrayLength = 10;
                sbyte* pTypeIds = (sbyte*)(memory + pageSize - arrayLength);

                Span<sbyte> typeIds = new Span<sbyte>(pTypeIds, arrayLength);
                // Use type 0 and type 1 alternating
                for (int i = 0; i < arrayLength; i++)
                {
                    typeIds[i] = (sbyte)(i % 2);
                }

                int[] source = new int[arrayLength];
                int[] destination = new int[arrayLength];
                Array.Fill(source, 10);

                // toAdd must have at least 8 elements for AVX path (padded)
                int[] toAdd = new int[8];
                toAdd[0] = 100;
                toAdd[1] = 200;

                AvxUtils.MemCopyAdditionByType(
                    source: source,
                    destination: destination,
                    typeIds: typeIds,
                    sourceIndex: 0,
                    destIndex: 0,
                    length: arrayLength,
                    toAdd: toAdd,
                    numberOfTypes: 2
                );

                for (int i = 0; i < arrayLength; i++)
                {
                    int expectedAdd = (i % 2 == 0) ? 100 : 200;
                    Assert.Equal(10 + expectedAdd, destination[i]);
                }
            }
            finally
            {
                if (memory != IntPtr.Zero && memory != new IntPtr(-1))
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        VirtualFree(memory, UIntPtr.Zero, MEM_RELEASE);
                    }
                    else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    {
                        munmap(memory, (UIntPtr)(pageSize * 2));
                    }
                }
            }
        }

        /// <summary>
        /// Tests the InPlaceMemCopyAdditionByType backward (overlap) path when the typeIds
        /// array is near a page boundary. LoadVector128 on line 326 reads 16 bytes but only 8
        /// are needed.
        /// </summary>
        [Fact]
        public unsafe void TestInPlaceMemCopyAdditionByTypeOverlapAtPageBoundary()
        {
            int pageSize = Environment.SystemPageSize;
            IntPtr memory = IntPtr.Zero;

            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    memory = VirtualAlloc(IntPtr.Zero, (UIntPtr)(pageSize * 2), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
                    if (memory == IntPtr.Zero) throw new OutOfMemoryException("VirtualAlloc failed");

                    VirtualProtect(memory + pageSize, (UIntPtr)pageSize, PAGE_NOACCESS, out _);
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                {
                    memory = mmap(IntPtr.Zero, (UIntPtr)(pageSize * 2), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, IntPtr.Zero);
                    if (memory == new IntPtr(-1)) throw new OutOfMemoryException("mmap failed");

                    mprotect(memory + pageSize, (UIntPtr)pageSize, PROT_NONE);
                }
                else
                {
                    Assert.Fail("OS not supported for guard page testing.");
                    return;
                }

                // We need sourceIndex < destIndex && sourceIndex + length > destIndex to trigger overlap path.
                // Place typeIds right before the guard page.
                // sourceIndex=27, destIndex=28, length=94 => processes typeIds[27..120]
                // Total typeIds span: 121 bytes, positioned so byte 120 is the last byte before the guard page.
                int arrayLength = 121;
                sbyte* pTypeIds = (sbyte*)(memory + pageSize - arrayLength);

                Span<sbyte> typeIds = new Span<sbyte>(pTypeIds, arrayLength);
                for (int i = 0; i < arrayLength; i++)
                {
                    typeIds[i] = (sbyte)(i % 3);
                }

                int[] array = new int[130];
                Array.Fill(array, 10);

                int[] toAdd = new int[8];
                toAdd[0] = 100;
                toAdd[1] = 200;
                toAdd[2] = 300;

                int sourceIndex = 27;
                int destIndex = 28;
                int length = 94;

                AvxUtils.InPlaceMemCopyAdditionByType(
                    array: array,
                    typeIds: typeIds,
                    sourceIndex: sourceIndex,
                    destIndex: destIndex,
                    length: length,
                    toAdd: toAdd,
                    numberOfTypes: 3
                );

                // Verify the first element moved
                int expectedTypeId = typeIds[sourceIndex] % 3;
                Assert.Equal(10 + toAdd[expectedTypeId], array[destIndex]);
            }
            finally
            {
                if (memory != IntPtr.Zero && memory != new IntPtr(-1))
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        VirtualFree(memory, UIntPtr.Zero, MEM_RELEASE);
                    }
                    else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    {
                        munmap(memory, (UIntPtr)(pageSize * 2));
                    }
                }
            }
        }

        /// <summary>
        /// Tests the InPlaceMemCopyAdditionByType forward (non-overlap) path when the typeIds
        /// array is near a page boundary. LoadVector128 on line 351 reads 16 bytes but only 8
        /// are needed.
        /// </summary>
        [Fact]
        public unsafe void TestInPlaceMemCopyAdditionByTypeForwardAtPageBoundary()
        {
            int pageSize = Environment.SystemPageSize;
            IntPtr memory = IntPtr.Zero;

            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    memory = VirtualAlloc(IntPtr.Zero, (UIntPtr)(pageSize * 2), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
                    if (memory == IntPtr.Zero) throw new OutOfMemoryException("VirtualAlloc failed");

                    VirtualProtect(memory + pageSize, (UIntPtr)pageSize, PAGE_NOACCESS, out _);
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                {
                    memory = mmap(IntPtr.Zero, (UIntPtr)(pageSize * 2), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, IntPtr.Zero);
                    if (memory == new IntPtr(-1)) throw new OutOfMemoryException("mmap failed");

                    mprotect(memory + pageSize, (UIntPtr)pageSize, PROT_NONE);
                }
                else
                {
                    Assert.Fail("OS not supported for guard page testing.");
                    return;
                }

                // Place exactly 10 typeId bytes before the guard page.
                // Forward path with sourceIndex=0, destIndex=0 (no overlap).
                // Processes 8 at a time; LoadVector128 at offset 0 reads 16 bytes.
                int arrayLength = 10;
                sbyte* pTypeIds = (sbyte*)(memory + pageSize - arrayLength);

                Span<sbyte> typeIds = new Span<sbyte>(pTypeIds, arrayLength);
                for (int i = 0; i < arrayLength; i++)
                {
                    typeIds[i] = (sbyte)(i % 2);
                }

                int[] array = new int[arrayLength];
                Array.Fill(array, 10);

                int[] toAdd = new int[8];
                toAdd[0] = 100;
                toAdd[1] = 200;

                AvxUtils.InPlaceMemCopyAdditionByType(
                    array: array,
                    typeIds: typeIds,
                    sourceIndex: 0,
                    destIndex: 0,
                    length: arrayLength,
                    toAdd: toAdd,
                    numberOfTypes: 2
                );

                for (int i = 0; i < arrayLength; i++)
                {
                    int expectedAdd = (i % 2 == 0) ? 100 : 200;
                    Assert.Equal(10 + expectedAdd, array[i]);
                }
            }
            finally
            {
                if (memory != IntPtr.Zero && memory != new IntPtr(-1))
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        VirtualFree(memory, UIntPtr.Zero, MEM_RELEASE);
                    }
                    else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    {
                        munmap(memory, (UIntPtr)(pageSize * 2));
                    }
                }
            }
        }
    }
}

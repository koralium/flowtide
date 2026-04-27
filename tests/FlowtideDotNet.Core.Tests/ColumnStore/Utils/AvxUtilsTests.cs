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
    }
}

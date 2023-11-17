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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.FileCache.Internal.Unix
{
    internal class AlignedBuffer : IDisposable
    {
        private IntPtr buffer;
        private int bufferSize;
        private int alignment;
        private IntPtr allocated;

        public IntPtr Buffer => buffer;
        public int Size => bufferSize;

        public AlignedBuffer(int size, int alignment)
        {
            this.bufferSize = size;
            this.alignment = alignment;
            allocated = Marshal.AllocHGlobal(size + alignment);
            long ptr = allocated.ToInt64();
            long alignedPtr = (ptr + alignment - 1) / alignment * alignment;
            buffer = new IntPtr(alignedPtr);
        }

        public void Dispose()
        {
            if (allocated != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(allocated);
                buffer = IntPtr.Zero;
                allocated = IntPtr.Zero;
            }
        }
    }
}

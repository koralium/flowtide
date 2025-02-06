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
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.FileCache.Internal.Unix
{
    internal class AlignedBuffer : MemoryManager<byte>, IDisposable
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

        protected override void Dispose(bool disposing)
        {
            if (allocated != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(allocated);
                buffer = IntPtr.Zero;
                allocated = IntPtr.Zero;
            }
        }

        public unsafe override Span<byte> GetSpan()
        {
            return new Span<byte>(buffer.ToPointer(), bufferSize);
        }

        public unsafe override MemoryHandle Pin(int elementIndex = 0)
        {
            return new MemoryHandle(((byte*)buffer.ToPointer()) + elementIndex, default, default);
        }

        public override void Unpin()
        {
        }
    }
}

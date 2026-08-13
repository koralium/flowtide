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

namespace FlowtideDotNet.Storage.Memory
{
    /// <summary>
    /// Exposes a memory block as Memory without owning it.
    /// </summary>
    public sealed unsafe class NativeMemoryView : MemoryManager<byte>
    {
        private void* _ptr;
        private int _length;

        public NativeMemoryView(void* ptr, int length)
        {
            _ptr = ptr;
            _length = length;
        }

        /// <summary>
        /// Re-points the view after a realloc.
        /// </summary>
        public void Update(void* ptr, int length)
        {
            _ptr = ptr;
            _length = length;
        }

        public override Span<byte> GetSpan()
        {
            return new Span<byte>(_ptr, _length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            return new MemoryHandle(((byte*)_ptr) + elementIndex, default, default);
        }

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}

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
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Memory
{
    internal unsafe class BatchMemoryOwner : MemoryManager<byte>
    {
        private readonly BatchMemoryManager _batchMemoryManager;
        private readonly void* _ptr;
        private readonly int _length;

        internal BatchMemoryOwner(BatchMemoryManager batchMemoryManager, void* ptr, int length)
        {
            this._batchMemoryManager = batchMemoryManager;
            this._ptr = ptr;
            this._length = length;
        }

        public override Span<byte> GetSpan()
        {
            return new Span<byte>(_ptr, _length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            return new MemoryHandle(_ptr, default, default);
        }

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
            _batchMemoryManager.Free(_ptr);
        }
    }
}

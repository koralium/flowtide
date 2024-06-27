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
    /// <summary>
    /// Class used to create memory owners where each owner is a part of a larger memory block.
    /// The root memory is only freed when all the owners are disposed.
    /// </summary>
    //internal unsafe class MultiBatchMemoryOwner : MemoryManager<byte>
    //{
    //    private readonly BatchMemoryManager _batchMemoryManager;
    //    private readonly void* _rootPtr;
    //    private readonly void* _ptr;
    //    private readonly int _length;

    //    public MultiBatchMemoryOwner(BatchMemoryManager batchMemoryManager, void* rootPtr, void* ptr, int length)
    //    {
    //        this._batchMemoryManager = batchMemoryManager;
    //        this._rootPtr = rootPtr;
    //        this._ptr = ptr;
    //        this._length = length;
    //    }

    //    public override Span<byte> GetSpan()
    //    {
    //        return new Span<byte>(_ptr, _length);
    //    }

    //    public override MemoryHandle Pin(int elementIndex = 0)
    //    {
    //        return new MemoryHandle(_ptr, default, default);
    //    }

    //    public override void Unpin()
    //    {
    //    }

    //    protected override void Dispose(bool disposing)
    //    {
    //        _batchMemoryManager.Free(_rootPtr);
    //    }
    //}
}

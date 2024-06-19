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

using Apache.Arrow;
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.Utils;
using SqlParser.Ast;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    /// <summary>
    /// Converts an Arrow array to an internal column.
    /// 
    /// This causes alot of pinning of memory, so it is important to dispose of the column when done.
    /// A converted arrow array should be shortlived and not stored for long periods of time.
    /// </summary>
    internal unsafe class ArrowToInternalVisitor : 
        IArrowArrayVisitor<Int64Array>,
        IArrowArrayVisitor<StringArray>
    {
        private readonly IMemoryOwner<byte> recordBatchMemoryOwner;
        private readonly BatchMemoryManager batchMemoryManager;
        private readonly void* _rootPtr;
        private int _rootUsageCount;

        public Column? Column { get; private set; }

        public ArrowToInternalVisitor(IMemoryOwner<byte> recordBatchMemoryOwner, BatchMemoryManager batchMemoryManager)
        {
            this.recordBatchMemoryOwner = recordBatchMemoryOwner;
            this.batchMemoryManager = batchMemoryManager;
            _rootPtr = recordBatchMemoryOwner.Memory.Pin().Pointer;
        }

        private IMemoryOwner<byte> GetMemoryOwner(ArrowBuffer buffer)
        {
            var memoryHandle = buffer.Memory.Pin();
            _rootUsageCount++;
            IMemoryOwner<byte> bitmapMemoryOwner = new MultiBatchMemoryOwner(batchMemoryManager, _rootPtr, memoryHandle.Pointer, buffer.Memory.Length);
            memoryHandle.Dispose();
            return bitmapMemoryOwner;
        }

        public void Finish()
        {
            batchMemoryManager.AddUsedMemory(new nint(_rootPtr), recordBatchMemoryOwner, _rootUsageCount);
        }

        public void Visit(Int64Array array)
        {
            BitmapList bitmapList;
            
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = GetMemoryOwner(array.NullBitmapBuffer);
                bitmapList = new BitmapList(bitmapMemoryOwner, array.Length, new NativeMemoryAllocator());
            }
            else
            {
                bitmapList = new BitmapList(batchMemoryManager);
            }

            Int64Column int64Column = new Int64Column(GetMemoryOwner(array.ValueBuffer), array.Length, batchMemoryManager);

            Column = new Column(array.NullCount, int64Column, bitmapList, ArrowTypeId.Int64);
        }

        public void Visit(IArrowArray array)
        {
            
            throw new NotImplementedException($"Type {array.GetType().Name} is not yet supported.");
        }

        public void Visit(StringArray array)
        {
            BitmapList bitmapList;

            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = GetMemoryOwner(array.NullBitmapBuffer);
                bitmapList = new BitmapList(bitmapMemoryOwner, array.Length, new NativeMemoryAllocator());
            }
            else
            {
                bitmapList = new BitmapList(batchMemoryManager);
            }

            var offsetMemoryOwner = GetMemoryOwner(array.ValueOffsetsBuffer);
            var dataMemoryOwner = GetMemoryOwner(array.ValueBuffer);
            var stringColumn = new StringColumn(offsetMemoryOwner, array.ValueOffsets.Length, dataMemoryOwner, batchMemoryManager);

            Column = new Column(array.NullCount, stringColumn, bitmapList, ArrowTypeId.String);
        }
    }
}

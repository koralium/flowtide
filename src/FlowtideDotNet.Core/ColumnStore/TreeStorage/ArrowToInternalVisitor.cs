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
    internal class ArrowToInternalVisitor : 
        IArrowArrayVisitor<Int64Array>,
        IArrowArrayVisitor<StringArray>
    {
        private static readonly FieldInfo _memoryOwnerField = GetMethodArrowBufferMemoryOwner();
        private static FieldInfo GetMethodArrowBufferMemoryOwner()
        {
            var fieldInfo = typeof(ArrowBuffer).GetField("_memoryOwner", BindingFlags.NonPublic | BindingFlags.Instance);
            return fieldInfo!;
        }

        public Column? Column { get; private set; }

        private static IMemoryOwner<byte> GetArrowBufferMemoryOwner(ArrowBuffer arrowBuffer)
        {
            return (IMemoryOwner<byte>)_memoryOwnerField.GetValue(arrowBuffer)!;
        }

        public void Visit(Int64Array array)
        {
            BitmapList bitmapList;
            
            if (array.NullCount > 0)
            {
                var bitmapMemory = GetArrowBufferMemoryOwner(array.NullBitmapBuffer);
                bitmapList = new BitmapList(bitmapMemory, array.Length, new NativeMemoryAllocator());
            }
            else
            {
                bitmapList = new BitmapList(new NativeMemoryAllocator());
            }

            Int64Column int64Column;
            var memoryOwner = GetArrowBufferMemoryOwner(array.ValueBuffer);
            
            // Try and use the memory owner if possible.
            if (memoryOwner != null)
            {
                int64Column = new Int64Column(memoryOwner, array.Length);
            }
            else
            {
                int64Column = new Int64Column(array.ValueBuffer.Memory, array.Length);
            }

            Column = new Column(array.NullCount, int64Column, bitmapList, ArrowTypeId.Int64);
        }

        public void Visit(IArrowArray array)
        {
            
            throw new NotImplementedException($"Type {array.GetType().Name} is not yet supported.");
        }

        public void Visit(StringArray array)
        {
            
            throw new NotImplementedException();
        }
    }
}

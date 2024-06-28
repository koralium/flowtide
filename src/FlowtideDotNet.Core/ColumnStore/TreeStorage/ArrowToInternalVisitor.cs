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
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.Serialization.CustomTypes;
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
        IArrowArrayVisitor<StringArray>,
        IArrowArrayVisitor<ListArray>,
        IArrowArrayVisitor<DenseUnionArray>,
        IArrowArrayVisitor<UnionArray>,
        IArrowArrayVisitor<NullArray>,
        IArrowArrayVisitor<MapArray>,
        IArrowArrayVisitor<BooleanArray>,
        IArrowArrayVisitor<DoubleArray>,
        IArrowArrayVisitor<BinaryArray>,
        IArrowArrayVisitor<FixedSizeBinaryArray>
    {
        private readonly IMemoryOwner<byte> recordBatchMemoryOwner;
        //private readonly BatchMemoryManager batchMemoryManager;
        private readonly PreAllocatedMemoryManager preAllocatedMemoryManager;
        private readonly void* _rootPtr;
        private int _rootUsageCount;

        private IDataColumn? _dataColumn;
        private BitmapList? _bitmapList;
        private int _nullCount;
        private ArrowTypeId _typeId;

        public Column? Column
        {
            get
            {
                if (_dataColumn == null)
                {
                    return null;
                }
                BitmapList? bitmapList = _bitmapList;
                if (bitmapList == null)
                {
                    bitmapList = new BitmapList(preAllocatedMemoryManager);
                }
                return new Column(_nullCount, _dataColumn, bitmapList, _typeId, preAllocatedMemoryManager);
            }
        }

        public Field? CurrentField { get; set; }

        public ArrowToInternalVisitor(IMemoryOwner<byte> recordBatchMemoryOwner, BatchMemoryManager batchMemoryManager)
        {
            this.recordBatchMemoryOwner = recordBatchMemoryOwner;
            //this.batchMemoryManager = batchMemoryManager;
            preAllocatedMemoryManager = new PreAllocatedMemoryManager();
            _rootPtr = recordBatchMemoryOwner.Memory.Pin().Pointer;
        }

        private IMemoryOwner<byte> GetMemoryOwner(ArrowBuffer buffer)
        {
            var memoryHandle = buffer.Memory.Pin();
            _rootUsageCount++;
            IMemoryOwner<byte> bitmapMemoryOwner = new PreAllocatedMemoryOwner(preAllocatedMemoryManager, memoryHandle.Pointer, buffer.Memory.Length); //new PreAllocatedMemoryOwner(batchMemoryManager, _rootPtr, memoryHandle.Pointer, buffer.Memory.Length);
            memoryHandle.Dispose();
            return bitmapMemoryOwner;
        }

        public void Finish()
        {
            preAllocatedMemoryManager.Initialize(recordBatchMemoryOwner, _rootUsageCount);
            //batchMemoryManager.AddUsedMemory(new nint(_rootPtr), recordBatchMemoryOwner, _rootUsageCount);
        }

        public void Visit(Int64Array array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = GetMemoryOwner(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length, preAllocatedMemoryManager);
            }
            else
            {
                _bitmapList = null;
            }

            Int64Column int64Column = Int64ColumnFactory.Get(GetMemoryOwner(array.ValueBuffer), array.Length, preAllocatedMemoryManager);
            _dataColumn = int64Column;
            _typeId = ArrowTypeId.Int64;
        }

        public void Visit(IArrowArray array)
        {
            
            throw new NotImplementedException($"Type {array.GetType().Name} is not yet supported.");
        }

        public void Visit(StringArray array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = GetMemoryOwner(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length, preAllocatedMemoryManager);
            }
            else
            {
                _bitmapList = null;
            }

            var offsetMemoryOwner = GetMemoryOwner(array.ValueOffsetsBuffer);
            var dataMemoryOwner = GetMemoryOwner(array.ValueBuffer);
            var stringColumn = new StringColumn(offsetMemoryOwner, array.ValueOffsets.Length, dataMemoryOwner, preAllocatedMemoryManager);

            _dataColumn = stringColumn;
            _typeId = ArrowTypeId.String;
        }

        public void Visit(ListArray array)
        {
            var previousField = CurrentField;
            var listType = (CurrentField!.DataType as ListType)!;
            CurrentField = listType.Fields[0];
            array.Values.Accept(this);
            var column = Column;
            CurrentField = previousField;

            if (column == null)
            {
                throw new InvalidOperationException("Internal list column is null");
            }

            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = GetMemoryOwner(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length, preAllocatedMemoryManager);
            }
            else
            {
                _bitmapList = null;
            }

            var offsetMemoryOwner = GetMemoryOwner(array.ValueOffsetsBuffer);

            _dataColumn = new ListColumn(column, offsetMemoryOwner, array.ValueOffsets.Length, preAllocatedMemoryManager);
            _typeId = ArrowTypeId.List;
        }

        public void Visit(DenseUnionArray array)
        {
            _nullCount = 0;
            _bitmapList = null;

            var typeMemory = GetMemoryOwner(array.TypeBuffer);
            var offsetMemory = GetMemoryOwner(array.ValueOffsetBuffer);

            List<IDataColumn> columns = new List<IDataColumn>();
            for (int i = 0; i < array.Fields.Count; i++)
            {
                array.Fields[i].Accept(this);

                if (_nullCount > 0 && _typeId != ArrowTypeId.Null)
                {
                    throw new InvalidOperationException("Inner columns in a union should not have null values, they should be on the union level");
                }
                
                columns.Add(_dataColumn ?? throw new InvalidOperationException("Internal column is null"));
            }

            _dataColumn = new UnionColumn(columns, typeMemory, offsetMemory, array.Length, preAllocatedMemoryManager);
            _typeId = ArrowTypeId.Union;
        }

        public void Visit(UnionArray array)
        {
            if (array.Mode == Apache.Arrow.Types.UnionMode.Dense)
            {
                Visit((DenseUnionArray)array);
                return;
            }
            throw new NotImplementedException();
        }

        public void Visit(NullArray array)
        {
            _dataColumn = new NullColumn(array.NullCount);
            _typeId = ArrowTypeId.Null;
            _bitmapList = null;
            _nullCount = array.NullCount;
        }

        public void Visit(MapArray array)
        {
            var previousField = CurrentField;
            var type = (MapType)CurrentField!.DataType;
            var structField = type.Fields[0];
            var structType = (StructType)structField.DataType;
            CurrentField = structType.Fields[0];
            array.Keys.Accept(this);
            var keyColumn = Column;
            CurrentField = structType.Fields[1];
            array.Values.Accept(this);
            var valueColumn = Column;
            CurrentField = previousField;

            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = GetMemoryOwner(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length, preAllocatedMemoryManager);
            }
            else
            {
                _bitmapList = null;
            }

            var offsetMemoryOwner = GetMemoryOwner(array.ValueOffsetsBuffer);
            
            _dataColumn = new MapColumn(keyColumn!, valueColumn!, offsetMemoryOwner, array.ValueOffsets.Length, preAllocatedMemoryManager);
            _typeId = ArrowTypeId.Map;
        }

        public void Visit(BooleanArray array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = GetMemoryOwner(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length, preAllocatedMemoryManager);
            }
            else
            {
                _bitmapList = null;
            }

            var valueMemoryOwner = GetMemoryOwner(array.ValueBuffer);
            _dataColumn = new BoolColumn(valueMemoryOwner, array.Length, preAllocatedMemoryManager);
            _typeId = ArrowTypeId.Boolean;
        }

        public void Visit(DoubleArray array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = GetMemoryOwner(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length, preAllocatedMemoryManager);
            }
            else
            {
                _bitmapList = null;
            }

            var valueBuffer = GetMemoryOwner(array.ValueBuffer);
            _dataColumn = new DoubleColumn(valueBuffer, array.Length, preAllocatedMemoryManager);
            _typeId = ArrowTypeId.Double;
        }

        public void Visit(BinaryArray array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = GetMemoryOwner(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length, preAllocatedMemoryManager);
            }
            else
            {
                _bitmapList = null;
            }
            var offsetBuffer = GetMemoryOwner(array.ValueOffsetsBuffer);
            var dataBuffer = GetMemoryOwner(array.ValueBuffer);

            _dataColumn = new BinaryColumn(offsetBuffer, array.ValueOffsets.Length, dataBuffer, preAllocatedMemoryManager);
            _typeId = ArrowTypeId.Binary;
        }

        public void Visit(FixedSizeBinaryArray array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = GetMemoryOwner(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length, preAllocatedMemoryManager);
            }
            else
            {
                _bitmapList = null;
            }

            if (CurrentField != null && CurrentField.HasMetadata &&
                CurrentField.Metadata.TryGetValue("ARROW:extension:name", out var typeName))
            {
                switch (typeName)
                {
                    case FloatingPointDecimalType.ExtensionName:
                        var dataMemory = GetMemoryOwner(array.ValueBuffer);
                        _dataColumn = new DecimalColumn(dataMemory, array.Length, preAllocatedMemoryManager);
                        _typeId = ArrowTypeId.Decimal128;
                        break;
                    default:
                        throw new NotImplementedException();
                }
                return;
            }
            throw new NotImplementedException();
        }
    }
}

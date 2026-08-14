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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization.CustomTypes;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Diagnostics;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    /// <summary>
    /// Converts an Arrow array to an internal column, copying every buffer.
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
        IArrowArrayVisitor<FixedSizeBinaryArray>,
        IArrowArrayVisitor<Int8Array>,
        IArrowArrayVisitor<Int16Array>,
        IArrowArrayVisitor<Int32Array>,
        IArrowArrayVisitor<StructArray>
    {
        private readonly IMemoryAllocator _memoryAllocator;

        private IDataColumn? _dataColumn;
        private BitmapList _bitmapList;
        private int _nullCount;
        private ArrowTypeId _typeId;

        public Column? Column
        {
            get
            {
                // We clear the field right after so the column owns it.
#pragma warning disable RS0042
                var column = Column.Create(_nullCount, _dataColumn, _bitmapList, _typeId, _memoryAllocator);
#pragma warning restore RS0042
                _bitmapList = default;
                return column;
            }
        }

        public Field? CurrentField { get; set; }

        public ArrowToInternalVisitor(IMemoryAllocator memoryManager)
        {
            _memoryAllocator = memoryManager;
        }

        private FlowtideMemory CopyBuffer(ArrowBuffer buffer)
        {
            if (buffer.Length == 0)
            {
                return default;
            }
            var memory = _memoryAllocator.AllocateMemory(buffer.Length);
            buffer.Span.CopyTo(memory.Span);
            return memory;
        }

        /// <summary>
        /// Frees state left behind when a visit throws.
        /// </summary>
        internal void DisposeUnconsumed()
        {
            _bitmapList.Dispose(_memoryAllocator);
            _dataColumn?.Dispose(_memoryAllocator);
            _dataColumn = null;
        }

        public void Visit(Int64Array array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }

            _dataColumn = new IntegerColumn(_memoryAllocator, CopyBuffer(array.ValueBuffer), array.Length, 64);
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
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }

            var offsetMemoryOwner = CopyBuffer(array.ValueOffsetsBuffer);
            var dataMemoryOwner = CopyBuffer(array.ValueBuffer);
            var stringColumn = new StringColumn(offsetMemoryOwner, array.ValueOffsets.Length, dataMemoryOwner, _memoryAllocator);

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
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }

            var offsetMemoryOwner = CopyBuffer(array.ValueOffsetsBuffer);

            _dataColumn = new ListColumn(column, offsetMemoryOwner, array.ValueOffsets.Length, _memoryAllocator);
            _typeId = ArrowTypeId.List;
        }

        public void Visit(DenseUnionArray array)
        {
            Debug.Assert(CurrentField != null);
            _nullCount = 0;
            _bitmapList = default;

            var type = (UnionType)CurrentField.DataType;

            var typeMemory = CopyBuffer(array.TypeBuffer);
            var offsetMemory = CopyBuffer(array.ValueOffsetBuffer);

            List<IDataColumn> columns = new List<IDataColumn>();
            try
            {
                for (int i = 0; i < array.Fields.Count; i++)
                {
                    var previousField = CurrentField;
                    CurrentField = type.Fields[i];
                    array.Fields[i].Accept(this);
                    CurrentField = previousField;
                    if (_typeId == ArrowTypeId.Null)
                    {
                        _dataColumn = new NullColumn(_nullCount);
                    }
                    if (_nullCount > 0 && _typeId != ArrowTypeId.Null)
                    {
                        throw new InvalidOperationException("Inner columns in a union should not have null values, they should be on the union level");
                    }

                    columns.Add(_dataColumn ?? throw new InvalidOperationException("Internal column is null"));
                    // Reset null counter
                    _nullCount = 0;
                }

                // The constructor can throw on corrupt input so it runs inside the try.
                _dataColumn = new UnionColumn(columns, typeMemory, offsetMemory, array.Length, _memoryAllocator);
            }
            catch
            {
                _memoryAllocator.Free(ref typeMemory);
                _memoryAllocator.Free(ref offsetMemory);
                foreach (var column in columns)
                {
                    // DisposeUnconsumed disposes _dataColumn.
                    if (!ReferenceEquals(column, _dataColumn))
                    {
                        column.Dispose(_memoryAllocator);
                    }
                }
                throw;
            }

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
            _dataColumn = null;
            _typeId = ArrowTypeId.Null;
            _bitmapList = default;
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
            Column? valueColumn;
            try
            {
                CurrentField = structType.Fields[1];
                array.Values.Accept(this);
                valueColumn = Column;
            }
            catch
            {
                keyColumn?.Dispose();
                throw;
            }
            CurrentField = previousField;

            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }

            var offsetMemoryOwner = CopyBuffer(array.ValueOffsetsBuffer);

            _dataColumn = new MapColumn(keyColumn!, valueColumn!, offsetMemoryOwner, array.ValueOffsets.Length, _memoryAllocator);
            _typeId = ArrowTypeId.Map;
        }

        public void Visit(BooleanArray array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }

            var valueMemoryOwner = CopyBuffer(array.ValueBuffer);
            _dataColumn = new BoolColumn(valueMemoryOwner, array.Length, _memoryAllocator);
            _typeId = ArrowTypeId.Boolean;
        }

        public void Visit(DoubleArray array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }

            var valueBuffer = CopyBuffer(array.ValueBuffer);
            _dataColumn = new DoubleColumn(valueBuffer, array.Length, _memoryAllocator);
            _typeId = ArrowTypeId.Double;
        }

        public void Visit(BinaryArray array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }
            var offsetBuffer = CopyBuffer(array.ValueOffsetsBuffer);
            var dataBuffer = CopyBuffer(array.ValueBuffer);

            _dataColumn = new BinaryColumn(offsetBuffer, array.ValueOffsets.Length, dataBuffer, _memoryAllocator);
            _typeId = ArrowTypeId.Binary;
        }

        public void Visit(FixedSizeBinaryArray array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }

            if (CurrentField != null && CurrentField.HasMetadata &&
                CurrentField.Metadata.TryGetValue("ARROW:extension:name", out var typeName))
            {
                switch (typeName)
                {
                    case FloatingPointDecimalType.ExtensionName:
                        var dataMemory = CopyBuffer(array.ValueBuffer);
                        _dataColumn = new DecimalColumn(dataMemory, array.Length, _memoryAllocator);
                        _typeId = ArrowTypeId.Decimal128;
                        break;
                    case TimestampTzType.ExtensionName:
                        var timestampMemory = CopyBuffer(array.ValueBuffer);
                        _dataColumn = new TimestampTzColumn(timestampMemory, array.Length, _memoryAllocator);
                        _typeId = ArrowTypeId.Timestamp;
                        break;
                    default:
                        throw new NotImplementedException(typeName);
                }
                return;
            }
            throw new NotImplementedException("No metadata field");
        }

        public void Visit(Int8Array array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }

            _dataColumn = new IntegerColumn(_memoryAllocator, CopyBuffer(array.ValueBuffer), array.Length, 8);
            _typeId = ArrowTypeId.Int64;
        }

        public void Visit(Int16Array array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }

            _dataColumn = new IntegerColumn(_memoryAllocator, CopyBuffer(array.ValueBuffer), array.Length, 16);
            _typeId = ArrowTypeId.Int64;
        }

        public void Visit(Int32Array array)
        {
            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }

            _dataColumn = new IntegerColumn(_memoryAllocator, CopyBuffer(array.ValueBuffer), array.Length, 32);
            _typeId = ArrowTypeId.Int64;
        }

        public void Visit(StructArray array)
        {
            var previousField = CurrentField;
            var type = (StructType)CurrentField!.DataType;

            string[] columnNames = new string[array.Fields.Count];
            Column[] columns = new Column[array.Fields.Count];
            try
            {
                for (int i = 0; i < array.Fields.Count; i++)
                {
                    var field = type.Fields[i];
                    CurrentField = field;
                    columnNames[i] = CurrentField.Name;
                    array.Fields[i].Accept(this);
                    var createdColumn = Column;
                    if (createdColumn == null)
                    {
                        throw new InvalidOperationException("Internal column is null");
                    }
                    columns[i] = createdColumn;
                }
            }
            catch
            {
                foreach (var column in columns)
                {
                    column?.Dispose();
                }
                throw;
            }
            CurrentField = previousField;

            _nullCount = array.NullCount;
            if (array.NullCount > 0)
            {
                var bitmapMemoryOwner = CopyBuffer(array.NullBitmapBuffer);
                _bitmapList = new BitmapList(bitmapMemoryOwner, array.Length);
            }
            else
            {
                _bitmapList = default;
            }
            var header = StructHeader.Create(columnNames);

            _dataColumn = new StructColumn(header, columns, array.Length);
            _typeId = ArrowTypeId.Struct;
        }
    }
}

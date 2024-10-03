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
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.Expression;
using static SqlParser.Ast.TableConstraint;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    /// <summary>
    /// Union column is is used to handle multiple types in a single column.
    /// 
    /// The union column does not store all data in sorted order.
    /// Only the offsets will be in a sorted order.
    /// Therefore it must be compacted before serializing.
    /// </summary>
    internal class UnionColumn : IDataColumn
    {
        private readonly PrimitiveList<sbyte> _typeList;
        private IntList _offsets;
        private List<IDataColumn> _valueColumns;
        private readonly sbyte[] _typeIds;
        private readonly IMemoryAllocator _memoryAllocator;
        private bool disposedValue;

        public int Count => _typeList.Count;

        public ArrowTypeId Type => ArrowTypeId.Union;

        public UnionColumn(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _typeIds = new sbyte[35]; //35 types exist
            _typeList = new PrimitiveList<sbyte>(memoryAllocator);
            _offsets = new IntList(memoryAllocator);
            _valueColumns = new List<IDataColumn>()
            {
                new NullColumn()
            };
        }

        internal UnionColumn(List<IDataColumn> columns, IMemoryOwner<byte> typeListMemory, IMemoryOwner<byte> offsetMemory, int count, IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _valueColumns = columns;
            _typeIds = new sbyte[35]; //35 types exist
            _typeList = new PrimitiveList<sbyte>(typeListMemory, count, memoryAllocator);
            _offsets = new IntList(offsetMemory, count, memoryAllocator);
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                _typeIds[(int)_valueColumns[i].Type] = (sbyte)i;
            }
        }


        private void CheckArrayExist(in ArrowTypeId type, sbyte[] typeIds, List<IDataColumn> valueColumns)
        {
            if (type == ArrowTypeId.Null)
            {
                return;
            }
            var typeByte = (byte)type;
            if (typeIds[typeByte] == 0)
            {
                typeIds[typeByte] = (sbyte)valueColumns.Count;
                switch (type)
                {
                    case ArrowTypeId.Int64:
                        valueColumns.Add(Int64ColumnFactory.Get(_memoryAllocator));
                        break;
                    case ArrowTypeId.String:
                        valueColumns.Add(new StringColumn(_memoryAllocator));
                        break;
                    case ArrowTypeId.Boolean:
                        valueColumns.Add(new BoolColumn(_memoryAllocator));
                        break;
                    case ArrowTypeId.Double:
                        valueColumns.Add(new DoubleColumn(_memoryAllocator));
                        break;
                    case ArrowTypeId.List:
                        valueColumns.Add(new ListColumn(_memoryAllocator));
                        break;
                    case ArrowTypeId.Binary:
                        valueColumns.Add(new BinaryColumn(_memoryAllocator));
                        break;
                    case ArrowTypeId.Map:
                        _valueColumns.Add(new MapColumn(_memoryAllocator));
                        break;
                    case ArrowTypeId.Decimal128:
                        valueColumns.Add(new DecimalColumn(_memoryAllocator));
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public void InsertAt<T>(in int index, in T value) where T: IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _typeList.InsertAt(index, (sbyte)ArrowTypeId.Null);
                _valueColumns[0].InsertAt(index, value);
                _offsets.InsertAt(index, 0);
                return;
            }

            var typeByte = (byte)value.Type;
            CheckArrayExist(value.Type, _typeIds, _valueColumns);
            var arrayIndex = _typeIds[typeByte];
            // Find the first occurence of the same type in the type list
            var nextOccurence = AvxUtils.FindFirstOccurence(_typeList.Span, index, arrayIndex);
                
            var valueColumn = _valueColumns[arrayIndex];
            var nextOccurenceOffset = 0;
            if (nextOccurence < 0)
            {
                nextOccurenceOffset = valueColumn.Count;
            }
            else 
            {
                // Get the offset of the next occurence so this can be directly infront of it.
                nextOccurenceOffset = _offsets.Get(nextOccurence);
            }
            // Insert the value at the next occurence index to displace the old one
            valueColumn.InsertAt(in nextOccurenceOffset, in value);
            // Insert the offset and add 1 to the offset to all other offsets that are greater than the next occurence offset.
            _offsets.InsertAtConditionalAddition(index, nextOccurenceOffset, _typeList.Span, arrayIndex, 1);

            // Type list must be added to last so the element count when adding to offsets are the same.
            _typeList.InsertAt(index, arrayIndex);
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            int index = _typeList.Count;
            InsertAt(_offsets.Count, in value);
            return index;
        }

        public int BinarySearch(in IDataValue dataValue, in int start, in int end)
        {
            throw new NotImplementedException();
        }

        public int CompareTo_NoLock<T>(in int index, in T value, in ReferenceSegment? child, in BitmapList? validityList) where T : IDataValue
        {
            var typeIndex = _typeList[index];
            var type = (sbyte)_valueColumns[typeIndex].Type;
            if (type == (sbyte)value.Type)
            {
                var valueColumnIndex = _typeIds[(byte)type];
                var valueColumn = _valueColumns[valueColumnIndex];
                return valueColumn.CompareTo(_offsets.Get(index), in value, child, default);
            }
            else
            {
                return type - (sbyte)value.Type;
            }
        }

        public int CompareTo<T>(in int index, in T value, in ReferenceSegment? child, in BitmapList? validityList) where T : IDataValue
        {
            return CompareTo_NoLock(index, in value, in child, in validityList);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            var thisValueColumnIndex = _typeList[thisIndex];
            var thisType = _valueColumns[thisValueColumnIndex].Type;
            if (otherColumn is UnionColumn unionColumn)
            {
                var otherValueColumnIndex = unionColumn._typeList[otherIndex];
                var otherType = unionColumn._valueColumns[otherValueColumnIndex].Type;
                if (thisType == otherType)
                {
                    if (thisType == 0)
                    {
                        return 0;
                    }
                    var thisValueColumn = _valueColumns[thisValueColumnIndex];
                    var otherValueColumn = unionColumn._valueColumns[otherValueColumnIndex];
                    return thisValueColumn.CompareTo(in otherValueColumn, _offsets.Get(thisIndex), unionColumn._offsets.Get(otherIndex));
                }
                else
                {
                    return thisType - otherType;
                }
            }
            else
            {
                var value = otherColumn.GetValueAt(otherIndex, default);
                return CompareTo_NoLock(thisIndex, value, default, default);
            }
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            var valueColumnIndex = _typeList[index];
            if (valueColumnIndex == 0)
            {
                return new NullValue();
            }
            var valueColumn = _valueColumns[valueColumnIndex];
            return valueColumn.GetValueAt(_offsets.Get(index), child);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            var valueColumnIndex = _typeList[index];
            var valueColumn = _valueColumns[valueColumnIndex];
            valueColumn.GetValueAt(_offsets.Get(index), dataValueContainer, child);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundriesForDataColumnDesc(this, in dataValue, in start, in end, child, default);
            }
            return BoundarySearch.SearchBoundriesForDataColumn(this, in dataValue, in start, in end, child, default);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            var currentArrayIndex = _typeList[index];
            CheckArrayExist(value.Type, _typeIds, _valueColumns);
            var newValueArrayIndex = _typeIds[(byte)value.Type];

            if (currentArrayIndex != newValueArrayIndex)
            {
                // Remove previous value
                _valueColumns[currentArrayIndex].RemoveAt(_offsets.Get(index));
                _offsets.RemoveAtConditionalAddition(index, _typeList.Span, currentArrayIndex, -1);

                if (value.Type == ArrowTypeId.Null)
                {
                    _offsets.InsertAt(index, 0);
                    _typeList.Update(index, 0);
                    return index;
                }
                _typeList.RemoveAt(index);

                var nextOccurence = AvxUtils.FindFirstOccurence(_typeList.Span, index, newValueArrayIndex);
                var valueColumn = _valueColumns[newValueArrayIndex];
                var nextOccurenceOffset = 0;
                if (nextOccurence < 0)
                {
                    nextOccurenceOffset = valueColumn.Count;
                }
                else
                {
                    // Get the offset of the next occurence so this can be directly infront of it.
                    nextOccurenceOffset = _offsets.Get(nextOccurence);
                }
                // Insert the value at the next occurence index to displace the old one
                valueColumn.InsertAt(in nextOccurenceOffset, in value);
                // Insert the offset and add 1 to the offset to all other offsets that are greater than the next occurence offset.
                _offsets.InsertAtConditionalAddition(index, nextOccurenceOffset, _typeList.Span, newValueArrayIndex, 1);

                // Type list must be added to last so the element count when adding to offsets are the same.
                _typeList.InsertAt(index, newValueArrayIndex);
            }
            else
            {
                // Same type
                var valueColumn = _valueColumns[currentArrayIndex];
                var currentOffset = _offsets.Get(index);
                valueColumn.Update(currentOffset, in value);
            }
            return index;
        }

        public void RemoveAt(in int index)
        {
            var arrayIndex = _typeList.Get(index);
            var valueOffset = _offsets.Get(index);
            _valueColumns[arrayIndex].RemoveAt(valueOffset);
            _offsets.RemoveAtConditionalAddition(index, _typeList.Span, arrayIndex, -1);
            _typeList.RemoveAt(index);
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            List<Field> fields = new List<Field>();
            List<int> typeIds = new List<int>();
            List<IArrowArray> childArrays = new List<IArrowArray>();
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                if (_valueColumns[i] != null)
                {
                    var (arrowArray, arrowType) = _valueColumns[i].ToArrowArray(nullBuffer, nullCount);
                    typeIds.Add((int)arrowType.TypeId);
                    fields.Add(new Field("", arrowType, true));
                    childArrays.Add(arrowArray);
                }
            }
            var unionType = new UnionType(fields, typeIds, UnionMode.Dense);
            var typeIdsBuffer = new ArrowBuffer(_typeList.Memory);
            var offsetBuffer = new ArrowBuffer(_offsets.Memory);
            return (new DenseUnionArray(unionType, Count, childArrays, typeIdsBuffer, offsetBuffer), unionType);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _typeList.Dispose();
                    _offsets.Dispose();
                    foreach (var column in _valueColumns)
                    {
                        column.Dispose();
                    }
                    _valueColumns.Clear();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            var valueColumnIndex = _typeList[index];
            if (valueColumnIndex == 0)
            {
                return ArrowTypeId.Null;
            }
            return _valueColumns[valueColumnIndex].GetTypeAt(_offsets.Get(index), child);
        }

        public void Clear()
        {
            _typeList.Clear();
            _offsets.Clear();
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                _valueColumns[i].Clear();
            }
        }

        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            CheckArrayExist(ArrowTypeId.List, _typeIds, _valueColumns);

            var typeByte = (byte)ArrowTypeId.List;
            var arrayIndex = _typeIds[typeByte];
            _valueColumns[arrayIndex].AddToNewList(in value);
        }

        public int EndNewList()
        {
            CheckArrayExist(ArrowTypeId.List, _typeIds, _valueColumns);
            var typeByte = (byte)ArrowTypeId.List;
            var arrayIndex = _typeIds[typeByte];
            var offset = _valueColumns[arrayIndex].EndNewList();
            int index = _typeList.Count;
            _typeList.InsertAt(index, arrayIndex);
            _offsets.InsertAt(index, offset);
            return index;
        }

        public unsafe void RemoveRange(int start, int count)
        {
            // All value columns store the data in order so it is possible to remove the range from all value columns.
            // The type list can be iterated and start offset and end offset can be calculated for each value column.
            // The values are stored using stack alloc
            // A difference stack is also allocated that contains the negative difference between the start and end offset for each value column.
            // That column is used when removing offsets to subtract the value from all offsets being moved down.

            // Max number of different types is 127
            var startOffets = stackalloc int[127];
            var endOffsets = stackalloc int[127];
            var difference = stackalloc int[127];

            for (int i = 0; i < _valueColumns.Count; i++)
            {
                startOffets[i] = 0;
                endOffsets[i] = 0;
                difference[i] = 0;
            }

            var end = start + count;

            for (int i = start; i < end; i++)
            {
                var type = _typeList.Get(i);
                var offset = _offsets.Get(i);

                if (startOffets[type] == 0)
                {
                    startOffets[type] = offset;
                }
                endOffsets[type] = offset;
            }

            bool anyColumnHaveDataRemoved = false;
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                var startOffset = startOffets[i];
                var endOffset = endOffsets[i];

                difference[i] = startOffset - endOffset;

                if (endOffset == 0)
                {
                    continue;
                }
                anyColumnHaveDataRemoved = true;
                // Remove the range from the value column
                _valueColumns[i].RemoveRange(startOffset, endOffset - startOffset);
            }

            if (anyColumnHaveDataRemoved)
            {
                _offsets.RemoveRangeTypeBasedAddition(start, count, _typeList.Span, new Span<int>(difference, 127), _valueColumns.Count);
            }
            else
            {
                _offsets.RemoveRange(start, count);
            }
            

            _typeList.RemoveRange(start, count);
        }

        public int GetByteSize(int start, int end)
        {
            int size = 0;
            for (int i = start; i <= end; i++)
            {
                var valueColumnIndex = _typeList[i];
                var valueColumn = _valueColumns[valueColumnIndex];
                size += valueColumn.GetByteSize(_offsets.Get(i), _offsets.Get(i));
                
            }
            return size + ((end - start + 1) * sizeof(int));
        }

        public int GetByteSize()
        {
            int size = 0;
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                size += _valueColumns[i].GetByteSize();
            }
            return size + (Count * sizeof(int));
        }
    }
}

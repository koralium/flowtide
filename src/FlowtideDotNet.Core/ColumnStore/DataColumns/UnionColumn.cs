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
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    /// <summary>
    /// Union column is is used to handle multiple types in a single column.
    /// 
    /// The union column does not store all data in sorted order.
    /// Only the offsets will be in a sorted order.
    /// Therefore it must be compacted before serializing.
    /// </summary>
    internal class UnionColumn : IDataColumn, IReadOnlyList<IDataValue>
    {
        private readonly TypeList _typeList;
        private IntList _offsets;
        private List<IDataColumn> _valueColumns;
        private readonly sbyte[] _typeIds;
        private readonly IMemoryAllocator _memoryAllocator;
        private Dictionary<StructHeader, sbyte>? _structLookup;
        private bool disposedValue;

        public int Count => _typeList.Count;

        public ArrowTypeId Type => ArrowTypeId.Union;

        public StructHeader StructHeader => throw new NotImplementedException();

        public IDataValue this[int index] => GetValueAt(index, default);

        public UnionColumn(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _typeIds = new sbyte[35]; //35 types exist
            _typeList = new TypeList(memoryAllocator);
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
            _typeList = new TypeList(typeListMemory, count, memoryAllocator);
            _offsets = new IntList(offsetMemory, count, memoryAllocator);
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                if (_valueColumns[i].Type == ArrowTypeId.Struct)
                {
                    // If it is a struct, the struct lookup must added with the struct headers
                    // This is required so different structs can be in the union
                    CheckStructLookup();
                    _structLookup.Add(_valueColumns[i].StructHeader, (sbyte)i);
                }
                _typeIds[(int)_valueColumns[i].Type] = (sbyte)i;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [MemberNotNull(nameof(_structLookup))]
        private void CheckStructLookup()
        {
            if (_structLookup == null)
            {
                _structLookup = new Dictionary<StructHeader, sbyte>();
            }
        }

        internal UnionColumn(List<IDataColumn> columns, TypeList typeList, IntList offsets, sbyte[] typeIds, IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _valueColumns = columns;
            _typeIds = typeIds;
            _typeList = typeList;
            _offsets = offsets;
        }

        internal IDataColumn GetDataColumn(int i)
        {
            return _valueColumns[i];
        }


        private sbyte CheckArrayExist<T>(in T value, sbyte[] typeIds, List<IDataColumn> valueColumns)
            where T : IDataValue
        {
            var type = value.Type;
            if (type == ArrowTypeId.Struct)
            {
                CheckStructLookup();
                if (!_structLookup.TryGetValue(value.AsStruct.Header, out var existingIndex))
                {
                    var newIndex = (sbyte)valueColumns.Count;
                    if (valueColumns.Count >= 127)
                    {
                        throw new InvalidOperationException("Cannot add more than 127 types to a union column.");
                    }
                    
                    _structLookup.Add(value.AsStruct.Header, newIndex);
                    valueColumns.Add(new StructColumn(value.AsStruct.Header, _memoryAllocator));
                    return newIndex;
                }
                return existingIndex;
            }
            return CheckArrayTypeExist(in type, typeIds, valueColumns);
        }

        private sbyte CheckArrayTypeExist(in ArrowTypeId type, sbyte[] typeIds, List<IDataColumn> valueColumns)
        {
            if (type == ArrowTypeId.Null)
            {
                return 0;
            }
            var typeByte = (byte)type;
            var existingIndex = typeIds[typeByte];
            if (existingIndex == 0)
            {
                var newIndex = (sbyte)valueColumns.Count;
                typeIds[typeByte] = newIndex;
                switch (type)
                {
                    case ArrowTypeId.Int64:
                        valueColumns.Add(new IntegerColumn(_memoryAllocator));
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
                    case ArrowTypeId.Timestamp:
                        valueColumns.Add(new TimestampTzColumn(_memoryAllocator));
                        break;
                    default:
                        throw new NotImplementedException();
                }
                return newIndex;
            }
            return existingIndex;
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _typeList.InsertAt(index, (sbyte)ArrowTypeId.Null);
                _valueColumns[0].InsertAt(index, value);
                _offsets.InsertAt(index, 0);
                return;
            }

            var typeByte = (byte)value.Type;
            var arrayIndex = CheckArrayExist(in value, _typeIds, _valueColumns);
            
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
            var newValueArrayIndex = CheckArrayExist(in value, _typeIds, _valueColumns);

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
                    var customMetadata = EventArrowSerializer.GetCustomMetadata(arrowType);
                    typeIds.Add((int)arrowType.TypeId);
                    fields.Add(new Field("", arrowType, true, metadata: customMetadata));
                    childArrays.Add(arrowArray);
                }
            }
            var unionType = new UnionType(fields, typeIds, UnionMode.Dense);
            var typeIdsBuffer = new ArrowBuffer(_typeList.SlicedMemory);
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
            var arrayIndex = CheckArrayTypeExist(ArrowTypeId.List, _typeIds, _valueColumns);
            _valueColumns[arrayIndex].AddToNewList(in value);
        }

        public int EndNewList()
        {
            var arrayIndex = CheckArrayTypeExist(ArrowTypeId.List, _typeIds, _valueColumns);
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

            int nullCount = 0;

            for (int i = start; i < end; i++)
            {
                var type = _typeList.Get(i);
                var offset = _offsets.Get(i);

                if (type == 0)
                {
                    nullCount++;
                }
                if (startOffets[type] == 0)
                {
                    startOffets[type] = offset;
                }
                endOffsets[type] = offset;
            }

            if (nullCount > 0)
            {
                _valueColumns[0].RemoveRange(start, nullCount);
            }

            bool anyColumnHaveDataRemoved = false;
            for (int i = 1; i < _valueColumns.Count; i++)
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

        private sbyte CheckOtherDataColumnTypeExists(IDataColumn other, sbyte[] typeIds, List<IDataColumn> valueColumns)
        {
            if (other.Type == ArrowTypeId.Struct)
            {
                CheckStructLookup();
                if (!_structLookup.TryGetValue(other.StructHeader, out var existingIndex))
                {
                    var newIndex = (sbyte)valueColumns.Count;
                    if (valueColumns.Count >= 127)
                    {
                        throw new InvalidOperationException("Cannot add more than 127 types to a union column.");
                    }
                    _structLookup.Add(other.StructHeader, newIndex);
                    valueColumns.Add(new StructColumn(other.StructHeader, _memoryAllocator));
                    return newIndex;
                }
                return existingIndex;
            }
            return CheckArrayTypeExist(other.Type, typeIds, valueColumns);
        }

        private void InsertRangeFromBasicColumn(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (validityList == null)
            {
                var valueColumnIndex = CheckOtherDataColumnTypeExists(other, _typeIds, _valueColumns);
                var valueColumn = _valueColumns[valueColumnIndex];

                var nextOccurence = AvxUtils.FindFirstOccurence(_typeList.Span, index, valueColumnIndex);
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
                valueColumn.InsertRangeFrom(nextOccurenceOffset, other, start, count, default);
                // Insert range to offsets, requires special method to only increase offset of the same type.
                _offsets.InsertIncrementalRangeConditionalAdditionOnExisting(index, nextOccurenceOffset, count, _typeList.Span, valueColumnIndex, count);
                // Need to insert the type in type list as well.
                _typeList.InsertStaticRange(index, valueColumnIndex, count);
            }
            else
            {
                // Find ranges where elements are not null and also ranges where all elements are null.
                // A range that is not null will be inserted into the value column
                // The range of null will be added to the null column
                var currentStart = start;
                var end = start + count;
                var currentIndex = index;

                IDataColumn? valueColumn = default;
                sbyte valueColumnIndex = 0;
                int nextOccurenceOffset = 0;

                while (currentStart < end)
                {
                    var nextNullLocation = validityList.FindNextFalseIndex(currentStart);

                    if (nextNullLocation < 0)
                    {
                        nextNullLocation = end;
                    }

                    if (currentStart < nextNullLocation)
                    {
                        // We only create value column if there is any non null values, since the range could be for only null values
                        // A new value column should not then be created.
                        if (valueColumn == null)
                        {
                            // Create the value column of it does not exist
                            valueColumnIndex = CheckOtherDataColumnTypeExists(other, _typeIds, _valueColumns);
                            valueColumn = _valueColumns[valueColumnIndex];

                            var nextOccurence = AvxUtils.FindFirstOccurence(_typeList.Span, index, valueColumnIndex);
                            if (nextOccurence < 0)
                            {
                                nextOccurenceOffset = valueColumn.Count;
                            }
                            else
                            {
                                // Get the offset of the next occurence so this can be directly infront of it.
                                nextOccurenceOffset = _offsets.Get(nextOccurence);
                            }
                        }
                        var toCopy = nextNullLocation - currentStart;
                        if (toCopy > count)
                        {
                            toCopy = count;
                        }
                        valueColumn.InsertRangeFrom(nextOccurenceOffset, other, currentStart, toCopy, default);
                        _offsets.InsertIncrementalRangeConditionalAdditionOnExisting(currentIndex, nextOccurenceOffset, toCopy, _typeList.Span, valueColumnIndex, toCopy);
                        nextOccurenceOffset += toCopy;
                        // Type list must be added after so the offset is not incremented
                        _typeList.InsertStaticRange(currentIndex, valueColumnIndex, toCopy);
                        currentIndex += toCopy;
                    }

                    var nextNotNullLocation = validityList.FindNextTrueIndex(nextNullLocation);
                    if (nextNotNullLocation < 0)
                    {
                        nextNotNullLocation = end;
                    }
                    if (nextNullLocation < nextNotNullLocation)
                    {
                        // Add the null range to the null column
                        _valueColumns[0].InsertRangeFrom(currentStart, other, nextNullLocation, nextNotNullLocation - nextNullLocation, default);
                        _offsets.InsertIncrementalRangeConditionalAdditionOnExisting(currentIndex, nextNullLocation, nextNotNullLocation - nextNullLocation, _typeList.Span, 0, nextNotNullLocation - nextNullLocation);
                        _typeList.InsertStaticRange(currentIndex, 0, nextNotNullLocation - nextNullLocation);
                        currentIndex += nextNotNullLocation - nextNullLocation;
                    }
                    currentStart = nextNotNullLocation;
                }
            }
        }

        [SkipLocalsInit]
        private unsafe void InsertRangeFromUnionColumn(int index, UnionColumn other, int start, int count, BitmapList? validityList)
        {
            // Must find which types are missing, and also what index they exist on in other and also in this column.
            // After that they should be able to be copied over to this column by a value column basis.

            // Used types contains true or false on value column indices if the column is used in the range
            var usedTypes = stackalloc bool[127];

            // Start and end offsets contains the offset for each type in the range, this is used when copying data over.
            var startOffsets = stackalloc int[127];
            var endOffsets = stackalloc int[127];
            var otherDifference = stackalloc int[127];
            var thisDifference = stackalloc int[127];

            // Mapping table contains the index of the value column in this column for the value column in other column
            var mappingTable = stackalloc sbyte[127];

            // Reset all required columns
            for (int i = 0; i < other._valueColumns.Count; i++)
            {
                usedTypes[i] = false;
                startOffsets[i] = -1;
                endOffsets[i] = -1;
                otherDifference[i] = 0;
                mappingTable[i] = 0;
            }
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                thisDifference[i] = 0;
            }

            int nullCount = 0;

            // Find all types that are used in the range
            // and also find the offsets that are in use.
            var end = start + count;
            for (int i = start; i < end; i++)
            {
                var type = other._typeList[i];
                usedTypes[type] = true;
                var offset = other._offsets.Get(i);

                if (type == 0)
                {
                    // Special case to handle nulls, since they do not have incrementing offsets.
                    nullCount++;
                }

                if (startOffsets[type] < 0)
                {
                    startOffsets[type] = offset;
                }
                endOffsets[type] = offset;
            }

            if (usedTypes[0])
            {
                _valueColumns[0].InsertNullRange(0, nullCount);
            }

            for (int i = 1; i < other._valueColumns.Count; i++)
            {
                if (usedTypes[i])
                {
                    sbyte destinationValueIndex = CheckOtherDataColumnTypeExists(other._valueColumns[i], _typeIds, _valueColumns);
                    mappingTable[i] = destinationValueIndex;

                    // Must find next occurence first
                    var nextOccurence = AvxUtils.FindFirstOccurence(_typeList.Span, index, destinationValueIndex);
                    var nextOccurenceOffset = 0;
                    if (nextOccurence < 0)
                    {
                        nextOccurenceOffset = _valueColumns[destinationValueIndex].Count;
                    }
                    else
                    {
                        // Get the offset of the next occurence so this can be directly infront of it.
                        nextOccurenceOffset = _offsets.Get(nextOccurence);
                    }

                    var countToMove = endOffsets[i] - startOffsets[i] + 1;

                    otherDifference[i] = nextOccurenceOffset - startOffsets[i];
                    thisDifference[destinationValueIndex] = countToMove;

                    _valueColumns[destinationValueIndex].InsertRangeFrom(nextOccurenceOffset, other._valueColumns[i], startOffsets[i], countToMove, default);
                }
            }
            // Add offsets and types, these can have changed values
            // Require offset insert type based addition
            _offsets.InsertRangeFromTypeBasedAddition(index, other._offsets, start, count, _typeList.Span, new Span<int>(thisDifference, 127), other._typeList.Span, new Span<int>(otherDifference, 127), _valueColumns.Count);
            _typeList.InsertRangeFrom(index, other._typeList, start, count, new Span<sbyte>(mappingTable, 127), _valueColumns.Count);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            // Check all column types since union column must be able to support all of them.
            if (other is UnionColumn unionColumn)
            {
                InsertRangeFromUnionColumn(index, unionColumn, start, count, validityList);
            }
            else
            {
                InsertRangeFromBasicColumn(index, other, start, count, validityList);
            }
        }

        private IEnumerable<IDataValue> Enumerate()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return GetValueAt(i, default);
            }
        }

        public IEnumerator<IDataValue> GetEnumerator()
        {
            return Enumerate().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return Enumerate().GetEnumerator();
        }

        public void InsertNullRange(int index, int count)
        {
            // Insert null type
            _typeList.InsertStaticRange(index, 0, count);
            // Insert offsets
            _offsets.InsertRangeStaticValue(index, count, 0);
            // Add to null column to increase its counter
            _valueColumns[0].InsertNullRange(index, count);
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            var valueColumnIndex = _typeList[index];
            if (valueColumnIndex == 0)
            {
                writer.WriteNullValue();
                return;
            }
            var valueColumn = _valueColumns[valueColumnIndex];
            valueColumn.WriteToJson(in writer, _offsets.Get(index));
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            List<IDataColumn> columns = new List<IDataColumn>(_valueColumns.Count);

            for (int i = 0; i < _valueColumns.Count; i++)
            {
                columns.Add(_valueColumns[i].Copy(memoryAllocator));
            }

            return new UnionColumn(columns, _typeList.Copy(memoryAllocator), _offsets.Copy(memoryAllocator), _typeIds.ToArray(), memoryAllocator);
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            var valueColumnIndex = _typeList[index];
            if (valueColumnIndex == 0)
            {
                hashAlgorithm.Append(ByteArrayUtils.nullBytes);
                return;
            }
            var valueColumn = _valueColumns[valueColumnIndex];
            valueColumn.AddToHash(_offsets.Get(index), child, hashAlgorithm);
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            int count = 0;
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                if (_valueColumns[i] != null)
                {
                    pointerStack[count] = (int)_valueColumns[i].Type;
                    count++;
                }
            }
            var typeIdsPointer = arrowSerializer.UnionCreateTypeIdsVector(pointerStack.Slice(0, count));
            var typePointer = arrowSerializer.AddUnionType(typeIdsPointer, ArrowUnionMode.Dense);

            var childrenStack = pointerStack.Slice(_valueColumns.Count);
            count = 0;
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                if (_valueColumns[i] != null)
                {
                    pointerStack[count] = _valueColumns[i].CreateSchemaField(ref arrowSerializer, emptyStringPointer, childrenStack);
                    count++;
                }
            }
            var childrenPointer = arrowSerializer.CreateChildrenVector(pointerStack.Slice(0, count));

            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Union, typePointer, childrenOffset: childrenPointer);
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            // Start with the size of the type list and the offsets type list is 1 byte per element and offsets is 4 bytes per element
            int bodyLength = Count * 5;
            int fieldCount = 1;
            int bufferCount = 2;
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                var estimate = _valueColumns[i].GetSerializationEstimate();
                bodyLength += estimate.bodyLength;
                fieldCount += estimate.fieldNodeCount;
                bufferCount += estimate.bufferCount;
            }
            bufferCount += _valueColumns.Count - 1; // Add validity buffers, except for the null array in the start
            return new SerializationEstimation(fieldCount, bufferCount, bodyLength);
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            for (int i = _valueColumns.Count - 1; i >= 0; i--)
            {
                if (_valueColumns[i] != null)
                {
                    _valueColumns[i].AddFieldNodes(ref arrowSerializer, 0);
                }
            }
            arrowSerializer.CreateFieldNode(Count, nullCount);
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            arrowSerializer.AddBufferForward(_typeList.SlicedMemory.Length);
            arrowSerializer.AddBufferForward(_offsets.Memory.Length);
            for (int i = 1; i < _valueColumns.Count; i++) // We start at 1 since the first array is a null array which does not have any buffers
            {
                if (_valueColumns[i] != null)
                {
                    arrowSerializer.AddBufferForward(0); // Empty validity buffer
                    _valueColumns[i].AddBuffers(ref arrowSerializer);
                }
            }
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            dataWriter.WriteArrowBuffer(_typeList.SlicedMemory.Span);
            dataWriter.WriteArrowBuffer(_offsets.Memory.Span);

            for (int i = 1; i < _valueColumns.Count; i++) // We start at 1 since the first array is a null array which does not have any buffers
            {
                if (_valueColumns[i] != null)
                {
                    dataWriter.WriteArrowBuffer(Span<byte>.Empty); // Empty validity buffer
                    _valueColumns[i].WriteDataToBuffer(ref dataWriter);
                }
            }
        }
    }
}

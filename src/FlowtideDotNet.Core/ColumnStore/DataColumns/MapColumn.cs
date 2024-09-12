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

using Apache.Arrow.Types;
using Apache.Arrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using System.Buffers;
using FlowtideDotNet.Core.ColumnStore.Serialization;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class MapColumn : IDataColumn
    {
        /// <summary>
        /// Contains all the property keys, must always be strings
        /// </summary>
        private Column _keyColumn;

        /// <summary>
        /// Contains the values, can be any type
        /// </summary>
        private Column _valueColumn;

        private IntList _offsets;
        private bool disposedValue;

        public int Count => _offsets.Count - 1;

        public ArrowTypeId Type => ArrowTypeId.Map;

        public MapColumn(IMemoryAllocator memoryAllocator)
        {
            _keyColumn = Column.Create(memoryAllocator);
            _valueColumn = Column.Create(memoryAllocator);
            _offsets = new IntList(memoryAllocator);
            _offsets.Add(0);
        }

        internal MapColumn(Column keyColumn, Column valueColumn, IMemoryOwner<byte> offsetMemory, int offsetLength, IMemoryAllocator memoryAllocator)
        {
            _keyColumn = keyColumn;
            _valueColumn = valueColumn;
            _offsets = new IntList(offsetMemory, offsetLength, memoryAllocator);
        }

        private (int, int) GetOffsets(in int index)
        {
            var startOffset = _offsets.Get(index);
            return (startOffset, _offsets.Get(index + 1));
        }

        public IEnumerable<KeyValuePair<IDataValue, IDataValue>> GetKeyValuePairs(int index)
        {
            var (startOffset, endOffset) = GetOffsets(in index);

            for (int i = startOffset; i < endOffset; i++)
            {
                var key = _keyColumn.GetValueAt(i, default);
                var value = _valueColumn.GetValueAt(i, default);
                yield return new KeyValuePair<IDataValue, IDataValue>(key, value);
            }
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            throw new NotImplementedException();
        }

        public int GetElementLength(in int index)
        {
            var (startOffset, endOffset) = GetOffsets(in index);
            return endOffset - startOffset;
        }

        public void GetKeyAt(in int rowIndex, in int keyIndex, in DataValueContainer dataValueContainer)
        {
            var (startOffset, endOffset) = GetOffsets(in rowIndex);
            var actualIndex = startOffset + keyIndex;
            if (actualIndex >= endOffset)
            {
                dataValueContainer._type = ArrowTypeId.Null;
                return;
            }

            _keyColumn.GetValueAt(actualIndex, dataValueContainer, default);
        }

        public void GetMapValueAt(in int rowIndex, in int keyIndex, in DataValueContainer dataValueContainer)
        {
            var (startOffset, endOffset) = GetOffsets(in rowIndex);
            var actualIndex = startOffset + keyIndex;
            if (actualIndex >= endOffset)
            {
                dataValueContainer._type = ArrowTypeId.Null;
                return;
            }

            _valueColumn.GetValueAt(actualIndex, dataValueContainer, default);
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            if (child != null)
            {
                if (child is MapKeyReferenceSegment mapKeyReferenceSegment)
                {
                    var (startOffset, endOffset) = GetOffsets(in index);
                    var (keyLocationStart, _) = _keyColumn.SearchBoundries(new StringValue(mapKeyReferenceSegment.Key), startOffset, endOffset - 1, default);
                    if (keyLocationStart < 0)
                    {
                        return NullValue.Instance;
                    }
                    return _valueColumn.GetValueAt(keyLocationStart, child.Child);
                }
            }
            return new ReferenceMapValue(this, index);
        }

        public int CompareTo<T>(in int index, in T value, in ReferenceSegment? child, in BitmapList? validityList) where T : IDataValue
        {
            if (validityList != null &&
                !validityList.Get(index))
            {
                if (value.Type == ArrowTypeId.Null)
                {
                    return 0;
                }
                return -1;
            }
            else if (value.Type == ArrowTypeId.Null)
            {
                return 1;
            }
            if (child != null)
            {
                if (child is MapKeyReferenceSegment mapKeyReferenceSegment)
                {
                    // Compare on property level

                    // Get the offsets that this index exist in.
                    var (startOffset, endOffset) = GetOffsets(in index);
                    // Search the keys for the key
                    var (keyLocationStart, _) = _keyColumn.SearchBoundries(new StringValue(mapKeyReferenceSegment.Key), startOffset, endOffset - 1, default);
                    // If the property does not exist, treat it as null
                    if (keyLocationStart < 0)
                    {
                        return ArrowTypeId.Null - value.Type;
                    }
                    // Compare with the inner column
                    return _valueColumn.CompareTo(keyLocationStart, value, mapKeyReferenceSegment.Child);
                }
                throw new NotImplementedException();
            }
            else
            {
                // Compare on the map level
                var map = value.AsMap;
                var (startOffset, endOffset) = GetOffsets(in index);
                if (map is ReferenceMapValue refmap)
                {
                    var (otherStart, otherEnd) = refmap.mapColumn.GetOffsets(refmap.index);

                    // Compare lengths
                    var length = endOffset - startOffset;
                    if (length != otherEnd - otherStart)
                    {
                        return length - (otherEnd - otherStart);
                    }
                    
                    for (int i = 0; i < length; i++)
                    {
                        var otherKeyVal = refmap.mapColumn._keyColumn.GetValueAt(otherStart + i, default);
                        var keyCompareVal = _keyColumn.CompareTo(startOffset + i, otherKeyVal, default);
                        if (keyCompareVal != 0)
                        {
                            return keyCompareVal;
                        }
                        var otherValueVal = refmap.mapColumn._valueColumn.GetValueAt(otherStart + i, default);
                        var valueCompareVal = _valueColumn.CompareTo(startOffset + i, otherValueVal, default);
                        if (valueCompareVal != 0)
                        {
                            return valueCompareVal;
                        }
                    }
                    return 0;
                }
                else if (map is MapValue mapValue)
                {
                    var length = endOffset - startOffset;
                    var otherLength = mapValue.GetLength();

                    if (length != otherLength)
                    {
                        return length - otherLength;
                    }

                    var dataValueContainer = new DataValueContainer();
                    for (int i = 0; i < length; i++)
                    {
                        mapValue.GetKeyAt(i, dataValueContainer);   
                        var keyCompareVal = _keyColumn.CompareTo(startOffset + i, dataValueContainer, default);
                        if (keyCompareVal != 0)
                        {
                            return keyCompareVal;
                        }
                        //var valueVal = _valueColumn.GetValueAt(startOffset + i, default);
                        mapValue.GetValueAt(i, dataValueContainer);
                        var valueCompareVal = _valueColumn.CompareTo(startOffset + i, dataValueContainer, default);
                        if (valueCompareVal != 0)
                        {
                            return valueCompareVal;
                        }
                    }
                    return 0;
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            var startOffset = _offsets.Count - 1;
            if (value.Type == ArrowTypeId.Null)
            {
                _offsets.Add(_valueColumn.Count);
                return startOffset;
            }
            var map = value.AsMap;
            // Sort keys so its possible to binary search after a key.
            // In future, can check if it is a reference map value or not to skip sorting
            var ordered = map.OrderBy(x => x.Key, new DataValueComparer()).ToList();
            
            foreach (var pair in ordered)
            {
                _keyColumn.Add(pair.Key);
                _valueColumn.Add(pair.Value);
            }
            _offsets.Add(_valueColumn.Count);

            return startOffset;
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            if (child != null)
            {
                if (child is MapKeyReferenceSegment mapKeyReferenceSegment)
                {
                    var (startOffset, endOffset) = GetOffsets(in index);
                    var (keyLocationStart, _) = _keyColumn.SearchBoundries(new StringValue(mapKeyReferenceSegment.Key), startOffset, endOffset - 1, default);
                    if (keyLocationStart < 0)
                    {
                        dataValueContainer._type = ArrowTypeId.Null;
                        return;
                    }
                    _valueColumn.GetValueAt(keyLocationStart, dataValueContainer, child.Child);
                    return;
                }
                throw new NotImplementedException();
            }
            dataValueContainer._type = ArrowTypeId.Map;
            dataValueContainer._mapValue = new ReferenceMapValue(this, index);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            var (startOffset, endOffset) = GetOffsets(in index);

            // Check if the value is null, if so, remove the whole map
            // Null will be added as an empty map
            if (value.Type == ArrowTypeId.Null)
            {
                for (int i = endOffset - 1; i >= startOffset; i--)
                {
                    _keyColumn.RemoveAt(i);
                    _valueColumn.RemoveAt(i);
                }
                _offsets.Update(index + 1, startOffset, startOffset - endOffset);
                return index;
            }

            var map = value.AsMap;
            var ordered = map.OrderBy(x => x.Key, new DataValueComparer()).ToList();

            // Remove the old values
            for (int i = endOffset - 1; i >= startOffset; i--)
            {
                _keyColumn.RemoveAt(i);
                _valueColumn.RemoveAt(i);
            }

            // Insert the new values
            for (int i = 0; i < ordered.Count; i++)
            {
                _keyColumn.InsertAt(startOffset + i, ordered[i].Key);
                _valueColumn.InsertAt(startOffset + i, ordered[i].Value);
            }

            // Update the offsets
            _offsets.Update(index + 1, startOffset + ordered.Count, ordered.Count - (endOffset - startOffset));

            return index;
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) 
            where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundriesForDataColumnDesc(this, dataValue, start, end, child, default);
            }
            return BoundarySearch.SearchBoundriesForDataColumn(this, dataValue, start, end, child, default);
        }

        public void RemoveAt(in int index)
        {
            var (startOffset, endOffset) = GetOffsets(index);

            for (int i = endOffset - 1; i >= startOffset; i--)
            {
                _keyColumn.RemoveAt(i);
                _valueColumn.RemoveAt(i);
            }
            // Remove the offset and shift all the offsets after it
            _offsets.RemoveAt(index, startOffset - endOffset);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _offsets.InsertAt(index, _valueColumn.Count);
                return;
            }
            var map = value.AsMap;
            // Sort keys so its possible to binary search after a key.
            // In future, can check if it is a reference map value or not to skip sorting
            var ordered = map.OrderBy(x => x.Key, new DataValueComparer()).ToList();

            var currentOffset = _offsets.Get(index);
            for (int i = 0; i < ordered.Count; i++)
            {
                _keyColumn.InsertAt(currentOffset + i, ordered[i].Key);
                _valueColumn.InsertAt(currentOffset + i, ordered[i].Value);
            }
            _offsets.InsertAt(index, currentOffset, ordered.Count);
        }

        public (IArrowArray, IArrowType) ToArrowArray(Apache.Arrow.ArrowBuffer nullBuffer, int nullCount)
        {
            var keyData = _keyColumn.ToArrowArray();
            var valueData = _valueColumn.ToArrowArray();
            //var keyData = _keyColumn.ToArrowArray(new ArrowBuffer(), 0);

            var customKeyMetadata = EventArrowSerializer.GetCustomMetadata(keyData.Item2);
            var customValueMetadata = EventArrowSerializer.GetCustomMetadata(valueData.Item2);
            var keyField = new Field("key", keyData.Item2, true, customKeyMetadata);
            var valueField = new Field("value", valueData.Item2, true, customValueMetadata);
            var mapType = new MapType(keyField, valueField, true);

            var structType = new StructType(new List<Field>()
            {
                new Field("keys", keyData.Item2, true),
                new Field("values", valueData.Item2, true)
            });
            var structArr = new StructArray(structType, _keyColumn.Count, new List<IArrowArray>() { keyData.Item1, valueData.Item1 }, nullBuffer, nullCount);
            var valueOffsetsBuffer = new ArrowBuffer(_offsets.Memory);
            var mapArr = new MapArray(mapType, Count, valueOffsetsBuffer, structArr, nullBuffer, nullCount);
            return (mapArr, mapType);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _keyColumn.Dispose();
                    _valueColumn.Dispose();
                    _offsets.Dispose();
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
            return ArrowTypeId.Map;
        }

        public void Clear()
        {
            _offsets.Clear();
            _offsets.Add(0);
            _keyColumn.Clear();
            _valueColumn.Clear();
        }

        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int EndNewList()
        {
            throw new NotImplementedException();
        }
    }
}

﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Buffers;
using System.Collections;
using System.Text.Json;
using System.IO.Hashing;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class MapColumn : IDataColumn, IEnumerable<List<KeyValuePair<IDataValue, IDataValue>>>
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

        public StructHeader StructHeader => throw new NotImplementedException();

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

        internal MapColumn(Column keyColumn, Column valueColumn, IntList offset)
        {
            _keyColumn = keyColumn;
            _valueColumn = valueColumn;
            _offsets = offset;
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
            if (otherColumn is MapColumn mapColumn)
            {
                var (startOffset, endOffset) = GetOffsets(thisIndex);
                var (otherStart, otherEnd) = mapColumn.GetOffsets(otherIndex);
                var length = endOffset - startOffset;
                if (length != otherEnd - otherStart)
                {
                    return length - (otherEnd - otherStart);
                }

                for (int i = 0; i < length; i++)
                {
                    var otherKeyVal = mapColumn._keyColumn.GetValueAt(otherStart + i, default);
                    var keyCompareVal = _keyColumn.CompareTo(startOffset + i, otherKeyVal, default);
                    if (keyCompareVal != 0)
                    {
                        return keyCompareVal;
                    }
                    var otherValueVal = mapColumn._valueColumn.GetValueAt(otherStart + i, default);
                    var valueCompareVal = _valueColumn.CompareTo(startOffset + i, otherValueVal, default);
                    if (valueCompareVal != 0)
                    {
                        return valueCompareVal;
                    }
                }
                return 0;
            }
            throw new NotImplementedException();
        }

        public int GetElementLength(in int index)
        {
            var (startOffset, endOffset) = GetOffsets(in index);
            return endOffset - startOffset;
        }

        public IDataValue GetKeyAt(in int rowIndex, in int keyIndex)
        {
            var (startOffset, endOffset) = GetOffsets(in rowIndex);
            var actualIndex = startOffset + keyIndex;
            if (actualIndex >= endOffset)
            {
                return NullValue.Instance;
            }

            return _keyColumn.GetValueAt(actualIndex, default);
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

        public IDataValue GetMapValueAt(in int rowIndex, in int keyIndex)
        {
            var (startOffset, endOffset) = GetOffsets(in rowIndex);
            var actualIndex = startOffset + keyIndex;
            if (actualIndex >= endOffset)
            {
                return NullValue.Instance;
            }

            return _valueColumn.GetValueAt(actualIndex, default);
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

            var mapLength = map.GetLength();

            DataValueContainer dataValueContainer = new DataValueContainer();
            for (int i = 0; i < mapLength; i++)
            {
                map.GetKeyAt(i, dataValueContainer);
                _keyColumn.Add(dataValueContainer);
                map.GetValueAt(i, dataValueContainer);
                _valueColumn.Add(dataValueContainer);
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
                _keyColumn.RemoveRange(startOffset, endOffset - startOffset);
                _valueColumn.RemoveRange(startOffset, endOffset - startOffset);
                _offsets.Update(index + 1, startOffset, startOffset - endOffset);
                return index;
            }

            var map = value.AsMap;
            var ordered = map.OrderBy(x => x.Key, new DataValueComparer()).ToList();

            // Remove the old values
            _keyColumn.RemoveRange(startOffset, endOffset - startOffset);
            _valueColumn.RemoveRange(startOffset, endOffset - startOffset);


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

            _keyColumn.RemoveRange(startOffset, endOffset - startOffset);
            _valueColumn.RemoveRange(startOffset, endOffset - startOffset);
            // Remove the offset and shift all the offsets after it
            _offsets.RemoveAt(index, startOffset - endOffset);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            var currentOffset = _offsets.Get(index);
            if (value.Type == ArrowTypeId.Null)
            {
                _offsets.InsertAt(index, currentOffset);
                return;
            }
            var map = value.AsMap;
            var mapLength = map.GetLength();

            if (map is ReferenceMapValue referenceMapValue)
            {
                var (copyStart, _) = referenceMapValue.mapColumn.GetOffsets(referenceMapValue.index);
                _keyColumn.InsertRangeFrom(currentOffset, referenceMapValue.mapColumn._keyColumn, copyStart, mapLength);
                _valueColumn.InsertRangeFrom(currentOffset, referenceMapValue.mapColumn._valueColumn, copyStart, mapLength);
            }
            else
            {
                var dataValueContainer = new DataValueContainer();

                for (int i = 0; i < mapLength; i++)
                {
                    map.GetKeyAt(i, dataValueContainer);
                    _keyColumn.InsertAt(currentOffset + i, dataValueContainer);
                    map.GetValueAt(i, dataValueContainer);
                    _valueColumn.InsertAt(currentOffset + i, dataValueContainer);
                }
            }

            _offsets.InsertAt(index, currentOffset, mapLength);
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

        private IEnumerable<List<KeyValuePair<IDataValue, IDataValue>>> GetEnumerable()
        {
            for (int i = 0; i < Count; i++)
            {
                var (startOffset, endOffset) = GetOffsets(in i);

                List<KeyValuePair<IDataValue, IDataValue>> output = new List<KeyValuePair<IDataValue, IDataValue>>();
                for (int j = startOffset; j < endOffset; j++)
                {
                    var key = _keyColumn.GetValueAt(j, default);
                    var value = _valueColumn.GetValueAt(j, default);
                    output.Add(new KeyValuePair<IDataValue, IDataValue>(key, value));
                }
                yield return output;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        IEnumerator<List<KeyValuePair<IDataValue, IDataValue>>> IEnumerable<List<KeyValuePair<IDataValue, IDataValue>>>.GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        public void RemoveRange(int start, int count)
        {
            var startOffset = _offsets.Get(start);
            var endOffset = _offsets.Get(start + count);

            // Remove offsets
            _offsets.RemoveRange(start, count, startOffset - endOffset);

            if (endOffset > startOffset)
            {
                // Remove the keys and values
                _keyColumn.RemoveRange(startOffset, endOffset - startOffset);
                _valueColumn.RemoveRange(startOffset, endOffset - startOffset);
            }
        }

        public int GetByteSize(int start, int end)
        {
            var startOffset = _offsets.Get(start);
            var endOffset = _offsets.Get(end + 1);

            if (startOffset == endOffset)
            {
                return sizeof(int);
            }

            return _keyColumn.GetByteSize(startOffset, endOffset - 1) + _valueColumn.GetByteSize(startOffset, endOffset - 1) + ((end - start + 1) * sizeof(int));
        }

        public int GetByteSize()
        {
            return _keyColumn.GetByteSize() + _valueColumn.GetByteSize() + (_offsets.Count * sizeof(int));
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (other is MapColumn mapColumn)
            {
                var startOffset = _offsets.Get(index);

                var otherStartOffset = mapColumn._offsets.Get(start);
                var otherEndOffset = mapColumn._offsets.Get(start + count);

                if (otherStartOffset < otherEndOffset)
                {
                    // Insert the keys and values if there are any values
                    _keyColumn.InsertRangeFrom(startOffset, mapColumn._keyColumn, otherStartOffset, otherEndOffset - otherStartOffset);
                    _valueColumn.InsertRangeFrom(startOffset, mapColumn._valueColumn, otherStartOffset, otherEndOffset - otherStartOffset);
                }

                // Insert the offsets
                _offsets.InsertRangeFrom(index, mapColumn._offsets, start, count, otherEndOffset - otherStartOffset, startOffset - otherStartOffset);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void InsertNullRange(int index, int count)
        {
            var offset = _offsets.Get(index);
            _offsets.InsertRangeStaticValue(index, count, offset);
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteStartObject();

            var (startOffset, endOffset) = GetOffsets(in index);

            for (int i = startOffset; i < endOffset; i++)
            {
                var key = _keyColumn.GetValueAt(i, default);
                writer.WritePropertyName(key.ToString()!);
                _valueColumn.WriteToJson(in writer, i);
            }

            writer.WriteEndObject();
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new MapColumn(_keyColumn.Copy(memoryAllocator), _valueColumn.Copy(memoryAllocator), _offsets.Copy(memoryAllocator));
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            var (startOffset, endOffset) = GetOffsets(in index);
            if (child != null)
            {
                if (child is MapKeyReferenceSegment mapKeyReferenceSegment)
                {
                    
                    var (keyLocationStart, _) = _keyColumn.SearchBoundries(new StringValue(mapKeyReferenceSegment.Key), startOffset, endOffset - 1, default);
                    if (keyLocationStart < 0)
                    {
                        hashAlgorithm.Append(ByteArrayUtils.nullBytes);
                        return;
                    }
                    _valueColumn.AddToHash(keyLocationStart, child.Child, hashAlgorithm);
                    return;
                }
                throw new NotImplementedException();
            }

            for (int i = startOffset; i < endOffset; i++)
            {
                _keyColumn.AddToHash(i, default, hashAlgorithm);
                _valueColumn.AddToHash(i, default, hashAlgorithm);
            }
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var typePointer = arrowSerializer.AddMapType(true);
            var structPointer = arrowSerializer.AddStructType();
            var childStack = pointerStack.Slice(2);
            pointerStack[0] = _keyColumn.CreateSchemaField(ref arrowSerializer, emptyStringPointer, childStack);
            pointerStack[1] = _valueColumn.CreateSchemaField(ref arrowSerializer, emptyStringPointer, childStack);
            var structChildrenPointer = arrowSerializer.CreateChildrenVector(pointerStack.Slice(0, 2));
            pointerStack[0] = arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Struct_, structPointer, childrenOffset: structChildrenPointer);

            var childrenPointer = arrowSerializer.CreateChildrenVector(pointerStack.Slice(0, 1));
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Map, typePointer, childrenOffset: childrenPointer);
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            var keyEstimate = _keyColumn.GetSerializationEstimate();
            var valueEstimate = _valueColumn.GetSerializationEstimate();
            return new SerializationEstimation(
                2 + keyEstimate.fieldNodeCount + valueEstimate.fieldNodeCount,
                2 + keyEstimate.bufferCount + valueEstimate.bufferCount,
                keyEstimate.bodyLength + valueEstimate.bodyLength + (_offsets.Count * sizeof(int)));
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            _valueColumn.AddFieldNodes(ref arrowSerializer);
            _keyColumn.AddFieldNodes(ref arrowSerializer);
            arrowSerializer.CreateFieldNode(_keyColumn.Count, 0);
            arrowSerializer.CreateFieldNode(Count, nullCount);
        }

        /// <summary>
        /// Adds buffers in the same way Apache Arrow adds the buffers
        /// The map column is treated as a List<Struct<KeyType, ValueType>>
        /// First the offset is saved same as list, then the validity buffer for the struct, it is not used so it is set to 0.
        /// Finally the two key and value buffers are added
        /// </summary>
        /// <param name="arrowSerializer"></param>
        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            arrowSerializer.AddBufferForward(_offsets.Memory.Length);
            arrowSerializer.AddBufferForward(0); // Struct validity, it is not used so we set it to 0
            _keyColumn.AddBuffers(ref arrowSerializer);
            _valueColumn.AddBuffers(ref arrowSerializer);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            dataWriter.WriteArrowBuffer(_offsets.Memory.Span);
            dataWriter.WriteArrowBuffer(Span<byte>.Empty); // Empty validity buffer
            _keyColumn.WriteDataToBuffer(ref dataWriter);
            _valueColumn.WriteDataToBuffer(ref dataWriter);
        }
    }
}

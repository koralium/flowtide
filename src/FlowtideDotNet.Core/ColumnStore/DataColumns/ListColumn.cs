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
using System.IO.Hashing;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class ListColumn : IDataColumn, IEnumerable<IEnumerable<IDataValue>>
    {
        private readonly Column _internalColumn;
        private readonly IntList _offsets;
        private bool disposedValue;

        public int Count => _offsets.Count - 1;

        public ArrowTypeId Type => ArrowTypeId.List;

        public StructHeader StructHeader => throw new NotImplementedException();

        public ListColumn(IMemoryAllocator memoryAllocator)
        {
            _internalColumn = Column.Create(memoryAllocator);
            _offsets = new IntList(memoryAllocator);
            _offsets.Add(0);
        }

        public ListColumn(Column internalColumn, IMemoryOwner<byte> offsetMemory, int offsetCount, IMemoryAllocator memoryAllocator)
        {
            _offsets = new IntList(offsetMemory, offsetCount, memoryAllocator);
            _internalColumn = internalColumn;
        }

        internal ListColumn(Column internalColumn, IntList offsets)
        {
            _internalColumn = internalColumn;
            _offsets = offsets;
        }

        public int Add(in IDataValue value)
        {
            var list = value.AsList;

            var currentOffset = Count;
            var listLength = list.Count;
            for (int i = 0; i < listLength; i++)
            {
                _internalColumn.Add(list.GetAt(i));
            }
            _offsets.Add(_internalColumn.Count);

            return currentOffset;
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            var currentOffset = Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _offsets.Add(_internalColumn.Count);
                return currentOffset;
            }

            var list = value.AsList;
            var listLength = list.Count;

            if (list is ReferenceListValue referenceListVal)
            {
                var lastOffset = _offsets.Get(Count);
                _internalColumn.InsertRangeFrom(lastOffset, referenceListVal.column, referenceListVal.start, referenceListVal.Count);
            }
            else
            {
                for (int i = 0; i < listLength; i++)
                {
                    _internalColumn.Add(list.GetAt(i));
                }
            }
            _offsets.Add(_internalColumn.Count);

            return currentOffset;
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is ListColumn otherList)
            {
                var startOffset = _offsets.Get(thisIndex);
                var endOffset = _offsets.Get(thisIndex + 1);
                var otherStartOffset = otherList._offsets.Get(otherIndex);
                var otherEndOffset = otherList._offsets.Get(otherIndex + 1);
                var thisListCount = endOffset - startOffset;
                var otherListCount = otherEndOffset - otherStartOffset;
                if (thisListCount != otherListCount)
                {
                    return thisListCount.CompareTo(otherListCount);
                }
                for (int i = 0; i < thisListCount; i++)
                {
                    var compare = _internalColumn.CompareTo(otherList._internalColumn, startOffset + i, otherStartOffset + i);
                    if (compare != 0)
                    {
                        return compare;
                    }
                }
                return 0;
            }
            throw new NotImplementedException();
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
            var otherList = value.AsList;

            var startOffset = _offsets.Get(index);
            var endOffset = _offsets.Get(index + 1);
            var thisListCount = endOffset - startOffset;
            var lengthCompare = thisListCount.CompareTo(otherList.Count);
            if (lengthCompare != 0)
            {
                return lengthCompare;
            }
            for (int i = 0; i < thisListCount; i++)
            {
                var otherValue = otherList.GetAt(i);
                var compare = _internalColumn.CompareTo(startOffset + i, otherValue, null);
                if (compare != 0)
                {
                    return compare;
                }
            }

            return 0;
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new ReferenceListValue(_internalColumn, _offsets.Get(index), _offsets.Get(index + 1));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._listValue = new ReferenceListValue(_internalColumn, _offsets.Get(index), _offsets.Get(index + 1));
            dataValueContainer._type = ArrowTypeId.List;
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc)
            where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundriesForDataColumnDesc(this, in dataValue, start, end, child, default);
            }
            return BoundarySearch.SearchBoundriesForDataColumn(this, in dataValue, start, end, child, default);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            var currentStart = _offsets.Get(index);
            var currentEnd = _offsets.Get(index + 1);

            if (value.Type == ArrowTypeId.Null)
            {
                _internalColumn.RemoveRange(currentStart, currentEnd - currentStart);
                _offsets.Update(index + 1, currentStart, currentStart - currentEnd);
                return index;
            }

            var list = value.AsList;
            var listLength = list.Count;
            var currentLength = currentEnd - currentStart;
            if (listLength <= currentLength)
            {
                for (int i = 0; i < listLength; i++)
                {
                    _internalColumn.UpdateAt(currentStart + i, list.GetAt(i));
                }

                if (currentLength > listLength)
                {
                    for (int i = currentLength - 1; i >= listLength; i--)
                    {
                        _internalColumn.RemoveAt(currentStart + i);
                    }
                    _offsets.Update(index + 1, currentStart + listLength, listLength - currentLength);
                }

            }
            else
            {
                // New list is larger than the current list

                // Update existing elements
                for (int i = 0; i < currentLength; i++)
                {
                    _internalColumn.UpdateAt(currentStart + i, list.GetAt(i));
                }

                // Insert new elements
                for (int i = 0; i < (listLength - currentLength); i++)
                {
                    _internalColumn.InsertAt(currentStart + currentLength + i, list.GetAt(currentLength + i));
                }


                // Update offset
                _offsets.Update(index + 1, currentStart + listLength, listLength - currentLength);
            }
            return index;
        }

        public void RemoveAt(in int index)
        {
            var startOffset = _offsets.Get(index);
            var endOffset = _offsets.Get(index + 1);

            if (endOffset > startOffset)
            {
                _internalColumn.RemoveRange(startOffset, endOffset - startOffset);
            }

            _offsets.RemoveAt(index + 1, startOffset - endOffset);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                var endOffset = _offsets.Get(index);
                _offsets.InsertAt(index, endOffset);
                return;
            }
            var list = value.AsList;

            var startOffset = _offsets.Get(index);

            if (list is ReferenceListValue referenceListVal)
            {
                _internalColumn.InsertRangeFrom(startOffset, referenceListVal.column, referenceListVal.start, referenceListVal.Count);
                _offsets.InsertAt(index + 1, startOffset + referenceListVal.Count, referenceListVal.Count);
            }
            else
            {
                for (int i = 0; i < list.Count; i++)
                {
                    _internalColumn.InsertAt(startOffset + i, list.GetAt(i));
                }

                _offsets.InsertAt(index + 1, startOffset + list.Count, list.Count);
            }
        }

        public int GetListLength(in int index)
        {
            Debug.Assert(index < Count);
            return _offsets.Get(index + 1) - _offsets.Get(index);
        }

        public void AppendToList<T>(in int index, in T value) where T : IDataValue
        {
            Debug.Assert(index < Count);
            var endOffset = _offsets.Get(index + 1);

            _internalColumn.InsertAt(endOffset, value);
            _offsets.Update(index + 1, endOffset + 1, 1);
        }

        public void UpdateListElement(in int index, in int listIndex, in IDataValue value)
        {
            Debug.Assert(index < Count);
            var startOffset = _offsets.Get(index);
            Debug.Assert(listIndex < (_offsets.Get(index + 1) - startOffset));
            _internalColumn.UpdateAt(startOffset + listIndex, value);
        }

        public void RemoveListElement(in int index, in int listIndex)
        {
            Debug.Assert(index < Count);
            var startOffset = _offsets.Get(index);
            var endOffset = _offsets.Get(index + 1);
            Debug.Assert(listIndex < (_offsets.Get(index + 1) - startOffset));
            _internalColumn.RemoveAt(startOffset + listIndex);
            _offsets.Update(index + 1, endOffset - 1, -1);
        }

        public IDataValue GetListElementValue(in int index, in int listIndex)
        {
            Debug.Assert(index < Count);
            var startOffset = _offsets.Get(index);
            Debug.Assert(listIndex < (_offsets.Get(index + 1) - startOffset));
            return _internalColumn.GetValueAt(startOffset + listIndex, default);
        }

        public void GetListElementValue(in int index, in int listIndex, DataValueContainer dataValueContainer)
        {
            Debug.Assert(index < Count);
            var startOffset = _offsets.Get(index);
            Debug.Assert(listIndex < (_offsets.Get(index + 1) - startOffset));
            _internalColumn.GetValueAt(startOffset + listIndex, dataValueContainer, default);
        }

        public (IArrowArray, IArrowType) ToArrowArray(Apache.Arrow.ArrowBuffer nullBuffer, int nullCount)
        {
            var (arr, type) = _internalColumn.ToArrowArray();
            var customMetadata = EventArrowSerializer.GetCustomMetadata(type);
            var field = new Field("item", type, true, customMetadata);
            var listType = new ListType(field);
            var offsetBuffer = new ArrowBuffer(_offsets.Memory);
            return (new Apache.Arrow.ListArray(listType, Count, offsetBuffer, arr, nullBuffer, nullCount), listType);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _internalColumn.Dispose();
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
            return ArrowTypeId.List;
        }

        public void Clear()
        {
            _offsets.Clear();
            _offsets.Add(0);
            _internalColumn.Clear();
        }

        /// <summary>
        /// Allows adding values to a new list directly without creating a list value type.
        /// This allows for more efficient list creation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            _internalColumn.Add(value);
        }

        /// <summary>
        /// Signals an end to a list created with AddToNewList.
        /// </summary>
        /// <returns></returns>
        public int EndNewList()
        {
            var currentOffset = _offsets.Count - 1;
            _offsets.Add(_internalColumn.Count);
            return currentOffset;
        }

        private IEnumerable<IDataValue> GetListValues(int index)
        {
            var startOffset = _offsets.Get(index);
            var endOffset = _offsets.Get(index + 1);

            for (int i = startOffset; i < endOffset; i++)
            {
                yield return _internalColumn.GetValueAt(i, default);
            }
        }

        private IEnumerable<IEnumerable<IDataValue>> GetEnumerable()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return GetListValues(i);
            }
        }

        public IEnumerator<IEnumerable<IDataValue>> GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        public void RemoveRange(int start, int count)
        {
            var startOffset = _offsets.Get(start);
            var endOffset = _offsets.Get(start + count);

            // Remove the offsets
            _offsets.RemoveRange(start, count, startOffset - endOffset);

            if (endOffset > startOffset)
            {
                // Remove the values in the internal column
                _internalColumn.RemoveRange(startOffset, endOffset - startOffset);
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
            return _internalColumn.GetByteSize(startOffset, endOffset - 1) + ((end - start + 1) * sizeof(int));
        }

        public int GetByteSize()
        {
            return _internalColumn.GetByteSize() + (_offsets.Count * sizeof(int));
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (other is ListColumn listColumn)
            {
                var startOffset = _offsets.Get(index);

                var otherStartOffset = listColumn._offsets.Get(start);
                var otherEndOffset = listColumn._offsets.Get(start + count);

                if (otherEndOffset > otherStartOffset)
                {
                    // Insert the values
                    _internalColumn.InsertRangeFrom(startOffset, listColumn._internalColumn, otherStartOffset, otherEndOffset - otherStartOffset);
                }

                // Insert the offsets
                _offsets.InsertRangeFrom(index + 1, listColumn._offsets, start + 1, count, otherEndOffset - otherStartOffset, startOffset - otherStartOffset);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void InsertNullRange(int index, int count)
        {
            var startOffset = _offsets.Get(index);
            _offsets.InsertRangeStaticValue(index, count, startOffset);
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteStartArray();

            var startOffset = _offsets.Get(index);
            var endOffset = _offsets.Get(index + 1);

            for (int i = startOffset; i < endOffset; i++)
            {
                _internalColumn.WriteToJson(in writer, i);
            }

            writer.WriteEndArray();
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new ListColumn(_internalColumn.Copy(memoryAllocator), _offsets.Copy(memoryAllocator));
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            var listStart = _offsets.Get(index);
            var listEnd = _offsets.Get(index + 1);

            for (int i = listStart; i < listEnd; i++)
            {
                _internalColumn.AddToHash(i, default, hashAlgorithm);
            }
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var typePointer = arrowSerializer.AddListType();
            var childStack = pointerStack.Slice(1);
            pointerStack[0] = _internalColumn.CreateSchemaField(ref arrowSerializer, emptyStringPointer, childStack);
            var childVectorPointer = arrowSerializer.CreateChildrenVector(pointerStack.Slice(0, 1));
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.List, typePointer, childrenOffset: childVectorPointer);
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            var innerEstimate = _internalColumn.GetSerializationEstimate();
            return new SerializationEstimation(innerEstimate.fieldNodeCount + 1, innerEstimate.bufferCount + 1, innerEstimate.bodyLength + (_offsets.Count * sizeof(int)));
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            _internalColumn.AddFieldNodes(ref arrowSerializer);
            arrowSerializer.CreateFieldNode(Count, nullCount);
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            arrowSerializer.AddBufferForward(_offsets.Memory.Length);
            _internalColumn.AddBuffers(ref arrowSerializer);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            dataWriter.WriteArrowBuffer(_offsets.Memory.Span);

            _internalColumn.WriteDataToBuffer(ref dataWriter);
        }
    }
}

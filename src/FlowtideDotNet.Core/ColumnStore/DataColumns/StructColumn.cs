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
using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    internal class StructColumn : IDataColumn
    {
        internal readonly StructHeader _header;
        internal readonly Column[] _columns;
        private bool disposedValue;
        private int _count;

        public StructColumn(StructHeader structHeader, IMemoryAllocator memoryAllocator)
        {
            _header = structHeader;
            _columns = new Column[structHeader.Count];
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i] = Column.Create(memoryAllocator);
            }
            _count = 0;
        }

        internal StructColumn(StructHeader header, Column[] columns, int count)
        {
            _header = header;
            _columns = columns;
            _count = count;
        }

        public int Count => _count;

        public ArrowTypeId Type => ArrowTypeId.Struct;

        public StructHeader StructHeader => _header;

        public int Add<T>(in T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                _count++;
                for (int i = 0; i < _columns.Length; i++)
                {
                    _columns[i].Add(NullValue.Instance);
                }
            }
            else if (value.Type == ArrowTypeId.Struct)
            {
                _count++;
                var structVal = value.AsStruct;
                for (int i = 0; i < _columns.Length; i++)
                {
                    var dataValue = structVal.GetAt(i);
                    _columns[i].Add(dataValue);
                }
            }
            else
            {
                throw new InvalidOperationException($"Cannot add value of type {value.Type} to StructColumn.");
            }
            return Count - 1;
        }

        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            throw new NotSupportedException("Add to new list is not supported in struct column");
        }

        public void Clear()
        {
            _count = 0;
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].Clear();
            }
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
                    var columnIndex = _header.FindIndex(mapKeyReferenceSegment.Key);

                    if (columnIndex < 0)
                    {
                        return ArrowTypeId.Null - value.Type;
                    }
                    // Compare with the inner column
                    return _columns[columnIndex].CompareTo(index, value, mapKeyReferenceSegment.Child);
                }
                else if (child is StructReferenceSegment structReferenceSegment)
                {
                    if (structReferenceSegment.Field >= _header.Count)
                    {
                        return ArrowTypeId.Null - value.Type;
                    }
                    // Compare with the inner column
                    return _columns[structReferenceSegment.Field].CompareTo(index, value, structReferenceSegment.Child);
                }
                throw new NotImplementedException();
            }
            else
            {
                var structValue = value.AsStruct;
                var headerCompare = _header.CompareTo(structValue.Header);

                if (headerCompare != 0)
                {
                    // If the headers dont match, return directly
                    return headerCompare;
                }

                if (structValue is ReferenceStructValue refStructValue)
                {
                    for (int i = 0; i < _columns.Length; i++)
                    {
                        var compare = _columns[i].CompareTo(refStructValue.column._columns[i], index, refStructValue.index);
                        if (compare != 0)
                        {
                            return compare;
                        }
                    }
                    return 0;
                }
                else if (structValue is StructValue structVal)
                {
                    for (int i = 0; i < _columns.Length; i++)
                    {
                        var compare = _columns[i].CompareTo(index, structVal._columnValues[i], default);
                        if (compare != 0)
                        {
                            return compare;
                        }
                    }
                    return 0;
                }
            }
            throw new NotImplementedException();
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is StructColumn structColumn)
            {
                var headerCompare = _header.CompareTo(structColumn._header);

                if (headerCompare != 0)
                {
                    // If the headers dont match, return directly
                    return headerCompare;
                }

                for (int i = 0; i < _columns.Length; i++)
                {
                    var compare = _columns[i].CompareTo(structColumn._columns[i], thisIndex, otherIndex);
                    if (compare != 0)
                    {
                        return compare;
                    }
                }
                return 0;
            }
            throw new NotImplementedException();
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            Column[] copiedColumns = new Column[_columns.Length];
            for (int i = 0; i < _columns.Length; i++)
            {
                copiedColumns[i] = _columns[i].Copy(memoryAllocator);
            }
            return new StructColumn(_header, copiedColumns, _count);
        }

        public int EndNewList()
        {
            throw new NotSupportedException("End new list is not supported in struct column");
        }

        public int GetByteSize(int start, int end)
        {
            int size = 0;
            for (int i = 0; i < _columns.Length; i++)
            {
                size += _columns[i].GetByteSize(start, end);
            }
            return size;
        }

        public int GetByteSize()
        {
            int size = 0;
            for (int i = 0; i < _columns.Length; i++)
            {
                size += _columns[i].GetByteSize();
            }
            return size;
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            var estimate = new SerializationEstimation();
            for (int i = 0; i < _columns.Length; i++)
            {
                var columnEstimate = _columns[i].GetSerializationEstimate();
                estimate.bodyLength += columnEstimate.bodyLength;
                estimate.bufferCount += columnEstimate.bufferCount;
                estimate.fieldNodeCount += columnEstimate.fieldNodeCount;
            }
            return estimate;
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.Struct;
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            if (child != null)
            {
                if (child is MapKeyReferenceSegment mapKeyReferenceSegment)
                {
                    var columnIndex = _header.FindIndex(mapKeyReferenceSegment.Key);
                    if (columnIndex < 0)
                    {
                        return NullValue.Instance;
                    }
                    return _columns[columnIndex].GetValueAt(index, child.Child);
                }
                else if (child is StructReferenceSegment structReferenceSegment)
                {
                    if (structReferenceSegment.Field >= _columns.Length)
                    {
                        return NullValue.Instance;
                    }
                    return _columns[structReferenceSegment.Field].GetValueAt(index, child.Child);
                }
                throw new NotImplementedException($"{child.GetType()} is not yet implemented as a reference segment in struct.");
            }
            
            return new ReferenceStructValue(this, index);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            if (child != null)
            {
                if (child is MapKeyReferenceSegment mapKeyReferenceSegment)
                {
                    var columnIndex = _header.FindIndex(mapKeyReferenceSegment.Key);
                    if (columnIndex < 0)
                    {
                        dataValueContainer._type = ArrowTypeId.Null;
                        return;
                    }
                    _columns[columnIndex].GetValueAt(in index, in dataValueContainer, child.Child);
                    return;
                }
                else if (child is StructReferenceSegment structReferenceSegment)
                {
                    if (structReferenceSegment.Field >= _columns.Length)
                    {
                        dataValueContainer._type = ArrowTypeId.Null;
                        return;
                    }
                    _columns[structReferenceSegment.Field].GetValueAt(in index, in dataValueContainer, child.Child);
                    return;
                }
                throw new NotImplementedException($"{child.GetType()} is not yet implemented as a reference segment in struct.");
            }
                
            dataValueContainer._structValue = new ReferenceStructValue(this, index);
            dataValueContainer._type = ArrowTypeId.Struct;
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                _count++;
                for (int i = 0; i < _columns.Length; i++)
                {
                    _columns[i].InsertAt(index, NullValue.Instance);
                }
            }
            else if (value.Type == ArrowTypeId.Struct)
            {
                _count++;
                var structVal = value.AsStruct;
                for (int i = 0; i < _columns.Length; i++)
                {
                    var dataValue = structVal.GetAt(i);
                    _columns[i].InsertAt(index, dataValue);
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void InsertNullRange(int index, int count)
        {
            _count += count;
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].InsertNullRange(index, count);
            }
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (other is StructColumn otherStructColumn)
            {
                _count += count;
                for (int i = 0; i < _columns.Length; i++)
                {
                    var otherColumn = otherStructColumn._columns[i];
                    _columns[i].InsertRangeFrom(index, otherColumn, start, count);
                }
            }
            else
            {
                throw new InvalidOperationException($"Cannot insert range from {other.Type} to StructColumn.");
            }
        }

        public void RemoveAt(in int index)
        {
            _count--;
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].RemoveAt(index);
            }
        }

        public void RemoveRange(int start, int count)
        {
            _count -= count;
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].RemoveRange(start, count);
            }
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            if (child == null)
            {
                var structVal = dataValue.AsStruct;

                var headerCompare = _header.CompareTo(structVal.Header);

                if (headerCompare != 0)
                {
                    if (headerCompare < 0)
                    {
                        if (desc)
                        {
                            return (~start, ~start);
                        }
                        else
                        {
                            var index = ~(end + 1);
                            return (index, index);
                        }
                        
                    }
                    else
                    {
                        if (desc)
                        {
                            var index = ~(end + 1);
                            return (index, index);
                        }
                        else
                        {
                            return (~start, ~start);
                        }
                        
                    }
                }

                int istart = start;
                int iend = end;

                for (int i = 0; i < _columns.Length; i++)
                {
                    var (low, high) = _columns[i].SearchBoundries(structVal.GetAt(i), istart, iend, default, desc);
                    if (low < 0)
                    {
                        return (low, high);
                    }
                    else
                    {
                        istart = low;
                        iend = high;
                    }
                }
                return (istart, iend);
            }
            else if (child != null)
            {
                if (child is MapKeyReferenceSegment mapKeyReferenceSegment)
                {
                    // Compare on property level
                    var columnIndex = _header.FindIndex(mapKeyReferenceSegment.Key);

                    if (columnIndex >= 0)
                    {
                        // Compare with the inner column
                        return _columns[columnIndex].SearchBoundries(dataValue, start, end, mapKeyReferenceSegment.Child, desc);
                    }
                }
                else if (child is StructReferenceSegment structReferenceSegment)
                {
                    if (structReferenceSegment.Field < _columns.Length)
                    {
                        // Compare with the inner column
                        return _columns[structReferenceSegment.Field].SearchBoundries(dataValue, start, end, structReferenceSegment.Child, desc);
                    }
                }
            }

            // Fallback
            if (desc)
            {
                return BoundarySearch.SearchBoundriesForDataColumnDesc(this, dataValue, start, end, child, default);
            }
            
            return BoundarySearch.SearchBoundriesForDataColumn(this, dataValue, start, end, child, default);
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var fields = new List<Field>();
            var arrays = new List<IArrowArray>();
            for (int i = 0; i < _columns.Length; i++)
            {
                var data = _columns[i].ToArrowArray();
                var customMetadata = EventArrowSerializer.GetCustomMetadata(data.Item2);
                var field = new Field(_header.GetColumnName(i), data.Item2, true, customMetadata);
                fields.Add(field);
                arrays.Add(data.Item1);
            }

            var structType = new StructType(fields);
            var structArr = new StructArray(structType, Count, arrays, nullBuffer, nullCount);
            return (structArr, structType);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Struct)
            {
                var structVal = value.AsStruct;
                for (int i = 0; i < _columns.Length; i++)
                {
                    var dataValue = structVal.GetAt(i);
                    _columns[i].UpdateAt(index, dataValue);
                }
            }
            return index;
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteStartObject();
            for (int i = 0; i < _columns.Length; i++)
            {
                writer.WritePropertyName(_header.GetColumnNameUtf8(i));
                _columns[i].WriteToJson(in writer, in index);
            }
            writer.WriteEndObject();
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].AddBuffers(ref arrowSerializer);
            }
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            for (int i = _columns.Length - 1; i >= 0; i--)
            {
                _columns[i].AddFieldNodes(ref arrowSerializer);
            }
            arrowSerializer.CreateFieldNode(Count, nullCount);
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var structPointer = arrowSerializer.AddStructType();
            var childStack = pointerStack.Slice(_columns.Length);

            for (int i = 0; i < _columns.Length; i++)
            {
                var fieldNamePointer = arrowSerializer.CreateStringUtf8(_header.GetColumnNameUtf8(i));
                pointerStack[i] = _columns[i].CreateSchemaField(ref arrowSerializer, fieldNamePointer, childStack);
            }

            var structChildrenPointer = arrowSerializer.CreateChildrenVector(pointerStack.Slice(0, _columns.Length));
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Struct_, structPointer, childrenOffset: structChildrenPointer);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].WriteDataToBuffer(ref dataWriter);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    foreach(var column in _columns)
                    {
                        column.Dispose();
                    }
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        ~StructColumn()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].AddToHash(index, child, hashAlgorithm);
            }
        }
    }
}

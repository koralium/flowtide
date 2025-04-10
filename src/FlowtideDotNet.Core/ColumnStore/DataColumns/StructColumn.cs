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
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
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

        public StructColumn(StructHeader structHeader, IMemoryAllocator memoryAllocator)
        {
            _header = structHeader;
            _columns = new Column[structHeader.Count];
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i] = Column.Create(memoryAllocator);
            }
        }

        public int Count => _columns[0].Count;

        public ArrowTypeId Type => ArrowTypeId.Struct;

        public StructHeader StructHeader => _header;

        public int Add<T>(in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Struct)
            {
                var structVal = value.AsStructValue;
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
            throw new NotImplementedException();
        }

        public void Clear()
        {
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
                    // Compare with the inner column
                    return _columns[columnIndex].CompareTo(index, value, mapKeyReferenceSegment.Child);
                }
                throw new NotImplementedException();
            }
            else
            {
                var structValue = value.AsStructValue;
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
            throw new NotImplementedException();
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            throw new NotImplementedException();
        }

        public int EndNewList()
        {
            throw new NotImplementedException();
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
            return new ReferenceStructValue(this, index);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._structValue = new ReferenceStructValue(this, index);
            dataValueContainer._type = ArrowTypeId.Struct;
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Struct)
            {
                var structVal = value.AsStructValue;
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
            for (int i = 0; i < _columns.Length; i++)
            {
                for (int c = 0; c < count; c++)
                {
                    _columns[i].InsertAt(index, NullValue.Instance);
                }
            }
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (other is StructColumn otherStructColumn)
            {
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
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].RemoveAt(index);
            }
        }

        public void RemoveRange(int start, int count)
        {
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].RemoveRange(start, count);
            }
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            throw new NotImplementedException();
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Struct)
            {
                var structVal = value.AsStructValue;
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
            throw new NotImplementedException();
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            throw new NotImplementedException();
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            throw new NotImplementedException();
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            throw new NotImplementedException();
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            throw new NotImplementedException();
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
    }
}

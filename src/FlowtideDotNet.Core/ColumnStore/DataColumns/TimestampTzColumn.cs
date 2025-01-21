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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.CustomTypes;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    /// <summary>
    /// A data column that contains timestamps with a short offset to handle timezone differences.
    /// </summary>
    internal class TimestampTzColumn : IDataColumn
    {
        private readonly PrimitiveList<TimestampTzValue> _values;
        private bool disposedValue;

        public TimestampTzColumn(IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<TimestampTzValue>(memoryAllocator);
        }

        public TimestampTzColumn(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<TimestampTzValue>(memory, length, memoryAllocator);
        }

        internal TimestampTzColumn(PrimitiveList<TimestampTzValue> values)
        {
            _values = values;
        }

        public int Count => _values.Count;

        public ArrowTypeId Type => ArrowTypeId.Timestamp;

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _values.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _values.Add(new TimestampTzValue());
            }
            else
            {
                _values.Add(value.AsTimestamp);
            }
            return index;
        }

        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            // No operation
            throw new NotImplementedException();
        }

        public void Clear()
        {
            _values.Clear();
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
            return _values[index].CompareTo(value.AsTimestamp);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            Debug.Assert(_values != null);

            if (otherColumn is TimestampTzColumn timestampColumn)
            {
                Debug.Assert(timestampColumn._values != null);
                return _values.Get(thisIndex).CompareTo(timestampColumn._values.Get(otherIndex));
            }
            throw new NotImplementedException();
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new TimestampTzColumn(_values.Copy(memoryAllocator));
        }

        public int EndNewList()
        {
            // No operation
            throw new NotImplementedException();
        }

        public unsafe int GetByteSize(int start, int end)
        {
            return (end - start + 1) * sizeof(TimestampTzValue);
        }

        public unsafe int GetByteSize()
        {
            return Count * sizeof(TimestampTzValue);
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.Timestamp;
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return _values.Get(index);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.Timestamp;
            dataValueContainer._timestampValue = _values.Get(index);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _values.InsertAt(index, new TimestampTzValue());
            }
            else
            {
                _values.InsertAt(index, value.AsTimestamp);
            }
        }

        public void InsertNullRange(int index, int count)
        {
            _values.InsertStaticRange(index, new TimestampTzValue(), count);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (other is TimestampTzColumn timestampColumn)
            {
                _values.InsertRangeFrom(index, timestampColumn._values, start, count);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void RemoveAt(in int index)
        {
            _values.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _values.RemoveRange(start, count);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundries(_values, dataValue.AsTimestamp, start, end, TimestampTzComparerDesc.Instance);
            }
            return BoundarySearch.SearchBoundries(_values, dataValue.AsTimestamp, start, end, TimestampTzComparer.Instance);
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var buffers = new ArrowBuffer[2]
            {
                nullBuffer,
                new ArrowBuffer(_values.SlicedMemory)
            };
            var array = new FixedSizeBinaryArray(new ArrayData(TimestampTzType.Default, Count, nullCount, 0, buffers));
            return (array, TimestampTzType.Default);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            _values.Update(index, value.AsTimestamp);
            return index;
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteStringValue(_values.Get(index).ToDateTimeOffset());
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _values.Dispose();
                }
                disposedValue = true;
            }
        }

        ~TimestampTzColumn()
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

        public int SchemaFieldCountEstimate()
        {
            throw new NotImplementedException();
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            throw new NotImplementedException();
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            throw new NotImplementedException();
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            throw new NotImplementedException();
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            throw new NotImplementedException();
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            throw new NotImplementedException();
        }
    }
}

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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Buffers;
using System.Diagnostics;
using System.IO.Hashing;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class BinaryColumn : IDataColumn
    {
        private readonly BinaryList _data;
        private bool disposedValue;

        public BinaryColumn(IMemoryAllocator memoryAllocator)
        {
            _data = new BinaryList(memoryAllocator);
        }

        public BinaryColumn(IMemoryOwner<byte> offsetMemory, int offsetLength, IMemoryOwner<byte>? dataMemory, IMemoryAllocator memoryAllocator)
        {
            _data = new BinaryList(offsetMemory, offsetLength, dataMemory, memoryAllocator);
        }

        internal BinaryColumn(BinaryList data)
        {
            _data = data;
        }

        public int Count => _data.Count;

        public ArrowTypeId Type => ArrowTypeId.Binary;

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _data.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _data.Add(Span<byte>.Empty);
                return index;
            }
            else
            {
                _data.Add(value.AsBinary);
                return index;
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
            return SpanByteComparer.Instance.Compare(_data.Get(index), value.AsBinary);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            Debug.Assert(_data != null);

            if (otherColumn is BinaryColumn binaryColumn)
            {
                Debug.Assert(binaryColumn._data != null);
                return _data.Get(thisIndex).SequenceCompareTo(binaryColumn._data.Get(otherIndex));
            }
            throw new NotImplementedException();
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new BinaryValue(_data.GetMemory(index));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.Binary;
            dataValueContainer._binaryValue = new BinaryValue(_data.GetMemory(index));
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _data.Insert(index, Span<byte>.Empty);
            }
            else
            {
                _data.Insert(index, value.AsBinary);
            }
        }

        public void RemoveAt(in int index)
        {
            _data.RemoveAt(index);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundries(_data, dataValue.AsBinary, start, end, SpanByteComparerDesc.Instance);
            }
            return BoundarySearch.SearchBoundries(_data, dataValue.AsBinary, start, end, SpanByteComparer.Instance);
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var valueOffsetBuffer = new ArrowBuffer(_data.OffsetMemory);
            var dataBuffer = new ArrowBuffer(_data.DataMemory);
            var array = new BinaryArray(BinaryType.Default, Count, valueOffsetBuffer, dataBuffer, nullBuffer, nullCount);
            return (array, BinaryType.Default);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            _data.UpdateAt(index, value.AsBinary);
            return index;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _data.Dispose();
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
            return ArrowTypeId.Binary;
        }

        public void Clear()
        {
            _data.Clear();
        }

        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int EndNewList()
        {
            throw new NotImplementedException();
        }

        public void RemoveRange(int start, int count)
        {
            _data.RemoveRange(start, count);
        }

        public int GetByteSize(int start, int end)
        {
            return _data.GetByteSize(start, end);
        }

        public int GetByteSize()
        {
            return _data.GetByteSize(0, Count - 1);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (other is BinaryColumn binaryColumn)
            {
                _data.InsertRangeFrom(index, binaryColumn._data, start, count);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void InsertNullRange(int index, int count)
        {
            _data.InsertNullRange(index, count);
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteBase64StringValue(_data.GetMemory(index).Span);
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new BinaryColumn(_data.Copy(memoryAllocator));
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            hashAlgorithm.Append(_data.GetMemory(index).Span);
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var binaryTypeOffset = arrowSerializer.AddBinaryType();
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Binary, binaryTypeOffset);
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            return new SerializationEstimation(1, 2, GetByteSize());
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            arrowSerializer.CreateFieldNode(Count, nullCount);
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            arrowSerializer.AddBufferForward(_data.OffsetMemory.Length);
            arrowSerializer.AddBufferForward(_data.DataMemory.Length);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            // Write offset data
            dataWriter.WriteArrowBuffer(_data.OffsetMemory.Span);


            // Write binary data
            dataWriter.WriteArrowBuffer(_data.DataMemory.Span);
        }
    }
}

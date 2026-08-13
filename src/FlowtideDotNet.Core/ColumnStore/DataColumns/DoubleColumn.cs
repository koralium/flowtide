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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Hashing;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class DoubleColumn : IDataColumn
    {
        // Not readonly, mutations would run on a copy.
        private NativeList<double> _data;
        private readonly IMemoryAllocator _memoryAllocator;
        private bool disposedValue;

        public int Count => _data.Count;

        public ArrowTypeId Type => ArrowTypeId.Double;

        public StructHeader StructHeader => throw new NotImplementedException();

        public DoubleColumn(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
        }

        public DoubleColumn(IMemoryAllocator memoryAllocator, ColumnSizeInfo columnSizeInfo)
        {
            _memoryAllocator = memoryAllocator;
            _data.EnsureCapacity(columnSizeInfo.TotalRows, memoryAllocator);
        }

        public DoubleColumn(FlowtideMemory memory, int count, IMemoryAllocator memoryAllocator)
        {
            _data = new NativeList<double>(memory, count);
            _memoryAllocator = memoryAllocator;
        }

#pragma warning disable RS0042 // The list moves into the column and is not used again.
        internal DoubleColumn(NativeList<double> data, IMemoryAllocator memoryAllocator)
        {
            _data = data;
            _memoryAllocator = memoryAllocator;
        }
#pragma warning restore RS0042

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _data.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _data.Add(0, _memoryAllocator);
                return index;
            }
            else
            {
                _data.Add(value.AsDouble, _memoryAllocator);
                return index;
            }
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is DoubleColumn doubleColumn)
            {
                return _data.Get(thisIndex).CompareTo(doubleColumn._data.Get(otherIndex));
            }
            throw new NotImplementedException();
        }

        public int CompareTo<T>(in int index, in T value, in ReferenceSegment? child, in BitmapList validityList) where T : IDataValue
        {
            if (!validityList.IsNull &&
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
            return _data[index].CompareTo(value.AsDouble);
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new DoubleValue(_data[index]);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.Double;
            dataValueContainer._doubleValue = new DoubleValue(_data[index]);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc)
            where T : IDataValue
        {
            var val = dataValue.AsDouble;
            if (desc)
            {
                return BoundarySearch.SearchBoundries(in _data, val, start, end, DoubleComparerDesc.Instance);
            }
            return BoundarySearch.SearchBoundries(in _data, val, start, end, DoubleComparer.Instance);
        }

        public int Update(in int index, in IDataValue value)
        {
            _data[index] = value.AsDouble;
            return index;
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            _data[index] = value.AsDouble;
            return index;
        }

        public void RemoveAt(in int index)
        {
            _data.RemoveAt(index, _memoryAllocator);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _data.InsertAt(index, default, _memoryAllocator);
            }
            else
            {
                _data.InsertAt(index, value.AsDouble, _memoryAllocator);
            }
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var dataBuffer = new ArrowBuffer(_data.SlicedMemory);
            var array = new DoubleArray(dataBuffer, nullBuffer, Count, nullCount, 0);
            return (array, new DoubleType());
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                // The struct has no finalizer so we free it here.
                _data.Dispose(_memoryAllocator);
                disposedValue = true;
            }
        }

        ~DoubleColumn()
        {
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.Double;
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
            _data.RemoveRange(start, count, _memoryAllocator);
        }

        public int GetByteSize(int start, int end)
        {
            return (end - start + 1) * sizeof(double);
        }

        public int GetByteSize()
        {
            return Count * sizeof(double);
        }

        public void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
        {
            _data.GetPrefixSumByteSizes(indices, sizes);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, in BitmapList validityList)
        {
            if (other is DoubleColumn doubleColumn)
            {
                _data.InsertRangeFrom(index, in doubleColumn._data, start, count, _memoryAllocator);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void InsertNullRange(int index, int count)
        {
            _data.InsertStaticRange(index, 0, count, _memoryAllocator);
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteNumberValue(_data.Get(index));
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new DoubleColumn(_data.Copy(memoryAllocator), memoryAllocator);
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            Span<byte> buffer = stackalloc byte[8];
            BinaryPrimitives.WriteDoubleLittleEndian(buffer, _data[index]);
            hashAlgorithm.Append(buffer);
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var typePointer = arrowSerializer.AddDoubleType();
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.FloatingPoint, typePointer);
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            return new SerializationEstimation(1, 1, GetByteSize());
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            arrowSerializer.CreateFieldNode(Count, nullCount);
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            arrowSerializer.AddBufferForward(_data.SlicedSpan.Length);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            dataWriter.WriteArrowBuffer(_data.SlicedSpan);
        }

        public void InsertFrom(in IDataColumn other, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex)
        {
            if (other is DoubleColumn doubleColumn)
            {
                _data.InsertFrom(in doubleColumn._data, in sortedLookup, in insertPositions, lookupNullIndex, _memoryAllocator);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void DeleteBatch(ReadOnlySpan<int> targets)
        {
            _data.DeleteBatch(targets, _memoryAllocator);
        }

        public ColumnSizeInfo GetColumnSizeInfo()
        {
            return new ColumnSizeInfo()
            {
                DataType = ArrowTypeId.Double,
                TotalRows = Count,
            };
        }

        unsafe void IDataColumn.SetSelfComparePointers(ref SelfComparePointers selfComparePointers)
        {
            selfComparePointers.dataPointer = _data.GetPointer_Unsafe();
        }

        System.Linq.Expressions.Expression IDataColumn.CreateSelfCompareExpression(
            System.Linq.Expressions.Expression selfComparePointerExpression, 
            System.Linq.Expressions.Expression xExpression, 
            System.Linq.Expressions.Expression yExpression)
        {
            return NativeSortHelpers.CallCompareDouble(selfComparePointerExpression, xExpression, yExpression);
        }

        bool IDataColumn.SupportSelfCompareExpression => true;

        public CompareColumnState GetColumnState()
        {
            return CompareColumnStateBuilder.Create(ArrowTypeId.Double);
        }
    }
}





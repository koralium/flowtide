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
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Buffers;
using System.Diagnostics;
using System.IO.Hashing;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class DecimalColumn : IDataColumn
    {
        // Not readonly, mutations would run on a copy.
        private NativeList<decimal> _values;
        private readonly IMemoryAllocator _memoryAllocator;
        private bool disposedValue;

        public int Count => _values.Count;

        public ArrowTypeId Type => ArrowTypeId.Decimal128;

        public StructHeader StructHeader => throw new NotImplementedException();

        public DecimalColumn(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
        }

        public DecimalColumn(IMemoryAllocator memoryAllocator, ColumnSizeInfo columnSizeInfo)
        {
            _memoryAllocator = memoryAllocator;
            _values.EnsureCapacity(columnSizeInfo.TotalRows, memoryAllocator);
        }

        public DecimalColumn(FlowtideMemory memory, int length, IMemoryAllocator memoryAllocator)
        {
            _values = new NativeList<decimal>(memory, length);
            _memoryAllocator = memoryAllocator;
        }

#pragma warning disable RS0042 // The list moves into the column and is not used again.
        internal DecimalColumn(NativeList<decimal> values, IMemoryAllocator memoryAllocator)
        {
            _values = values;
            _memoryAllocator = memoryAllocator;
        }
#pragma warning restore RS0042

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _values.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _values.Add(0, _memoryAllocator);
            }
            else
            {
                _values.Add(value.AsDecimal, _memoryAllocator);
            }
            return index;
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is DecimalColumn decimalColumn)
            {
                return _values.Get(thisIndex).CompareTo(decimalColumn._values.Get(otherIndex));
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
            return _values[index].CompareTo(value.AsDecimal);
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new DecimalValue(_values[index]);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.Decimal128;
            dataValueContainer._decimalValue = new DecimalValue(_values[index]);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc)
            where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundries(in _values, dataValue.AsDecimal, start, end, DecimalComparerDesc.Instance);
            }
            return BoundarySearch.SearchBoundries(in _values, dataValue.AsDecimal, start, end, DecimalComparer.Instance);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            _values.Update(index, value.AsDecimal);
            return index;
        }

        public void RemoveAt(in int index)
        {
            _values.RemoveAt(index, _memoryAllocator);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _values.InsertAt(index, 0, _memoryAllocator);
            }
            else
            {
                _values.InsertAt(index, value.AsDecimal, _memoryAllocator);
            }
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var buffers = new ArrowBuffer[2]
            {
                nullBuffer,
                new ArrowBuffer(_values.SlicedMemory)
            };
            var array = new FixedSizeBinaryArray(new ArrayData(FloatingPointDecimalType.Default, Count, nullCount, 0, buffers));
            return (array, FloatingPointDecimalType.Default);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                // The struct has no finalizer so we free it here.
                _values.Dispose(_memoryAllocator);
                disposedValue = true;
            }
        }

        ~DecimalColumn()
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
            return ArrowTypeId.Decimal128;
        }

        public void Clear()
        {
            _values.Clear();
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
            _values.RemoveRange(start, count, _memoryAllocator);
        }

        public int GetByteSize(int start, int end)
        {
            return (end - start + 1) * sizeof(decimal);
        }

        public int GetByteSize()
        {
            return Count * sizeof(decimal);
        }

        public void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
        {
            _values.GetPrefixSumByteSizes(indices, sizes);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, in BitmapList validityList)
        {
            if (other is DecimalColumn decimalColumn)
            {
                _values.InsertRangeFrom(index, in decimalColumn._values, start, count, _memoryAllocator);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void InsertNullRange(int index, int count)
        {
            _values.InsertStaticRange(index, 0, count, _memoryAllocator);
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteNumberValue(_values.Get(index));
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new DecimalColumn(_values.Copy(memoryAllocator), memoryAllocator);
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            Span<byte> buffer = stackalloc byte[16];
            var decimalSpan = MemoryMarshal.Cast<byte, decimal>(buffer);
            decimalSpan[0] = _values[index];
            hashAlgorithm.Append(buffer);
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var extensionKeyPointer = arrowSerializer.CreateStringUtf8("ARROW:extension:name"u8);
            var extensionValuePointer = arrowSerializer.CreateStringUtf8("flowtide.floatingdecimaltype"u8);
            var typePointer = arrowSerializer.AddFixedSizeBinaryType(16);
            pointerStack[0] = arrowSerializer.CreateKeyValue(extensionKeyPointer, extensionValuePointer);
            var customMetadataPointer = arrowSerializer.CreateCustomMetadataVector(pointerStack.Slice(0, 1));
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.FixedSizeBinary, typePointer, custom_metadataOffset: customMetadataPointer);
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
            arrowSerializer.AddBufferForward(_values.SlicedSpan.Length);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            dataWriter.WriteArrowBuffer(_values.SlicedSpan);
        }

        public void InsertFrom(in IDataColumn other, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex)
        {
            if (other is DecimalColumn decimalColumn)
            {
                _values.InsertFrom(in decimalColumn._values, in sortedLookup, in insertPositions, lookupNullIndex, _memoryAllocator);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void DeleteBatch(ReadOnlySpan<int> targets)
        {
            _values.DeleteBatch(targets, _memoryAllocator);
        }

        public ColumnSizeInfo GetColumnSizeInfo()
        {
            return new ColumnSizeInfo()
            {
                DataType = ArrowTypeId.Decimal128,
                TotalRows = Count
            };
        }

        public CompareColumnState GetColumnState()
        {
            return CompareColumnStateBuilder.Create(ArrowTypeId.Decimal128);
        }

        unsafe void IDataColumn.SetSelfComparePointers(ref SelfComparePointers selfComparePointers)
        {
            selfComparePointers.dataPointer = _values.GetPointer_Unsafe();
        }

        System.Linq.Expressions.Expression IDataColumn.CreateSelfCompareExpression(System.Linq.Expressions.Expression selfComparePointerExpression, System.Linq.Expressions.Expression xExpression, System.Linq.Expressions.Expression yExpression)
        {
            return NativeSortHelpers.CallCompareDecimal128(selfComparePointerExpression, xExpression, yExpression);
        }

        bool IDataColumn.SupportSelfCompareExpression => true;
    }
}



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
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Hashing;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    /// <summary>
    /// A data column that contains timestamps with a short offset to handle timezone differences.
    /// </summary>
    internal class TimestampTzColumn : IDataColumn
    {
        // Not readonly, mutations would run on a copy.
        private NativeList<TimestampTzValue> _values;

        public TimestampTzColumn(IMemoryAllocator memoryAllocator)
        {
        }

        public TimestampTzColumn(IMemoryAllocator memoryAllocator, ColumnSizeInfo columnSizeInfo)
        {
            _values.EnsureCapacity(columnSizeInfo.TotalRows, memoryAllocator);
        }

        public TimestampTzColumn(FlowtideMemory memory, int length, IMemoryAllocator memoryAllocator)
        {
            _values = new NativeList<TimestampTzValue>(memory, length);
        }

#pragma warning disable RS0042 // The list moves into the column and is not used again.
        internal TimestampTzColumn(NativeList<TimestampTzValue> values)
        {
            _values = values;
        }
#pragma warning restore RS0042

        public int Count => _values.Count;

        public ArrowTypeId Type => ArrowTypeId.Timestamp;

        public StructHeader StructHeader => throw new NotImplementedException();

        public int Add<T>(in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            var index = _values.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _values.Add(new TimestampTzValue(), memoryAllocator);
            }
            else
            {
                _values.Add(value.AsTimestamp, memoryAllocator);
            }
            return index;
        }

        public void AddToNewList<T>(in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            // No operation
            throw new NotImplementedException();
        }

        public void Clear(IMemoryAllocator memoryAllocator)
        {
            _values.Clear();
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
            return _values[index].CompareTo(value.AsTimestamp);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is TimestampTzColumn timestampColumn)
            {
                return _values.Get(thisIndex).CompareTo(timestampColumn._values.Get(otherIndex));
            }
            throw new NotImplementedException();
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new TimestampTzColumn(_values.Copy(memoryAllocator));
        }

        public int EndNewList(IMemoryAllocator memoryAllocator)
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

        public void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
        {
            _values.GetPrefixSumByteSizes(indices, sizes);
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

        public void InsertAt<T>(in int index, in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _values.InsertAt(index, new TimestampTzValue(), memoryAllocator);
            }
            else
            {
                _values.InsertAt(index, value.AsTimestamp, memoryAllocator);
            }
        }

        public void InsertNullRange(int index, int count, IMemoryAllocator memoryAllocator)
        {
            _values.InsertStaticRange(index, new TimestampTzValue(), count, memoryAllocator);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, in BitmapList validityList, IMemoryAllocator memoryAllocator)
        {
            if (other is TimestampTzColumn timestampColumn)
            {
                _values.InsertRangeFrom(index, in timestampColumn._values, start, count, memoryAllocator);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void RemoveAt(in int index, IMemoryAllocator memoryAllocator)
        {
            _values.RemoveAt(index, memoryAllocator);
        }

        public void RemoveRange(int start, int count, IMemoryAllocator memoryAllocator)
        {
            _values.RemoveRange(start, count, memoryAllocator);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundries(in _values, dataValue.AsTimestamp, start, end, TimestampTzComparerDesc.Instance);
            }
            return BoundarySearch.SearchBoundries(in _values, dataValue.AsTimestamp, start, end, TimestampTzComparer.Instance);
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

        public int Update<T>(in int index, in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            _values.Update(index, value.AsTimestamp);
            return index;
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            var dt = _values.Get(index).ToDateTimeOffset();
            writer.WriteStringValue(dt);
        }

        public void Dispose(IMemoryAllocator memoryAllocator)
        {
            _values.Dispose(memoryAllocator);
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            Span<byte> buffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(buffer, _values.Get(index).ticks);
            hashAlgorithm.Append(buffer);
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            return new SerializationEstimation(1, 1, GetByteSize());
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var extensionKeyPointer = arrowSerializer.CreateStringUtf8("ARROW:extension:name"u8);
            var extensionValuePointer = arrowSerializer.CreateStringUtf8("flowtide.timestamptz"u8);
            var typePointer = arrowSerializer.AddFixedSizeBinaryType(16);
            pointerStack[0] = arrowSerializer.CreateKeyValue(extensionKeyPointer, extensionValuePointer);
            var customMetadataPointer = arrowSerializer.CreateCustomMetadataVector(pointerStack.Slice(0, 1));
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.FixedSizeBinary, typePointer, custom_metadataOffset: customMetadataPointer);
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

        public void InsertFrom(in IDataColumn other, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex, IMemoryAllocator memoryAllocator)
        {
            if (other is TimestampTzColumn timestampColumn)
            {
                _values.InsertFrom(in timestampColumn._values, in sortedLookup, in insertPositions, lookupNullIndex, memoryAllocator);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void DeleteBatch(ReadOnlySpan<int> targets, IMemoryAllocator memoryAllocator)
        {
            _values.DeleteBatch(targets, memoryAllocator);
        }

        public ColumnSizeInfo GetColumnSizeInfo()
        {
            return new ColumnSizeInfo()
            {
                DataType = ArrowTypeId.Timestamp,
                TotalRows = Count,
            };
        }

        public CompareColumnState GetColumnState()
        {
            return CompareColumnStateBuilder.Create(ArrowTypeId.Timestamp);
        }

        unsafe void IDataColumn.SetSelfComparePointers(ref SelfComparePointers selfComparePointers)
        {
            selfComparePointers.dataPointer = _values.GetPointer_Unsafe();
        }

        System.Linq.Expressions.Expression IDataColumn.CreateSelfCompareExpression(System.Linq.Expressions.Expression selfComparePointerExpression, System.Linq.Expressions.Expression xExpression, System.Linq.Expressions.Expression yExpression)
        {
            return NativeSortHelpers.CallCompareTimestampTzValues(selfComparePointerExpression, xExpression, yExpression);
        }

        bool IDataColumn.SupportSelfCompareExpression => true;
    }
}





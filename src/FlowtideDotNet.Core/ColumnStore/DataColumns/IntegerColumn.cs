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
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    internal class IntegerColumn : IDataColumn
    {
        // Not readonly, mutations would run on a copy.
        private IntegerList _list;

        public IntegerColumn(IMemoryAllocator memoryAllocator)
        {
        }

        public IntegerColumn(IMemoryAllocator memoryAllocator, ColumnSizeInfo columnSizeInfo)
        {
            switch (columnSizeInfo.BitWidth)
            {
                case 8:
                    _list = new IntegerList(1);
                    break;
                case 16:
                    _list = new IntegerList(2);
                    break;
                case 32:
                    _list = new IntegerList(4);
                    break;
                case 64:
                    _list = new IntegerList(8);
                    break;
                default:
                    throw new NotImplementedException();
            }
            _list.EnsureCapacity(columnSizeInfo.TotalRows, memoryAllocator);
        }

        public IntegerColumn(IMemoryAllocator memoryAllocator, FlowtideMemory memory, int length, int bitWidth)
        {
            switch (bitWidth)
            {
                case 8:
                    _list = new IntegerList(memory, length, 1);
                    break;
                case 16:
                    _list = new IntegerList(memory, length, 2);
                    break;
                case 32:
                    _list = new IntegerList(memory, length, 4);
                    break;
                case 64:
                    _list = new IntegerList(memory, length, 8);
                    break;
                default:
                    // Nothing owns the memory yet so we free it here.
                    memoryAllocator.Free(ref memory);
                    throw new NotImplementedException();
            }
        }

        public int Count => _list.Count;

        public ArrowTypeId Type => ArrowTypeId.Int64;

        public StructHeader StructHeader => throw new NotImplementedException();

        private static byte ElementSizeFor(in long value)
        {
            if (value <= sbyte.MaxValue && value >= sbyte.MinValue)
            {
                return 1;
            }
            else if (value <= short.MaxValue && value >= short.MinValue)
            {
                return 2;
            }
            else if (value <= int.MaxValue && value >= int.MinValue)
            {
                return 4;
            }
            return 8;
        }

        private void WidenToBitWidth(int targetBitWidth, IMemoryAllocator memoryAllocator)
        {
            if (_list.ElementSize != 0 && targetBitWidth < _list.BitWidth)
            {
                throw new ArgumentOutOfRangeException(nameof(targetBitWidth), $"Cannot narrow integer column from {_list.BitWidth} bits to {targetBitWidth} bits.");
            }

            switch (targetBitWidth)
            {
                case 8:
                    _list.Widen(1, memoryAllocator);
                    break;
                case 16:
                    _list.Widen(2, memoryAllocator);
                    break;
                case 32:
                    _list.Widen(4, memoryAllocator);
                    break;
                case 64:
                    _list.Widen(8, memoryAllocator);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(targetBitWidth), targetBitWidth, $"Unsupported bit width: {targetBitWidth}");
            }
        }

        private void CheckSize(in long value, IMemoryAllocator memoryAllocator)
        {
            if (_list.ElementSize == 0)
            {
                _list.Widen(ElementSizeFor(in value), memoryAllocator);
            }
            else if (value < _list.MinValue || value > _list.MaxValue)
            {
                _list.Widen(ElementSizeFor(in value), memoryAllocator);
            }
        }

        public int Add<T>(in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            if (value.IsNull)
            {
                CheckSize(0, memoryAllocator);
                return _list.Add(0, memoryAllocator);
            }

            var val = value.AsLong;

            CheckSize(in val, memoryAllocator);

            return _list.Add(val, memoryAllocator);
        }

        public void AddToNewList<T>(in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public void Clear(IMemoryAllocator memoryAllocator)
        {
            _list.Clear();
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
            var longValue = value.AsLong;
            return _list.Get(index).CompareTo(longValue);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is IntegerColumn integerColumn)
            {
                return _list.Get(thisIndex).CompareTo(integerColumn._list.Get(otherIndex));
            }
            throw new NotImplementedException();
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            if (_list.ElementSize == 0)
            {
                return new IntegerColumn(memoryAllocator);
            }
            return new IntegerColumn(memoryAllocator) { _list = _list.Copy(memoryAllocator) };
        }

        public void Dispose(IMemoryAllocator memoryAllocator)
        {
            _list.Dispose(memoryAllocator);
        }

        public int EndNewList(IMemoryAllocator memoryAllocator)
        {
            throw new NotImplementedException();
        }

        public int GetByteSize(int start, int end)
        {
            return (end - start + 1) * _list.ElementSize;
        }

        public int GetByteSize()
        {
            return Count * _list.ElementSize;
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            return new SerializationEstimation(1, 1, GetByteSize());
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.Int64;
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new Int64Value(_list.Get(index));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.Int64;
            dataValueContainer._int64Value = new Int64Value(_list.Get(index));
        }

        public void InsertAt<T>(in int index, in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            if (value.IsNull)
            {
                CheckSize(0, memoryAllocator);
                _list.InsertAt(index, 0, memoryAllocator);
                return;
            }

            var val = value.AsLong;

            CheckSize(val, memoryAllocator);
            _list.InsertAt(index, val, memoryAllocator);
        }

        public void InsertNullRange(int index, int count, IMemoryAllocator memoryAllocator)
        {
            CheckSize(0, memoryAllocator);
            _list.InsertNullRange(index, count, memoryAllocator);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, in BitmapList validityList, IMemoryAllocator memoryAllocator)
        {
            if (other is IntegerColumn integerColumn)
            {
                Debug.Assert(integerColumn._list.ElementSize != 0 || count == 0);
                // Check if we need to resize, this also sets a width if there is none
                CheckSize(0, memoryAllocator);
                if (_list.ElementSize == integerColumn._list.ElementSize)
                {
                    _list.InsertRangeFrom(index, in integerColumn._list, start, count, memoryAllocator);
                    return;
                }
                else
                {
                    // Create space for the new values, this increases the count as well
                    _list.MoveRangeAt(index, count, memoryAllocator);

                    // Missmatch in bitwidth insert one by one to check if there is any size change
                    for (int i = 0; i < count; i++)
                    {
                        var val = integerColumn._list.Get(start + i);
                        // Check if we need to resize
                        CheckSize(val, memoryAllocator);
                        // Update the value
                        _list.Update(index + i, val);
                    }
                }
                return;
            }
            throw new NotImplementedException();
        }

        public void RemoveAt(in int index, IMemoryAllocator memoryAllocator)
        {
            _list.RemoveAt(index, memoryAllocator);
        }

        public void RemoveRange(int start, int count, IMemoryAllocator memoryAllocator)
        {
            _list.RemoveRange(start, count, memoryAllocator);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            if (_list.ElementSize == 0)
            {
                if (!desc)
                {
                    return (~start, ~start);
                }
                else
                {
                    var index = ~(end + 1);
                    return (index, index);
                }
            }
            return _list.SearchBoundries(dataValue.AsLong, start, end, desc);
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            if (_list.ElementSize == 0)
            {
                return (new Int8Array(new ArrowBuffer(), nullBuffer, 0, nullCount, 0), Int8Type.Default);
            }
            return _list.ToArrowArray(nullBuffer, nullCount);
        }

        public int Update<T>(in int index, in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            if (value.IsNull)
            {
                CheckSize(0, memoryAllocator);
                _list.Update(index, 0);
                return index;
            }

            var val = value.AsLong;

            CheckSize(val, memoryAllocator);

            _list.Update(index, val);
            return index;
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteNumberValue(_list.Get(index));
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            arrowSerializer.AddBufferForward(_list.SlicedSpan.Length);
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            arrowSerializer.CreateFieldNode(Count, nullCount);
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var typePointer = arrowSerializer.AddIntType(_list.BitWidth);
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Int, typePointer);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            dataWriter.WriteArrowBuffer(_list.SlicedSpan);
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            // Always use 8 bytes for the hash to produce same hash for say value 3 even if it is 1,2,4 or 8 bytes.
            Span<byte> buffer = stackalloc byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(buffer, _list.Get(index));
            hashAlgorithm.Append(buffer);
        }

        public void InsertFrom(in IDataColumn other, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex, IMemoryAllocator memoryAllocator)
        {
            if (other is IntegerColumn integerColumn)
            {
                Debug.Assert(integerColumn._list.ElementSize != 0);
                // Check if we need to resize, this also sets a width if there is none
                if (_list.ElementSize == 0 || _list.ElementSize != integerColumn._list.ElementSize)
                {
                    if (integerColumn._list.ElementSize > _list.ElementSize)
                    {
                        // Other is wider
                        WidenToBitWidth(integerColumn._list.BitWidth, memoryAllocator);
                    }
                    else
                    {
                        CheckSize(0, memoryAllocator);
                        // This is wider, for now to just inserts
                        for (int i = sortedLookup.Length - 1; i >= 0; i--)
                        {
                            _list.InsertAt(insertPositions[i], integerColumn._list.Get(sortedLookup[i]), memoryAllocator);
                        }
                        return;
                    }
                }
                _list.InsertFrom(in integerColumn._list, in sortedLookup, in insertPositions, lookupNullIndex, memoryAllocator);
                return;
            }
            throw new NotImplementedException();
        }

        public void DeleteBatch(ReadOnlySpan<int> targets, IMemoryAllocator memoryAllocator)
        {
            if (_list.ElementSize != 0)
            {
                _list.DeleteBatch(targets, memoryAllocator);
            }
        }

        public ColumnSizeInfo GetColumnSizeInfo()
        {
            return new ColumnSizeInfo()
            {
                DataType = ArrowTypeId.Int64,
                TotalRows = Count,
                BitWidth = _list.BitWidth
            };
        }

        void IDataColumn.SetSelfComparePointers(ref SelfComparePointers selfComparePointers)
        {
            if (_list.ElementSize != 0)
            {
                _list.SetSelfComparePointers(ref selfComparePointers);
            }
        }

        System.Linq.Expressions.Expression IDataColumn.CreateSelfCompareExpression(
            System.Linq.Expressions.Expression selfComparePointerExpression,
            System.Linq.Expressions.Expression xExpression,
            System.Linq.Expressions.Expression yExpression)
        {
            if (_list.ElementSize != 0)
            {
                return _list.CreateSelfCompareExpression(selfComparePointerExpression, xExpression, yExpression);
            }
            return System.Linq.Expressions.Expression.Constant(0); // No elements
        }

        bool IDataColumn.SupportSelfCompareExpression => true;

        public CompareColumnState GetColumnState()
        {
            return _list.GetColumnState();
        }

        public void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
        {
            if (_list.ElementSize != 0)
            {
                int length = indices.Length;
                var elementSize = _list.ElementSize;
                ref int sizesHead = ref MemoryMarshal.GetReference(sizes);

                int cumulativeMass = elementSize;

                for (int i = 0; i < length; i++)
                {
                    Unsafe.Add(ref sizesHead, i) += cumulativeMass;
                    cumulativeMass += elementSize;
                }
            }
        }

        RadixCapability IDataColumn.SupportsRadixSort(int bytesLeft)
        {
            switch (_list.ElementSize)
            {
                case 1:
                    return bytesLeft >= 1 ? RadixCapability.Full(1) : RadixCapability.None();
                case 2:
                    return bytesLeft >= 2 ? RadixCapability.Full(2) : RadixCapability.None();
                case 4:
                    return bytesLeft >= 4 ? RadixCapability.Full(4) : RadixCapability.None();
                case 8:
                    return bytesLeft >= 8 ? RadixCapability.Full(8) : RadixCapability.None();
                default:
                    return RadixCapability.None();
            }
        }

        int IDataColumn.SetRadixPrefix(Span<RadixItem> items, int insertBytePosition, ReadOnlySpan<int> selectionVector)
        {
            if (_list.ElementSize != 0)
            {
                return _list.SetRadixPrefix(items, insertBytePosition, selectionVector);
            }
            return 0;
        }
    }
}

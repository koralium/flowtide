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
using SqlParser.Ast;
using System.Buffers;
using System.Diagnostics;
using System.IO.Hashing;
using System.Text.Json;
using static SqlParser.Ast.FetchDirection;
using static SqlParser.Ast.MatchRecognizeSymbol;

namespace FlowtideDotNet.Core.ColumnStore
{
    /// <summary>
    /// Binary column that is also the MemoryManager over its data.
    /// </summary>
    public class BinaryColumn : MemoryManager<byte>, IDataColumn
    {
        // Not readonly, mutations would run on a copy.
        private BinaryList _data;

        public override Span<byte> GetSpan()
        {
            return _data.CapacitySpan;
        }

        public override unsafe MemoryHandle Pin(int elementIndex = 0)
        {
            return new MemoryHandle(((byte*)_data.GetDataPointer_Unsafe()) + elementIndex, default, default);
        }

        public override void Unpin()
        {
        }

        public BinaryColumn(IMemoryAllocator memoryAllocator)
        {
            _data = new BinaryList(memoryAllocator);
        }

        public BinaryColumn(IMemoryAllocator memoryAllocator, ColumnSizeInfo columnSizeInfo)
        {
            _data = new BinaryList(memoryAllocator, columnSizeInfo.TotalRows, columnSizeInfo.TotalVariableBytes);
        }

        public BinaryColumn(FlowtideMemory offsetMemory, int offsetLength, FlowtideMemory dataMemory, IMemoryAllocator memoryAllocator)
        {
            _data = new BinaryList(offsetMemory, offsetLength, dataMemory);
        }

        internal BinaryColumn(BinaryList data)
        {
            // The argument is a fresh copy that is never used again.
#pragma warning disable RS0042
            _data = data;
#pragma warning restore RS0042
        }

        public int Count => _data.Count;

        public ArrowTypeId Type => ArrowTypeId.Binary;

        public StructHeader StructHeader => throw new NotImplementedException();

        public int Add<T>(in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            var index = _data.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _data.Add(Span<byte>.Empty, memoryAllocator);
                return index;
            }
            else
            {
                _data.Add(value.AsBinary, memoryAllocator);
                return index;
            }
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
            return SpanByteComparer.Instance.Compare(_data.Get(index), value.AsBinary);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is BinaryColumn binaryColumn)
            {
                return _data.Get(thisIndex).SequenceCompareTo(binaryColumn._data.Get(otherIndex));
            }
            throw new NotImplementedException();
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            var (offset, length) = _data.GetOffsetAndLength(in index);
            return new BinaryValue(CreateMemory(offset, length));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            var (offset, length) = _data.GetOffsetAndLength(in index);
            dataValueContainer._type = ArrowTypeId.Binary;
            dataValueContainer._binaryValue = new BinaryValue(CreateMemory(offset, length));
        }

        public void InsertAt<T>(in int index, in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _data.Insert(index, Span<byte>.Empty, memoryAllocator);
            }
            else
            {
                _data.Insert(index, value.AsBinary, memoryAllocator);
            }
        }

        public void RemoveAt(in int index, IMemoryAllocator memoryAllocator)
        {
            _data.RemoveAt(index, memoryAllocator);
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
            var dataBuffer = new ArrowBuffer(CreateMemory(0, _data.DataSpan.Length));
            var array = new BinaryArray(BinaryType.Default, Count, valueOffsetBuffer, dataBuffer, nullBuffer, nullCount);
            return (array, BinaryType.Default);
        }

        public int Update<T>(in int index, in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            _data.UpdateAt(index, value.AsBinary, memoryAllocator);
            return index;
        }

        public void Dispose(IMemoryAllocator memoryAllocator)
        {
            _data.Dispose(memoryAllocator);
        }

        protected override void Dispose(bool disposing)
        {
            // Freeing needs the allocator, the owning column calls Dispose(IMemoryAllocator).
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.Binary;
        }

        public void Clear(IMemoryAllocator memoryAllocator)
        {
            _data.Clear(memoryAllocator);
        }

        public void AddToNewList<T>(in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int EndNewList(IMemoryAllocator memoryAllocator)
        {
            throw new NotImplementedException();
        }

        public void RemoveRange(int start, int count, IMemoryAllocator memoryAllocator)
        {
            _data.RemoveRange(start, count, memoryAllocator);
        }

        public int GetByteSize(int start, int end)
        {
            return _data.GetByteSize(start, end);
        }

        public int GetByteSize()
        {
            return _data.GetByteSize(0, Count - 1);
        }

        public void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
        {
            _data.GetPrefixSumByteSizes(indices, sizes);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, in BitmapList validityList, IMemoryAllocator memoryAllocator)
        {
            if (other is BinaryColumn binaryColumn)
            {
                _data.InsertRangeFrom(index, binaryColumn._data, start, count, memoryAllocator);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void InsertNullRange(int index, int count, IMemoryAllocator memoryAllocator)
        {
            _data.InsertNullRange(index, count, memoryAllocator);
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteBase64StringValue(_data.Get(in index));
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new BinaryColumn(_data.Copy(memoryAllocator));
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            hashAlgorithm.Append(_data.Get(in index));
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
            arrowSerializer.AddBufferForward(_data.OffsetSpan.Length);
            arrowSerializer.AddBufferForward(_data.DataSpan.Length);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            // Write offset data
            dataWriter.WriteArrowBuffer(_data.OffsetSpan);


            // Write binary data
            dataWriter.WriteArrowBuffer(_data.DataSpan);
        }

        public void InsertFrom(in IDataColumn other, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex, IMemoryAllocator memoryAllocator)
        {
            if (other is BinaryColumn binaryColumn)
            {
                _data.InsertFrom(in binaryColumn._data, in sortedLookup, in insertPositions, lookupNullIndex, memoryAllocator);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void DeleteBatch(ReadOnlySpan<int> targets, IMemoryAllocator memoryAllocator)
        {
            _data.DeleteBatch(targets, memoryAllocator);
        }

        public ColumnSizeInfo GetColumnSizeInfo()
        {
            return new ColumnSizeInfo()
            {
                TotalRows = Count,
                DataType = ArrowTypeId.Binary,
                TotalVariableBytes = _data.DataSpan.Length,
            };
        }

        public CompareColumnState GetColumnState()
        {
            return CompareColumnStateBuilder.Create(ArrowTypeId.Binary);
        }

        unsafe void IDataColumn.SetSelfComparePointers(ref SelfComparePointers selfComparePointers)
        {
            selfComparePointers.dataPointer = _data.GetDataPointer_Unsafe();
            selfComparePointers.secondaryPointer = _data.GetOffsetPointer_Unsafe();
        }

        System.Linq.Expressions.Expression IDataColumn.CreateSelfCompareExpression(System.Linq.Expressions.Expression selfComparePointerExpression, System.Linq.Expressions.Expression xExpression, System.Linq.Expressions.Expression yExpression)
        {
            return NativeSortHelpers.CallCompareBinary(selfComparePointerExpression, xExpression, yExpression);
        }

        bool IDataColumn.SupportSelfCompareExpression => true;
    }
}





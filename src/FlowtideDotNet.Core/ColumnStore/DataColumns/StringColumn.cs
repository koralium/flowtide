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
using System.Buffers;
using System.Collections;
using System.IO.Hashing;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore
{
    /// <summary>
    /// String column that is also the MemoryManager over its data.
    /// </summary>
    public class StringColumn : MemoryManager<byte>, IDataColumn, IEnumerable<string>
    {
        // Not readonly, mutations would run on a copy.
        private BinaryList _binaryList;

        public int Count => _binaryList.Count;

        public override Span<byte> GetSpan()
        {
            return _binaryList.CapacitySpan;
        }

        public override unsafe MemoryHandle Pin(int elementIndex = 0)
        {
            return new MemoryHandle(((byte*)_binaryList.GetDataPointer_Unsafe()) + elementIndex, default, default);
        }

        public override void Unpin()
        {
        }

        public ArrowTypeId Type => ArrowTypeId.String;

        public StructHeader StructHeader => throw new NotImplementedException();

        public StringColumn(IMemoryAllocator memoryAllocator)
        {
            _binaryList = new BinaryList(memoryAllocator);
        }

        public StringColumn(IMemoryAllocator memoryAllocator, ColumnSizeInfo columnSizeInfo)
        {
            _binaryList = new BinaryList(memoryAllocator, columnSizeInfo.TotalRows, columnSizeInfo.TotalVariableBytes);
        }

        public StringColumn(FlowtideMemory offsetMemory, int offsetLength, FlowtideMemory dataMemory, IMemoryAllocator memoryAllocator)
        {
            _binaryList = new BinaryList(offsetMemory, offsetLength, dataMemory);
        }

        private StringColumn(BinaryList binaryList)
        {
            // The argument is a fresh copy that is never used again.
#pragma warning disable RS0042
            _binaryList = binaryList;
#pragma warning restore RS0042
        }

        public int Add<T>(in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            var index = _binaryList.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _binaryList.AddEmpty(memoryAllocator);
                return index;
            }
            _binaryList.Add(value.AsString.Span, memoryAllocator);
            return index;
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
            return SpanByteComparer.Instance.Compare(_binaryList.Get(index), value.AsString.Span);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is StringColumn stringColumn)
            {
                return SpanByteComparer.Instance.Compare(_binaryList.Get(thisIndex), stringColumn._binaryList.Get(otherIndex));
            }
            throw new NotImplementedException();
        }

        public IEnumerator<string> GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            var (offset, length) = _binaryList.GetOffsetAndLength(in index);
            return new StringValue(CreateMemory(offset, length));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            var (offset, length) = _binaryList.GetOffsetAndLength(in index);
            dataValueContainer._type = ArrowTypeId.String;
            dataValueContainer._stringValue = new StringValue(CreateMemory(offset, length));
        }

        public void InsertAt<T>(in int index, in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _binaryList.InsertEmpty(index, memoryAllocator);
                return;
            }
            _binaryList.Insert(index, value.AsString.Span, memoryAllocator);
        }

        public void RemoveAt(in int index, IMemoryAllocator memoryAllocator)
        {
            _binaryList.RemoveAt(index, memoryAllocator);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundries(_binaryList, dataValue.AsString.Span, start, end, SpanByteComparerDesc.Instance);
            }
            else
            {
                return BoundarySearch.SearchBoundries(_binaryList, dataValue.AsString.Span, start, end, SpanByteComparer.Instance);
            }

        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var offsetBuffer = new ArrowBuffer(_binaryList.OffsetMemory);
            var dataBuffer = new ArrowBuffer(CreateMemory(0, _binaryList.DataSpan.Length));
            return (new StringArray(Count, offsetBuffer, dataBuffer, nullBuffer, nullCount), StringType.Default);
        }

        public int Update<T>(in int index, in T value, IMemoryAllocator memoryAllocator) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _binaryList.UpdateAt(index, Span<byte>.Empty, memoryAllocator);
                return index;
            }
            _binaryList.UpdateAt(index, value.AsString.Span, memoryAllocator);
            return index;
        }

        private IEnumerable<string> GetEnumerable()
        {
            for (int i = 0; i < _binaryList.Count; i++)
            {
                yield return Encoding.UTF8.GetString(_binaryList.Get(i));
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        public void Dispose(IMemoryAllocator memoryAllocator)
        {
            _binaryList.Dispose(memoryAllocator);
        }

        protected override void Dispose(bool disposing)
        {
            // Freeing needs the allocator, the owning column calls Dispose(IMemoryAllocator).
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.String;
        }

        public void Clear(IMemoryAllocator memoryAllocator)
        {
            _binaryList.Clear(memoryAllocator);
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
            _binaryList.RemoveRange(start, count, memoryAllocator);
        }

        public int GetByteSize(int start, int end)
        {
            return _binaryList.GetByteSize(start, end);
        }

        public int GetByteSize()
        {
            return _binaryList.GetByteSize(0, Count - 1);
        }

        public void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
        {
            _binaryList.GetPrefixSumByteSizes(indices, sizes);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, in BitmapList validityList, IMemoryAllocator memoryAllocator)
        {
            if (other is StringColumn stringColumn)
            {
                _binaryList.InsertRangeFrom(index, stringColumn._binaryList, start, count, memoryAllocator);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void InsertNullRange(int index, int count, IMemoryAllocator memoryAllocator)
        {
            _binaryList.InsertNullRange(index, count, memoryAllocator);
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteStringValue(_binaryList.Get(index));
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new StringColumn(_binaryList.Copy(memoryAllocator));
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            hashAlgorithm.Append(_binaryList.Get(in index));
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var typePointer = arrowSerializer.AddUtf8Type();
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Utf8, typePointer);
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
            arrowSerializer.AddBufferForward(_binaryList.OffsetSpan.Length);
            arrowSerializer.AddBufferForward(_binaryList.DataSpan.Length);

        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            dataWriter.WriteArrowBuffer(_binaryList.OffsetSpan);
            dataWriter.WriteArrowBuffer(_binaryList.DataSpan);
        }

        public void InsertFrom(in IDataColumn other, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex, IMemoryAllocator memoryAllocator)
        {
            if (other is StringColumn stringColumn)
            {
                _binaryList.InsertFrom(in stringColumn._binaryList, in sortedLookup, in insertPositions, lookupNullIndex, memoryAllocator);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void DeleteBatch(ReadOnlySpan<int> targets, IMemoryAllocator memoryAllocator)
        {
            _binaryList.DeleteBatch(targets, memoryAllocator);
        }

        public ColumnSizeInfo GetColumnSizeInfo()
        {
            return new ColumnSizeInfo()
            {
                DataType = ArrowTypeId.String,
                TotalRows = Count,
                TotalVariableBytes = _binaryList.DataSpan.Length
            };
        }

        unsafe void IDataColumn.SetSelfComparePointers(ref SelfComparePointers selfComparePointers)
        {
            selfComparePointers.dataPointer = _binaryList.GetDataPointer_Unsafe();
            selfComparePointers.secondaryPointer = _binaryList.GetOffsetPointer_Unsafe();
        }

        System.Linq.Expressions.Expression IDataColumn.CreateSelfCompareExpression(System.Linq.Expressions.Expression selfComparePointerExpression, System.Linq.Expressions.Expression xExpression, System.Linq.Expressions.Expression yExpression)
        {
            return NativeSortHelpers.CallCompareBinary(selfComparePointerExpression, xExpression, yExpression);
        }

        bool IDataColumn.SupportSelfCompareExpression => true;

        public CompareColumnState GetColumnState()
        {
            return CompareColumnStateBuilder.Create(ArrowTypeId.String);
        }
    }
}

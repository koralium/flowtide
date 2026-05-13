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
    public class StringColumn : IDataColumn, IEnumerable<string>
    {
        private BinaryViewList _binaryList;
        private bool disposedValue;

        public int Count => _binaryList.Count;

        public ArrowTypeId Type => ArrowTypeId.String;

        public StructHeader StructHeader => throw new NotImplementedException();

        public StringColumn(IMemoryAllocator memoryAllocator)
        {
            _binaryList = new BinaryViewList(memoryAllocator);
        }

        public StringColumn(IMemoryAllocator memoryAllocator, ColumnSizeInfo columnSizeInfo)
        {
            _binaryList = new BinaryViewList(memoryAllocator, columnSizeInfo.TotalRows, columnSizeInfo.TotalVariableBytes);
        }

        public StringColumn(
            IMemoryOwner<byte> viewMemory, 
            int viewCount, 
            IMemoryOwner<byte>? dataMemory, 
            IMemoryAllocator memoryAllocator,
            int deletedSize,
            int insertPointer = -1)
        {
            _binaryList = new BinaryViewList(viewMemory, viewCount, dataMemory, memoryAllocator, deletedSize, insertPointer);
        }

        public StringColumn(IMemoryOwner<byte> offsetMemory, int offsetLength, IMemoryOwner<byte>? dataMemory, IMemoryAllocator memoryAllocator, bool isLegacyUtf8)
        {
            _binaryList = new BinaryViewList(memoryAllocator, offsetLength - 1, dataMemory?.Memory.Length ?? 0);
            var offsets = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(offsetMemory.Memory.Span);
            var dataSpan = dataMemory != null ? dataMemory.Memory.Span : default;
            for (int i = 0; i < offsetLength - 1; i++)
            {
                int start = offsets[i];
                int end = offsets[i + 1];
                _binaryList.Add(dataSpan.Slice(start, end - start));
            }
            offsetMemory.Dispose();
            if (dataMemory != null) dataMemory.Dispose();
        }

        private StringColumn(BinaryViewList binaryList)
        {
            _binaryList = binaryList;
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _binaryList.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _binaryList.AddEmpty();
                return index;
            }
            _binaryList.Add(value.AsString.Span);
            return index;
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
            return new StringValue(_binaryList.GetMemory(in index));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.String;
            dataValueContainer._stringValue = new StringValue(_binaryList.GetMemory(in index));
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _binaryList.InsertEmpty(index);
                return;
            }
            _binaryList.Insert(index, value.AsString.Span);
        }

        public void RemoveAt(in int index)
        {
            _binaryList.RemoveAt(index);
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
            var viewsBuffer = new ArrowBuffer(_binaryList.ViewsMemory);
            var dataBuffer = new ArrowBuffer(_binaryList.DataMemory);
            var array = new Apache.Arrow.StringViewArray(Count, viewsBuffer, dataBuffer, nullBuffer, nullCount, 0);
            return (array, Apache.Arrow.Types.StringViewType.Default);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _binaryList.UpdateAt(index, Span<byte>.Empty);
                return index;
            }
            _binaryList.UpdateAt(index, value.AsString.Span);
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

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _binaryList.Dispose();
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
            return ArrowTypeId.String;
        }

        public void Clear()
        {
            _binaryList.Clear();
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
            _binaryList.RemoveRange(start, count);
        }

        public int GetByteSize(int start, int end)
        {
            return _binaryList.GetByteSize(start, end);
        }

        public int GetByteSize()
        {
            return _binaryList.GetByteSize();
        }

        public void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
        {
            _binaryList.GetPrefixSumByteSizes(indices, sizes);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (other is StringColumn stringColumn)
            {
                _binaryList.InsertRangeFrom(index, stringColumn._binaryList, start, count);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void InsertNullRange(int index, int count)
        {
            _binaryList.InsertNullRange(index, count);
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
            hashAlgorithm.Append(_binaryList.GetMemory(in index).Span);
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var insertPointerKeyPointer = arrowSerializer.CreateStringUtf8("insertPointer"u8);

            Span<byte> buffer = stackalloc byte[64];
            int written = Encoding.UTF8.GetBytes(_binaryList.InsertPointer.ToString(), buffer);
            ReadOnlySpan<byte> readOnlyBuffer = buffer.Slice(0, written);
            var insertPointerValuePointer = arrowSerializer.CreateStringUtf8(readOnlyBuffer);
            pointerStack[0] = arrowSerializer.CreateKeyValue(insertPointerKeyPointer, insertPointerValuePointer);

            var deletedSizeKeyPointer = arrowSerializer.CreateStringUtf8("deletedSize"u8);
            written = Encoding.UTF8.GetBytes(_binaryList.DeletedDataSize.ToString(), buffer);
            readOnlyBuffer = buffer.Slice(0, written);
            var deletedSizeValuePointer = arrowSerializer.CreateStringUtf8(readOnlyBuffer);
            pointerStack[1] = arrowSerializer.CreateKeyValue(deletedSizeKeyPointer, deletedSizeValuePointer);

            var customMetadataPointer = arrowSerializer.CreateCustomMetadataVector(pointerStack.Slice(0, 2));
            var typePointer = arrowSerializer.AddUtf8ViewType();
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Utf8View, typePointer, custom_metadataOffset: customMetadataPointer);
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            return new SerializationEstimation(1, 2, _binaryList.ViewsMemory.Length + _binaryList.DataMemory.Length);
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            arrowSerializer.CreateFieldNode(Count, nullCount);
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            arrowSerializer.AddBufferForward(_binaryList.ViewsMemory.Length);
            arrowSerializer.AddBufferForward(_binaryList.DataMemory.Length);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            dataWriter.WriteArrowBuffer(_binaryList.ViewsMemory.Span);
            dataWriter.WriteArrowBuffer(_binaryList.DataMemory.Span);
        }

        public void InsertFrom(in IDataColumn other, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex)
        {
            if (other is StringColumn stringColumn)
            {
                _binaryList.InsertFrom(in stringColumn._binaryList, in sortedLookup, in insertPositions, lookupNullIndex);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void DeleteBatch(ReadOnlySpan<int> targets)
        {
            _binaryList.DeleteBatch(targets);
        }

        public ColumnSizeInfo GetColumnSizeInfo()
        {
            return new ColumnSizeInfo()
            {
                DataType = ArrowTypeId.String,
                TotalRows = Count,
                TotalVariableBytes = 0
            };
        }

        unsafe void IDataColumn.SetSelfComparePointers(ref SelfComparePointers selfComparePointers)
        {
            selfComparePointers.secondaryPointer = _binaryList.GetViewsPointer_Unsafe();
            selfComparePointers.dataPointer = _binaryList.GetDataPointer_Unsafe();
        }

        System.Linq.Expressions.Expression IDataColumn.CreateSelfCompareExpression(System.Linq.Expressions.Expression selfComparePointerExpression, System.Linq.Expressions.Expression xExpression, System.Linq.Expressions.Expression yExpression)
        {
            return NativeSortHelpers.CallCompareBinaryView(selfComparePointerExpression, xExpression, yExpression);
        }

        bool IDataColumn.SupportSelfCompareExpression => true;

        public CompareColumnState GetColumnState()
        {
            return CompareColumnStateBuilder.Create(ArrowTypeId.String);
        }
    }
}

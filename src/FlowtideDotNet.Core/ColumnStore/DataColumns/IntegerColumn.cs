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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    internal class IntegerColumn : IDataColumn
    {
        private interface IIntData : IDisposable
        {
            long MaxSize { get; }
            long MinSize { get; }

            int BitWidth { get; }

            int Count { get; }

            int Add(in long value);

            int Update(in int index, in long value);

            void InsertAt(in int index, in long value);

            long Get(in int index);

            void RemoveAt(in int index);

            void Clear();

            void RemoveRange(int start, int count);

            void InsertNullRange(int index, int count);

            (int, int) SearchBoundries(in long dataValue, in int start, in int end, in ReferenceSegment? child, bool desc);

            int GetByteSize(int start, int end);

            Memory<byte> SlicedMemory { get; }

            IIntData Copy(IMemoryAllocator memoryAllocator);

            void MoveRangeAt(int index, int count);

            void InsertRangeFrom(int index, IIntData other, int start, int count);

            (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount);
        }

        private sealed class Int8Data : IIntData
        {
            private PrimitiveList<sbyte> _list;

            public Int8Data(IMemoryAllocator memoryAllocator)
            {
                _list = new PrimitiveList<sbyte>(memoryAllocator);
            }

            public Int8Data(PrimitiveList<sbyte> list)
            {
                _list = list;
            }

            public long MaxSize => sbyte.MaxValue;

            public long MinSize => sbyte.MinValue;

            public int Count => _list.Count;

            public int BitWidth => 8;

            public Memory<byte> SlicedMemory => _list.SlicedMemory;

            public int Add(in long value)
            {
                var index = _list.Count;
                _list.Add((sbyte)value);
                return index;
            }

            public void Clear()
            {
                _list.Clear();
            }

            public IIntData Copy(IMemoryAllocator memoryAllocator)
            {
                return new Int8Data(_list.Copy(memoryAllocator));
            }

            public void Dispose()
            {
                _list.Dispose();
            }

            public long Get(in int index)
            {
                return _list[index];
            }

            public int GetByteSize(int start, int end)
            {
                return (end - start + 1);
            }

            public void InsertAt(in int index, in long value)
            {
                _list.InsertAt(index, (sbyte)value);
            }

            public void InsertNullRange(int index, int count)
            {
                _list.InsertStaticRange(index, 0, count);
            }

            public void InsertRangeFrom(int index, IIntData other, int start, int count)
            {
                if (other is Int8Data int8data)
                {
                    _list.InsertRangeFrom(index, int8data._list, start, count);
                    return;
                }
                throw new NotImplementedException();
            }

            public void MoveRangeAt(int index, int count)
            {
                _list.MoveAtIndex(index, count);
            }

            public void RemoveAt(in int index)
            {
                _list.RemoveAt(index);
            }

            public void RemoveRange(int start, int count)
            {
                _list.RemoveRange(start, count);
            }

            public (int, int) SearchBoundries(in long dataValue, in int start, in int end, in ReferenceSegment? child, bool desc)
            {
                if (desc)
                {
                    if (dataValue < sbyte.MinValue)
                    {
                        var index = ~(end + 1);
                        return (index, index);
                    }
                    if (dataValue > sbyte.MaxValue)
                    {
                        return (~start, ~start);
                    }
                    return BoundarySearch.SearchBoundries(_list, (sbyte)dataValue, start, end, Int8ComparerDesc.Instance);
                }
                else
                {
                    if (dataValue < sbyte.MinValue)
                    {
                        return (~start, ~start);
                    }
                    if (dataValue > sbyte.MaxValue)
                    {
                        var index = ~(end + 1);
                        return (index, index);
                    }
                    return BoundarySearch.SearchBoundries(_list, (sbyte)dataValue, start, end, Int8Comparer.Instance);
                }
            }

            public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
            {
                var valueBuffer = new ArrowBuffer(_list.SlicedMemory);
                return (new Int8Array(valueBuffer, nullBuffer, _list.Count, nullCount, 0), Int8Type.Default);
            }

            public int Update(in int index, in long value)
            {
                _list[index] = (sbyte)value;
                return index;
            }
        }

        private sealed class Int16Data : IIntData
        {
            private PrimitiveList<short> _list;

            public Int16Data(IMemoryAllocator memoryAllocator)
            {
                _list = new PrimitiveList<short>(memoryAllocator);
            }

            public Int16Data(PrimitiveList<short> list)
            {
                _list = list;
            }

            public long MaxSize => short.MaxValue;

            public long MinSize => short.MinValue;

            public int Count => _list.Count;

            public int BitWidth => 16;

            public Memory<byte> SlicedMemory => _list.SlicedMemory;

            public int Add(in long value)
            {
                var index = _list.Count;
                _list.Add((short)value);
                return index;
            }

            public void InsertAt(in int index, in long value)
            {
                _list.InsertAt(index, (short)value);
            }

            public int Update(in int index, in long value)
            {
                _list[index] = (short)value;
                return index;
            }

            public long Get(in int index)
            {
                return _list[index];
            }

            public void Dispose()
            {
                _list.Dispose();
            }

            public void RemoveAt(in int index)
            {
                _list.RemoveAt(index);
            }

            public void Clear()
            {
                _list.Clear();
            }

            public void RemoveRange(int start, int count)
            {
                _list.RemoveRange(start, count);
            }

            public void InsertNullRange(int index, int count)
            {
                _list.InsertStaticRange(index, 0, count);
            }

            public (int, int) SearchBoundries(in long dataValue, in int start, in int end, in ReferenceSegment? child, bool desc)
            {
                if (desc)
                {
                    if (dataValue < short.MinValue)
                    {
                        var index = ~(end + 1);
                        return (index, index);
                    }
                    if (dataValue > short.MaxValue)
                    {
                        return (~start, ~start);
                    }
                    return BoundarySearch.SearchBoundries(_list, (short)dataValue, start, end, Int16ComparerDesc.Instance);
                }
                else
                {
                    if (dataValue < short.MinValue)
                    {
                        return (~start, ~start);
                    }
                    if (dataValue > short.MaxValue)
                    {
                        var index = ~(end + 1);
                        return (index, index);
                    }
                    return BoundarySearch.SearchBoundries(_list, (short)dataValue, start, end, Int16Comparer.Instance);
                }
            }

            public int GetByteSize(int start, int end)
            {
                return (end - start + 1) * sizeof(short);
            }

            public IIntData Copy(IMemoryAllocator memoryAllocator)
            {
                return new Int16Data(_list.Copy(memoryAllocator));
            }

            public void MoveRangeAt(int index, int count)
            {
                _list.MoveAtIndex(index, count);
            }

            public void InsertRangeFrom(int index, IIntData other, int start, int count)
            {
                if (other is Int16Data int16Data)
                {
                    _list.InsertRangeFrom(index, int16Data._list, start, count);
                    return;
                }
                throw new NotImplementedException();
            }

            public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
            {
                var valueBuffer = new ArrowBuffer(_list.SlicedMemory);
                return (new Int16Array(valueBuffer, nullBuffer, _list.Count, nullCount, 0), Int16Type.Default);
            }
        }

        private sealed class Int32Data : IIntData
        {
            private PrimitiveList<int> _list;


            public Int32Data(IMemoryAllocator memoryAllocator)
            {
                _list = new PrimitiveList<int>(memoryAllocator);
            }

            public Int32Data(PrimitiveList<int> list)
            {
                _list = list;
            }

            public long MaxSize => int.MaxValue;

            public long MinSize => int.MinValue;

            public int Count => _list.Count;

            public int BitWidth => 32;

            public Memory<byte> SlicedMemory => _list.SlicedMemory;

            public int Add(in long value)
            {
                var index = _list.Count;
                _list.Add((int)value);
                return index;
            }

            public void InsertAt(in int index, in long value)
            {
                _list.InsertAt(index, (int)value);
            }

            public int Update(in int index, in long value)
            {
                _list[index] = (int)value;
                return index;
            }

            public long Get(in int index)
            {
                return _list[index];
            }

            public void Dispose()
            {
                _list.Dispose();
            }

            public void RemoveAt(in int index)
            {
                _list.RemoveAt(index);
            }

            public void Clear()
            {
                _list.Clear();
            }

            public void RemoveRange(int start, int count)
            {
                _list.RemoveRange(start, count);
            }

            public void InsertNullRange(int index, int count)
            {
                _list.InsertStaticRange(index, 0, count);
            }

            public (int, int) SearchBoundries(in long dataValue, in int start, in int end, in ReferenceSegment? child, bool desc)
            {
                if (desc)
                {
                    if (dataValue < int.MinValue)
                    {
                        var index = ~(end + 1);
                        return (index, index);
                    }
                    if (dataValue > int.MaxValue)
                    {
                        return (~start, ~start);
                    }
                    return BoundarySearch.SearchBoundries(_list, (int)dataValue, start, end, Int32ComparerDesc.Instance);
                }
                else
                {
                    if (dataValue < int.MinValue)
                    {
                        return (~start, ~start);
                    }
                    if (dataValue > int.MaxValue)
                    {
                        var index = ~(end + 1);
                        return (index, index);
                    }
                    return BoundarySearch.SearchBoundries(_list, (int)dataValue, start, end, Int32Comparer.Instance);
                }
            }

            public int GetByteSize(int start, int end)
            {
                return (end - start + 1) * sizeof(int);
            }

            public IIntData Copy(IMemoryAllocator memoryAllocator)
            {
                return new Int32Data(_list.Copy(memoryAllocator));
            }

            public void MoveRangeAt(int index, int count)
            {
                _list.MoveAtIndex(index, count);
            }

            public void InsertRangeFrom(int index, IIntData other, int start, int count)
            {
                if (other is Int32Data int32Data)
                {
                    _list.InsertRangeFrom(index, int32Data._list, start, count);
                    return;
                }
                throw new NotImplementedException();
            }

            public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
            {
                var valueBuffer = new ArrowBuffer(_list.SlicedMemory);
                return (new Int32Array(valueBuffer, nullBuffer, _list.Count, nullCount, 0), Int32Type.Default);
            }
        }

        private sealed class Int64Data : IIntData
        {
            private PrimitiveList<long> _list;

            public Int64Data(IMemoryAllocator memoryAllocator)
            {
                _list = new PrimitiveList<long>(memoryAllocator);
            }

            public Int64Data(PrimitiveList<long> list)
            {
                _list = list;
            }

            public long MaxSize => long.MaxValue;

            public long MinSize => long.MinValue;

            public int Count => _list.Count;

            public int BitWidth => 64;

            public Memory<byte> SlicedMemory => _list.SlicedMemory;

            public int Add(in long value)
            {
                var index = _list.Count;
                _list.Add(value);
                return index;
            }

            public void InsertAt(in int index, in long value)
            {
                _list.InsertAt(index, value);
            }

            public int Update(in int index, in long value)
            {
                _list[index] = value;
                return index;
            }

            public long Get(in int index)
            {
                return _list[index];
            }

            public void Dispose()
            {
                _list.Dispose();
            }

            public void RemoveAt(in int index)
            {
                _list.RemoveAt(index);
            }

            public void Clear()
            {
                _list.Clear();
            }

            public void RemoveRange(int start, int count)
            {
                _list.RemoveRange(start, count);
            }

            public void InsertNullRange(int index, int count)
            {
                _list.InsertStaticRange(index, 0, count);
            }

            public (int, int) SearchBoundries(in long dataValue, in int start, in int end, in ReferenceSegment? child, bool desc)
            {
                if (desc)
                {
                    if (dataValue < long.MinValue)
                    {
                        var index = ~(end + 1);
                        return (index, index);
                    }
                    if (dataValue > long.MaxValue)
                    {
                        return (~start, ~start);
                    }
                    return BoundarySearch.SearchBoundries(_list, dataValue, start, end, Int64ComparerDesc.Instance);
                }
                else
                {
                    if (dataValue < long.MinValue)
                    {
                        return (~start, ~start);
                    }
                    if (dataValue > long.MaxValue)
                    {
                        var index = ~(end + 1);
                        return (index, index);
                    }
                    return BoundarySearch.SearchBoundries(_list, dataValue, start, end, Int64Comparer.Instance);
                }
            }

            public int GetByteSize(int start, int end)
            {
                return (end - start + 1) * sizeof(long);
            }

            public IIntData Copy(IMemoryAllocator memoryAllocator)
            {
                return new Int64Data(_list.Copy(memoryAllocator));
            }

            public void MoveRangeAt(int index, int count)
            {
                _list.MoveAtIndex(index, count);
            }

            public void InsertRangeFrom(int index, IIntData other, int start, int count)
            {
                if (other is Int64Data int64Data)
                {
                    _list.InsertRangeFrom(index, int64Data._list, start, count);
                    return;
                }
                throw new NotImplementedException();
            }

            public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
            {
                var valueBuffer = new ArrowBuffer(_list.SlicedMemory);
                return (new Int64Array(valueBuffer, nullBuffer, _list.Count, nullCount, 0), Int64Type.Default);
            }
        }

        private IIntData? _data;
        private readonly IMemoryAllocator _memoryAllocator;

        public IntegerColumn(IMemoryAllocator memoryAllocator)
        {
            this._memoryAllocator = memoryAllocator;
        }

        public IntegerColumn(IMemoryAllocator memoryAllocator, IMemoryOwner<byte> memory, int length, int bitWidth)
        {
            this._memoryAllocator = memoryAllocator;

            switch (bitWidth)
            {
                case 8:
                    _data = new Int8Data(new PrimitiveList<sbyte>(memory, length, memoryAllocator));
                    break;
                case 16:
                    _data = new Int16Data(new PrimitiveList<short>(memory, length, memoryAllocator));
                    break;
                case 32:
                    _data = new Int32Data(new PrimitiveList<int>(memory, length, memoryAllocator));
                    break;
                case 64:
                    _data = new Int64Data(new PrimitiveList<long>(memory, length, memoryAllocator));
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        public int Count => _data?.Count ?? 0;

        public ArrowTypeId Type => ArrowTypeId.Int64;

        [MemberNotNull(nameof(_data))]
        private void IncreaseSize(in long value)
        {
            IIntData? newData;
            if (value < sbyte.MaxValue && value > sbyte.MinValue)
            {
                newData = new Int8Data(_memoryAllocator);
            }
            else if (value < short.MaxValue && value > short.MinValue)
            {
                newData = new Int16Data(_memoryAllocator);
            }
            else if (value < int.MaxValue && value > int.MinValue)
            {
                newData = new Int32Data(_memoryAllocator);
            }
            else
            {
                newData = new Int64Data(_memoryAllocator);
            }

            if (_data != null)
            {
                for (int i = 0; i < _data.Count; i++)
                {
                    newData.Add(_data.Get(i));
                }
                _data.Dispose();
            }
            _data = newData;
        }

        [MemberNotNull(nameof(_data))]
        private void CheckSize(in long value)
        {
            if (_data == null)
            {
                IncreaseSize(in value);
            }
            else
            {
                if (value < _data.MinSize || value > _data.MaxSize)
                {
                    IncreaseSize(in value);
                }
            }
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                CheckSize(0);
                return _data.Add(0);
            }

            var val = value.AsLong;

            CheckSize(in val);

            return _data.Add(in val);
        }

        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            if (_data != null)
            {
                _data.Clear();
            }
        }

        public int CompareTo<T>(in int index, in T value, in ReferenceSegment? child, in BitmapList? validityList) where T : IDataValue
        {
            Debug.Assert(_data != null);
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
            var longValue = value.AsLong;
            return _data.Get(index).CompareTo(longValue);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            Debug.Assert(_data != null);

            if (otherColumn is IntegerColumn integerColumn)
            {
                Debug.Assert(integerColumn._data != null);
                return _data.Get(thisIndex).CompareTo(integerColumn._data.Get(otherIndex));
            }
            throw new NotImplementedException();
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            if (_data == null)
            {
                return new IntegerColumn(memoryAllocator);
            }
            return new IntegerColumn(memoryAllocator) { _data = _data.Copy(memoryAllocator) };
        }

        public void Dispose()
        {
            if (_data != null)
            {
                _data.Dispose();
                _data = null;
            }
        }

        public int EndNewList()
        {
            throw new NotImplementedException();
        }

        public int GetByteSize(int start, int end)
        {
            return _data?.GetByteSize(start, end) ?? 0;
        }

        public int GetByteSize()
        {
            return _data?.GetByteSize(0, Count - 1) ?? 0;
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
            Debug.Assert(_data != null);
            return new Int64Value(_data.Get(index));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            Debug.Assert(_data != null);

            dataValueContainer._type = ArrowTypeId.Int64;
            dataValueContainer._int64Value = new Int64Value(_data.Get(index));
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                CheckSize(0);
                _data.InsertAt(index, 0);
                return;
            }

            var val = value.AsLong;

            CheckSize(val);
            _data.InsertAt(index, val);
        }

        public void InsertNullRange(int index, int count)
        {
            CheckSize(0);
            _data.InsertNullRange(index, count);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (other is IntegerColumn integerColumn)
            {
                // Check if we need to resize, this also creates an int8 data if it is null
                CheckSize(0);
                Debug.Assert(integerColumn._data != null);
                if (_data.BitWidth == integerColumn._data.BitWidth)
                {
                    _data.InsertRangeFrom(index, integerColumn._data, start, count);
                    return;
                }
                else
                {
                    // Create space for the new values, this increases the count as well
                    _data.MoveRangeAt(index, count);

                    // Missmatch in bitwidth insert one by one to check if there is any size change
                    for (int i = 0; i < count; i++)
                    {
                        var val = integerColumn._data.Get(start + i);
                        // Check if we need to resize
                        CheckSize(val);
                        // Update the value
                        _data.Update(index + i, val);
                    }
                }
                return;
            }
            throw new NotImplementedException();
        }

        public void RemoveAt(in int index)
        {
            Debug.Assert(_data != null);
            _data.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            Debug.Assert(_data != null);
            _data.RemoveRange(start, count);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            if (_data == null)
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
            return _data.SearchBoundries(dataValue.AsLong, start, end, child, desc);
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            if (_data == null)
            {
                return (new Int8Array(new ArrowBuffer(), nullBuffer, 0, nullCount, 0), Int8Type.Default);
            }
            return _data.ToArrowArray(nullBuffer, nullCount);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            if (value.IsNull)
            {
                CheckSize(0);
                return _data.Update(index, 0);
            }

            var val = value.AsLong;

            CheckSize(val);

            return _data.Update(index, val);
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            Debug.Assert(_data != null);
            writer.WriteNumberValue(_data.Get(index));
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            var memoryLength = _data?.SlicedMemory.Length ?? 0;
            arrowSerializer.AddBufferForward(memoryLength);
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            arrowSerializer.CreateFieldNode(Count, nullCount);
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            int bitWidth = 8;

            if (_data != null)
            {
                bitWidth = _data.BitWidth;
            }

            var typePointer = arrowSerializer.AddIntType(bitWidth);
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Int, typePointer);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            if (_data != null)
            {
                dataWriter.WriteArrowBuffer(_data.SlicedMemory.Span);
            }
            else
            {
                dataWriter.WriteArrowBuffer(Span<byte>.Empty);
            }
        }
    }
}

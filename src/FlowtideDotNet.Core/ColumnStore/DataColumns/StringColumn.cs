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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class StringColumn : IDataColumn, IEnumerable<string>
    {
        private BinaryList _binaryList;
        private bool disposedValue;

        public int Count => _binaryList.Count;

        public ArrowTypeId Type => ArrowTypeId.String;

        public StringColumn(IMemoryAllocator memoryAllocator)
        {
            _binaryList = new BinaryList(memoryAllocator);
        }

        public StringColumn(IMemoryOwner<byte> offsetMemory, int offsetLength, IMemoryOwner<byte> dataMemory, IMemoryAllocator memoryAllocator)
        {
            _binaryList = new BinaryList(offsetMemory, offsetLength, dataMemory, memoryAllocator);
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
            var offsetBuffer = new ArrowBuffer(_binaryList.OffsetMemory);
            var dataBuffer = new ArrowBuffer(_binaryList.DataMemory);
            return (new StringArray(Count, offsetBuffer, dataBuffer, nullBuffer, nullCount), StringType.Default);
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
            return _binaryList.GetByteSize(0, Count - 1);
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
    }
}

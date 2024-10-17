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
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class Int64Column : IDataColumn, IEnumerable<long>
    {
        //private List<long> _data;
        private NativeLongList? _data;
        private bool disposedValue;

        public int Count => _data!.Count;

        public ArrowTypeId Type => ArrowTypeId.Int64;

        public Int64Column()
        {
            
        }

        public void Assign(IMemoryAllocator memoryAllocator)
        {
            _data = NativeLongListFactory.Get(memoryAllocator);
            disposedValue = false;
        }

        public void Assign(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            _data = NativeLongListFactory.Get(memory, length, memoryAllocator);
            disposedValue = false;
        }

        public Int64Column(IMemoryAllocator memoryAllocator)
        {
            _data = NativeLongListFactory.Get(memoryAllocator);
        }

        public Int64Column(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            _data = NativeLongListFactory.Get(memory, length, memoryAllocator);
        }

        public int Add<T>(in T value) where T: IDataValue
        {
            Debug.Assert(_data != null);
            var index = _data.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _data.Add(0);
                return index;
            }
            _data.Add(value.AsLong);
            return index;
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            Debug.Assert(_data != null);
            
            if (otherColumn is Int64Column int64Column)
            {
                Debug.Assert(int64Column._data != null);
                return _data[thisIndex].CompareTo(int64Column._data[otherIndex]);
            }
            throw new NotImplementedException();
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
            return _data.GetRef(index).CompareTo(longValue);
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            Debug.Assert(_data != null);
            return new Int64Value(_data.GetRef(index));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            Debug.Assert(_data != null);
            dataValueContainer._type = ArrowTypeId.Int64;
            dataValueContainer._int64Value = new Int64Value(_data.GetRef(index));
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc)
            where T: IDataValue
        {
            Debug.Assert(_data != null);
            var val = dataValue.AsLong;
            if (desc)
            {
                return BoundarySearch.SearchBoundriesInt64Desc(_data, start, end, val);
            }
            return BoundarySearch.SearchBoundriesInt64Asc(_data, start, end, val);
        }

        public int Update(in int index, in IDataValue value)
        {
            Debug.Assert(_data != null);
            _data[index] = value.AsLong;
            return index;
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            Debug.Assert(_data != null);
            if (value.Type == ArrowTypeId.Null)
            {
                _data[index] = 0;
                return index;
            }
            _data[index] = value.AsLong;
            return index;
        }

        public void RemoveAt(in int index)
        {
            Debug.Assert(_data != null);
            _data.RemoveAt(index);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            Debug.Assert(_data != null);
            if (value.Type == ArrowTypeId.Null)
            {
                _data.InsertAt(index, 0);
            }
            else
            {
                _data.InsertAt(index, value.AsLong);
            }
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            Debug.Assert(_data != null);
            var valueBuffer = new ArrowBuffer(_data.Memory);
            return (new Int64Array(valueBuffer, nullBuffer, _data.Count, nullCount, 0), Int64Type.Default);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                disposedValue = true;
                if (disposing)
                {
                    if (_data != null)
                    {
                        _data.Dispose();
                        _data = null;
                    }
                    Int64ColumnFactory.Return(this);
                }
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
            return ArrowTypeId.Int64;
        }

        public void Clear()
        {
            Debug.Assert(_data != null);
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

        private IEnumerable<long> GetEnumerable()
        {
            Debug.Assert(_data != null);

            for (int i = 0; i < _data.Count; i++)
            {
                yield return _data.Get(i);
            }
        }

        public IEnumerator<long> GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        public void RemoveRange(int start, int count)
        {
            Debug.Assert(_data != null);
            _data.RemoveRange(start, count);
        }

        public int GetByteSize(int start, int end)
        {
            return (end - start + 1) * sizeof(long);
        }

        public int GetByteSize()
        {
            return Count * sizeof(long);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            Debug.Assert(_data != null);
            if (other is Int64Column int64Column)
            {
                Debug.Assert(int64Column._data != null);
                _data.InsertRangeFrom(index, int64Column._data, start, count);
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}

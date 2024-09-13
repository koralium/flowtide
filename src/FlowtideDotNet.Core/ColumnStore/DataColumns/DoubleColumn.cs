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
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class DoubleColumn : IDataColumn
    {
        private readonly PrimitiveList<double> _data;
        private bool disposedValue;

        public int Count => _data.Count;

        public ArrowTypeId Type => ArrowTypeId.Double;

        public DoubleColumn(IMemoryAllocator memoryAllocator)
        {
            _data = new PrimitiveList<double>(memoryAllocator);
        }

        public DoubleColumn(IMemoryOwner<byte> memory, int count, IMemoryAllocator memoryAllocator)
        {
            _data = new PrimitiveList<double>(memory, count, memoryAllocator);
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _data.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _data.Add(0);
                return index;
            }
            else
            {
                _data.Add(value.AsDouble);
                return index;
            }
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            Debug.Assert(_data != null);

            if (otherColumn is DoubleColumn doubleColumn)
            {
                Debug.Assert(doubleColumn._data != null);
                return _data.Get(thisIndex).CompareTo(doubleColumn._data.Get(otherIndex));
            }
            throw new NotImplementedException();
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
                return BoundarySearch.SearchBoundries<double>(_data, val, start, end, DoubleComparerDesc.Instance);
            }
            return BoundarySearch.SearchBoundries<double>(_data, val, start, end, DoubleComparer.Instance);
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
            _data.RemoveAt(index);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _data.InsertAt(index, default);
            }
            else
            {
                _data.InsertAt(index, value.AsDouble);
            }
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var dataBuffer = new ArrowBuffer(_data.Memory);
            var array = new DoubleArray(dataBuffer, nullBuffer, Count, nullCount, 0);
            return (array, new DoubleType());
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _data.Dispose();
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
    }
}

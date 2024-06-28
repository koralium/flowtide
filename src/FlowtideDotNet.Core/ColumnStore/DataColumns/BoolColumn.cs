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
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class BoolColumn : IDataColumn
    {
        private readonly BitmapList _data;
        private bool disposedValue;

        public BoolColumn(IMemoryAllocator memoryAllocator)
        {
            _data = BitmapListFactory.Get(memoryAllocator);
        }

        public BoolColumn(IMemoryOwner<byte> memory, int count, IMemoryAllocator memoryAllocator)
        {
            _data = BitmapListFactory.Get(memory, count, memoryAllocator);
        }

        public int Count => _data.Count;

        public ArrowTypeId Type => ArrowTypeId.Boolean;

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _data.Count;

            if (value.Type == ArrowTypeId.Null)
            {
                _data.Add(false);
                return index;
            }

            if (value.AsBool)
            {
                _data.Add(true);
            }
            else
            {
                _data.Add(false);
            }
            return index;
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
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
            return _data.Get(index).CompareTo(value.AsBool);
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new BoolValue(_data.Get(index));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.Boolean;
            dataValueContainer._boolValue = new BoolValue(_data.Get(index));
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child)
            where T : IDataValue
        {
            var val = dataValue.AsBool;
            return BoundarySearch.SearchBoundries<bool>(_data, val, start, end, BoolComparer.Instance);
        }

        public int Update(in int index, in IDataValue value)
        {
            if (value.AsBool)
            {
                _data.Set(index);
            }
            else
            {
                _data.Unset(index);
            }
            return index;
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            if (value.AsBool)
            {
                _data.Set(index);
            }
            else
            {
                _data.Unset(index);
            }
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
                _data.InsertAt(index, false);
                return;
            }
            if (value.AsBool)
            {                 
                _data.InsertAt(index, true);
            }
            else
            {
                _data.InsertAt(index, false);
            } 
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var valueBuffer = new ArrowBuffer(_data.Memory);
            var arr = new BooleanArray(valueBuffer, nullBuffer, Count, nullCount, 0);
            return (arr, new BooleanType());
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
            return ArrowTypeId.Boolean;
        }
    }
}

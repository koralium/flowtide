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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class Int64Column : IDataColumn
    {
        //private List<long> _data;
        private NativeLongList _data;

        public int Count => _data.Count;

        public ArrowTypeId Type => ArrowTypeId.Int64;

        public Int64Column()
        {
            _data = new NativeLongList(new NativeMemoryAllocator());
            //_data = new List<long>();
        }

        public int Add<T>(in T value) where T: IDataValue
        {
            var index = _data.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _data.Add(0);
                return index;
            }
            _data.Add(value.AsLong);
            return index;
        }

        public int CompareToStrict(in int index, in IDataValue value)
        {
            return _data[index].CompareTo(value.AsLong);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is Int64Column int64Column)
            {
                return _data[thisIndex].CompareTo(int64Column._data[otherIndex]);
            }
            throw new NotImplementedException();
        }

        public int CompareTo<T>(in int index, in T value) where T : IDataValue
        {
            var longValue = value.AsLong;
            return _data[index].CompareTo(longValue);
        }

        public IDataValue GetValueAt(in int index)
        {
            return new Int64Value(_data[index]);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            dataValueContainer._type = ArrowTypeId.Int64;
            dataValueContainer._int64Value = new Int64Value(_data[index]);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end)
            where T: IDataValue
        {
            var val = dataValue.AsLong;
            return BoundarySearch.SearchBoundries(_data, val, start, end - start, Int64Comparer.Instance);
        }

        public int Update(in int index, in IDataValue value)
        {
            _data[index] = value.AsLong;
            return index;
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
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
            _data.RemoveAt(index);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _data.InsertAt(index, 0);
            }
            _data.InsertAt(index, value.AsLong);
        }

        public IArrowArray ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var valueBuffer = new ArrowBuffer(_data.Memory);
            return new Int64Array(valueBuffer, nullBuffer, _data.Count, nullCount, 0);
        }
    }
}

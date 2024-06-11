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

using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class Int64Column : IDataColumn
    {
        private List<long> _data;

        public Int64Column()
        {
            _data = new List<long>();
        }

        public int Add(in IDataValue value)
        {
            var index = _data.Count;
            _data.Add(value.AsLong);
            return index;
        }

        public int Add<T>(in T value) where T : struct, IDataValue
        {
            var index = _data.Count;
            _data.Add(value.AsLong);
            return index;
        }

        public int BinarySearch(in IDataValue dataValue, in int start, in int end)
        {
            var longVal = dataValue.AsLong;
            return _data.BinarySearch(start, end - start, longVal, default);
        }

        public int CompareToStrict(in int index, in IDataValue value)
        {
            return _data[index].CompareTo(value.AsLong);
        }

        public int CompareToStrict(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is Int64Column int64Column)
            {
                return _data[thisIndex].CompareTo(int64Column._data[otherIndex]);
            }
            throw new NotImplementedException();
        }

        public int CompareToStrict<T>(in int index, in T value) where T : IDataValue
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
            throw new NotImplementedException();
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end)
            where T: IDataValue
        {
            var val = dataValue.AsLong;
            return BoundarySearch.SearchBoundries<long>(_data, val, start, end - start, Int64Comparer.Instance);
        }

        public int Update(in int index, in IDataValue value)
        {
            _data[index] = value.AsLong;
            return index;
        }

        public int Update<T>(in int index, in T value) where T : struct, IDataValue
        {
            _data[index] = value.AsLong;
            return index;
        }
    }
}

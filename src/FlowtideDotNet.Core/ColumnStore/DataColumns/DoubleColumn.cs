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
    internal class DoubleColumn : IDataColumn
    {
        private readonly List<double> _data;

        public DoubleColumn()
        {
            _data = new List<double>();
        }

        public int Add(in IDataValue value)
        {
            var index = _data.Count;
            _data.Add(value.AsDouble);
            return index;
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _data.Count;
            _data.Add(value.AsDouble);
            return index;
        }

        public int BinarySearch(in IDataValue dataValue)
        {
            throw new NotImplementedException();
        }

        public int BinarySearch(in IDataValue dataValue, in int start, in int end)
        {
            throw new NotImplementedException();
        }

        public int CompareToStrict(in int index, in IDataValue value)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            throw new NotImplementedException();
        }

        public int CompareTo<T>(in int index, in T value) where T : IDataValue
        {
            return _data[index].CompareTo(value.AsDouble);
        }

        public IDataValue GetValueAt(in int index)
        {
            return new DoubleValue(_data[index]);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            throw new NotImplementedException();
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end) 
            where T : IDataValue
        {
            var val = dataValue.AsDouble;
            return BoundarySearch.SearchBoundries<double>(_data, val, start, end - start, DoubleComparer.Instance);
        }

        public int Update(in int index, in IDataValue value)
        {
            _data[index] = value.AsDouble;
            return index;
        }

        public int Update<T>(in int index, in T value) where T : struct, IDataValue
        {
            throw new NotImplementedException();
        }
    }
}

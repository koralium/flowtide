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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class ListColumn : IDataColumn
    {
        private readonly Column _internalColumn;
        private readonly List<int> _offsets;

        public ListColumn()
        {
            _internalColumn = new Column();
            _offsets = new List<int>();
        }

        public int Add(in IDataValue value)
        {
            var list = value.AsList;

            var currentOffset = _offsets.Count;
            _offsets.Add(_internalColumn.Count);
            var listLength = list.Count;
            for (int i = 0; i < listLength; i++)
            {
                _internalColumn.Add(list.GetAt(i));
            }

            return currentOffset;
        }

        public int Add<T>(in T value) where T : struct, IDataValue
        {
            throw new NotImplementedException();
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

        public int CompareToStrict(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            throw new NotImplementedException();
        }

        public int CompareToStrict<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public IDataValue GetValueAt(in int index)
        {
            if (index + 1 < _offsets.Count)
            {
                return new ReferenceListValue(_internalColumn, _offsets[index], _offsets[index + 1]);
            }
            else
            {
                return new ReferenceListValue(_internalColumn, _offsets[index], _internalColumn.Count);
            }
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            throw new NotImplementedException();
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end) 
            where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int Update(in int index, in IDataValue value)
        {
            return Add(value);
        }

        public int Update<T>(in int index, in T value) where T : struct, IDataValue
        {
            throw new NotImplementedException();
        }
    }
}

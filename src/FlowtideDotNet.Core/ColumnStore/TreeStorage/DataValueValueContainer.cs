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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    internal class DataValueValueContainer : IValueContainer<IDataValue>
    {
        internal Column _column;

        internal DataValueValueContainer(IMemoryAllocator memoryAllocator)
        {
            _column = new Column(memoryAllocator);
        }

        internal DataValueValueContainer(Column column)
        {
            _column = column;
        }

        public int Count => _column.Count;

        public void AddRangeFrom(IValueContainer<IDataValue> container, int start, int count)
        {
            if (container is DataValueValueContainer dataValueValueContainer)
            {
                _column.InsertRangeFrom(_column.Count, dataValueValueContainer._column, start, count);
            }
            else
            {
                throw new InvalidOperationException("Invalid container type");
            }
        }

        public void Dispose()
        {
            _column.Dispose();
        }

        public IDataValue Get(int index)
        {
            return _column.GetValueAt(index, default);
        }

        public int GetByteSize()
        {
            return _column.GetByteSize();
        }

        public int GetByteSize(int start, int end)
        {
            return _column.GetByteSize(start, end);
        }

        public ref IDataValue GetRef(int index)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, IDataValue value)
        {
            _column.InsertAt(index, value);
        }

        public void RemoveAt(int index)
        {
            _column.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _column.RemoveRange(start, count);
        }

        public void Update(int index, IDataValue value)
        {
            _column.UpdateAt(index, value);
        }
    }
}

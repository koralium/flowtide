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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Normalization
{
    internal class NormalizeValueStorage : IValueContainer<ColumnRowReference>
    {
        private readonly List<int> _columnsToStore;
        internal readonly EventBatchData _data;
        private int _length;
        private DataValueContainer _dataValueContainer = new DataValueContainer();

        public NormalizeValueStorage(List<int> columnsToStore, IMemoryAllocator memoryAllocator)
        {
            this._columnsToStore = columnsToStore;
            IColumn[] columns = new IColumn[columnsToStore.Count];
            var memoryManager = memoryAllocator;
            for (int i = 0; i < columnsToStore.Count; i++)
            {
                columns[i] = Column.Create(memoryManager);
            }
            _data = new EventBatchData(columns);
        }

        public NormalizeValueStorage(List<int> columnsToStore, EventBatchData data, int length)
        {
            _columnsToStore = columnsToStore;
            _data = data;
            _length = length;
        }

        public int Count => _length;

        public void Add(ColumnRowReference key)
        {
            // Add is only run internally in the tree, so we dont use the columnsToStore
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContainer, default);
                _data.Columns[i].Add(_dataValueContainer);
            }
            _length++;
        }

        public void AddRangeFrom(IValueContainer<ColumnRowReference> container, int start, int count)
        {
            if (container is NormalizeValueStorage columnKeyStorageContainer)
            {
                for (int i = start; i < start + count; i++)
                {
                    Add(columnKeyStorageContainer.Get(i));
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public ColumnRowReference Get(int index)
        {
            return new ColumnRowReference()
            {
                referenceBatch = _data,
                RowIndex = index
            };
        }

        public void Insert(int index, ColumnRowReference value)
        {
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                value.referenceBatch.Columns[_columnsToStore[i]].GetValueAt(value.RowIndex, _dataValueContainer, default);
                _data.Columns[i].InsertAt(index, _dataValueContainer);
            }
            _length++;
        }

        public void RemoveAt(int index)
        {
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                _data.Columns[i].RemoveAt(index);
            }
            _length--;
        }

        public void RemoveRange(int start, int count)
        {
            var end = start + count;
            for (int i = end - 1; i >= start; i--)
            {
                RemoveAt(i);
            }
        }

        public void Update(int index, ColumnRowReference value)
        {
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                value.referenceBatch.Columns[_columnsToStore[i]].GetValueAt(value.RowIndex, _dataValueContainer, default);
                _data.Columns[i].UpdateAt(index, _dataValueContainer);
            }
        }

        public void Dispose()
        {
            _data.Dispose();
        }

        public ref ColumnRowReference GetRef(int index)
        {
            throw new NotImplementedException("Get by ref is not supported");
        }

        public int GetByteSize()
        {
            return _data.GetByteSize();
        }

        public int GetByteSize(int start, int end)
        {
            return _data.GetByteSize(start, end);
        }
    }
}

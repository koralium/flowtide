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
    internal class ColumnKeyStorageContainer : IKeyContainer<ColumnRowReference>
    {
        private readonly int columnCount;
        internal EventBatchData _data;
        private DataValueContainer _dataValueContainer;

        public ColumnKeyStorageContainer(int columnCount, IMemoryAllocator memoryAllocator)
        {
            IColumn[] columns = new IColumn[columnCount];
            var memoryManager = memoryAllocator;
            for (int i = 0; i < columnCount; i++)
            {
                columns[i] = Column.Create(memoryManager);
            }
            _data = new EventBatchData(columns);
            this.columnCount = columnCount;
            _dataValueContainer = new DataValueContainer();
        }

        internal ColumnKeyStorageContainer(int columnCount, EventBatchData eventBatchData)
        {
            this.columnCount = columnCount;
            _data = eventBatchData;
            _dataValueContainer = new DataValueContainer();
        }

        public int Count => _data.Count;

        public void Add(ColumnRowReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContainer, default);
                _data.Columns[i].Add(_dataValueContainer);
            }
        }

        public void AddRangeFrom(IKeyContainer<ColumnRowReference> container, int start, int count)
        {
            if (container is ColumnKeyStorageContainer columnKeyStorageContainer)
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

        public int BinarySearch(ColumnRowReference key, IComparer<ColumnRowReference> comparer)
        {
            throw new NotImplementedException();
        }

        ~ColumnKeyStorageContainer()
        {
            Dispose();
        }

        public void Dispose()
        {
            _data.Dispose();
            GC.SuppressFinalize(this);
        }

        public ColumnRowReference Get(in int index)
        {
            return new ColumnRowReference()
            {
                referenceBatch = _data,
                RowIndex = index
            };
        }

        public void Insert(int index, ColumnRowReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContainer, default);
                _data.Columns[i].InsertAt(index, _dataValueContainer);
                //_data.Columns[i].InsertAt(index, key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, default));
            }
        }

        public void Insert_Internal(int index, ColumnRowReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContainer, default);
                _data.Columns[i].InsertAt(index, _dataValueContainer);
            }
        }

        public void RemoveAt(int index)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _data.Columns[i].RemoveAt(index);
            }
        }

        public void RemoveRange(int start, int count)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _data.Columns[i].RemoveRange(start, count);
            }
        }

        public void Update(int index, ColumnRowReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContainer, default);
                _data.Columns[i].UpdateAt(index, _dataValueContainer);
            }
        }
    }
}

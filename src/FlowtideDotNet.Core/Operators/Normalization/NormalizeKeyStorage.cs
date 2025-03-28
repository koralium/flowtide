﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

namespace FlowtideDotNet.Core.Operators.Normalization
{
    internal class NormalizeKeyStorage : IKeyContainer<ColumnRowReference>
    {
        private readonly List<int> _columnsToStore;
        internal readonly EventBatchData _data;
        private DataValueContainer _dataValueContaienr;
        public NormalizeKeyStorage(List<int> columnsToStore, IMemoryAllocator memoryAllocator)
        {
            _dataValueContaienr = new DataValueContainer();
            _columnsToStore = columnsToStore;
            IColumn[] columns = new IColumn[columnsToStore.Count];
            var memoryManager = memoryAllocator;
            for (int i = 0; i < columnsToStore.Count; i++)
            {
                columns[i] = Column.Create(memoryManager);
            }
            _data = new EventBatchData(columns);
        }

        internal NormalizeKeyStorage(List<int> columnsToStore, EventBatchData eventBatchData)
        {
            _dataValueContaienr = new DataValueContainer();
            _columnsToStore = columnsToStore;
            _data = eventBatchData;
        }

        public int Count => _data.Count;

        public void Add(ColumnRowReference key)
        {
            // Add is only run internally to copy values in the tree
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContaienr, default);
                _data.Columns[i].Add(_dataValueContaienr);
            }
        }

        public void AddRangeFrom(IKeyContainer<ColumnRowReference> container, int start, int count)
        {
            if (container is NormalizeKeyStorage columnKeyStorageContainer)
            {
                for (int i = 0; i < _columnsToStore.Count; i++)
                {
                    _data.Columns[i].InsertRangeFrom(_data.Columns[i].Count, columnKeyStorageContainer._data.Columns[i], start, count);
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
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                key.referenceBatch.Columns[_columnsToStore[i]].GetValueAt(key.RowIndex, _dataValueContaienr, default);
                _data.Columns[i].InsertAt(index, _dataValueContaienr);
            }
        }

        public void RemoveAt(int index)
        {
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                _data.Columns[i].RemoveAt(index);
            }
        }

        public void RemoveRange(int start, int count)
        {
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                _data.Columns[i].RemoveRange(start, count);
            }
        }

        public void Update(int index, ColumnRowReference key)
        {
            // Update is only run internally to copy values in the tree
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContaienr, default);
                _data.Columns[i].UpdateAt(index, _dataValueContaienr);
            }
        }

        public void Insert_Internal(int index, ColumnRowReference key)
        {
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContaienr, default);
                _data.Columns[i].InsertAt(index, _dataValueContaienr);
            }
        }

        public void Dispose()
        {
            _data.Dispose();
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

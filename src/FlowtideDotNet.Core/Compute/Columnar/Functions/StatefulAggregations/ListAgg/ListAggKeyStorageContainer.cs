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
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations
{
    internal class ListAggKeyStorageContainer : IKeyContainer<ListAggColumnRowReference>
    {
        private readonly int _groupingKeyLength;
        internal EventBatchData _data;
        private DataValueContainer _dataValueContainer;

        public ListAggKeyStorageContainer(int groupingKeyLength, IMemoryAllocator memoryAllocator)
        {
            this._groupingKeyLength = groupingKeyLength;
            IColumn[] columns = new IColumn[groupingKeyLength + 1];
            var memoryManager = memoryAllocator;
            for (int i = 0; i < (groupingKeyLength + 1); i++)
            {
                columns[i] = Column.Create(memoryManager);
            }
            _data = new EventBatchData(columns);
            _dataValueContainer = new DataValueContainer();
        }

        internal ListAggKeyStorageContainer(int groupingKeyLength, EventBatchData eventBatchData)
        {
            _groupingKeyLength = groupingKeyLength;
            _data = eventBatchData;
            _dataValueContainer = new DataValueContainer();
        }

        public int Count => _data.Count;

        public void Add(ListAggColumnRowReference key)
        {
            // Add is only run internally to copy values in the tree
            for (int i = 0; i < (_groupingKeyLength + 1); i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                _data.Columns[i].Add(_dataValueContainer);
            }
        }

        public void AddRangeFrom(IKeyContainer<ListAggColumnRowReference> container, int start, int count)
        {
            if (container is ListAggKeyStorageContainer columnKeyStorageContainer)
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

        public int BinarySearch(ListAggColumnRowReference key, IComparer<ListAggColumnRowReference> comparer)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            _data.Dispose();
        }

        public ListAggColumnRowReference Get(in int index)
        {
            return new ListAggColumnRowReference()
            {
                batch = _data,
                index = index
            };
        }

        public void Insert(int index, ListAggColumnRowReference key)
        {
            // Insert grouping keys
            for (int i = 0; i < _groupingKeyLength; i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                _data.Columns[i].InsertAt(index, _dataValueContainer);
            }
            // Insert the value in the last column
            _data.Columns[_groupingKeyLength].InsertAt(index, key.insertValue);
        }

        public void Insert_Internal(int index, ListAggColumnRowReference key)
        {
            for (int i = 0; i < (_groupingKeyLength + 1); i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                _data.Columns[i].InsertAt(index, _dataValueContainer);
            }
        }

        public void RemoveAt(int index)
        {
            for (int i = 0; i < (_groupingKeyLength + 1); i++)
            {
                _data.Columns[i].RemoveAt(index);
            }
        }

        public void RemoveRange(int start, int count)
        {
            for (int i = 0; i < (_groupingKeyLength + 1); i++)
            {
                _data.Columns[i].RemoveRange(start, count);
            }
        }

        public void Update(int index, ListAggColumnRowReference key)
        {
            // Update is only run internally to copy values in the tree
            for (int i = 0; i < (_groupingKeyLength + 1); i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                _data.Columns[i].UpdateAt(index, _dataValueContainer);
            }
        }
    }
}

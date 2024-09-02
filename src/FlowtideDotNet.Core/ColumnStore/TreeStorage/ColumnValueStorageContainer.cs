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

using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    internal class ColumnValueStorageContainer : IValueContainer<ColumnRowReference>
    {
        private readonly int columnCount;
        internal EventBatchData _data;

        public ColumnValueStorageContainer(int columnCount)
        {
            IColumn[] columns = new IColumn[columnCount];
            var memoryManager = new BatchMemoryManager(columnCount);
            for (int i = 0; i < columnCount; i++)
            {
                columns[i] = Column.Create(memoryManager);
            }
            _data = new EventBatchData(columns);
            this.columnCount = columnCount;
        }

        internal ColumnValueStorageContainer(int columnCount, EventBatchData eventBatchData)
        {
            this.columnCount = columnCount;
            _data = eventBatchData;
        }

        public int Count => _data.Count;

        public void Add(ColumnRowReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _data.Columns[i].Add(key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, default));
            }
        }

        public void AddRangeFrom(IValueContainer<ColumnRowReference> container, int start, int count)
        {
            if (container is ColumnValueStorageContainer columnKeyStorageContainer)
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

        public void Dispose()
        {
            _data.Dispose();
        }

        public ColumnRowReference Get(int index)
        {
            return new ColumnRowReference()
            {
                referenceBatch = _data,
                RowIndex = index
            };
        }

        public ref ColumnRowReference GetRef(int index)
        {
            throw new NotImplementedException("Getting by ref in column value storage is not supported");
        }

        public void Insert(int index, ColumnRowReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _data.Columns[i].InsertAt(index, key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, default));
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
            var end = start + count;
            for (int i = end - 1; i >= start; i--)
            {
                RemoveAt(i);
            }
        }

        public void Update(int index, ColumnRowReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _data.Columns[i].UpdateAt(index, key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, default));
            }
        }
    }
}

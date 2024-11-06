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
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Storage.Memory;
using static SqlParser.Ast.TableConstraint;

namespace FlowtideDotNet.Core.Operators.Aggregate.Column
{
    internal class AggregateKeyStorageContainer : IKeyContainer<ColumnRowReference>
    {
        private readonly int columnCount;
        internal EventBatchData _data;
        private DataValueContainer _dataValueContainer;
        private int _length;

        public AggregateKeyStorageContainer(int columnCount, IMemoryAllocator memoryAllocator)
        {
            IColumn[] columns = new IColumn[columnCount];
            var memoryManager = memoryAllocator;
            for (int i = 0; i < columnCount; i++)
            {
                columns[i] = ColumnStore.Column.Create(memoryManager);
            }
            _data = new EventBatchData(columns);
            this.columnCount = columnCount;
            _dataValueContainer = new DataValueContainer();
            _length = 0;
        }

        internal AggregateKeyStorageContainer(int columnCount, EventBatchData eventBatchData, int length)
        {
            this.columnCount = columnCount;
            _data = eventBatchData;
            _dataValueContainer = new DataValueContainer();
            _length = length;
        }

        public int Count => _length;

        public void Add(ColumnRowReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContainer, default);
                _data.Columns[i].Add(_dataValueContainer);
            }
            _length++;
        }

        public void AddRangeFrom(IKeyContainer<ColumnRowReference> container, int start, int count)
        {
            if (container is AggregateKeyStorageContainer columnKeyStorageContainer)
            {
                for (int i = 0; i < columnCount; i++)
                {
                    _data.Columns[i].InsertRangeFrom(_data.Columns[i].Count, columnKeyStorageContainer._data.Columns[i], start, count);
                }
                _length += count;
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

        public void Dispose()
        {
            _data.Dispose();
        }

        public ColumnRowReference Get(in int index)
        {
            return new ColumnRowReference()
            {
                referenceBatch = _data,
                RowIndex = index
            };
        }

        public int GetByteSize()
        {
            return _data.GetByteSize();
        }

        public int GetByteSize(int start, int end)
        {
            return _data.GetByteSize(start, end);
        }

        public void Insert(int index, ColumnRowReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContainer, default);
                _data.Columns[i].InsertAt(index, _dataValueContainer);
                //_data.Columns[i].InsertAt(index, key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, default));
            }
            _length++;
        }

        public void Insert_Internal(int index, ColumnRowReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, _dataValueContainer, default);
                _data.Columns[i].InsertAt(index, _dataValueContainer);
            }
            _length++;
        }

        public void RemoveAt(int index)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _data.Columns[i].RemoveAt(index);
            }
            _length--;
        }

        public void RemoveRange(int start, int count)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _data.Columns[i].RemoveRange(start, count);
            }
            _length -= count;
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

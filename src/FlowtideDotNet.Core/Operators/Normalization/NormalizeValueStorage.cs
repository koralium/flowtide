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

namespace FlowtideDotNet.Core.Operators.Normalization
{
    internal class NormalizeValueStorage : IValueContainer<ColumnRowReference>
    {
        private readonly List<int> _columnsToStore;
        internal readonly EventBatchData _data;
        private int _length;

        public NormalizeValueStorage(List<int> columnsToStore)
        {
            this._columnsToStore = columnsToStore;
            List<IColumn> columns = new List<IColumn>();
            for (int i = 0; i < columnsToStore.Count; i++)
            {
                columns.Add(new Column());
            }
            _data = new EventBatchData(columns);
        }

        public int Count => _length;

        public void Add(ColumnRowReference key)
        {
            for (int i = 0; i < _columnsToStore.Count; i++)
            {
                _data.Columns[i].Add(key.referenceBatch.Columns[_columnsToStore[i]].GetValueAt(key.RowIndex, default));
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
                _data.Columns[i].InsertAt(index, value.referenceBatch.Columns[_columnsToStore[i]].GetValueAt(value.RowIndex, default));
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
                _data.Columns[i].UpdateAt(index, value.referenceBatch.Columns[_columnsToStore[i]].GetValueAt(value.RowIndex, default));
            }
        }
    }
}

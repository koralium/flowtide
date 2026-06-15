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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful
{
    public class BulkGroupValueKeyContainer : IKeyContainer<BulkGroupValueRowReference>
    {
        private readonly int _groupingKeyLength;
        internal EventBatchData _data;
        private DataValueContainer _dataValueContainer;

        public BulkGroupValueKeyContainer(int groupingKeyLength, IMemoryAllocator memoryAllocator)
        {
            this._groupingKeyLength = groupingKeyLength;
            IColumn[] columns = new IColumn[groupingKeyLength + 1];
            for (int i = 0; i < (groupingKeyLength + 1); i++)
            {
                columns[i] = Column.Create(memoryAllocator);
            }
            _data = new EventBatchData(columns);
            _dataValueContainer = new DataValueContainer();
        }

        internal BulkGroupValueKeyContainer(int groupingKeyLength, EventBatchData eventBatchData)
        {
            _groupingKeyLength = groupingKeyLength;
            _data = eventBatchData;
            _dataValueContainer = new DataValueContainer();
        }

        public int Count => _data.Count;

        public void Add(BulkGroupValueRowReference key)
        {
            for (int i = 0; i < (_groupingKeyLength + 1); i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                _data.Columns[i].Add(_dataValueContainer);
            }
        }

        public void AddRangeFrom(IKeyContainer<BulkGroupValueRowReference> container, int start, int count)
        {
            if (container is BulkGroupValueKeyContainer other)
            {
                for (int i = 0; i < (_groupingKeyLength + 1); i++)
                {
                    _data.Columns[i].InsertRangeFrom(_data.Columns[i].Count, other._data.Columns[i], start, count);
                }
            }
        }

        public int BinarySearch(BulkGroupValueRowReference key, IComparer<BulkGroupValueRowReference> comparer)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, BulkGroupValueRowReference key)
        {
            for (int i = 0; i < (_groupingKeyLength + 1); i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                _data.Columns[i].InsertAt(index, _dataValueContainer);
            }
        }

        public void Insert_Internal(int index, BulkGroupValueRowReference key)
        {
            Insert(index, key);
        }

        public void InsertFrom(BulkGroupValueRowReference[] keys, ReadOnlySpan<int> sortedLookup, ReadOnlySpan<int> targetPositions, Span<int> lookupBuffer)
        {
            var batchReference = keys[0].batch;

            for (int i = 0; i < (_groupingKeyLength + 1); i++)
            {
                var column = _data.Columns[i];
                var sourceColumn = batchReference.Columns[i];

                if (sourceColumn is ColumnWithOffset columnWithOffset)
                {
                    var offsets = columnWithOffset.Offsets;
                    for (int j = 0; j < sortedLookup.Length; j++)
                    {
                        var key = keys[sortedLookup[j]];
                        lookupBuffer[j] = offsets[key.index];
                    }
                    ReadOnlySpan<int> lb = lookupBuffer; 
                    column.InsertFrom(columnWithOffset.InnerColumn, in lb, in targetPositions, ColumnWithOffset.NullValueIndex);
                }
                else
                {
                    for (int j = 0; j < sortedLookup.Length; j++)
                    {
                        var key = keys[sortedLookup[j]];
                        lookupBuffer[j] = key.index;
                    }
                    ReadOnlySpan<int> lb = lookupBuffer; 
                    column.InsertFrom(sourceColumn, in lb, in targetPositions, ColumnWithOffset.NullValueIndex);
                }
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

        public void Update(int index, BulkGroupValueRowReference key)
        {
            for (int i = 0; i < (_groupingKeyLength + 1); i++)
            {
                key.batch.Columns[i].GetValueAt(key.index, _dataValueContainer, default);
                _data.Columns[i].UpdateAt(index, _dataValueContainer);
            }
        }

        public void DeleteBatch(ReadOnlySpan<int> positions)
        {
            for (int i = 0; i < _data.Columns.Count; i++)
            {
                _data.Columns[i].DeleteBatch(positions);
            }
        }

        public BulkGroupValueRowReference Get(in int index)
        {
            return new BulkGroupValueRowReference()
            {
                batch = _data,
                index = index
            };
        }

        public int GetByteSize() => _data.GetByteSize();
        public int GetByteSize(int start, int end) => _data.GetByteSize(start, end);
        public void Dispose() => _data.Dispose();
    }
}

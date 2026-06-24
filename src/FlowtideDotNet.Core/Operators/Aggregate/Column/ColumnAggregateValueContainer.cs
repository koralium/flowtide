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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;

namespace FlowtideDotNet.Core.Operators.Aggregate.Column
{
    internal class ColumnAggregateValueContainer : IValueContainer<ColumnAggregateStateReference>
    {
        private readonly int columnCount;
        internal EventBatchData _eventBatch;
        internal PrimitiveList<int> _weights;
        internal PrimitiveList<bool> _previousValueSent;
        public ColumnAggregateValueContainer(int measureCount, IMemoryAllocator memoryAllocator)
        {
            this.columnCount = measureCount * 2;
            ColumnStore.Column[] columns = new ColumnStore.Column[columnCount];

            for (int i = 0; i < columnCount; i++)
            {
                columns[i] = ColumnStore.Column.Create(memoryAllocator);
            }

            _eventBatch = new EventBatchData(columns);
            _weights = new PrimitiveList<int>(memoryAllocator);
            _previousValueSent = new PrimitiveList<bool>(memoryAllocator);
        }

        public ColumnAggregateValueContainer(int measureCount, EventBatchData eventBatch, PrimitiveList<int> weights, PrimitiveList<bool> previousValueSent)
        {
            this.columnCount = measureCount * 2;
            _eventBatch = eventBatch;
            _weights = weights;
            _previousValueSent = previousValueSent;
        }

        public int Count => _weights.Count;

        public void Add(ColumnAggregateStateReference key)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _eventBatch.Columns[i].Add(key.referenceBatch.Columns[i].GetValueAt(key.RowIndex, default));
            }
            _weights.Add(key.weight);
            _previousValueSent.Add(key.valueSent);
        }

        public void AddRangeFrom(IValueContainer<ColumnAggregateStateReference> container, int start, int count)
        {
            if (container is ColumnAggregateValueContainer columnKeyStorageContainer)
            {
                for (int i = 0; i < columnCount; i++)
                {
                    _eventBatch.Columns[i].InsertRangeFrom(_eventBatch.Columns[i].Count, columnKeyStorageContainer._eventBatch.Columns[i], start, count);
                }
                _weights.AddRangeFrom(columnKeyStorageContainer._weights, start, count);
                _previousValueSent.AddRangeFrom(columnKeyStorageContainer._previousValueSent, start, count);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void DeleteBatch(ReadOnlySpan<int> positions)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _eventBatch.Columns[i].DeleteBatch(positions);
            }
            _weights.DeleteBatch(positions);
            _previousValueSent.DeleteBatch(positions);
        }

        public void Dispose()
        {
            _eventBatch.Dispose();
            _weights.Dispose();
            _previousValueSent.Dispose();
        }

        public ColumnAggregateStateReference Get(int index)
        {
            if (index < 0 || index >= _weights.Count)
            {
                throw new InvalidOperationException($"Out of bounds in Get: index={index}, _weights.Count={_weights.Count}, _eventBatch.Count={_eventBatch.Count}, _previousValueSent.Count={_previousValueSent.Count}");
            }
            return new ColumnAggregateStateReference()
            {
                referenceBatch = _eventBatch,
                RowIndex = index,
                weight = _weights.Get(index),
                valueSent = _previousValueSent.Get(index)
            };
        }

        public int GetByteSize()
        {
            return _eventBatch.GetByteSize();
        }

        public int GetByteSize(int start, int end)
        {
            return _eventBatch.GetByteSize(start, end);
        }

        public ref ColumnAggregateStateReference GetRef(int index)
        {
            throw new NotImplementedException("Get by ref is not supported");
        }

        public void Insert(int index, ColumnAggregateStateReference value)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _eventBatch.Columns[i].InsertAt(index, value.referenceBatch.Columns[i].GetValueAt(value.RowIndex, default));
            }
            _weights.InsertAt(index, value.weight);
            _previousValueSent.InsertAt(index, value.valueSent);
        }

        public void InsertFrom(ColumnAggregateStateReference[] values, ReadOnlySpan<int> sortedLookup, ReadOnlySpan<int> targetPositions)
        {
            if (sortedLookup.Length == 0)
            {
                return;
            }

            var firstIndex = sortedLookup[0];
            var valuesBatch = values[firstIndex].referenceBatch;
            for (int i = 0; i < columnCount; i++)
            {
                var col = valuesBatch.Columns[i];
                _eventBatch.Columns[i].InsertFrom(col, ref sortedLookup, ref targetPositions, -1);
            }

            int count = sortedLookup.Length;

            // Rent temporary array buffers to extract primitive values
            int[] tempWeights = ArrayPool<int>.Shared.Rent(count);
            bool[] tempSent = ArrayPool<bool>.Shared.Rent(count);
            int[] sequentialLookup = ArrayPool<int>.Shared.Rent(count);
            for (int i = 0; i < count; i++)
            {
                var sortedIndex = sortedLookup[i];
                tempWeights[i] = values[sortedIndex].weight;
                tempSent[i] = values[sortedIndex].valueSent;
                sequentialLookup[i] = i;
            }
            // Perform bulk single-pass block copy insertions
            _weights.InsertFrom(tempWeights, sequentialLookup.AsSpan(0, count), targetPositions);
            _previousValueSent.InsertFrom(tempSent, sequentialLookup.AsSpan(0, count), targetPositions);
            // Return rented arrays to the pool
            ArrayPool<int>.Shared.Return(tempWeights);
            ArrayPool<bool>.Shared.Return(tempSent);
            ArrayPool<int>.Shared.Return(sequentialLookup);
        }

        public void RemoveAt(int index)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _eventBatch.Columns[i].RemoveAt(index);
            }
            _weights.RemoveAt(index);
            _previousValueSent.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            for (int i = 0; i < columnCount; i++)
            {
                _eventBatch.Columns[i].RemoveRange(start, count);
            }
            _weights.RemoveRange(start, count);
            _previousValueSent.RemoveRange(start, count);
        }

        public void Update(int index, ColumnAggregateStateReference value)
        {
            _weights.Update(index, value.weight);
            _previousValueSent.Update(index, value.valueSent);
        }
    }
}

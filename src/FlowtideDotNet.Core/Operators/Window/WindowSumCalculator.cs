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
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Window
{
    internal delegate void AddOutputRow<T>(ColumnRowReference partitionValues, int rowIndex, T value)
        where T: IDataValue;
    internal class WindowSumCalculator
    {
        private IWindowAddOutputRow? _addOutputRow;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _updateIterator;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private IMemoryAllocator? _memoryAllocator;
        private IFlowtideQueue<IDataValue, DataValueValueContainer>? _queue;
        private PartitionIterator? _updatePartitionIterator;
        private PartitionIterator? _windowPartitionIterator;

        public async Task Initialize(
            IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer> persistentTree,
            int partitionColumnCount,
            IMemoryAllocator memoryAllocator,
            IStateManagerClient stateManagerClient,
            IWindowAddOutputRow addOutputRow)
        {
            _addOutputRow = addOutputRow;
            _windowIterator = persistentTree.CreateIterator();
            _updateIterator = persistentTree.CreateIterator();

            _updatePartitionIterator = new PartitionIterator(addOutputRow);
            _windowPartitionIterator = new PartitionIterator();

            _queue = await stateManagerClient.GetOrCreateQueue("queue", new FlowtideQueueOptions<IDataValue, DataValueValueContainer>()
            {
                MemoryAllocator = memoryAllocator,
                ValueSerializer = new DataValueValueContainerSerializer(memoryAllocator)
            });
            _memoryAllocator = memoryAllocator;
        }

        private static void DoSum<T>(T value, DataValueContainer currentState, long weight)
            where T : IDataValue
        {
            if (currentState.Type == ArrowTypeId.Int64)
            {
                if (value.Type == ArrowTypeId.Int64)
                {
                    var count = currentState.AsLong + (value.AsLong * weight);
                    currentState._type = ArrowTypeId.Int64;
                    currentState._int64Value = new Int64Value(count);
                }
                else if (value.Type == ArrowTypeId.Double)
                {
                    var floatCount = currentState.AsLong + (value.AsDouble * weight);
                    currentState._type = ArrowTypeId.Double;
                    currentState._doubleValue = new DoubleValue(floatCount);
                }
            }
            else if (currentState.Type == ArrowTypeId.Double)
            {
                if (value.Type == ArrowTypeId.Int64)
                {
                    var count = currentState.AsDouble + (value.AsLong * weight);
                    currentState._type = ArrowTypeId.Double;
                    currentState._doubleValue = new DoubleValue(count);
                }
                else if (value.Type == ArrowTypeId.Double)
                {
                    var count = currentState.AsDouble + (value.AsDouble * weight);
                    currentState._type = ArrowTypeId.Double;
                    currentState._doubleValue = new DoubleValue(count);
                }
            }
            else if (currentState.Type == ArrowTypeId.Null)
            {
                if (value.Type == ArrowTypeId.Int64)
                {
                    var count = (value.AsLong * weight);
                    currentState._type = ArrowTypeId.Int64;
                    currentState._int64Value = new Int64Value(count);
                }
                else if (value.Type == ArrowTypeId.Double)
                {
                    var count = (value.AsDouble * weight);
                    currentState._type = ArrowTypeId.Double;
                    currentState._doubleValue = new DoubleValue(count);
                }
            }
        }


        public async IAsyncEnumerable<EventBatchWeighted> ComputeRowSlidingWindow(
            ColumnRowReference partitionValues,
            WindowPartitionStartSearchComparer partitionStartSearchComparer,
            int from,
            int to
            )
        {
            Debug.Assert(_windowIterator != null);
            Debug.Assert(_updateIterator != null);
            Debug.Assert(_queue != null);
            Debug.Assert(_addOutputRow != null);
            Debug.Assert(_windowPartitionIterator != null);
            Debug.Assert(_updatePartitionIterator != null);

            await _queue.Clear();
            await _windowIterator.Seek(partitionValues, partitionStartSearchComparer);
            // Copy the seek result to the other iterator
            _windowIterator.CloneSeekResultTo(_updateIterator);

            _windowPartitionIterator.Reset(partitionValues, _windowIterator, partitionStartSearchComparer);
            _updatePartitionIterator.Reset(partitionValues, _updateIterator, partitionStartSearchComparer);

            var windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            var updateEnumerator = _updatePartitionIterator.GetAsyncEnumerator();
            
            int updateRowIndex = 0;
            int windowRowIndex = 0;

            var currentValue = new DataValueContainer();
            currentValue._type = ArrowTypeId.Null;

            while (await updateEnumerator.MoveNextAsync())
            {
                while (windowRowIndex <= (updateRowIndex + to) && await windowEnumerator.MoveNextAsync())
                {
                    var val = windowEnumerator.Current.Key.referenceBatch.Columns[1].GetValueAt(windowEnumerator.Current.Key.RowIndex, default);
                    await _queue.Enqueue(val);
                    windowRowIndex++;
                    DoSum(val, currentValue, 1);
                }

                while (_queue.Count > 0 && windowRowIndex - _queue.Count < updateRowIndex + from)
                {
                    var firstVal = await _queue.Dequeue();
                    DoSum(firstVal, currentValue, -1);
                }

                updateRowIndex++;
                
                updateEnumerator.Current.Value.UpdateStateValue(currentValue);

                if (_addOutputRow.Count >= 100)
                {
                    yield return _addOutputRow.GetCurrentBatch();
                }
            }

            if (_addOutputRow.Count > 0)
            {
                yield return _addOutputRow.GetCurrentBatch();
            }
        }
    }
}

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
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.FetchDirection;
using static Substrait.Protobuf.Expression.Types.FieldReference.Types;

namespace FlowtideDotNet.Core.Operators.Window
{
    internal class WindowSumCalculator
    {
        private IBPlusTreeIterator<ColumnRowReference, ColumnAggregateStateReference, ColumnKeyStorageContainer, ColumnAggregateValueContainer>? _updateIterator;
        private IBPlusTreeIterator<ColumnRowReference, ColumnAggregateStateReference, ColumnKeyStorageContainer, ColumnAggregateValueContainer>? _windowIterator;
        private IMemoryAllocator? _memoryAllocator;
        private Column? _slidingWindow;
        public Task Initialize(
            IBPlusTree<ColumnRowReference, ColumnAggregateStateReference, ColumnKeyStorageContainer, ColumnAggregateValueContainer> persistentTree,
            int partitionCount,
            IMemoryAllocator memoryAllocator
            )
        {
            _windowIterator = persistentTree.CreateIterator();
            _updateIterator = persistentTree.CreateIterator();
            _memoryAllocator = memoryAllocator;
            _slidingWindow = new Column(_memoryAllocator);
            return Task.CompletedTask;
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


        public async Task ComputeRowSlidingWindow(
            ColumnRowReference partitionValues,
            WindowPartitionStartSearchComparer partitionStartSearchComparer,
            int from,
            int to
            )
        {
            Debug.Assert(_windowIterator != null);
            Debug.Assert(_updateIterator != null);
            Debug.Assert(_slidingWindow != null);

            _slidingWindow.Clear();
            await _windowIterator.Seek(partitionValues, partitionStartSearchComparer);
            // This can be made quicker, where the window operator copies the leaf and index to the update iterator
            await _updateIterator.Seek(partitionValues, partitionStartSearchComparer);

            // Partition iterators make sure we only iterate inside of a partition
            var windowIterator = new PartitionIterator(partitionValues, _windowIterator, partitionStartSearchComparer);
            var updateIterator = new PartitionIterator(partitionValues, _updateIterator, partitionStartSearchComparer);
            
            var windowEnumerator = windowIterator.GetAsyncEnumerator();
            var updateEnumerator = updateIterator.GetAsyncEnumerator();
            
            int updateRowIndex = 0;
            int windowRowIndex = 0;

            var currentValue = new DataValueContainer();
            currentValue._type = ArrowTypeId.Null;

            while (await updateEnumerator.MoveNextAsync())
            {
                while (windowRowIndex <= (updateRowIndex + to) && await windowEnumerator.MoveNextAsync())
                {
                    var val = windowEnumerator.Current.Key.referenceBatch.Columns[1].GetValueAt(windowEnumerator.Current.Key.RowIndex, default);
                    _slidingWindow.Add(val);
                    windowRowIndex++;
                    DoSum(val, currentValue, 1);
                }

                while (_slidingWindow.Count > 0 && windowRowIndex - _slidingWindow.Count < updateRowIndex + from)
                {
                    var firstVal = _slidingWindow.GetValueAt(0, default);
                    _slidingWindow.RemoveAt(0);
                    DoSum(firstVal, currentValue, -1);
                }

                updateRowIndex++;
                // Update row and send output
            }
        }
    }
}

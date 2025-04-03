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
using FlowtideDotNet.Core.Operators.Window;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions
{
    internal class SumWindowFunctionDefinition : WindowFunctionDefinition
    {
        public override IWindowFunction Create(WindowFunction windowFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[0], functionsRegister);

            bool isRowBounded = false;
            long lowerBoundRowOffset = long.MinValue;
            long upperBoundRowOffset = long.MaxValue;
            if (windowFunction.LowerBound != null)
            {
                if (windowFunction.LowerBound is PreceedingRowWindowBound lowerPreceedingRow)
                {
                    isRowBounded = true;
                    lowerBoundRowOffset = -lowerPreceedingRow.Offset;
                }
                else if (windowFunction.LowerBound is CurrentRowWindowBound lowerCurrentRow)
                {
                    isRowBounded = true;
                    lowerBoundRowOffset = 0;
                }
                else if (windowFunction.LowerBound is FollowingRowWindowBound lowerFollowingRow)
                {
                    isRowBounded = true;
                    lowerBoundRowOffset = lowerFollowingRow.Offset;
                }
                else if (windowFunction.LowerBound is UnboundedWindowBound)
                {
                    isRowBounded = true;
                    lowerBoundRowOffset = long.MinValue;
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
            if (windowFunction.UpperBound != null)
            {
                if (windowFunction.UpperBound is PreceedingRowWindowBound lowerPreceedingRow)
                {
                    isRowBounded = true;
                    upperBoundRowOffset = -lowerPreceedingRow.Offset;
                }
                else if (windowFunction.UpperBound is CurrentRowWindowBound lowerCurrentRow)
                {
                    isRowBounded = true;
                    upperBoundRowOffset = 0;
                }
                else if (windowFunction.UpperBound is FollowingRowWindowBound lowerFollowingRow)
                {
                    isRowBounded = true;
                    upperBoundRowOffset = lowerFollowingRow.Offset;
                }
                else if (windowFunction.UpperBound is UnboundedWindowBound)
                {
                    isRowBounded = true;
                    upperBoundRowOffset = long.MaxValue;
                }
                else
                {
                    throw new NotImplementedException();
                }
            }

            if (isRowBounded && lowerBoundRowOffset != long.MinValue)
            {
                return new SumWindowFunctionBounded(compiledValue, lowerBoundRowOffset, upperBoundRowOffset);
            }
            else if (isRowBounded && lowerBoundRowOffset == long.MinValue && upperBoundRowOffset == long.MaxValue)
            {
                return new SumWindowFunctionUnbounded(compiledValue);
            }
            else if(isRowBounded && lowerBoundRowOffset == long.MinValue)
            {
                return new SumWindowFunctionBoundedUnboundedFrom(compiledValue, upperBoundRowOffset);
            }

            return new SumWindowFunctionUnbounded(compiledValue);
        }
    }

    internal static class SumWindowUtils
    {
        internal static void DoSum<T>(T value, DataValueContainer currentState, long weight)
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
    }

    internal class SumWindowFunctionBounded : IWindowFunction
    {
        private IWindowAddOutputRow? _addOutputRow;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _updateIterator;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private IFlowtideQueue<IDataValue, DataValueValueContainer>? _queue;
        private PartitionIterator? _updatePartitionIterator;
        private PartitionIterator? _windowPartitionIterator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;
        private readonly long _to;

        public SumWindowFunctionBounded(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from, long to)
        {
            this._fetchValueFunction = fetchValueFunction;
            this._from = from;
            this._to = to;
        }

        public async IAsyncEnumerable<EventBatchWeighted> ComputePartition(ColumnRowReference partitionValues, WindowPartitionStartSearchComparer partitionStartSearchComparer)
        {
            Debug.Assert(_windowIterator != null);
            Debug.Assert(_updateIterator != null);
            Debug.Assert(_queue != null);
            Debug.Assert(_addOutputRow != null);
            Debug.Assert(_windowPartitionIterator != null);
            Debug.Assert(_updatePartitionIterator != null);

            await _queue.Clear();
            await _windowIterator.Seek(partitionValues, partitionStartSearchComparer);
            await _updateIterator.Seek(partitionValues, partitionStartSearchComparer);
            // Copy the seek result to the other iterator
            //_windowIterator.CloneSeekResultTo(_updateIterator);

            _windowPartitionIterator.Reset(partitionValues, _windowIterator, partitionStartSearchComparer);
            _updatePartitionIterator.Reset(partitionValues, _updateIterator, partitionStartSearchComparer);

            var windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            var updateEnumerator = _updatePartitionIterator.GetAsyncEnumerator();

            long updateRowIndex = 0;
            long windowRowIndex = 0;

            var currentValue = new DataValueContainer();
            currentValue._type = ArrowTypeId.Null;

            while (await updateEnumerator.MoveNextAsync())
            {
                while (windowRowIndex <= (updateRowIndex + _to) && await windowEnumerator.MoveNextAsync())
                {
                    var val = _fetchValueFunction(windowEnumerator.Current.Key.referenceBatch, windowEnumerator.Current.Key.RowIndex);
                    await _queue.Enqueue(val);
                    windowRowIndex++;
                    SumWindowUtils.DoSum(val, currentValue, 1);
                }

                while (_queue.Count > 0 && windowRowIndex - _queue.Count < updateRowIndex + _from)
                {
                    var firstVal = await _queue.Dequeue();
                    SumWindowUtils.DoSum(firstVal, currentValue, -1);
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

        public async Task Initialize(IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer> persistentTree, int partitionColumnCount, IMemoryAllocator memoryAllocator, IStateManagerClient stateManagerClient, IWindowAddOutputRow addOutputRow)
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
        }
    }

    internal class SumWindowFunctionBoundedUnboundedFrom : IWindowFunction
    {
        private IWindowAddOutputRow? _addOutputRow;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _updateIterator;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _updatePartitionIterator;
        private PartitionIterator? _windowPartitionIterator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _to;

        public SumWindowFunctionBoundedUnboundedFrom(Func<EventBatchData, int, IDataValue> fetchValueFunction, long to)
        {
            this._fetchValueFunction = fetchValueFunction;
            this._to = to;
        }

        public async IAsyncEnumerable<EventBatchWeighted> ComputePartition(ColumnRowReference partitionValues, WindowPartitionStartSearchComparer partitionStartSearchComparer)
        {
            Debug.Assert(_windowIterator != null);
            Debug.Assert(_updateIterator != null);
            Debug.Assert(_addOutputRow != null);
            Debug.Assert(_windowPartitionIterator != null);
            Debug.Assert(_updatePartitionIterator != null);

            await _windowIterator.Seek(partitionValues, partitionStartSearchComparer);
            await _updateIterator.Seek(partitionValues, partitionStartSearchComparer);
            // Copy the seek result to the other iterator
            //_windowIterator.CloneSeekResultTo(_updateIterator);

            _windowPartitionIterator.Reset(partitionValues, _windowIterator, partitionStartSearchComparer);
            _updatePartitionIterator.Reset(partitionValues, _updateIterator, partitionStartSearchComparer);

            var windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            var updateEnumerator = _updatePartitionIterator.GetAsyncEnumerator();

            long updateRowIndex = 0;
            long windowRowIndex = 0;

            var currentValue = new DataValueContainer();
            currentValue._type = ArrowTypeId.Null;

            while (await updateEnumerator.MoveNextAsync())
            {
                while (windowRowIndex <= (updateRowIndex + _to) && await windowEnumerator.MoveNextAsync())
                {
                    var val = _fetchValueFunction(windowEnumerator.Current.Key.referenceBatch, windowEnumerator.Current.Key.RowIndex);
                    windowRowIndex++;
                    SumWindowUtils.DoSum(val, currentValue, 1);
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

        public Task Initialize(IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer> persistentTree, int partitionColumnCount, IMemoryAllocator memoryAllocator, IStateManagerClient stateManagerClient, IWindowAddOutputRow addOutputRow)
        {
            _addOutputRow = addOutputRow;
            _windowIterator = persistentTree.CreateIterator();
            _updateIterator = persistentTree.CreateIterator();

            _updatePartitionIterator = new PartitionIterator(addOutputRow);
            _windowPartitionIterator = new PartitionIterator();

            return Task.CompletedTask;
        }
    }

    internal class SumWindowFunctionUnbounded : IWindowFunction
    {
        private IWindowAddOutputRow? _addOutputRow;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _updateIterator;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _updatePartitionIterator;
        private PartitionIterator? _windowPartitionIterator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;

        public SumWindowFunctionUnbounded(Func<EventBatchData, int, IDataValue> fetchValueFunction)
        {
            this._fetchValueFunction = fetchValueFunction;
        }

        public async IAsyncEnumerable<EventBatchWeighted> ComputePartition(ColumnRowReference partitionValues, WindowPartitionStartSearchComparer partitionStartSearchComparer)
        {
            Debug.Assert(_windowIterator != null);
            Debug.Assert(_updateIterator != null);
            Debug.Assert(_addOutputRow != null);
            Debug.Assert(_windowPartitionIterator != null);
            Debug.Assert(_updatePartitionIterator != null);

            await _windowIterator.Seek(partitionValues, partitionStartSearchComparer);
            await _updateIterator.Seek(partitionValues, partitionStartSearchComparer);
            // Copy the seek result to the other iterator
            //_windowIterator.CloneSeekResultTo(_updateIterator);

            _windowPartitionIterator.Reset(partitionValues, _windowIterator, partitionStartSearchComparer);
            _updatePartitionIterator.Reset(partitionValues, _updateIterator, partitionStartSearchComparer);

            var windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            var updateEnumerator = _updatePartitionIterator.GetAsyncEnumerator();

            var currentValue = new DataValueContainer();
            currentValue._type = ArrowTypeId.Null;

            while (await windowEnumerator.MoveNextAsync())
            {
                var val = _fetchValueFunction(windowEnumerator.Current.Key.referenceBatch, windowEnumerator.Current.Key.RowIndex);
                SumWindowUtils.DoSum(val, currentValue, 1);
            }

            while (await updateEnumerator.MoveNextAsync())
            {
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

        public Task Initialize(IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer> persistentTree, int partitionColumnCount, IMemoryAllocator memoryAllocator, IStateManagerClient stateManagerClient, IWindowAddOutputRow addOutputRow)
        {
            _addOutputRow = addOutputRow;
            _windowIterator = persistentTree.CreateIterator();
            _updateIterator = persistentTree.CreateIterator();

            _updatePartitionIterator = new PartitionIterator(addOutputRow);
            _windowPartitionIterator = new PartitionIterator();

            return Task.CompletedTask;
        }
    }
}

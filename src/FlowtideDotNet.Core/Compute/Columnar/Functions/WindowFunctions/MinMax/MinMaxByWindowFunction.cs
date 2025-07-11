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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Window;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.MinMax
{
    internal static class MinMaxBoundUtils
    {
        public static void GetBoundInfo(WindowFunction windowFunction, out bool isRowBounded, out long lowerBoundRowOffset, out long upperBoundRowOffset)
        {
            isRowBounded = false;
            lowerBoundRowOffset = long.MinValue;
            upperBoundRowOffset = long.MaxValue;
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
        }
    }
    internal class MinByWindowFunctionDefinition : WindowFunctionDefinition
    {
        public override IWindowFunction Create(WindowFunction windowFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[0], functionsRegister);
            var compiledCompareValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[1], functionsRegister);

            MinMaxBoundUtils.GetBoundInfo(windowFunction, out var isRowBounded, out var lowerBoundRowOffset, out var upperBoundRowOffset);
            
            if (isRowBounded && lowerBoundRowOffset != long.MinValue)
            {
                return new MinMaxByWindowFunctionBounded(compiledValue, compiledCompareValue, lowerBoundRowOffset, upperBoundRowOffset, true);
            }
            else if (isRowBounded && lowerBoundRowOffset == long.MinValue && upperBoundRowOffset == long.MaxValue)
            {
                return new MinMaxByWindowFunctionUnbounded(compiledValue, compiledCompareValue, true);
            }
            else if (isRowBounded && lowerBoundRowOffset == long.MinValue)
            {
                return new MinMaxByWindowFunctionUnboundedFrom(compiledValue, compiledCompareValue, upperBoundRowOffset, true);
            }

            throw new NotImplementedException();
        }
    }

    internal class MaxByWindowFunctionDefinition : WindowFunctionDefinition
    {
        public override IWindowFunction Create(WindowFunction windowFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[0], functionsRegister);
            var compiledCompareValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[1], functionsRegister);

            MinMaxBoundUtils.GetBoundInfo(windowFunction, out var isRowBounded, out var lowerBoundRowOffset, out var upperBoundRowOffset);

            if (isRowBounded && lowerBoundRowOffset != long.MinValue)
            {
                return new MinMaxByWindowFunctionBounded(compiledValue, compiledCompareValue, lowerBoundRowOffset, upperBoundRowOffset, false);
            }
            else if (isRowBounded && lowerBoundRowOffset == long.MinValue && upperBoundRowOffset == long.MaxValue)
            {
                return new MinMaxByWindowFunctionUnbounded(compiledValue, compiledCompareValue, false);
            }
            else if (isRowBounded && lowerBoundRowOffset == long.MinValue)
            {
                return new MinMaxByWindowFunctionUnboundedFrom(compiledValue, compiledCompareValue, upperBoundRowOffset, false);
            }

            throw new NotImplementedException();
        }
    }

    internal class MinMaxByWindowFunctionBounded : IWindowFunction
    {
        private IFlowtideQueue<MinMaxByIndexValue, MinMaxByIndexValueContainer>? _queue;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _windowPartitionIterator;
        private long _windowRowIndex = 0;
        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly long _from;
        private readonly long _to;
        private readonly bool _isMin;
        private Action? _returnAction;

        public MinMaxByWindowFunctionBounded(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            long from, 
            long to,
            bool isMin)
        {
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _from = from;
            _to = to;
            _isMin = isMin;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }
        public async ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_queue != null);
            Debug.Assert(_windowPartitionIterator != null);
            await _queue.Clear();

            await _windowPartitionIterator.Reset(partitionValues);
            _windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            _windowRowIndex = 0;
        }

        public async ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            Debug.Assert(_queue != null);
            Debug.Assert(_windowEnumerator != null);

            ReturnPageIfNeeded();

            var windowEnd = partitionRowIndex + _to;
            var windowStart = partitionRowIndex + _from;

            while (_windowRowIndex <= windowEnd && await _windowEnumerator.MoveNextAsync())
            {
                var compareVal = _fetchCompareValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);
                var val = _fetchValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);

                if (compareVal.IsNull)
                {
                    _windowRowIndex++;
                    continue;
                }

                while (_queue.Count > 0)
                {
                    var peekedVal = await _queue.PeekPop();
                    bool popped = false;
                    if (_isMin)
                    {
                        if (DataValueComparer.Instance.Compare(peekedVal.value.CompareValue, compareVal) > 0)
                        {
                            popped = true;
                            await _queue.Pop();
                        }
                    }
                    else
                    {
                        if (DataValueComparer.Instance.Compare(peekedVal.value.CompareValue, compareVal) < 0)
                        {
                            popped = true;
                            await _queue.Pop();
                        }
                    }
                    
                    if (peekedVal.returnFunc != null)
                    {
                        // Return the page, since we peeked across page boundries
                        peekedVal.returnFunc();
                    }
                    if (!popped)
                    {
                        break;
                    }
                }

                await _queue.Enqueue(new MinMaxByIndexValue()
                {
                    Index = _windowRowIndex,
                    Value = val,
                    CompareValue = compareVal
                });
                _windowRowIndex++;
            }

            MinMaxByIndexValue peekValue = default;

            while (_queue.Count > 0)
            {
                (peekValue, _returnAction) = await _queue.Peek();
                if (peekValue.Index < windowStart)
                {
                    await _queue.Dequeue();
                    ReturnPageIfNeeded();
                }
                else
                {
                    break;
                }
            }

            if (_queue.Count > 0)
            {
                return peekValue.Value!;
            }

            return NullValue.Instance;
        }

        public ValueTask EndPartition(ColumnRowReference partitionValues)
        {
            ReturnPageIfNeeded();
            return ValueTask.CompletedTask;
        }

        private void ReturnPageIfNeeded()
        {
            if (_returnAction != null)
            {
                _returnAction();
                _returnAction = null;
            }
        }

        public async Task Initialize(IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree, List<int> partitionColumns, IMemoryAllocator memoryAllocator, IStateManagerClient stateManagerClient)
        {
            if (persistentTree == null)
            {
                throw new ArgumentNullException(nameof(persistentTree));
            }
            _windowIterator = persistentTree.CreateIterator();

            _windowPartitionIterator = new PartitionIterator(_windowIterator, partitionColumns);

            _queue = await stateManagerClient.GetOrCreateQueue("queue", new FlowtideDotNet.Storage.Queue.FlowtideQueueOptions<MinMaxByIndexValue, MinMaxByIndexValueContainer>()
            {
                MemoryAllocator = memoryAllocator,
                ValueSerializer = new MinMaxByIndexValueContainerSerializer(memoryAllocator),
            });
        }
    }

    internal class MinMaxByWindowFunctionUnboundedFrom : IWindowFunction
    {
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _windowPartitionIterator;
        private long _windowRowIndex = 0;
        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly long _to;
        private readonly bool _isMin;

        /// <summary>
        /// Column used to store the minimum value.
        /// It is a column to store the actual data and not just a reference
        /// </summary>
        private Column? _minValueCompareColumn;
        private Column? _minValueColumn;

        public MinMaxByWindowFunctionUnboundedFrom(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            long to,
            bool isMin)
        {
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _to = to;
            _isMin = isMin;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_windowPartitionIterator != null);

            await _windowPartitionIterator.Reset(partitionValues);
            _windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            _windowRowIndex = 0;
        }

        public async ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            Debug.Assert(_windowEnumerator != null);
            Debug.Assert(_minValueColumn != null);
            Debug.Assert(_minValueCompareColumn != null);

            var windowEnd = partitionRowIndex + _to;

            var currentVal = _minValueCompareColumn.GetValueAt(0, default);
            while (_windowRowIndex <= windowEnd && await _windowEnumerator.MoveNextAsync())
            {
                var compareVal = _fetchCompareValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);
                var val = _fetchValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);

                if (_isMin)
                {
                    if (currentVal.IsNull || DataValueComparer.Instance.Compare(compareVal, currentVal) < 0)
                    {
                        _minValueColumn.UpdateAt(0, val);
                        _minValueCompareColumn.UpdateAt(0, compareVal);
                        // Use reference from the column instead of the fetched value since it can leave memory
                        currentVal = _minValueCompareColumn.GetValueAt(0, default);
                    }
                }
                else
                {
                    if (currentVal.IsNull || DataValueComparer.Instance.Compare(compareVal, currentVal) > 0)
                    {
                        _minValueColumn.UpdateAt(0, val);
                        _minValueCompareColumn.UpdateAt(0, compareVal);
                        // Use reference from the column instead of the fetched value since it can leave memory
                        currentVal = _minValueCompareColumn.GetValueAt(0, default);
                    }
                }
                

                _windowRowIndex++;
            }

            return currentVal;
        }

        public ValueTask EndPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_minValueColumn != null);
            Debug.Assert(_minValueCompareColumn != null);

            _minValueColumn.UpdateAt(0, NullValue.Instance);
            _minValueCompareColumn.UpdateAt(0, NullValue.Instance);
            return ValueTask.CompletedTask;
        }

        public Task Initialize(IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree, List<int> partitionColumns, IMemoryAllocator memoryAllocator, IStateManagerClient stateManagerClient)
        {
            if (persistentTree == null)
            {
                throw new ArgumentNullException(nameof(persistentTree));
            }
            _windowIterator = persistentTree.CreateIterator();

            _windowPartitionIterator = new PartitionIterator(_windowIterator, partitionColumns);

            _minValueCompareColumn = Column.Create(memoryAllocator);
            _minValueColumn = Column.Create(memoryAllocator);

            // Add null to both
            _minValueCompareColumn.Add(NullValue.Instance);
            _minValueColumn.Add(NullValue.Instance);

            return Task.CompletedTask;
        }
    }

    internal class MinMaxByWindowFunctionUnbounded : IWindowFunction
    {
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _windowPartitionIterator;
        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly bool _isMin;

        /// <summary>
        /// Column used to store the minimum value.
        /// It is a column to store the actual data and not just a reference
        /// </summary>
        private Column? _minValueCompareColumn;
        private Column? _minValueColumn;

        public MinMaxByWindowFunctionUnbounded(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            bool isMin)
        {
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _isMin = isMin;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_windowPartitionIterator != null);
            Debug.Assert(_minValueColumn != null);
            Debug.Assert(_minValueCompareColumn != null);

            await _windowPartitionIterator.Reset(partitionValues);
            _windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();

            var currentVal = _minValueCompareColumn.GetValueAt(0, default);
            while (await _windowEnumerator.MoveNextAsync())
            {
                var compareVal = _fetchCompareValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);
                var val = _fetchValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);

                if (_isMin)
                {
                    if (currentVal.IsNull || DataValueComparer.Instance.Compare(compareVal, currentVal) < 0)
                    {
                        _minValueColumn.UpdateAt(0, val);
                        _minValueCompareColumn.UpdateAt(0, compareVal);
                        // Use reference from the column instead of the fetched value since it can leave memory
                        currentVal = _minValueCompareColumn.GetValueAt(0, default);
                    }
                }
                else
                {
                    if (currentVal.IsNull || DataValueComparer.Instance.Compare(compareVal, currentVal) > 0)
                    {
                        _minValueColumn.UpdateAt(0, val);
                        _minValueCompareColumn.UpdateAt(0, compareVal);
                        // Use reference from the column instead of the fetched value since it can leave memory
                        currentVal = _minValueCompareColumn.GetValueAt(0, default);
                    }
                }
                
            }
        }

        public ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            Debug.Assert(_minValueColumn != null);
            return ValueTask.FromResult(_minValueColumn.GetValueAt(0, default));
        }

        public ValueTask EndPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_minValueColumn != null);
            Debug.Assert(_minValueCompareColumn != null);

            _minValueColumn.UpdateAt(0, NullValue.Instance);
            _minValueCompareColumn.UpdateAt(0, NullValue.Instance);
            return ValueTask.CompletedTask;
        }

        public Task Initialize(IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree, List<int> partitionColumns, IMemoryAllocator memoryAllocator, IStateManagerClient stateManagerClient)
        {
            if (persistentTree == null)
            {
                throw new ArgumentNullException(nameof(persistentTree));
            }
            _windowIterator = persistentTree.CreateIterator();

            _windowPartitionIterator = new PartitionIterator(_windowIterator, partitionColumns);

            _minValueCompareColumn = Column.Create(memoryAllocator);
            _minValueColumn = Column.Create(memoryAllocator);

            // Add null to both
            _minValueCompareColumn.Add(NullValue.Instance);
            _minValueColumn.Add(NullValue.Instance);

            return Task.CompletedTask;
        }
    }
}

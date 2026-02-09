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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Window;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions
{
    internal class AverageWindowFunctionDefinition : WindowFunctionDefinition
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
                return new AverageWindowFunctionBounded(compiledValue, lowerBoundRowOffset, upperBoundRowOffset);
            }
            else if (isRowBounded && lowerBoundRowOffset == long.MinValue && upperBoundRowOffset == long.MaxValue)
            {
                return new AverageWindowFunctionUnbounded(compiledValue);
            }
            else if (isRowBounded && lowerBoundRowOffset == long.MinValue)
            {
                return new AverageWindowUnboundedFrom(compiledValue, upperBoundRowOffset);
            }

            return new AverageWindowFunctionUnbounded(compiledValue);
        }
    }

    internal static class AverageWindowUtils
    {
        internal static IDataValue DivideWithCount<T>(T value, long count)
            where T : IDataValue
        {
            if (count == 0)
            {
                return NullValue.Instance;
            }
            if (value.Type == ArrowTypeId.Int64)
            {
                var sum = value.AsLong;
                return new DoubleValue((double)sum / count);
            }
            else if (value.Type == ArrowTypeId.Double)
            {
                var floatSum = value.AsDouble;
                return new DoubleValue(floatSum / count);
            }
            else if (value.Type == ArrowTypeId.Decimal128)
            {
                var sum = value.AsDecimal;
                return new DecimalValue(sum / count);
            }
            return NullValue.Instance;
        }

        internal static void ModifyCount(ref long count, int addition, ArrowTypeId valueType)
        {
            if (valueType == ArrowTypeId.Int64 || valueType == ArrowTypeId.Double || valueType == ArrowTypeId.Decimal128)
            {
                count += addition;
            }
        }
    }

    internal class AverageWindowFunctionBounded : IWindowFunction
    {
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private IFlowtideQueue<IDataValue, DataValueValueContainer>? _queue;
        private PartitionIterator? _windowPartitionIterator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;
        private readonly long _to;

        public AverageWindowFunctionBounded(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from, long to)
        {
            this._fetchValueFunction = fetchValueFunction;
            this._from = from;
            this._to = to;
        }

        private long _windowRowIndex = 0;
        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private readonly DataValueContainer _currentSumContainer = new DataValueContainer();
        private long _countCounter = 0;

        public async ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_queue != null);
            Debug.Assert(_windowPartitionIterator != null);
            await _queue.Clear();

            await _windowPartitionIterator.Reset(partitionValues);
            _windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            _windowRowIndex = 0;
            _currentSumContainer._type = ArrowTypeId.Null;
            _countCounter = 0;
        }

        public async ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            Debug.Assert(_queue != null);
            Debug.Assert(_windowEnumerator != null);

            long updateRowIndex = partitionRowIndex;

            while (_windowRowIndex <= (updateRowIndex + _to) && await _windowEnumerator.MoveNextAsync())
            {
                var val = _fetchValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);
                await _queue.Enqueue(val);
                _windowRowIndex++;
                SumWindowUtils.DoSum(val, _currentSumContainer, 1);
                AverageWindowUtils.ModifyCount(ref _countCounter, 1, val.Type);
            }

            while (_queue.Count > 0 && _windowRowIndex - _queue.Count < updateRowIndex + _from)
            {
                var firstVal = await _queue.Dequeue();
                SumWindowUtils.DoSum(firstVal, _currentSumContainer, -1);
                AverageWindowUtils.ModifyCount(ref _countCounter, -1, firstVal.Type);
            }

            return AverageWindowUtils.DivideWithCount(_currentSumContainer, _countCounter);
        }

        public async Task Initialize(
            IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree,
            List<int> partitionColumns,
            IMemoryAllocator memoryAllocator,
            IStateManagerClient stateManagerClient)
        {
            if (persistentTree == null)
            {
                throw new ArgumentNullException(nameof(persistentTree));
            }
            _windowIterator = persistentTree.CreateIterator();

            _windowPartitionIterator = new PartitionIterator(_windowIterator, partitionColumns);

            _queue = await stateManagerClient.GetOrCreateQueue("queue", new FlowtideQueueOptions<IDataValue, DataValueValueContainer>()
            {
                MemoryAllocator = memoryAllocator,
                ValueSerializer = new DataValueValueContainerSerializer(memoryAllocator)
            });
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask EndPartition(ColumnRowReference partitionValues)
        {
            return ValueTask.CompletedTask;
        }
    }

    internal class AverageWindowUnboundedFrom : IWindowFunction
    {
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _windowPartitionIterator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _to;
        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private long _windowRowIndex = 0;
        private readonly DataValueContainer _currentSumContainer = new DataValueContainer();
        private long _countCounter = 0;

        public AverageWindowUnboundedFrom(Func<EventBatchData, int, IDataValue> fetchValueFunction, long to)
        {
            this._fetchValueFunction = fetchValueFunction;
            this._to = to;
        }

        public async ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_windowPartitionIterator != null);

            await _windowPartitionIterator.Reset(partitionValues);
            _windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            _windowRowIndex = 0;
            _currentSumContainer._type = ArrowTypeId.Null;
            _countCounter = 0;
        }

        public async ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            Debug.Assert(_windowPartitionIterator != null);
            Debug.Assert(_windowEnumerator != null);

            long updateRowIndex = partitionRowIndex;

            while (_windowRowIndex <= (updateRowIndex + _to) && await _windowEnumerator.MoveNextAsync())
            {
                var val = _fetchValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);
                _windowRowIndex++;
                SumWindowUtils.DoSum(val, _currentSumContainer, 1);
                AverageWindowUtils.ModifyCount(ref _countCounter, 1, val.Type);
            }

            return AverageWindowUtils.DivideWithCount(_currentSumContainer, _countCounter);
        }

        public Task Initialize(IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree, List<int> partitionColumns, IMemoryAllocator memoryAllocator, IStateManagerClient stateManagerClient)
        {
            if (persistentTree == null)
            {
                throw new ArgumentNullException(nameof(persistentTree));
            }
            _windowIterator = persistentTree.CreateIterator();
            _windowPartitionIterator = new PartitionIterator(_windowIterator, partitionColumns);

            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask EndPartition(ColumnRowReference partitionValues)
        {
            return ValueTask.CompletedTask;
        }
    }

    internal class AverageWindowFunctionUnbounded : IWindowFunction
    {
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _windowPartitionIterator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private readonly DataValueContainer _currentSumContainer = new DataValueContainer();
        private long _countCounter = 0;

        public AverageWindowFunctionUnbounded(Func<EventBatchData, int, IDataValue> fetchValueFunction)
        {
            this._fetchValueFunction = fetchValueFunction;
        }

        public async ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_windowPartitionIterator != null);

            await _windowPartitionIterator.Reset(partitionValues);
            _windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            _currentSumContainer._type = ArrowTypeId.Null;
            _countCounter = 0;

            while (await _windowEnumerator.MoveNextAsync())
            {
                var val = _fetchValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);
                SumWindowUtils.DoSum(val, _currentSumContainer, 1);
                AverageWindowUtils.ModifyCount(ref _countCounter, 1, val.Type);
            }
        }

        public ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            return ValueTask.FromResult<IDataValue>(AverageWindowUtils.DivideWithCount(_currentSumContainer, _countCounter));
        }

        public Task Initialize(
            IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree,
            List<int> partitionColumns,
            IMemoryAllocator memoryAllocator,
            IStateManagerClient stateManagerClient)
        {
            if (persistentTree == null)
            {
                throw new ArgumentNullException(nameof(persistentTree));
            }

            _windowIterator = persistentTree.CreateIterator();

            _windowPartitionIterator = new PartitionIterator(_windowIterator, partitionColumns);

            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask EndPartition(ColumnRowReference partitionValues)
        {
            return ValueTask.CompletedTask;
        }
    }
}

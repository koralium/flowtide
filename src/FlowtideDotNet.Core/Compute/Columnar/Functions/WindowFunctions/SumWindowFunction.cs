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

using FlowtideDotNet.Base.Utils;
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
using System.Collections;
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
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private IFlowtideQueue<IDataValue, DataValueValueContainer>? _queue;
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

        private long _windowRowIndex = 0;
        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private DataValueContainer _currentValueContainer = new DataValueContainer();

        public async ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_queue != null);
            Debug.Assert(_windowPartitionIterator != null);
            await _queue.Clear();

            await _windowPartitionIterator.Reset(partitionValues);
            _windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            _windowRowIndex = 0;
            _currentValueContainer._type = ArrowTypeId.Null;
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
                SumWindowUtils.DoSum(val, _currentValueContainer, 1);
            }

            while (_queue.Count > 0 && _windowRowIndex - _queue.Count < updateRowIndex + _from)
            {
                var firstVal = await _queue.Dequeue();
                SumWindowUtils.DoSum(firstVal, _currentValueContainer, -1);
            }

            return _currentValueContainer;
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

    internal class SumWindowFunctionBoundedUnboundedFrom : IWindowFunction
    {
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _windowPartitionIterator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _to;
        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private long _windowRowIndex = 0;
        private DataValueContainer _currentValueContainer = new DataValueContainer();

        public SumWindowFunctionBoundedUnboundedFrom(Func<EventBatchData, int, IDataValue> fetchValueFunction, long to)
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
            _currentValueContainer._type = ArrowTypeId.Null;
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
                SumWindowUtils.DoSum(val, _currentValueContainer, 1);
            }

            return _currentValueContainer;
        }


        public Task Initialize(
            IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree, 
            List<int> partitionColumns, IMemoryAllocator memoryAllocator, IStateManagerClient stateManagerClient)
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

    internal class SumWindowFunctionUnbounded : IWindowFunction
    {
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _windowPartitionIterator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private DataValueContainer _currentValueContainer = new DataValueContainer();

        public SumWindowFunctionUnbounded(Func<EventBatchData, int, IDataValue> fetchValueFunction)
        {
            this._fetchValueFunction = fetchValueFunction;
        }

        public async ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_windowPartitionIterator != null);

            await _windowPartitionIterator.Reset(partitionValues);
            _windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            _currentValueContainer._type = ArrowTypeId.Null;

            while (await _windowEnumerator.MoveNextAsync())
            {
                var val = _fetchValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);
                SumWindowUtils.DoSum(val, _currentValueContainer, 1);
            }
        }

        public ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            return ValueTask.FromResult<IDataValue>(_currentValueContainer);
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

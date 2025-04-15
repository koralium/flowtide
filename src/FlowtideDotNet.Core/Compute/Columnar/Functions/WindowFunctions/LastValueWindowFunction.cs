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
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions
{
    internal class LastValueWindowFunctionDefinition : WindowFunctionDefinition
    {
        public override IWindowFunction Create(WindowFunction windowFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[0], functionsRegister);

            bool ignoreNull = false;
            if (windowFunction.Options != null &&
                windowFunction.Options.TryGetValue("NULL_TREATMENT", out var ignoreNullStr) &&
                ignoreNullStr.Equals("IGNORE_NULLS", StringComparison.OrdinalIgnoreCase))
            {
                ignoreNull = true;
            }

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
                if (ignoreNull)
                {
                    return new LastValueIgnoreNullWindowFunctionBounded(compiledValue, lowerBoundRowOffset, upperBoundRowOffset);
                }
            }

            throw new NotImplementedException();
        }
    }

    internal class LastValueIgnoreNullWindowFunctionBounded : IWindowFunction
    {
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _windowPartitionIterator;
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;
        private readonly long _to;
        private long _windowRowIndex = 0;
        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private DataValueContainer _currentValueContainer = new DataValueContainer();

        private long _lastNonNullIndex = -1;

        public LastValueIgnoreNullWindowFunctionBounded(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from, long to)
        {
            this._fetchValueFunction = fetchValueFunction;
            this._from = from;
            this._to = to;
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
            _currentValueContainer._type = ArrowTypeId.Null;
            _lastNonNullIndex = -1;
        }

        public async ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            Debug.Assert(_windowEnumerator != null);

            long updateRowIndex = partitionRowIndex;

            while (_windowRowIndex <= (updateRowIndex + _to) && await _windowEnumerator.MoveNextAsync())
            {
                var val = _fetchValueFunction(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);

                if (!val.IsNull)
                {
                    val.CopyToContainer(_currentValueContainer);
                    _lastNonNullIndex = _windowRowIndex;
                }

                _windowRowIndex++;
            }

            long frameStart = updateRowIndex + _from;
            if (_lastNonNullIndex < frameStart)
            {
                _currentValueContainer._type = ArrowTypeId.Null;
            }

            return _currentValueContainer;
        }

        public ValueTask EndPartition(ColumnRowReference partitionValues)
        {
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
            return Task.CompletedTask;
        }
    }
}

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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Window;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions
{
    internal class LagWindowFunctionDefinition : WindowFunctionDefinition
    {
        public override IWindowFunction Create(WindowFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            if (aggregateFunction.Arguments.Count < 1)
            {
                throw new ArgumentException("lag function requires at least one argument");
            }

            var lagValueFunc = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[0], functionsRegister);

            Func<EventBatchData, int, IDataValue>? lagOffsetFunc = default;
            if (aggregateFunction.Arguments.Count > 1)
            {
                lagOffsetFunc = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[1], functionsRegister);
            }

            Func<EventBatchData, int, IDataValue>? defaultFunc = default;
            if (aggregateFunction.Arguments.Count > 2)
            {
                defaultFunc = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[2], functionsRegister);
            }

            return new LagWindowFunction(lagValueFunc, lagOffsetFunc, defaultFunc);
        }
    }
    internal class LagWindowFunction : IWindowFunction
    {
        private IWindowAddOutputRow? _addOutputRow;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _updateIterator;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _updatePartitionIterator;
        private PartitionIterator? _windowPartitionIterator;

        private readonly Func<EventBatchData, int, IDataValue> _lagValueFunc;
        private readonly Func<EventBatchData, int, IDataValue>? _lagOffsetFunc;
        private readonly Func<EventBatchData, int, IDataValue>? _defaultValueFunc;

        public bool RequirePartitionCompute => true;

        public LagWindowFunction(
            Func<EventBatchData, int, IDataValue> lagValueFunc,
            Func<EventBatchData, int, IDataValue>? lagOffsetFunc,
            Func<EventBatchData, int, IDataValue>? defaultValueFunc)
        {
            _lagValueFunc = lagValueFunc;
            _lagOffsetFunc = lagOffsetFunc;
            _defaultValueFunc = defaultValueFunc;
        }

        public async IAsyncEnumerable<EventBatchWeighted> ComputePartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_addOutputRow != null);
            Debug.Assert(_windowPartitionIterator != null);
            Debug.Assert(_updatePartitionIterator != null);

            await _windowPartitionIterator.Reset(partitionValues);
            _updatePartitionIterator.ResetCopyFrom(_windowPartitionIterator);

            var windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
            var updateEnumerator = _updatePartitionIterator.GetAsyncEnumerator();

            long updateRowIndex = 0;
            long windowRowIndex = 0;

            var currentValue = new DataValueContainer();
            currentValue._type = ArrowTypeId.Null;

            while (await updateEnumerator.MoveNextAsync())
            {
                int rowOffset = 1;
                if (_lagOffsetFunc != null)
                {
                    var offsetValue = _lagOffsetFunc(updateEnumerator.Current.Key.referenceBatch, updateEnumerator.Current.Key.RowIndex);
                    if (offsetValue is Int64Value int64Value)
                    {
                        rowOffset = (int)int64Value.AsLong;
                    }
                }

                long targetRowIndex = updateRowIndex - rowOffset;

                if (targetRowIndex < 0)
                {
                    // Smaller than 0, use default value
                    IDataValue defaultValue = _defaultValueFunc != null
                        ? _defaultValueFunc(updateEnumerator.Current.Key.referenceBatch, updateEnumerator.Current.Key.RowIndex)
                        : NullValue.Instance;

                    updateEnumerator.Current.Value.UpdateStateValue(defaultValue);
                }
                else
                {
                    if (windowRowIndex > targetRowIndex)
                    {
                        // Reset to beginning if window is ahead of where we need to be
                        _windowPartitionIterator.ResetCopyFrom(_updatePartitionIterator);
                        windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
                        windowRowIndex = 0;
                    }

                    bool movedNext = false;
                    while (windowRowIndex <= targetRowIndex)
                    {
                        movedNext = await windowEnumerator.MoveNextAsync();
                        if (!movedNext)
                        {
                            break;
                        }
                        windowRowIndex++;
                    }

                    IDataValue val;
                    if (!movedNext || windowRowIndex - 1 != targetRowIndex)
                    {
                        val = _defaultValueFunc != null
                            ? _defaultValueFunc(updateEnumerator.Current.Key.referenceBatch, updateEnumerator.Current.Key.RowIndex)
                            : NullValue.Instance;
                    }
                    else
                    {
                        val = _lagValueFunc(windowEnumerator.Current.Key.referenceBatch, windowEnumerator.Current.Key.RowIndex);
                    }
                    updateEnumerator.Current.Value.UpdateStateValue(val);
                }

                updateRowIndex++;

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

        public Task Initialize(IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree, List<int> partitionColumns, IMemoryAllocator memoryAllocator, IStateManagerClient stateManagerClient, IWindowAddOutputRow addOutputRow)
        {
            if (persistentTree == null)
            {
                throw new ArgumentNullException(nameof(persistentTree));
            }
            _addOutputRow = addOutputRow;
            _windowIterator = persistentTree.CreateIterator();
            _updateIterator = persistentTree.CreateIterator();

            _updatePartitionIterator = new PartitionIterator(_updateIterator, partitionColumns, addOutputRow);
            _windowPartitionIterator = new PartitionIterator(_windowIterator, partitionColumns);

            return Task.CompletedTask;
        }

        public IAsyncEnumerable<EventBatchWeighted> OnReceive(ColumnRowReference partitionValues, ColumnRowReference inputRow, int weight)
        {
            return EmptyAsyncEnumerable<EventBatchWeighted>.Instance;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }
    }
}

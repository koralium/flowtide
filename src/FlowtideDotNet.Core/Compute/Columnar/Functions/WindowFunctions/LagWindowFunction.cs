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
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _windowIterator;
        private PartitionIterator? _windowPartitionIterator;

        private readonly Func<EventBatchData, int, IDataValue> _lagValueFunc;
        private readonly Func<EventBatchData, int, IDataValue>? _lagOffsetFunc;
        private readonly Func<EventBatchData, int, IDataValue>? _defaultValueFunc;

        private IAsyncEnumerator<KeyValuePair<ColumnRowReference, WindowStateReference>>? _windowEnumerator;
        private long _windowRowIndex;
        private DataValueContainer _valueContainer = new DataValueContainer();
        private ColumnRowReference _partitionStartValues;

        public LagWindowFunction(
            Func<EventBatchData, int, IDataValue> lagValueFunc,
            Func<EventBatchData, int, IDataValue>? lagOffsetFunc,
            Func<EventBatchData, int, IDataValue>? defaultValueFunc)
        {
            _lagValueFunc = lagValueFunc;
            _lagOffsetFunc = lagOffsetFunc;
            _defaultValueFunc = defaultValueFunc;
        }

        public async ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_windowPartitionIterator != null);

            await _windowPartitionIterator.Reset(partitionValues);
            _partitionStartValues = partitionValues;
            _windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();

            _windowRowIndex = 0;
            _valueContainer._type = ArrowTypeId.Null;
        }

        public async ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            Debug.Assert(_windowPartitionIterator != null);
            Debug.Assert(_windowEnumerator != null);

            var updateRowIndex = partitionRowIndex;

            int rowOffset = 1;
            if (_lagOffsetFunc != null)
            {
                var offsetValue = _lagOffsetFunc(row.Key.referenceBatch, row.Key.RowIndex);
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
                    ? _defaultValueFunc(row.Key.referenceBatch, row.Key.RowIndex)
                    : NullValue.Instance;

                return defaultValue;
            }
            else
            {
                if (_windowRowIndex > targetRowIndex)
                {
                    // Reset to beginning if window is ahead of where we need to be
                    await _windowPartitionIterator.Reset(_partitionStartValues);
                    _windowEnumerator = _windowPartitionIterator.GetAsyncEnumerator();
                    _windowRowIndex = 0;
                }

                bool movedNext = false;
                while (_windowRowIndex <= targetRowIndex)
                {
                    movedNext = await _windowEnumerator.MoveNextAsync();
                    if (!movedNext)
                    {
                        break;
                    }
                    _windowRowIndex++;
                }

                IDataValue val;
                if (!movedNext || _windowRowIndex - 1 != targetRowIndex)
                {
                    val = _defaultValueFunc != null
                        ? _defaultValueFunc(row.Key.referenceBatch, row.Key.RowIndex)
                        : NullValue.Instance;
                }
                else
                {
                    val = _lagValueFunc(_windowEnumerator.Current.Key.referenceBatch, _windowEnumerator.Current.Key.RowIndex);
                }
                return val;
            }
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
}

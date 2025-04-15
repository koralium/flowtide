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
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions
{
    internal class RowNumberWindowFunctionDefinition : WindowFunctionDefinition
    {
        public override IWindowFunction Create(WindowFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            return new RowNumberWindowFunction();
        }
    }
    internal class RowNumberWindowFunction : IWindowFunction
    {
        private IWindowAddOutputRow? _addOutputRow;
        private IBPlusTreeIterator<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? _updateIterator;
        private PartitionIterator? _updatePartitionIterator;

        public bool RequirePartitionCompute => true;

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async IAsyncEnumerable<EventBatchWeighted> ComputePartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_updatePartitionIterator != null);
            Debug.Assert(_addOutputRow != null);

            await _updatePartitionIterator.Reset(partitionValues);
            var updateEnumerator = _updatePartitionIterator.GetAsyncEnumerator();

            long updateRowIndex = 1;

            while (await updateEnumerator.MoveNextAsync())
            {
                var current = updateEnumerator.Current;

                updateEnumerator.Current.Value.UpdateStateValue(new Int64Value(updateRowIndex));
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

        public ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            throw new NotImplementedException();
        }

        public Task Initialize(
            IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree, 
            List<int> partitionColumns, 
            IMemoryAllocator memoryAllocator, 
            IStateManagerClient stateManagerClient, 
            IWindowAddOutputRow addOutputRow)
        {
            if (persistentTree == null)
            {
                throw new ArgumentNullException(nameof(persistentTree));
            }

            _addOutputRow = addOutputRow;
            _updateIterator = persistentTree.CreateIterator();

            _updatePartitionIterator = new PartitionIterator(_updateIterator, partitionColumns, addOutputRow);

            return Task.CompletedTask;
        }

        public ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            throw new NotImplementedException();
        }

        public IAsyncEnumerable<EventBatchWeighted> OnReceive(ColumnRowReference partitionValues, ColumnRowReference inputRow, int weight)
        {
            return EmptyAsyncEnumerable<EventBatchWeighted>.Instance;
        }
    }
}

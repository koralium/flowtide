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
using System.Diagnostics;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.SurrogateKey
{
    internal class SurrogateKeyInt64WindowFunctionDefinition : WindowFunctionDefinition
    {
        public override IWindowFunction Create(WindowFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            return new SurrogateKeyInt64Function();
        }
    }

    internal class SurrogateKeyInt64Function : IWindowFunction
    {
        private IBPlusTree<ColumnRowReference, SurrogateKeyValue, ColumnKeyStorageContainer, SurrogateKeyValueContainer>? _tree;
        private IObjectState<long>? _keyCounterState;
        private IDataValue? _currentValue;
        private bool _rowFound = false;

        public bool RequirePartitionCompute => false;

        public async ValueTask Commit()
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_keyCounterState != null);

            await _tree.Commit();
            await _keyCounterState.Commit();
        }

        public ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            Debug.Assert(_currentValue != null);

            _rowFound = true;

            return ValueTask.FromResult(_currentValue);
        }

        public async ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_keyCounterState != null);

            _rowFound = false;
            var result = await _tree.GetValue(in partitionValues);
            
            if (result.found)
            {
                _currentValue = result.value.Value;
            }
            else
            {
                _currentValue = new Int64Value(_keyCounterState.Value++);
                await _tree.Upsert(partitionValues, new SurrogateKeyValue()
                {
                    Value = _currentValue,
                    Weight = 1
                });
            }
        }

        public async ValueTask EndPartition(ColumnRowReference partitionValues)
        {
            Debug.Assert(_tree != null);
            if (!_rowFound)
            {
                // Cleanup the tree if no rows exist in the partition
                await _tree.Delete(partitionValues);
            }
        }

        public async Task Initialize(
            IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree, 
            List<int> partitionColumns, 
            IMemoryAllocator memoryAllocator, 
            IStateManagerClient stateManagerClient)
        {
            _tree = await stateManagerClient.GetOrCreateTree("partitions", 
                new BPlusTreeOptions<ColumnRowReference, SurrogateKeyValue, ColumnKeyStorageContainer, SurrogateKeyValueContainer>()
                {
                    Comparer = new ColumnComparer(partitionColumns.Count),
                    KeySerializer = new ColumnStoreSerializer(partitionColumns.Count, memoryAllocator),
                    ValueSerializer = new SurrogateKeyValueContainerSerializer(memoryAllocator),
                    MemoryAllocator = memoryAllocator
                });
            _keyCounterState = await stateManagerClient.GetOrCreateObjectStateAsync<long>("key_counter");
        }
    }
}

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
        private IBPlusTreeIterator<ColumnRowReference, SurrogateKeyValue, ColumnKeyStorageContainer, SurrogateKeyValueContainer>? _iterator;
        private IWindowAddOutputRow? _addOutputRow;
        private IObjectState<long>? _keyCounterState;

        public bool RequirePartitionCompute => false;

        public async ValueTask Commit()
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_keyCounterState != null);

            await _tree.Commit();
            await _keyCounterState.Commit();
        }

        public IAsyncEnumerable<EventBatchWeighted> ComputePartition(ColumnRowReference partitionValues)
        {
            return EmptyAsyncEnumerable<EventBatchWeighted>.Instance;
        }

        public ValueTask<IDataValue> ComputeRow(KeyValuePair<ColumnRowReference, WindowStateReference> row, long partitionRowIndex)
        {
            throw new NotImplementedException();
        }

        public async Task Initialize(
            IBPlusTree<ColumnRowReference, WindowValue, ColumnKeyStorageContainer, WindowValueContainer>? persistentTree, 
            List<int> partitionColumns, 
            IMemoryAllocator memoryAllocator, 
            IStateManagerClient stateManagerClient, 
            IWindowAddOutputRow addOutputRow)
        {
            _addOutputRow = addOutputRow;
            _tree = await stateManagerClient.GetOrCreateTree("partitions", 
                new BPlusTreeOptions<ColumnRowReference, SurrogateKeyValue, ColumnKeyStorageContainer, SurrogateKeyValueContainer>()
                {
                    Comparer = new ColumnComparer(partitionColumns.Count),
                    KeySerializer = new ColumnStoreSerializer(partitionColumns.Count, memoryAllocator),
                    ValueSerializer = new SurrogateKeyValueContainerSerializer(memoryAllocator),
                    MemoryAllocator = memoryAllocator
                });
            _iterator = _tree.CreateIterator();
            _keyCounterState = await stateManagerClient.GetOrCreateObjectStateAsync<long>("key_counter");
        }

        public ValueTask NewPartition(ColumnRowReference partitionValues)
        {
            throw new NotImplementedException();
        }

        public async IAsyncEnumerable<EventBatchWeighted> OnReceive(
            ColumnRowReference partitionValues, 
            ColumnRowReference inputRow, 
            int weight)
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_addOutputRow != null);
            Debug.Assert(_keyCounterState != null);

            var inputValue = new SurrogateKeyValue()
            {
                Value = NullValue.Instance,
                Weight = weight
            };
            await _tree.RMWNoResult(partitionValues, inputValue, (input, current, exists) =>
            {
                if (exists)
                {
                    var value = current.Value;
                    _addOutputRow.AddOutputRow(inputRow, value, input.Weight);
                    var newWeight = current.Weight + input.Weight;
                    current.Weight = newWeight;
                    if (newWeight == 0)
                    {
                        return (current, GenericWriteOperation.Delete);
                    }

                    return (current, GenericWriteOperation.Upsert);
                }
                else
                {
                    var newKey = _keyCounterState.Value++;
                    var newValue = new SurrogateKeyValue()
                    {
                        Value = new Int64Value(newKey),
                        Weight = input.Weight
                    };
                    _addOutputRow.AddOutputRow(inputRow, newValue.Value, input.Weight);
                    return (newValue, GenericWriteOperation.Upsert);
                }
            });
            
            if (_addOutputRow.Count >= 100)
            {
                yield return _addOutputRow.GetCurrentBatch();
            }
        }
    }
}

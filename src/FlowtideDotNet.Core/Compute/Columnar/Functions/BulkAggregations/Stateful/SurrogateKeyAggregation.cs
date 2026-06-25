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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful
{
    internal class SurrogateKeyAggregationDefinition : IBulkAggregationDefinition
    {
        public IColumnBulkAggregation Create(AggregateFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            return new SurrogateKeyAggregation();
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkAggregationFunction(FunctionsAggregateGeneric.Uri, FunctionsAggregateGeneric.SurrogateKeyInt64, new SurrogateKeyAggregationDefinition());
        }
    }

    internal class SurrogateKeyAggregation : IColumnBulkAggregation
    {
        private IObjectState<long>? _state;

        public async Task CommitAsync()
        {
            Debug.Assert(_state != null);
            await _state.Commit();
        }

        public bool Compute(ReadOnlySpan<int> indices, PrimitiveList<int> weights, EventBatchData data, ColumnReference groupState, int sortedIndex)
        {
            Debug.Assert(_state != null);

            var stateValue = groupState.GetValue();

            if (stateValue.IsNull)
            {
                var nextId = _state.Value++;
                groupState.Update(new Int64Value(nextId));
                return true;
            }
            else
            {
                return false;
            }
        }

        public ValueTask FetchValuesAsync(IColumn[] groupingValuesSorted, int length, Column outputColumn)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, int startIndex, int length, Column outputColumn)
        {
            Debug.Assert(_state != null);
            for (int i = startIndex; i < startIndex + length; i++)
            {
                var groupState = groupStates[i];
                var stateValue = groupState.GetValue();
                if (stateValue.IsNull)
                {
                    // The group has no assigned key yet. This happens for the synthetic group that the
                    // operator inserts for a groupless aggregate over empty input (Compute is never run
                    // for it). Assign a surrogate key here so the scalar aggregate still emits one row,
                    // mirroring Compute, instead of throwing.
                    var nextId = _state!.Value++;
                    groupState.Update(new Int64Value(nextId));
                    outputColumn.Add(new Int64Value(nextId));
                }
                else
                {
                    outputColumn.Add(stateValue);
                }
            }
            return ValueTask.CompletedTask;
        }

        public async Task InitializeAsync(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            _state = await stateManagerClient.GetOrCreateObjectStateAsync<long>("counter");
        }

        public void NewBatch(PrimitiveList<int> weights, EventBatchData batchData)
        {
        }

        public ValueTask StoreAsync(PrimitiveList<int> weights, IColumn[] groupValueColumns, EventBatchData incoming, ReadOnlySpan<int> sortedByGroupIndices)
        {
            return ValueTask.CompletedTask;
        }
    }
}

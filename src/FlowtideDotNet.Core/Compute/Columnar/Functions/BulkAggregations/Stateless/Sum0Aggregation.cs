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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateless
{
    internal class Sum0AggregationDefinition : IBulkAggregationDefinition
    {
        public IColumnBulkAggregation Create(AggregateFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[0], functionsRegister);
            return new Sum0Aggregation(compiledValue);
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkAggregationFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Sum0, new Sum0AggregationDefinition());
        }
    }

    internal class Sum0Aggregation : IColumnBulkAggregation
    {
        private readonly Func<EventBatchData, int, IDataValue> projectionFunction;
        private readonly DataValueContainer _dataValueContainer;

        public Sum0Aggregation(Func<EventBatchData, int, IDataValue> projectionFunction)
        {
            this.projectionFunction = projectionFunction;
            _dataValueContainer = new DataValueContainer();
        }

        public Task CommitAsync()
        {
            return Task.CompletedTask;
        }

        public bool Compute(ReadOnlySpan<int> indices, PrimitiveList<int> weights, EventBatchData data, ColumnReference groupState, int sortedIndex)
        {
            groupState.GetValue(_dataValueContainer);
            for (int i = 0; i < indices.Length; i++)
            {
                var value = projectionFunction(data, indices[i]);
                SumAggregation.DoSum(value, _dataValueContainer, weights[indices[i]]);
            }
            groupState.Update(_dataValueContainer);
            return true;
        }

        public ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, int startIndex, int length, Column outputColumn)
        {
            for (int i = startIndex; i < startIndex + length; i++)
            {
                groupStates[i].GetValue(_dataValueContainer);
                if (_dataValueContainer.Type == ArrowTypeId.Null)
                {
                    outputColumn.Add(new DoubleValue(0.0));
                }
                else
                {
                    outputColumn.Add(_dataValueContainer);
                }
            }
            return ValueTask.CompletedTask;
        }

        public Task InitializeAsync(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            return Task.CompletedTask;
        }

        public void NewBatch(PrimitiveList<int> weights, EventBatchData batchData)
        {
        }

        public ValueTask StoreAsync(PrimitiveList<int> weights, IColumn[] groupValueColumns, EventBatchData incoming, ReadOnlySpan<int> sortedByGroupIndices)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask FetchValuesAsync(IColumn[] groupingValuesSorted, int length, Column outputColumn)
        {
            return ValueTask.CompletedTask;
        }
    }
}

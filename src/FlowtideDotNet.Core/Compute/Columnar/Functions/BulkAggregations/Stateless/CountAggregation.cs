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
    internal class CountAggregationDefinition : IBulkAggregationDefinition
    {
        public string Name => "Count";

        public IColumnBulkAggregation Create(AggregateFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            if (aggregateFunction.Arguments.Count == 0)
            {
                return new CountAggregation();
            }
            else if (aggregateFunction.Arguments.Count == 1)
            {
                
                var argument = aggregateFunction.Arguments[0];
                var compiledValue = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[0], functionsRegister);
                return new CountAggregationWithArg(compiledValue);
            }
            throw new InvalidOperationException($"Count aggregation does not support {aggregateFunction.Arguments.Count} arguments");
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkAggregationFunction(FunctionsAggregateGeneric.Uri, FunctionsAggregateGeneric.Count, new CountAggregationDefinition());
        }
    }

    internal class CountAggregation : IColumnBulkAggregation
    {
        private DataValueContainer _valueContainer;

        public CountAggregation()
        {
            _valueContainer = new DataValueContainer();
        }

        public Task CommitAsync()
        {
            return Task.CompletedTask;
        }

        public bool Compute(ReadOnlySpan<int> indices, PrimitiveList<int> weights, EventBatchData data, ColumnReference groupState, int sortedIndex)
        {
            groupState.GetValue(_valueContainer);
            long count = 0;
            if (_valueContainer.Type == ArrowTypeId.Int64)
            {
                count = _valueContainer.AsLong;
            }
            for (int i = 0; i < indices.Length; i++)
            {
                count += weights.Get(i);
            }
            _valueContainer._type = ArrowTypeId.Int64;
            _valueContainer._int64Value = new Int64Value(count);
            groupState.Update(_valueContainer);
            return true;
        }

        public ValueTask FetchValuesAsync(IColumn[] groupingValuesSorted, int length, Column outputColumn)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, int startIndex, int length, Column outputColumn)
        {
            for (int i = startIndex; i < startIndex + length; i++)
            {
                groupStates[i].GetValue(_valueContainer);
                outputColumn.Add(_valueContainer);
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
    }

    internal class CountAggregationWithArg : IColumnBulkAggregation
    {
        private readonly Func<EventBatchData, int, IDataValue> projectionFunction;
        private DataValueContainer _valueContainer;

        public CountAggregationWithArg(Func<EventBatchData, int, IDataValue> projectionFunction)
        {
            _valueContainer = new DataValueContainer();
            this.projectionFunction = projectionFunction;
        }

        public Task CommitAsync()
        {
            return Task.CompletedTask;
        }

        public bool Compute(ReadOnlySpan<int> indices, PrimitiveList<int> weights, EventBatchData data, ColumnReference groupState, int sortedIndex)
        {
            groupState.GetValue(_valueContainer);
            long count = 0;
            if (_valueContainer.Type == ArrowTypeId.Int64)
            {
                count = _valueContainer.AsLong;
            }
            for (int i = 0; i < indices.Length; i++)
            {
                var value = projectionFunction(data, indices[i]);
                if (value.Type != ArrowTypeId.Null)
                {
                    count += weights.Get(i);
                }
            }
            _valueContainer._type = ArrowTypeId.Int64;
            _valueContainer._int64Value = new Int64Value(count);
            groupState.Update(_valueContainer);
            return true;
        }

        public ValueTask FetchValuesAsync(IColumn[] groupingValuesSorted, int length, Column outputColumn)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, int startIndex, int length, Column outputColumn)
        {
            for (int i = startIndex; i < startIndex + length; i++)
            {
                groupStates[i].GetValue(_valueContainer);
                outputColumn.Add(_valueContainer);
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
    }
}

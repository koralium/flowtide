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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    internal class ColumnStreamingAggregateContainer : IColumnAggregateContainer
    {
        private readonly Action<EventBatchData, int, ColumnReference, long> mapFunction;
        private readonly Action<ColumnReference, Column> stateToValueFunc;

        public ColumnStreamingAggregateContainer(
            Action<EventBatchData, int, ColumnReference, long> mapFunction,
            Action<ColumnReference, ColumnStore.Column> stateToValueFunc
            )
        {
            this.mapFunction = mapFunction;
            this.stateToValueFunc = stateToValueFunc;
        }
        public Task Commit()
        {
            return Task.CompletedTask;
        }

        public ValueTask Compute(ColumnRowReference key, EventBatchData rowBatch, int rowIndex, ColumnReference state, long weight)
        {
            mapFunction(rowBatch, rowIndex, state, weight);
            return ValueTask.CompletedTask;
        }

        public void Disponse()
        {
        }

        public ValueTask GetValue(ColumnRowReference key, ColumnReference state, Column outputColumn)
        {
            stateToValueFunc(state, outputColumn);
            return ValueTask.CompletedTask;
        }
    }

    internal class ColumnStreamingAggregateFunctionDefinition : ColumnAggregateFunctionDefinition
    {
        public ColumnStreamingAggregateFunctionDefinition(
            string uri,
            string name,
            Func<AggregateFunction, ColumnParameterInfo, ColumnarExpressionVisitor, ParameterExpression, ParameterExpression, System.Linq.Expressions.Expression> updateStateFunc,
            Action<ColumnReference, ColumnStore.Column> stateToValueFunc)
        {
            Uri = uri;
            Name = name;
            UpdateStateFunc = updateStateFunc;
            StateToValueFunc = stateToValueFunc;
        }

        public string Uri { get; }

        public string Name { get; }

        public Action<ColumnReference, ColumnStore.Column> StateToValueFunc { get; }

        public Func<AggregateFunction, ColumnParameterInfo, ColumnarExpressionVisitor, ParameterExpression, ParameterExpression, System.Linq.Expressions.Expression> UpdateStateFunc { get; }

        public override Task<IColumnAggregateContainer> CreateContainer(
            int groupingLength, 
            IStateManagerClient stateManagerClient,
            IMemoryAllocator memoryAllocator,
            AggregateFunction aggregateFunction, 
            ColumnParameterInfo parametersInfo, 
            ColumnarExpressionVisitor visitor, 
            ParameterExpression eventBatchParameter, 
            ParameterExpression indexParameter, 
            ParameterExpression stateParameter, 
            ParameterExpression weightParameter, 
            ParameterExpression groupingKeyParameter)
        {
            var mapFunc = UpdateStateFunc(aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter);
            var lambda = System.Linq.Expressions.Expression.Lambda<Action<EventBatchData, int, ColumnReference, long>>(mapFunc, eventBatchParameter, indexParameter, stateParameter, weightParameter);
            var compiled = lambda.Compile();
            return Task.FromResult<IColumnAggregateContainer>(new ColumnStreamingAggregateContainer(compiled, StateToValueFunc));
        }
    }
}

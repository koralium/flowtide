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
using static FlowtideDotNet.Core.Compute.IFunctionsRegister;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    internal abstract class ColumnAggregateFunctionDefinition 
    {
        public abstract Task<IColumnAggregateContainer> CreateContainer(
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
            ParameterExpression groupingKeyParameter);
    }

    internal abstract class StatefulColumnAggregateContainer : IColumnAggregateContainer
    {
        public abstract Task Commit();
        public abstract ValueTask Compute(ColumnRowReference key, EventBatchData rowBatch, int rowIndex, ColumnReference state, long weight);
        public abstract void Disponse();
        public abstract ValueTask GetValue(ColumnRowReference key, ColumnReference state, Column outputColumn);
    }

    internal class StatefulColumnAggregateContainer<T> : StatefulColumnAggregateContainer
    {
        private readonly Action<T> disposeFunction;
        private readonly Func<T, Task> commitFunction;
        private readonly ColumnAggregateStateToValueFunction<T> stateToValueFunc;

        public StatefulColumnAggregateContainer(
            T singleton,
            Func<EventBatchData, int, ColumnReference, long, T, ColumnRowReference, ValueTask> mapFunction,
            Action<T> disposeFunction,
            Func<T, Task> commitFunction,
            ColumnAggregateStateToValueFunction<T> stateToValueFunc)
        {
            Singleton = singleton;
            MapFunction = mapFunction;
            this.disposeFunction = disposeFunction;
            this.commitFunction = commitFunction;
            this.stateToValueFunc = stateToValueFunc;
        }

        public T Singleton { get; }
        public Func<EventBatchData, int, ColumnReference, long, T, ColumnRowReference, ValueTask> MapFunction { get; }

        public override Task Commit()
        {
            return commitFunction(Singleton);
        }

        public override ValueTask Compute(ColumnRowReference key, EventBatchData rowBatch, int rowIndex, ColumnReference state, long weight)
        {
            return MapFunction(rowBatch, rowIndex, state, weight, Singleton, key);
        }

        public override void Disponse()
        {
            disposeFunction(Singleton);
        }

        public override ValueTask GetValue(ColumnRowReference key, ColumnReference state, Column outputColumn)
        {
            return stateToValueFunc(state, key, Singleton, outputColumn);
        }
    }

    internal class StatefulColumnAggregateFunctionDefinition<T> : ColumnAggregateFunctionDefinition
    {
        public StatefulColumnAggregateFunctionDefinition(
            AggregateInitializeFunction<T> initializeFunction,
            Action<T> disposeFunction,
            Func<T, Task> commitFunction,
            ColumnAggregateMapFunction mapFunc,
            ColumnAggregateStateToValueFunction<T> stateToValueFunc)
        {
            InitializeFunction = initializeFunction;
            DisposeFunction = disposeFunction;
            CommitFunction = commitFunction;
            MapFunc = mapFunc;
            StateToValueFunc = stateToValueFunc;
        }

        public AggregateInitializeFunction<T> InitializeFunction { get; }

        public Action<T> DisposeFunction { get; }
        public Func<T, Task> CommitFunction { get; }
        public ColumnAggregateMapFunction MapFunc { get; }

        public ColumnAggregateStateToValueFunction<T> StateToValueFunc { get; }

        public override async Task<IColumnAggregateContainer> CreateContainer(
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
            var singleton = await InitializeFunction(groupingLength, stateManagerClient, memoryAllocator);

            var singletonParameter = System.Linq.Expressions.Expression.Parameter(typeof(T));
            var mapResult = MapFunc(aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter, singletonParameter, groupingKeyParameter);
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<EventBatchData, int, ColumnReference, long, T, ColumnRowReference, ValueTask>>(mapResult, eventBatchParameter, indexParameter, stateParameter, weightParameter, singletonParameter, groupingKeyParameter);
            var compiled = lambda.Compile();

            var container = new StatefulColumnAggregateContainer<T>(singleton, compiled, DisposeFunction, CommitFunction, StateToValueFunc);
            return container;
        }
    }
}

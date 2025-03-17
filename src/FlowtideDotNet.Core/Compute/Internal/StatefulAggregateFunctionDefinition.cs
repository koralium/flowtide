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

using FlexBuffers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using System.Linq.Expressions;
using static FlowtideDotNet.Core.Compute.IFunctionsRegister;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal abstract class StatefulAggregateContainer : IAggregateContainer
    {
        public abstract Task Commit();
        public abstract ValueTask<byte[]> Compute(RowEvent key, RowEvent row, byte[]? state, long weight);
        public abstract void Disponse();
        public abstract ValueTask<FlxValue> GetValue(RowEvent key, byte[]? state);
    }

    internal class StatefulAggregateContainer<T> : StatefulAggregateContainer
    {
        private readonly Action<T> disposeFunction;
        private readonly Func<T, Task> commitFunction;
        private readonly AggregateStateToValueFunction<T> stateToValueFunc;

        public StatefulAggregateContainer(
            T singleton,
            Func<RowEvent, byte[]?, long, T, RowEvent, ValueTask<byte[]>> mapFunction,
            Action<T> disposeFunction,
            Func<T, Task> commitFunction,
            AggregateStateToValueFunction<T> stateToValueFunc)
        {
            Singleton = singleton;
            MapFunction = mapFunction;
            this.disposeFunction = disposeFunction;
            this.commitFunction = commitFunction;
            this.stateToValueFunc = stateToValueFunc;
        }

        public T Singleton { get; }
        public Func<RowEvent, byte[]?, long, T, RowEvent, ValueTask<byte[]>> MapFunction { get; }

        public override Task Commit()
        {
            return commitFunction(Singleton);
        }

        public override ValueTask<byte[]> Compute(RowEvent key, RowEvent row, byte[]? state, long weight)
        {
            return MapFunction(row, state, weight, Singleton, key);
        }

        public override void Disponse()
        {
            disposeFunction(Singleton);
        }

        public override ValueTask<FlxValue> GetValue(RowEvent key, byte[]? state)
        {
            return stateToValueFunc(state, key, Singleton);
        }
    }

    internal class StatefulAggregateFunctionDefinition<T> : AggregateFunctionDefinition
    {
        public StatefulAggregateFunctionDefinition(
            AggregateInitializeFunction<T> initializeFunction,
            Action<T> disposeFunction,
            Func<T, Task> commitFunction,
            AggregateMapFunction mapFunc,
            AggregateStateToValueFunction<T> stateToValueFunc)
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
        public AggregateMapFunction MapFunc { get; }

        public AggregateStateToValueFunction<T> StateToValueFunc { get; }

        public override async Task<IAggregateContainer> CreateContainer(
            int groupingLength,
            IStateManagerClient stateManagerClient,
            IMemoryAllocator memoryAllocator,
            AggregateFunction aggregateFunction,
            ParametersInfo parametersInfo,
            ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo> visitor,
            ParameterExpression eventParameter,
            ParameterExpression stateParameter,
            ParameterExpression weightParameter,
            ParameterExpression groupingKeyParameter)
        {
            var singleton = await InitializeFunction(groupingLength, stateManagerClient, memoryAllocator);

            var singletonParameter = System.Linq.Expressions.Expression.Parameter(typeof(T));
            var mapResult = MapFunc(aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter, singletonParameter, groupingKeyParameter);
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<RowEvent, byte[]?, long, T, RowEvent, ValueTask<byte[]>>>(mapResult, eventParameter, stateParameter, weightParameter, singletonParameter, groupingKeyParameter);
            var compiled = lambda.Compile();

            var container = new StatefulAggregateContainer<T>(singleton, compiled, DisposeFunction, CommitFunction, StateToValueFunc);
            return container;
        }
    }
}

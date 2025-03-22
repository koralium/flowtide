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

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal class StreamingAggregateContainer : IAggregateContainer
    {
        private readonly Func<RowEvent, byte[]?, long, byte[]> mapFunc;
        private readonly Func<byte[]?, FlxValue> stateToValueFunc;

        public StreamingAggregateContainer(Func<RowEvent, byte[]?, long, byte[]> mapFunc, Func<byte[]?, FlxValue> stateToValueFunc)
        {
            this.mapFunc = mapFunc;
            this.stateToValueFunc = stateToValueFunc;
        }

        public Task Commit()
        {
            return Task.CompletedTask;
        }

        public ValueTask<byte[]> Compute(RowEvent key, RowEvent row, byte[]? state, long weight)
        {
            return ValueTask.FromResult(mapFunc(row, state, weight));
        }

        public void Disponse()
        {
        }

        public ValueTask<FlxValue> GetValue(RowEvent key, byte[]? state)
        {
            return ValueTask.FromResult(stateToValueFunc(state));
        }
    }

    internal class StreamingAggregateFunctionDefinition : AggregateFunctionDefinition
    {
        public StreamingAggregateFunctionDefinition(
            string uri,
            string name,
            Func<AggregateFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, ParameterExpression, ParameterExpression, System.Linq.Expressions.Expression> updateStateFunc,
            Func<byte[]?, FlxValue> stateToValueFunc)
        {
            Uri = uri;
            Name = name;
            UpdateStateFunc = updateStateFunc;
            StateToValueFunc = stateToValueFunc;
        }

        public string Uri { get; }

        public string Name { get; }

        public Func<byte[]?, FlxValue> StateToValueFunc { get; }

        /// <summary>
        /// Function that updates the state of the aggregate function
        /// Arguments:
        /// - AggregateFunction
        /// - ParametersInfo used by the expression visitor
        /// - ExpressionVisitor, visitor to visit function arguments
        /// - ParameterExpression, byte[] parameter for the state
        /// - ParameterExpression, int, parameter for the weight
        /// Should return a byte[] with the new state.
        /// </summary>
        public Func<AggregateFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, ParameterExpression, ParameterExpression, System.Linq.Expressions.Expression> UpdateStateFunc { get; }

        public override Task<IAggregateContainer> CreateContainer(
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
            var mapFunc = UpdateStateFunc(aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter);
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<RowEvent, byte[]?, long, byte[]>>(mapFunc, eventParameter, stateParameter, weightParameter);
            var compiled = lambda.Compile();
            return Task.FromResult<IAggregateContainer>(new StreamingAggregateContainer(compiled, StateToValueFunc));
        }
    }
}

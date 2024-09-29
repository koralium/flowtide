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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute
{
    public interface IFunctionsRegister
    {
        void RegisterColumnScalarFunction(
            string uri, 
            string name, 
            Func<ScalarFunction, ColumnParameterInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ColumnParameterInfo>, System.Linq.Expressions.Expression> mapFunc);

        /// <summary>
        /// Register a scalar function, this is the low level call where the user has to visit the arguments with the visitor.
        /// </summary>
        /// <param name="uri">file uri of the scalar function</param>
        /// <param name="name">name of the scalar function</param>
        /// <param name="mapFunc">Mapping function to return a C# expression</param>
        void RegisterScalarFunction(
            string uri,
            string name,
            Func<ScalarFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, System.Linq.Expressions.Expression> mapFunc);

        void RegisterScalarFunctionWithExpression(
            string uri,
            string name,
            System.Linq.Expressions.Expression<Func<FlxValue, FlxValue>> expression);

        /// <summary>
        /// Register an aggregate function, this is the low level call which requires the user to visit the expressions with the visitor.
        /// </summary>
        /// <param name="uri"></param>
        /// <param name="name"></param>
        void RegisterStreamingAggregateFunction(
            string uri, 
            string name,
            Func<AggregateFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, ParameterExpression, ParameterExpression, System.Linq.Expressions.Expression> mapFunc,
            Func<byte[]?, FlxValue> stateToValueFunc);


        delegate Task<T> AggregateInitializeFunction<T>(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator);

        delegate System.Linq.Expressions.Expression ColumnAggregateMapFunction(
            AggregateFunction function,
            ColumnParameterInfo parametersInfo,
            ColumnarExpressionVisitor visitor,
            ParameterExpression stateParameters,
            ParameterExpression weightParameter,
            ParameterExpression singletonAccess,
            ParameterExpression groupingKeyParameter);

        delegate System.Linq.Expressions.Expression AggregateMapFunction(
            AggregateFunction function,
            ParametersInfo parametersInfo,
            ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo> visitor,
            ParameterExpression stateParameters,
            ParameterExpression weightParameter,
            ParameterExpression singletonAccess,
            ParameterExpression groupingKeyParameter);

        delegate ValueTask<FlxValue> AggregateStateToValueFunction<T>(byte[]? state, RowEvent groupingKey, T singleton);

        delegate ValueTask ColumnAggregateStateToValueFunction<T>(ColumnReference state, ColumnRowReference groupingKey, T singleton, ColumnStore.Column outputColumn);

        /// <summary>
        /// Register a stateful aggregate function.
        /// This allows a function to handle its own persistent state.
        /// This is useful in functions such as MIN/MAX or MEDIAN, where all values must be known.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="uri"></param>
        /// <param name="name"></param>
        /// <param name="stateFunction">State creation function</param>
        /// <param name="disposeFunction">Disposing function of the state, if required</param>
        /// <param name="mapFunc"></param>
        /// <param name="stateToValueFunc"></param>
        void RegisterStatefulAggregateFunction<T>(
            string uri,
            string name,
            AggregateInitializeFunction<T> initializeFunction,
            Action<T> disposeFunction,
            Func<T, Task> commitFunction,
            AggregateMapFunction mapFunc,
            AggregateStateToValueFunction<T> stateToValueFunc);

        void RegisterStatefulColumnAggregateFunction<T>(
            string uri,
            string name,
            AggregateInitializeFunction<T> initializeFunction,
            Action<T> disposeFunction,
            Func<T, Task> commitFunction,
            ColumnAggregateMapFunction mapFunc,
            ColumnAggregateStateToValueFunction<T> stateToValueFunc);

        bool TryGetScalarFunction(string uri, string name, [NotNullWhen(true)] out FunctionDefinition? functionDefinition);

        bool TryGetAggregateFunction(string uri, string name, [NotNullWhen(true)] out AggregateFunctionDefinition? aggregateFunctionDefinition);

        bool TryGetColumnScalarFunction(string uri, string name, [NotNullWhen(true)] out ColumnFunctionDefinition? functionDefinition);

        /// <summary>
        /// Register a table function, this is the low level call which requires the user to visit the expressions with the visitor.
        /// The expression result should be of type IEnumerable<RowEvent>.
        /// </summary>
        /// <param name="uri"></param>
        /// <param name="name"></param>
        /// <param name="mapFunc"></param>
        void RegisterTableFunction(
            string uri,
            string name, 
            Func<TableFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, System.Linq.Expressions.Expression> mapFunc);

        bool TryGetTableFunction(string uri, string name, [NotNullWhen(true)] out TableFunctionDefinition? tableFunctionDefinition);
    }
}

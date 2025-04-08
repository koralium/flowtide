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
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute
{
    internal class FunctionsRegister : IFunctionsRegister
    {
        private readonly Dictionary<string, ColumnFunctionDefinition> _columnScalarFunctions;
        private readonly Dictionary<string, FunctionDefinition> _scalarFunctions;
        private readonly Dictionary<string, AggregateFunctionDefinition> _aggregateFunctions;
        private readonly Dictionary<string, TableFunctionDefinition> _tableFunctions;
        private readonly Dictionary<string, ColumnAggregateFunctionDefinition> _columnAggregateFunctions;
        private readonly Dictionary<string, ColumnTableFunctionDefinition> _columnTableFunctions;
        private readonly Dictionary<string, WindowFunctionDefinition> _windowFunctions;
        private readonly FunctionServices _functionServices;

        public IFunctionServices FunctionServices => _functionServices;

        public FunctionsRegister()
        {
            _scalarFunctions = new Dictionary<string, FunctionDefinition>(StringComparer.OrdinalIgnoreCase);
            _aggregateFunctions = new Dictionary<string, AggregateFunctionDefinition>(StringComparer.OrdinalIgnoreCase);
            _tableFunctions = new Dictionary<string, TableFunctionDefinition>(StringComparer.OrdinalIgnoreCase);

            _columnScalarFunctions = new Dictionary<string, ColumnFunctionDefinition>(StringComparer.OrdinalIgnoreCase);
            _columnAggregateFunctions = new Dictionary<string, ColumnAggregateFunctionDefinition>(StringComparer.OrdinalIgnoreCase);
            _columnTableFunctions = new Dictionary<string, ColumnTableFunctionDefinition>(StringComparer.OrdinalIgnoreCase);
            _windowFunctions = new Dictionary<string, WindowFunctionDefinition>(StringComparer.OrdinalIgnoreCase);

            _functionServices = new FunctionServices();
        }

        public bool TryGetScalarFunction(string uri, string name, [NotNullWhen(true)] out FunctionDefinition? functionDefinition)
        {
            return _scalarFunctions.TryGetValue($"{uri}:{name}", out functionDefinition);
        }

        public bool TryGetColumnScalarFunction(string uri, string name, [NotNullWhen(true)] out ColumnFunctionDefinition? functionDefinition)
        {
            return _columnScalarFunctions.TryGetValue($"{uri}:{name}", out functionDefinition);
        }

        public void RegisterColumnScalarFunction(string uri, string name, Func<ScalarFunction, ColumnParameterInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ColumnParameterInfo>, IFunctionServices, System.Linq.Expressions.Expression> mapFunc)
        {
            _columnScalarFunctions.Add($"{uri}:{name}", new ColumnFunctionDefinition(uri, name, mapFunc));
        }

        public void RegisterScalarFunction(string uri, string name, Func<ScalarFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, System.Linq.Expressions.Expression> mapFunc)
        {
            _scalarFunctions.Add($"{uri}:{name}", new FunctionDefinition(uri, name, mapFunc));
        }

        public void RegisterScalarFunctionWithExpression(string uri, string name, Expression<Func<FlxValue, FlxValue>> expression)
        {
            RegisterScalarFunction(uri, name, (scalarFunction, parametersInfo, expressionVisitor) =>
            {
                if (scalarFunction.Arguments.Count != 1)
                {
                    throw new ArgumentException($"Scalar function {name} must have one argument");
                }

                var p1 = expressionVisitor.Visit(scalarFunction.Arguments[0], parametersInfo);
                if (p1 == null) { throw new InvalidOperationException("First parameter could not be compiled"); }

                var expressionBody = expression.Body;
                // TODO: Replace parameter with p1
                var expressionBodyWithParameter = new ParameterReplacerVisitor(expression.Parameters[0], p1).Visit(expressionBody);
                return expressionBodyWithParameter;
            });
        }

        public void RegisterScalarFunctionWithExpression(string uri, string name, Expression<Func<FlxValue, FlxValue, FlxValue>> expression)
        {
            RegisterScalarFunction(uri, name, (scalarFunction, parametersInfo, expressionVisitor) =>
            {
                if (scalarFunction.Arguments.Count != 2)
                {
                    throw new ArgumentException($"Scalar function {name} must have two arguments");
                }

                var p1 = expressionVisitor.Visit(scalarFunction.Arguments[0], parametersInfo);
                var p2 = expressionVisitor.Visit(scalarFunction.Arguments[1], parametersInfo);

                if (p1 == null) { throw new InvalidOperationException("First parameter could not be compiled"); }
                if (p2 == null) { throw new InvalidOperationException("Second parameter could not be compiled"); }

                var expressionBody = expression.Body;
                // TODO: Replace parameter with p1
                expressionBody = new ParameterReplacerVisitor(expression.Parameters[0], p1).Visit(expressionBody);
                expressionBody = new ParameterReplacerVisitor(expression.Parameters[1], p2).Visit(expressionBody);
                return expressionBody;
            });
        }

        public void RegisterScalarFunctionWithExpression(string uri, string name, Expression<Func<FlxValue, FlxValue, FlxValue, FlxValue>> expression)
        {
            RegisterScalarFunction(uri, name, (scalarFunction, parametersInfo, expressionVisitor) =>
            {
                if (scalarFunction.Arguments.Count != 3)
                {
                    throw new ArgumentException($"Scalar function {name} must have three arguments");
                }

                var p1 = expressionVisitor.Visit(scalarFunction.Arguments[0], parametersInfo);
                var p2 = expressionVisitor.Visit(scalarFunction.Arguments[1], parametersInfo);
                var p3 = expressionVisitor.Visit(scalarFunction.Arguments[2], parametersInfo);
                var expressionBody = expression.Body;

                if (p1 == null) { throw new InvalidOperationException("First parameter could not be compiled"); }
                if (p2 == null) { throw new InvalidOperationException("Second parameter could not be compiled"); }
                if (p3 == null) { throw new InvalidOperationException("Third parameter could not be compiled"); }
                // TODO: Replace parameter with p1
                expressionBody = new ParameterReplacerVisitor(expression.Parameters[0], p1).Visit(expressionBody);
                expressionBody = new ParameterReplacerVisitor(expression.Parameters[1], p2).Visit(expressionBody);
                expressionBody = new ParameterReplacerVisitor(expression.Parameters[2], p3).Visit(expressionBody);
                return expressionBody;
            });
        }

        public void RegisterStreamingAggregateFunction(
            string uri,
            string name,
            Func<AggregateFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, ParameterExpression, ParameterExpression, System.Linq.Expressions.Expression> mapFunc,
            Func<byte[]?, FlxValue> stateToValueFunc)
        {
            _aggregateFunctions.Add($"{uri}:{name}", new StreamingAggregateFunctionDefinition(uri, name, mapFunc, stateToValueFunc));
        }

        /// <summary>
        /// Register a streaming aggregate function that uses columnar data.
        /// </summary>
        /// <param name="uri"></param>
        /// <param name="name"></param>
        /// <param name="mapFunc"></param>
        /// <param name="stateToValueFunc"></param>
        public void RegisterStreamingColumnAggregateFunction(
            string uri,
            string name,
            Func<AggregateFunction, ColumnParameterInfo, ColumnarExpressionVisitor, ParameterExpression, ParameterExpression, System.Linq.Expressions.Expression> mapFunc,
            Action<ColumnReference, ColumnStore.Column> stateToValueFunc)
        {
            _columnAggregateFunctions.Add($"{uri}:{name}", new ColumnStreamingAggregateFunctionDefinition(uri, name, mapFunc, stateToValueFunc));
        }

        public bool TryGetAggregateFunction(string uri, string name, [NotNullWhen(true)] out AggregateFunctionDefinition? aggregateFunctionDefinition)
        {
            return _aggregateFunctions.TryGetValue($"{uri}:{name}", out aggregateFunctionDefinition);
        }

        public void RegisterStatefulAggregateFunction<T>(
            string uri,
            string name,
            IFunctionsRegister.AggregateInitializeFunction<T> stateFunction,
            Action<T> disposeFunction,
            Func<T, Task> commitFunction,
            IFunctionsRegister.AggregateMapFunction mapFunc,
            IFunctionsRegister.AggregateStateToValueFunction<T> stateToValueFunc)
        {
            _aggregateFunctions.Add($"{uri}:{name}", new StatefulAggregateFunctionDefinition<T>(
                stateFunction,
                disposeFunction,
                commitFunction,
                mapFunc,
                stateToValueFunc));
        }

        public void RegisterTableFunction(string uri, string name, Func<TableFunction, ParametersInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo>, System.Linq.Expressions.Expression> mapFunc)
        {
            _tableFunctions.Add($"{uri}:{name}", new TableFunctionDefinition(uri, name, mapFunc));
        }

        public void RegisterColumnTableFunction(
            string uri,
            string name,
            Func<TableFunction, ColumnParameterInfo, ColumnarExpressionVisitor, IMemoryAllocator, TableFunctionResult> mapFunc)
        {
            _columnTableFunctions.Add($"{uri}:{name}", new ColumnTableFunctionDefinition(uri, name, mapFunc));
        }

        public bool TryGetTableFunction(string uri, string name, [NotNullWhen(true)] out TableFunctionDefinition? tableFunctionDefinition)
        {
            return _tableFunctions.TryGetValue($"{uri}:{name}", out tableFunctionDefinition);
        }

        public bool TryGetColumnTableFunction(string uri, string name, [NotNullWhen(true)] out ColumnTableFunctionDefinition? tableFunctionDefinition)
        {
            return _columnTableFunctions.TryGetValue($"{uri}:{name}", out tableFunctionDefinition);
        }

        public void RegisterStatefulColumnAggregateFunction<T>(string uri, string name, IFunctionsRegister.AggregateInitializeFunction<T> initializeFunction, Action<T> disposeFunction, Func<T, Task> commitFunction, IFunctionsRegister.ColumnAggregateMapFunction mapFunc, IFunctionsRegister.ColumnAggregateStateToValueFunction<T> stateToValueFunc)
        {
            _columnAggregateFunctions.Add($"{uri}:{name}", new StatefulColumnAggregateFunctionDefinition<T>(
                initializeFunction,
                disposeFunction,
                commitFunction,
                mapFunc,
                stateToValueFunc
                ));
        }

        public bool TryGetColumnAggregateFunction(string uri, string name, [NotNullWhen(true)] out ColumnAggregateFunctionDefinition? aggregateFunctionDefinition)
        {
            return _columnAggregateFunctions.TryGetValue($"{uri}:{name}", out aggregateFunctionDefinition);
        }

        public void RegisterWindowFunction(string uri, string name, WindowFunctionDefinition windowFunctionDefinition)
        {
            _windowFunctions.Add($"{uri}:{name}", windowFunctionDefinition);
        }

        public bool TryGetWindowFunction(WindowFunction windowFunction, [NotNullWhen(true)] out IWindowFunction? windowFunc)
        {
            if(_windowFunctions.TryGetValue($"{windowFunction.ExtensionUri}:{windowFunction.ExtensionName}", out var windowFunctionDefinition))
            {
                windowFunc = windowFunctionDefinition.Create(windowFunction, this);
                return true;
            }
            windowFunc = default;
            return false;
        }

        public void SetCheckNotificationReceiver(ICheckNotificationReciever checkNotificationReceiver)
        {
            _functionServices.SetCheckNotificationReceiver(checkNotificationReceiver);
        }
    }
}

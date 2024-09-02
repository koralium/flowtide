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
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Internal;
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

        public FunctionsRegister()
        {
            _scalarFunctions = new Dictionary<string, FunctionDefinition>(StringComparer.OrdinalIgnoreCase);
            _aggregateFunctions = new Dictionary<string, AggregateFunctionDefinition>(StringComparer.OrdinalIgnoreCase);
            _tableFunctions = new Dictionary<string, TableFunctionDefinition>(StringComparer.OrdinalIgnoreCase);

            _columnScalarFunctions = new Dictionary<string, ColumnFunctionDefinition>(StringComparer.OrdinalIgnoreCase);
        }

        public bool TryGetScalarFunction(string uri, string name, [NotNullWhen(true)] out FunctionDefinition? functionDefinition)
        {
            return _scalarFunctions.TryGetValue($"{uri}:{name}", out functionDefinition);
        }

        public bool TryGetColumnScalarFunction(string uri, string name, [NotNullWhen(true)] out ColumnFunctionDefinition? functionDefinition)
        {
            return _columnScalarFunctions.TryGetValue($"{uri}:{name}", out functionDefinition);
        }

        public void RegisterColumnScalarFunction(string uri, string name, Func<ScalarFunction, ColumnParameterInfo, ExpressionVisitor<System.Linq.Expressions.Expression, ColumnParameterInfo>, System.Linq.Expressions.Expression> mapFunc)
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

        public bool TryGetTableFunction(string uri, string name, [NotNullWhen(true)] out TableFunctionDefinition? tableFunctionDefinition)
        {
            return _tableFunctions.TryGetValue($"{uri}:{name}", out tableFunctionDefinition);
        }
    }
}

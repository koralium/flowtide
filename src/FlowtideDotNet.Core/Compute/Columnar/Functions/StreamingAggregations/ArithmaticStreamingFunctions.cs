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
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StreamingAggregations
{
    internal static class ArithmaticStreamingFunctions
    {
        public static void AddBuiltInArithmaticFunctions(FunctionsRegister functionsRegister)
        {
            ColumnMinMaxAggregation.RegisterMin(functionsRegister);
            ColumnMinMaxAggregation.RegisterMax(functionsRegister);

            functionsRegister.RegisterStreamingColumnAggregateFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Sum,
                (aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter) =>
                {
                    if (aggregateFunction.Arguments.Count != 1)
                    {
                        throw new InvalidOperationException("Sum must have one argument.");
                    }
                    var arg = visitor.Visit(aggregateFunction.Arguments[0], parametersInfo);
                    var expr = GetSumBody(arg!.Type);
                    var body = expr.Body;
                    var replacer = new ParameterReplacerVisitor(expr.Parameters[0], arg!);
                    Expression e = replacer.Visit(body);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[1], stateParameter);
                    e = replacer.Visit(e);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[2], weightParameter);
                    e = replacer.Visit(e);
                    return e;
                }, GetSumValue);

            functionsRegister.RegisterStreamingColumnAggregateFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Sum0,
                (aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter) =>
                {
                    if (aggregateFunction.Arguments.Count != 1)
                    {
                        throw new InvalidOperationException("Sum must have one argument.");
                    }
                    var arg = visitor.Visit(aggregateFunction.Arguments[0], parametersInfo);
                    var expr = GetSumBody(arg!.Type);
                    var body = expr.Body;
                    var replacer = new ParameterReplacerVisitor(expr.Parameters[0], arg!);
                    Expression e = replacer.Visit(body);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[1], stateParameter);
                    e = replacer.Visit(e);
                    replacer = new ParameterReplacerVisitor(expr.Parameters[2], weightParameter);
                    e = replacer.Visit(e);
                    return e;
                }, GetSum0Value);
        }

        private static LambdaExpression GetSumBody(System.Type inputType)
        {
            var input = Expression.Parameter(inputType, "input");
            var state = Expression.Parameter(typeof(ColumnReference), "state");
            var weight = Expression.Parameter(typeof(long), "weight");
            var methodInfo = typeof(ArithmaticStreamingFunctions).GetMethod(nameof(DoSum), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            var body = Expression.Call(null, methodInfo!.MakeGenericMethod(inputType), input, state, weight);
            return Expression.Lambda(body, input, state, weight);

        }

        private static void DoSum<T>(T value, ColumnReference state, long weight)
            where T : IDataValue
        {
            var currentState = state.GetValue();
            if (currentState.Type == ArrowTypeId.Int64)
            {
                if (value.Type == ArrowTypeId.Int64)
                {
                    var count = currentState.AsLong + (value.AsLong * weight);
                    state.Update(new Int64Value(count));
                }
                else if (value.Type == ArrowTypeId.Double)
                {
                    var floatCount = currentState.AsLong + (value.AsDouble * weight);
                    state.Update(new DoubleValue(floatCount));
                }
            }
            else if (currentState.Type == ArrowTypeId.Double)
            {
                if (value.Type == ArrowTypeId.Int64)
                {
                    var count = currentState.AsDouble + (value.AsLong * weight);
                    state.Update(new DoubleValue(count));
                }
                else if (value.Type == ArrowTypeId.Double)
                {
                    var count = currentState.AsDouble + (value.AsDouble * weight);
                    state.Update(new DoubleValue(count));
                }
            }
            else if (currentState.Type == ArrowTypeId.Null)
            {
                if (value.Type == ArrowTypeId.Int64)
                {
                    var count = (value.AsLong * weight);
                    state.Update(new Int64Value(count));
                }
                else if (value.Type == ArrowTypeId.Double)
                {
                    var count = (value.AsDouble * weight);
                    state.Update(new DoubleValue(count));
                }
            }
        }

        private static void GetSumValue(ColumnReference state, ColumnStore.Column outputColumn)
        {
            var stateVal = state.GetValue();
            outputColumn.Add(stateVal);
        }

        private static void GetSum0Value(ColumnReference state, ColumnStore.Column outputColumn)
        {
            var stateVal = state.GetValue();

            if (stateVal.Type == ArrowTypeId.Null)
            {
                outputColumn.Add(new DoubleValue(0.0));
            }
            else
            {
                outputColumn.Add(stateVal);
            }
        }
    }
}

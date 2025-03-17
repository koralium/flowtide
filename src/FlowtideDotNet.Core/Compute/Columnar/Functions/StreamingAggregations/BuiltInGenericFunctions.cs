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
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Diagnostics;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StreamingAggregations
{
    internal static class BuiltInGenericFunctions
    {
        public static void AddBuiltInAggregateGenericFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStreamingColumnAggregateFunction(
                FunctionsAggregateGeneric.Uri,
                FunctionsAggregateGeneric.Count,
                (aggregateFunction, parametersInfo, visitor, stateParameter, weightParameter) =>
                {
                    if (aggregateFunction.Arguments.Count == 1)
                    {
                        var arg = visitor.Visit(aggregateFunction.Arguments[0], parametersInfo);
                        Debug.Assert(arg != null);
                        var expr = GetCountWithArgBody(arg.Type);
                        var body = expr.Body;
                        var replacer = new ParameterReplacerVisitor(expr.Parameters[0], arg!);
                        Expression e = replacer.Visit(body);
                        replacer = new ParameterReplacerVisitor(expr.Parameters[1], stateParameter);
                        e = replacer.Visit(e);
                        replacer = new ParameterReplacerVisitor(expr.Parameters[2], weightParameter);
                        e = replacer.Visit(e);
                        return e;
                    }
                    else if (aggregateFunction.Arguments.Count == 0)
                    {
                        var expr = GetCountBody();
                        var body = expr.Body;
                        var replacer = new ParameterReplacerVisitor(expr.Parameters[0], parametersInfo.BatchParameters[0]);
                        Expression e = replacer.Visit(body);
                        replacer = new ParameterReplacerVisitor(expr.Parameters[1], parametersInfo.IndexParameters[0]);
                        e = replacer.Visit(e);
                        replacer = new ParameterReplacerVisitor(expr.Parameters[2], stateParameter);
                        e = replacer.Visit(e);
                        replacer = new ParameterReplacerVisitor(expr.Parameters[3], weightParameter);
                        e = replacer.Visit(e);
                        return e;
                    }
                    throw new ArgumentException("Count function does not take any arguments");

                },
                GetCountValue
                );
        }

        private static LambdaExpression GetCountWithArgBody(System.Type inputType)
        {
            var input = Expression.Parameter(inputType, "input");
            var state = Expression.Parameter(typeof(ColumnReference), "state");
            var weight = Expression.Parameter(typeof(long), "weight");
            var methodInfo = typeof(BuiltInGenericFunctions).GetMethod(nameof(DoCountWithArg), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            var body = Expression.Call(null, methodInfo!.MakeGenericMethod(inputType), input, state, weight);
            return Expression.Lambda(body, input, state, weight);
        }

        private static Expression<Action<EventBatchData, int, ColumnReference, long>> GetCountBody()
        {
            return (batch, index, bytes, weight) => DoCount(batch, index, bytes, weight);
        }

        private static void DoCountWithArg<T>(T value, ColumnReference state, long weight)
            where T : IDataValue
        {
            if (value.IsNull)
            {
                return;
            }
            var currentState = state.GetValue();
            long count = 0;
            if (currentState.Type == ArrowTypeId.Int64)
            {
                count = currentState.AsLong;
            }
            var newCount = count + weight;
            state.Update(new Int64Value(newCount));
        }

        private static void DoCount(EventBatchData data, int index, ColumnReference state, long weight)
        {
            var currentState = state.GetValue();
            long count = 0;
            if (currentState.Type == ArrowTypeId.Int64)
            {
                count = currentState.AsLong;
            }
            if (weight < 0)
            {

            }
            var newCount = count + weight;
            state.Update(new Int64Value(newCount));
        }

        private static void GetCountValue(ColumnReference state, Column outputColumn)
        {
            var stateValue = state.GetValue();
            if (stateValue.Type == ArrowTypeId.Null)
            {
                outputColumn.Add(new Int64Value(0));
                return;
            }
            outputColumn.Add(stateValue);
        }
    }
}

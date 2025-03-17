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
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInBooleanFunctions
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());
        private static readonly FlxValue TrueVal = FlxValue.FromBytes(FlexBuffer.SingleValue(true));
        private static readonly FlxValue FalseVal = FlxValue.FromBytes(FlexBuffer.SingleValue(false));

        // Helper expression to convert output to flexbuffer format.
        private static Expression<Func<FlxValue, bool?>> getBoolValueExpr = (x) => GetBoolValue(x);
        private static Expression<Func<bool?, FlxValue>> convertToFlxValue = (x) => GetFlxValue(x);
        private static Expression<Func<bool?, bool?, bool?>> andExpr = (x, y) => x & y;
        private static Expression<Func<bool?, bool?, bool?>> orExpr = (x, y) => x | y;
        private static Expression<Func<bool?, bool?, bool?>> xorExpr = (x, y) => x ^ y;

        public static void AddBooleanFunctions(FunctionsRegister functionsRegister)
        {
            // And function based on Kleene logic.
            // Microsoft has support for kleene logic on nullable types see here:
            // https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/operators/boolean-logical-operators#nullable-boolean-logical-operators
            functionsRegister.RegisterScalarFunction(FunctionsBoolean.Uri, FunctionsBoolean.And,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    return BuildExpression(visitor, parametersInfo, scalarFunction, andExpr);
                });
            functionsRegister.RegisterScalarFunction(FunctionsBoolean.Uri, FunctionsBoolean.Or,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    return BuildExpression(visitor, parametersInfo, scalarFunction, orExpr);
                });
            functionsRegister.RegisterScalarFunction(FunctionsBoolean.Uri, FunctionsBoolean.Xor,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    return BuildExpression(visitor, parametersInfo, scalarFunction, xorExpr);
                });

            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsBoolean.Uri, FunctionsBoolean.Not, x => NotImplementation(x));
        }

        private static FlxValue NotImplementation(FlxValue value)
        {
            return GetFlxValue(!GetBoolValue(value));
        }

        private static System.Linq.Expressions.Expression BuildExpression(
            Substrait.Expressions.ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo> visitor,
            ParametersInfo parametersInfo,
            ScalarFunction scalarFunction,
            Expression<Func<bool?, bool?, bool?>> condition)
        {
            var expr = visitor.Visit(scalarFunction.Arguments[0], parametersInfo);

            if (expr == null)
            {
                throw new InvalidOperationException("Expression cannot be null");
            }

            var getBoolValueBody = getBoolValueExpr.Body;
            expr = new ParameterReplacerVisitor(getBoolValueExpr.Parameters[0], expr).Visit(getBoolValueBody);

            for (int i = 1; i < scalarFunction.Arguments.Count; i++)
            {
                var resolved = visitor.Visit(scalarFunction.Arguments[i], parametersInfo);

                if (resolved == null)
                {
                    throw new InvalidOperationException("Expression cannot be null");
                }

                getBoolValueBody = getBoolValueExpr.Body;
                getBoolValueBody = new ParameterReplacerVisitor(getBoolValueExpr.Parameters[0], resolved).Visit(getBoolValueBody);


                var conditionBody = condition.Body;

                conditionBody = new ParameterReplacerVisitor(condition.Parameters[0], expr).Visit(conditionBody);
                expr = new ParameterReplacerVisitor(condition.Parameters[1], getBoolValueBody).Visit(conditionBody);
            }

            // We only need to convert to flexbuffer in the end
            var convertToFlxValueBody = convertToFlxValue.Body;
            convertToFlxValueBody = new ParameterReplacerVisitor(convertToFlxValue.Parameters[0], expr).Visit(convertToFlxValueBody);

            return convertToFlxValueBody;
        }

        private static bool? GetBoolValue(FlxValue x)
        {
            if (x.IsNull)
            {
                return default;
            }
            if (x.ValueType == FlexBuffers.Type.Bool)
            {
                return x.AsBool;
            }
            return false;
        }

        private static FlxValue GetFlxValue(bool? x)
        {
            if (x.HasValue)
            {
                if (x.Value)
                {
                    return TrueVal;
                }
                else
                {
                    return FalseVal;
                }
            }
            return NullValue;
        }
    }
}

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
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInBooleanFunctions
    {
        private static DataValueContainer TrueVal = new DataValueContainer()
        {
            _type = ArrowTypeId.Boolean,
            _boolValue = new BoolValue(true)
        };
        private static DataValueContainer FalseVal = new DataValueContainer()
        {
            _type = ArrowTypeId.Boolean,
            _boolValue = new BoolValue(false)
        };
        private static DataValueContainer NullVal = new DataValueContainer()
        {
            _type = ArrowTypeId.Null,
        };
        private static Expression<Func<bool?, IDataValue>> convertToDataValue = (x) => GetDataValue(x);

        private static Expression<Func<bool?, bool?, bool?>> andExpr = (x, y) => x & y;
        private static Expression<Func<bool?, bool?, bool?>> orExpr = (x, y) => x | y;
        private static Expression<Func<bool?, bool?, bool?>> xorExpr = (x, y) => x ^ y;

        private static IDataValue GetDataValue(bool? val)
        {
            if (val.HasValue)
            {
                if (val.Value)
                {
                    return TrueVal;
                }
                else
                {
                    return FalseVal;
                }
            }
            return NullVal;
        }

        private static LambdaExpression GetBoolValueLambda(System.Type inputType)
        {
            var methodInfo = typeof(BuiltInBooleanFunctions).GetMethod(nameof(GetBoolValue), BindingFlags.Static | BindingFlags.NonPublic)!.MakeGenericMethod(inputType);

            var parameter = System.Linq.Expressions.Expression.Parameter(inputType, "x");
            var call = System.Linq.Expressions.Expression.Call(methodInfo, parameter);
            return System.Linq.Expressions.Expression.Lambda(call, parameter);
        }

        private static bool? GetBoolValue<T>(T x)
            where T : IDataValue
        {
            if (x.IsNull)
            {
                return default;
            }
            if (x.Type == ArrowTypeId.Boolean)
            {
                return x.AsBool;
            }
            return false;
        }

        public static void AddBooleanFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterColumnScalarFunction(FunctionsBoolean.Uri, FunctionsBoolean.And,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    return BuildExpression(visitor, parametersInfo, scalarFunction, andExpr);
                });
            functionsRegister.RegisterColumnScalarFunction(FunctionsBoolean.Uri, FunctionsBoolean.Or,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    return BuildExpression(visitor, parametersInfo, scalarFunction, orExpr);
                });
            functionsRegister.RegisterColumnScalarFunction(FunctionsBoolean.Uri, FunctionsBoolean.Xor,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    return BuildExpression(visitor, parametersInfo, scalarFunction, xorExpr);
                });

            functionsRegister.RegisterScalarMethod(FunctionsBoolean.Uri, FunctionsBoolean.Not, typeof(BuiltInBooleanFunctions), nameof(NotImplementation));
        }

        private static IDataValue NotImplementation<T>(T value)
            where T : IDataValue
        {
            return GetDataValue(!GetBoolValue(value));
        }

        private static System.Linq.Expressions.Expression BuildExpression(
            ExpressionVisitor<System.Linq.Expressions.Expression, ColumnParameterInfo> visitor,
            ColumnParameterInfo parametersInfo,
            ScalarFunction scalarFunction,
            Expression<Func<bool?, bool?, bool?>> condition)
        {
            var expr = visitor.Visit(scalarFunction.Arguments[0], parametersInfo);

            if (expr == null)
            {
                throw new InvalidOperationException("Expression cannot be null");
            }

            var getBoolValueExpr = GetBoolValueLambda(expr.Type);
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
            var convertToFlxValueBody = convertToDataValue.Body;
            convertToFlxValueBody = new ParameterReplacerVisitor(convertToDataValue.Parameters[0], expr).Visit(convertToFlxValueBody);

            return convertToFlxValueBody;
        }
    }
}

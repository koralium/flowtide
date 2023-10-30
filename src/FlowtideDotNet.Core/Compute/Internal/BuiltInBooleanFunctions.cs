﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using SqlParser;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

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
        }

        private static System.Linq.Expressions.Expression BuildExpression(
            Substrait.Expressions.ExpressionVisitor<System.Linq.Expressions.Expression, ParametersInfo> visitor,
            ParametersInfo parametersInfo,
            ScalarFunction scalarFunction,
            Expression<Func<bool?, bool?, bool?>> condition)
        {
            var expr = visitor.Visit(scalarFunction.Arguments.First(), parametersInfo);

            var getBoolValueBody = getBoolValueExpr.Body;
            expr = new ParameterReplacerVisitor(getBoolValueExpr.Parameters[0], expr).Visit(getBoolValueBody);

            for (int i = 1; i < scalarFunction.Arguments.Count; i++)
            {
                var resolved = visitor.Visit(scalarFunction.Arguments[i], parametersInfo);

                getBoolValueBody = getBoolValueExpr.Body;
                getBoolValueBody = new ParameterReplacerVisitor(getBoolValueExpr.Parameters[0], resolved).Visit(getBoolValueBody);
                if (resolved == null)
                {
                    throw new Exception();
                }

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
            return x.IsNull ? default(bool?) : x.ValueType == FlexBuffers.Type.Bool ? x.AsBool : false;
        }

        private static FlxValue GetFlxValue(bool? x)
        {
            return x.HasValue ? (x.Value ? TrueVal : FalseVal) : NullValue;
        }
    }
}

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
using FlowtideDotNet.Core.Compute.Internal.StatefulAggregations;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInStringFunctions
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());
        private static FlxValue TrueValue = FlxValue.FromBytes(FlexBuffer.SingleValue(true));
        private static FlxValue FalseValue = FlxValue.FromBytes(FlexBuffer.SingleValue(false));

        private static System.Linq.Expressions.MethodCallExpression ConcatExpr(System.Linq.Expressions.Expression array)
        {
            MethodInfo? toStringMethod = typeof(FlxValueStringFunctions).GetMethod("Concat", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(toStringMethod != null);
            return System.Linq.Expressions.Expression.Call(toStringMethod, array);
        }

        public static void AddStringFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarFunction(FunctionsString.Uri, FunctionsString.Concat,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    List<System.Linq.Expressions.Expression> expressions = new List<System.Linq.Expressions.Expression>();
                    foreach (var expr in scalarFunction.Arguments)
                    {
                        expressions.Add(visitor.Visit(expr, parametersInfo)!);
                    }
                    var array = System.Linq.Expressions.Expression.NewArrayInit(typeof(FlxValue), expressions);
                    return ConcatExpr(array);
                });

            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.Lower, (x) => LowerImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.Upper, (x) => UpperImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.Trim, (x) => TrimImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.LTrim, (x) => LTrimImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.RTrim, (x) => RTrimImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.To_String, (x) => ToStringImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsString.Uri, FunctionsString.StartsWith, (x, y) => StartsWithImplementation(x, y));

            functionsRegister.RegisterScalarFunction(FunctionsString.Uri, FunctionsString.Substring,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    if (scalarFunction.Arguments.Count < 2)
                    {
                        throw new InvalidOperationException("Substring function must have atleast 2 arguments");
                    }
                    var expr = visitor.Visit(scalarFunction.Arguments[0], parametersInfo)!;
                    var start = visitor.Visit(scalarFunction.Arguments[1], parametersInfo)!;

                    Expression? length = default;
                    if (scalarFunction.Arguments.Count == 3)
                    {
                        length = visitor.Visit(scalarFunction.Arguments[2], parametersInfo)!;
                    }
                    else
                    {
                        length = Expression.Constant(FlxValue.FromBytes(FlexBuffer.SingleValue(-1)));
                    }
                    MethodInfo? toStringMethod = typeof(FlxValueStringFunctions).GetMethod("Substring", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
                    Debug.Assert(toStringMethod != null);
                    return System.Linq.Expressions.Expression.Call(toStringMethod, expr, start, length);
                });

            StringAggAggregation.Register(functionsRegister);
        }

        private static FlxValue ToStringImplementation(in FlxValue val)
        {
            if (val.IsNull)
            {
                return NullValue;
            }
            return FlxValue.FromBytes(FlexBuffer.SingleValue(FlxValueStringFunctions.ToString(val)));
        }

        private static FlxValue LowerImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.ToLower()));
        }

        private static FlxValue UpperImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.ToUpper()));
        }

        private static FlxValue TrimImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.Trim()));
        }

        private static FlxValue LTrimImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.TrimStart()));
        }

        private static FlxValue RTrimImplementation(in FlxValue val)
        {
            if (val.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }

            return FlxValue.FromBytes(FlexBuffer.SingleValue(val.AsString.TrimEnd()));
        }

        private static FlxValue StartsWithImplementation(in FlxValue val1, in FlxValue val2)
        {
            if (val1.ValueType != FlexBuffers.Type.String || val2.ValueType != FlexBuffers.Type.String)
            {
                return FalseValue;
            }

            if (val1.AsString.StartsWith(val2.AsString))
            {
                return TrueValue;
            }
            return FalseValue;
        }
    }
}

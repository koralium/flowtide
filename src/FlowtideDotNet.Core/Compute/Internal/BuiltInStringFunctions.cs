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
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInStringFunctions
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());

        private static System.Linq.Expressions.MethodCallExpression ConcatExpr(System.Linq.Expressions.Expression array)
        {
            MethodInfo toStringMethod = typeof(FlxValueStringFunctions).GetMethod("Concat", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
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
    }
}

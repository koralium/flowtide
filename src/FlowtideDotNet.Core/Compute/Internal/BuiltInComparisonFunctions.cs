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
using System.Diagnostics;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInComparisonFunctions
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());
        private static readonly FlxValue TrueVal = FlxValue.FromBytes(FlexBuffer.SingleValue(true));
        private static readonly FlxValue FalseVal = FlxValue.FromBytes(FlexBuffer.SingleValue(false));

        private static System.Linq.Expressions.MethodCallExpression Compare(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo? compareMethod = typeof(FlxValueComparer).GetMethod("CompareTo", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(compareMethod != null);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        private static System.Linq.Expressions.Expression AccessIsNullProperty(System.Linq.Expressions.Expression p)
        {
            var props = typeof(FlxValue).GetProperties().FirstOrDefault(x => x.Name == "IsNull");
            Debug.Assert(props != null);
            var getMethod = props.GetMethod;
            Debug.Assert(getMethod != null);
            return System.Linq.Expressions.Expression.Property(p, getMethod);
        }

        public static void AddComparisonFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.Equal, (x, y) => EqualImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.NotEqual, (x, y) => NotEqualImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.GreaterThan, (x, y) => GreaterThanImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.GreaterThanOrEqual, (x, y) => GreaterThanOrEqualImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.LessThan, (x, y) => LessThanImplementation(x, y));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.LessThanOrEqual, (x, y) => LessThanOrEqualImplementation(x, y));

            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.IsNotNull, (x) => x.IsNull ? FalseVal : TrueVal);

            functionsRegister.RegisterScalarFunction(FunctionsComparison.Uri, FunctionsComparison.Coalesce,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    // Start from bottom and build up
                    var lastArg = visitor.Visit(scalarFunction.Arguments[scalarFunction.Arguments.Count - 1], parametersInfo);

                    if (lastArg == null)
                    {
                        throw new InvalidOperationException("Could not compile coalesce function");
                    }

                    var expr = lastArg;
                    for (int i = scalarFunction.Arguments.Count - 2; i >= 0; i--)
                    {
                        var newArg = visitor.Visit(scalarFunction.Arguments[i], parametersInfo);
                        if (newArg == null) 
                        {                             
                            throw new InvalidOperationException("Could not compile coalesce function");
                        }
                        var condition = System.Linq.Expressions.Expression.Not(AccessIsNullProperty(newArg));
                        expr = System.Linq.Expressions.Expression.Condition(condition, newArg, expr);
                    }

                    return expr;
                });

            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.isInfinite, (x) => IsInfiniteImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.IsFinite, (x) => IsFiniteImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.Between, (x, y, z) => BetweenImplementation(x, y, z));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.IsNull, (x) => x.IsNull ? TrueVal : FalseVal);
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.IsNan, (x) => IsNanImplementation(x));
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsComparison.Uri, FunctionsComparison.NullIf, (x, y) => NullIfImplementation(x, y));
        }

        /// <summary>
        /// Equals with Kleene logic
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        private static FlxValue EqualImplementation(in FlxValue x, in FlxValue y)
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                return NullValue;
            }
            else if (FlxValueComparer.CompareTo(x, y) == 0)
            {
                return TrueVal;
            }
            else
            {
                return FalseVal;
            }
        }

        private static FlxValue NotEqualImplementation(in FlxValue x, in FlxValue y)
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                return NullValue;
            }
            else if (FlxValueComparer.CompareTo(x, y) != 0)
            {
                return TrueVal;
            }
            else
            {
                return FalseVal;
            }
        }

        private static FlxValue GreaterThanImplementation(in FlxValue x, in FlxValue y)
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                return NullValue;
            }
            else if (FlxValueComparer.CompareTo(x, y) > 0)
            {
                return TrueVal;
            }
            else
            {
                return FalseVal;
            }
        }

        private static FlxValue GreaterThanOrEqualImplementation(in FlxValue x, in FlxValue y)
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                return NullValue;
            }
            else if (FlxValueComparer.CompareTo(x, y) >= 0)
            {
                return TrueVal;
            }
            else
            {
                return FalseVal;
            }
        }

        private static FlxValue LessThanImplementation(in FlxValue x, in FlxValue y)
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                return NullValue;
            }
            else if (FlxValueComparer.CompareTo(x, y) < 0)
            {
                return TrueVal;
            }
            else
            {
                return FalseVal;
            }
        }

        private static FlxValue LessThanOrEqualImplementation(in FlxValue x, in FlxValue y)
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                return NullValue;
            }
            else if (FlxValueComparer.CompareTo(x, y) <= 0)
            {
                return TrueVal;
            }
            else
            {
                return FalseVal;
            }
        }



        private static FlxValue IsInfiniteImplementation(in FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                var val = x.AsDouble;
                if (val == double.PositiveInfinity || val == double.NegativeInfinity)
                {
                    return TrueVal;
                }
                else
                {
                    return FalseVal;
                }
            }
            return FalseVal;
        }

        private static FlxValue IsFiniteImplementation(in FlxValue x)
        {
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                var val = x.AsDouble;
                if (val == double.PositiveInfinity || val == double.NegativeInfinity || double.IsNaN(val))
                {
                    return FalseVal;
                }
                else
                {
                    return TrueVal;
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return TrueVal;
            }
            return FalseVal;
        }

        private static FlxValue BetweenImplementation(in FlxValue expr, in FlxValue low, in FlxValue high)
        {
            if (FlxValueComparer.CompareTo(expr, low) >= 0 && FlxValueComparer.CompareTo(expr, high) <= 0)
            {
                return TrueVal;
            }
            else
            {
                return FalseVal;
            }
        }

        private static FlxValue IsNanImplementation(in FlxValue x)
        {
            if (x.IsNull)
            {
                return NullValue;
            }
            if (x.ValueType == FlexBuffers.Type.Float)
            {
                var val = x.AsDouble;
                if (double.IsNaN(val))
                {
                    return TrueVal;
                }
                else
                {
                    return FalseVal;
                }
            }
            else if (x.ValueType == FlexBuffers.Type.Int)
            {
                return FalseVal;
            }
            return TrueVal;
        }

        private static FlxValue NullIfImplementation(in FlxValue x, in FlxValue y)
        {
            if (FlxValueComparer.CompareTo(x, y) == 0)
            {
                return NullValue;
            }
            else
            {
                return x;
            }
        }
    }
}

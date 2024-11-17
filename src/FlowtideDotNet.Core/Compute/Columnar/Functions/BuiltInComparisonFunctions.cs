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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInComparisonFunctions
    {
        private static System.Linq.Expressions.Expression AccessIsNullProperty(System.Linq.Expressions.Expression p)
        {
            var props = Array.Find(typeof(IDataValue).GetProperties(), x => x.Name == "IsNull");
            Debug.Assert(props != null);
            var getMethod = props.GetMethod;
            Debug.Assert(getMethod != null);
            return System.Linq.Expressions.Expression.Property(p, getMethod);
        }

        public static void AddComparisonFunctions(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarMethod(FunctionsComparison.Uri, FunctionsComparison.Equal, typeof(BuiltInComparisonFunctions), nameof(EqualImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsComparison.Uri, FunctionsComparison.NotEqual, typeof(BuiltInComparisonFunctions), nameof(NotEqualImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsComparison.Uri, FunctionsComparison.GreaterThan, typeof(BuiltInComparisonFunctions), nameof(GreaterThanImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsComparison.Uri, FunctionsComparison.IsNull, typeof(BuiltInComparisonFunctions), nameof(IsNullImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsComparison.Uri, FunctionsComparison.Between, typeof(BuiltInComparisonFunctions), nameof(BetweenImplementation));

            functionsRegister.RegisterColumnScalarFunction(FunctionsComparison.Uri, FunctionsComparison.Coalesce,
                (scalarFunction, parametersInfo, visitor) =>
                {
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


        }

        private static IDataValue EqualImplementation<T1, T2>(in T1 x, in T2 y, in DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            else if (DataValueComparer.CompareTo(x, y) == 0)
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(true);
                return result;
            }
            else
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(false);
                return result;
            }
        }

        private static IDataValue NotEqualImplementation<T1, T2>(in T1 x, in T2 y, in DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            else if (DataValueComparer.CompareTo(x, y) != 0)
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(true);
                return result;
            }
            else
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(false);
                return result;
            }
        }

        private static IDataValue GreaterThanImplementation<T1, T2>(in T1 x, in T2 y, in DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            // If either is null, return null
            if (x.IsNull || y.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            else if (DataValueComparer.CompareTo(x, y) > 0)
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(true);
                return result;
            }
            else
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(false);
                return result;
            }
        }

        private static IDataValue IsNullImplementation<T>(in T x, in DataValueContainer result)
            where T : IDataValue
        {
            result._type = ArrowTypeId.Boolean;
            result._boolValue = new BoolValue(x.IsNull);
            return result;
        }

        private static IDataValue BetweenImplementation<T1, T2, T3>(in T1 expr, in T2 low, in T3 high, in DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
            where T3 : IDataValue
        {

            if (DataValueComparer.CompareTo(expr, low) >= 0 && DataValueComparer.CompareTo(expr, high) <= 0)
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(true);
                return result;
            }
            else
            {
                result._type = ArrowTypeId.Boolean;
                result._boolValue = new BoolValue(false);
                return result;
            }
        }
    }
}

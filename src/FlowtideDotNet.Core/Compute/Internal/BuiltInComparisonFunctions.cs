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
    internal static class BuiltInComparisonFunctions
    {
        private static System.Linq.Expressions.MethodCallExpression Compare(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo compareMethod = typeof(FlxValueComparer).GetMethod("CompareTo", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        private static System.Linq.Expressions.Expression AccessIsNullProperty(System.Linq.Expressions.Expression p)
        {
            var props = typeof(FlxValue).GetProperties().FirstOrDefault(x => x.Name == "IsNull");
            var getMethod = props.GetMethod;
            return System.Linq.Expressions.Expression.Property(p, getMethod);
        }

        public static void AddComparisonFunctions(FunctionsRegister functionsRegister)
        {
            // Equals
            functionsRegister.RegisterScalarFunction(FunctionsComparison.Uri, FunctionsComparison.Equal,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    if (scalarFunction.Arguments.Count != 2)
                        throw new ArgumentException("Equal function requires 2 arguments");

                    var left = visitor.Visit(scalarFunction.Arguments[0], parametersInfo);
                    var right = visitor.Visit(scalarFunction.Arguments[1], parametersInfo);
                    return System.Linq.Expressions.Expression.Equal(Compare(left, right), System.Linq.Expressions.Expression.Constant(0));
                });
            // Not equal
            functionsRegister.RegisterScalarFunction(FunctionsComparison.Uri, FunctionsComparison.NotEqual,
                 (scalarFunction, parametersInfo, visitor) =>
                 {
                     if (scalarFunction.Arguments.Count != 2)
                         throw new ArgumentException("Not equal function requires 2 arguments");

                     var left = visitor.Visit(scalarFunction.Arguments[0], parametersInfo);
                     var right = visitor.Visit(scalarFunction.Arguments[1], parametersInfo);
                     return System.Linq.Expressions.Expression.NotEqual(Compare(left, right), System.Linq.Expressions.Expression.Constant(0));
                 });
            // Greater than
            functionsRegister.RegisterScalarFunction(FunctionsComparison.Uri, FunctionsComparison.GreaterThan,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    if (scalarFunction.Arguments.Count != 2)
                        throw new ArgumentException("Greater than function requires 2 arguments");

                    var left = visitor.Visit(scalarFunction.Arguments[0], parametersInfo);
                    var right = visitor.Visit(scalarFunction.Arguments[1], parametersInfo);
                    return System.Linq.Expressions.Expression.GreaterThan(Compare(left, right), System.Linq.Expressions.Expression.Constant(0));
                });
            // Greater than or equal
            functionsRegister.RegisterScalarFunction(FunctionsComparison.Uri, FunctionsComparison.GreaterThanOrEqual,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    if (scalarFunction.Arguments.Count != 2)
                        throw new ArgumentException("Greater than or equal function requires 2 arguments");

                    var left = visitor.Visit(scalarFunction.Arguments[0], parametersInfo);
                    var right = visitor.Visit(scalarFunction.Arguments[1], parametersInfo);
                    return System.Linq.Expressions.Expression.GreaterThanOrEqual(Compare(left, right), System.Linq.Expressions.Expression.Constant(0));
                });
            // Less than
            functionsRegister.RegisterScalarFunction(FunctionsComparison.Uri, FunctionsComparison.LessThan,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    if (scalarFunction.Arguments.Count != 2)
                        throw new ArgumentException("Less than function requires 2 arguments");

                    var left = visitor.Visit(scalarFunction.Arguments[0], parametersInfo);
                    var right = visitor.Visit(scalarFunction.Arguments[1], parametersInfo);
                    return System.Linq.Expressions.Expression.LessThan(Compare(left, right), System.Linq.Expressions.Expression.Constant(0));
                });
            // Less than or equal
            functionsRegister.RegisterScalarFunction(FunctionsComparison.Uri, FunctionsComparison.LessThanOrEqual,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    if (scalarFunction.Arguments.Count != 2)
                        throw new ArgumentException("Less than or equal function requires 2 arguments");

                    var left = visitor.Visit(scalarFunction.Arguments[0], parametersInfo);
                    var right = visitor.Visit(scalarFunction.Arguments[1], parametersInfo);
                    return System.Linq.Expressions.Expression.LessThanOrEqual(Compare(left, right), System.Linq.Expressions.Expression.Constant(0));
                });
            // Is not null
            functionsRegister.RegisterScalarFunction(FunctionsComparison.Uri, FunctionsComparison.IsNotNull,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    if (scalarFunction.Arguments.Count != 1)
                        throw new ArgumentException("Is not null function requires 1 argument");

                    var arg = visitor.Visit(scalarFunction.Arguments[0], parametersInfo);
                    return System.Linq.Expressions.Expression.Not(AccessIsNullProperty(arg));
                });

            functionsRegister.RegisterScalarFunction(FunctionsComparison.Uri, FunctionsComparison.Coalesce,
                (scalarFunction, parametersInfo, visitor) =>
                {
                    // Start from bottom and build up
                    var lastArg = visitor.Visit(scalarFunction.Arguments[scalarFunction.Arguments.Count - 1], parametersInfo);

                    var expr = lastArg;
                    for (int i = scalarFunction.Arguments.Count - 2; i >= 0; i--)
                    {
                        var newArg = visitor.Visit(scalarFunction.Arguments[i], parametersInfo);
                        var condition = System.Linq.Expressions.Expression.Not(AccessIsNullProperty(newArg));
                        expr = System.Linq.Expressions.Expression.Condition(condition, newArg, expr);
                    }

                    return expr;
                });
        }
    }
}

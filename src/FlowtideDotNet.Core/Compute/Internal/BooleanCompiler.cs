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
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BooleanCompiler
    {
        /// <summary>
        /// Compiles to a function that will always return a boolean
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        /// <param name="functionsRegister"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static Func<T, bool> Compile<T>(Substrait.Expressions.Expression expression, FunctionsRegister functionsRegister)
        {
            var param = Expression.Parameter(typeof(T));

            var visitor = new FlowtideExpressionVisitor(functionsRegister, typeof(T));
            var expr = visitor.Visit(expression, new ParametersInfo(new List<ParameterExpression>() { param }, new List<int> { 0 }));

            if (expr == null)
            {
                throw new InvalidOperationException("Could not compile a filter.");
            }

            if (expr.Type.Equals(typeof(FlxValue)))
            {
                var toBoolMethod = typeof(FlxValueBoolFunctions).GetMethod("ToBool", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);

                if (toBoolMethod == null)
                {
                    throw new InvalidOperationException("Could not found ToBool method");
                }

                expr = System.Linq.Expressions.Expression.Call(toBoolMethod, expr);
            }
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<T, bool>>(expr, param);
            return lambda.Compile();
        }

        public static Func<T, T, bool> Compile<T>(Substrait.Expressions.Expression expression, FunctionsRegister functionsRegister, int leftSize)
        {
            var param1 = Expression.Parameter(typeof(T));
            var param2 = Expression.Parameter(typeof(T));

            var visitor = new FlowtideExpressionVisitor(functionsRegister, typeof(T));
            var expr = visitor.Visit(expression, new ParametersInfo(new List<ParameterExpression>() { param1, param2 }, new List<int> { 0, leftSize }));

            if (expr == null)
            {
                throw new InvalidOperationException("Could not compile a filter.");
            }

            if (expr.Type.Equals(typeof(FlxValue)))
            {
                var toBoolMethod = typeof(FlxValueBoolFunctions).GetMethod("ToBool", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);

                if (toBoolMethod == null)
                {
                    throw new InvalidOperationException("Could not found ToBool method");

                }
                expr = System.Linq.Expressions.Expression.Call(toBoolMethod, expr);
            }

            var lambda = System.Linq.Expressions.Expression.Lambda<Func<T, T, bool>>(expr, param1, param2);
            return lambda.Compile();
        }
    }
}

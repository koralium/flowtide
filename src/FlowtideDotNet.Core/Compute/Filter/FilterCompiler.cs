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

using FlowtideDotNet.Core.Operators.Read;
using FlexBuffers;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Core.Compute.Project;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Filter
{
    internal static class FilterCompiler
    {
        public static Func<StreamEvent, bool> Compile(Expression expression)
        {
            var visitor = new FilterCompilerVisitor<StreamEvent>();

            var param = System.Linq.Expressions.Expression.Parameter(typeof(StreamEvent));
            var expr = visitor.Visit(expression, param);

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
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<StreamEvent, bool>>(expr, param);
            return lambda.Compile();
        }

        public static Func<IngressData, bool> CompileIngress(Expression expression)
        {
            var visitor = new FilterCompilerVisitor<IngressData>();

            var param = System.Linq.Expressions.Expression.Parameter(typeof(IngressData));
            var expr = visitor.Visit(expression, param);
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<IngressData, bool>>(expr, param);
            return lambda.Compile();
        }

        public static Func<StreamEvent, FlxValue, bool> CompileWithDate(Expression expression)
        {
            var visitor = new FilterCompilerVisitor<StreamEvent>();

            var param = System.Linq.Expressions.Expression.Parameter(typeof(StreamEvent));
            var dateParam = System.Linq.Expressions.Expression.Parameter(typeof(FlxValue));
            visitor._dateParam = dateParam;
            var expr = visitor.Visit(expression, param);
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<StreamEvent, FlxValue, bool>>(expr, param, dateParam);
            return lambda.Compile();
        }

        public static System.Linq.Expressions.Expression CompileJoinFilterExpression<T>(System.Linq.Expressions.ParameterExpression left, System.Linq.Expressions.ParameterExpression right, Expression expression, int leftSize)
        {
            var visitor = new JoinFilterCompilerVisitor<T>();

            var expr = visitor.Visit(expression, new JoinFilterCompilerState()
            {
                Parameters = new List<System.Linq.Expressions.ParameterExpression>() { left, right },
                RelativeIndices = new List<int>() { 0, leftSize }
            });

            return expr;
        }

        public static Func<T, T, bool> CompileJoinFilter<T>(Expression expression, int leftSize)
        {
            var visitor = new JoinFilterCompilerVisitor<T>();

            var leftParam = System.Linq.Expressions.Expression.Parameter(typeof(T));
            var rightParam = System.Linq.Expressions.Expression.Parameter(typeof(T));

            var expr = visitor.Visit(expression, new JoinFilterCompilerState()
            {
                Parameters = new List<System.Linq.Expressions.ParameterExpression>() { leftParam, rightParam },
                RelativeIndices = new List<int>() { 0, leftSize }
            });

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

            var lambda = System.Linq.Expressions.Expression.Lambda<Func<T, T, bool>>(expr, leftParam, rightParam);
            return lambda.Compile();
        }
    }
}

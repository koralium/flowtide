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

using FlowtideDotNet.Core.Operators.Join;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Index
{
    internal static class IndexCompiler
    {
        private static IndexCompilerVisitor compilerVisitor = new IndexCompilerVisitor();

        /// <summary>
        /// Creates an IComparer function for an expression to be used in index creation
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns></returns>
        public static System.Linq.Expressions.Expression Compile(FlowtideDotNet.Substrait.Expressions.Expression expression, int relativeSize, ParameterExpression left, ParameterExpression right)
        {
            var l = compilerVisitor.Visit(expression, new IndexCompilerVisitor.State() { Parameter = left, RelativeIndex = relativeSize });
            var r = compilerVisitor.Visit(expression, new IndexCompilerVisitor.State() { Parameter = right, RelativeIndex = relativeSize });

            return Compare(l, r);
        }

        public static System.Linq.Expressions.Expression<Func<JoinStreamEvent, JoinStreamEvent, int>> CompileIndexFunction(FlowtideDotNet.Substrait.Expressions.Expression expression, int relativeSize)
        {
            var p1 = Expression.Parameter(typeof(JoinStreamEvent));
            var p2 = Expression.Parameter(typeof(JoinStreamEvent));

            var compiledExpr = Compile(expression, relativeSize, p1, p2);

            // Should check if it is 0, one should do comparison on the actual data on the object.

            var lambda = Expression.Lambda<Func<JoinStreamEvent, JoinStreamEvent, int>>(compiledExpr, p1, p2);

            return lambda;
        }

        internal static System.Linq.Expressions.MethodCallExpression Compare(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo compareMethod = typeof(FlxValueComparer).GetMethod("CompareTo", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        internal static System.Linq.Expressions.MethodCallExpression CompareRef(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo compareMethod = typeof(FlxValueRefComparer).GetMethod("CompareTo", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }
    }
}

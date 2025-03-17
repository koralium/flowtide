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
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    internal static class ColumnBooleanCompiler
    {
        public static Func<EventBatchData, int, bool> Compile(Substrait.Expressions.Expression expression, IFunctionsRegister functionsRegister)
        {
            var batchParam = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData));
            var intParam = System.Linq.Expressions.Expression.Parameter(typeof(int));

            var visitor = new ColumnarExpressionVisitor(functionsRegister);
            var resultContainer = System.Linq.Expressions.Expression.Constant(new DataValueContainer());
            var expr = visitor.Visit(expression, new ColumnParameterInfo(new List<ParameterExpression>() { batchParam }, new List<ParameterExpression>() { intParam }, new List<int> { 0 }, resultContainer));

            if (expr == null)
            {
                throw new InvalidOperationException("Could not compile a filter.");
            }

            if (!expr.Type.Equals(typeof(bool)))
            {
                MethodInfo? genericToBoolMethod = typeof(DataValueBoolFunctions).GetMethod("ToBool", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
                var toBoolMethod = genericToBoolMethod!.MakeGenericMethod(expr.Type);

                if (toBoolMethod == null)
                {
                    throw new InvalidOperationException("Could not found ToBool method");
                }

                expr = System.Linq.Expressions.Expression.Call(toBoolMethod, expr);
            }
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<EventBatchData, int, bool>>(expr, batchParam, intParam);
            return lambda.Compile();
        }

        public static Func<EventBatchData, int, EventBatchData, int, bool> CompileTwoInputs(Substrait.Expressions.Expression expression, IFunctionsRegister functionsRegister, int leftSize)
        {
            var batchParam1 = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData));
            var intParam1 = System.Linq.Expressions.Expression.Parameter(typeof(int));

            var batchParam2 = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData));
            var intParam2 = System.Linq.Expressions.Expression.Parameter(typeof(int));

            var visitor = new ColumnarExpressionVisitor(functionsRegister);
            var resultContainer = System.Linq.Expressions.Expression.Constant(new DataValueContainer());
            var expr = visitor.Visit(expression, new ColumnParameterInfo(
                new List<ParameterExpression>() { batchParam1, batchParam2 }
                , new List<ParameterExpression>() { intParam1, intParam2 }, 
                new List<int> { 0, leftSize }, resultContainer));

            if (expr == null)
            {
                throw new InvalidOperationException("Could not compile a filter.");
            }

            if (!expr.Type.Equals(typeof(bool)))
            {
                MethodInfo? genericToBoolMethod = typeof(DataValueBoolFunctions).GetMethod("ToBool", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
                var toBoolMethod = genericToBoolMethod!.MakeGenericMethod(expr.Type);

                if (toBoolMethod == null)
                {
                    throw new InvalidOperationException("Could not found ToBool method");
                }

                expr = System.Linq.Expressions.Expression.Call(toBoolMethod, expr);
            }

            var lambda = System.Linq.Expressions.Expression.Lambda<Func<EventBatchData, int, EventBatchData, int, bool>>(expr, batchParam1, intParam1, batchParam2, intParam2);
            return lambda.Compile();
        }
    }
}

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
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.WildcardExpression;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    internal static class ColumnProjectCompiler
    {
        /// <summary>
        /// Creates a method that will execute a expression and add the result to a column
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="functionsRegister"></param>
        /// <returns></returns>
        public static Action<EventBatchData, int, Column> Compile(Substrait.Expressions.Expression expression, IFunctionsRegister functionsRegister)
        {
            var visitor = new ColumnarExpressionVisitor(functionsRegister);
            var batchParam = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData));
            var indexParam = System.Linq.Expressions.Expression.Parameter(typeof(int));

            var resultInfo = System.Linq.Expressions.Expression.Constant(new DataValueContainer());

            var paramInfo = new ColumnParameterInfo(new List<ParameterExpression>() { batchParam }, new List<ParameterExpression>() { indexParam }, new List<int>() { 0 }, resultInfo);

            var resultExpr = visitor.Visit(expression, paramInfo);
            var columnParameter = System.Linq.Expressions.Expression.Parameter(typeof(Column));

            var addMethod = typeof(Column).GetMethod("Add");
            var genericMethod = addMethod.MakeGenericMethod(resultExpr.Type);
            var addToColumnExpr = System.Linq.Expressions.Expression.Call(columnParameter, genericMethod, resultExpr);
            

            var lambda = System.Linq.Expressions.Expression.Lambda<Action<EventBatchData, int, Column>>(addToColumnExpr, batchParam, indexParam, columnParameter);
            return lambda.Compile();
        }
    }
}

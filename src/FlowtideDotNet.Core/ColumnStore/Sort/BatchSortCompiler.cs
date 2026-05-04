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

using FlowtideDotNet.Core.ColumnStore.Comparers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    internal static class BatchSortCompiler
    {
        public static UInt128 CreateCompareKey(EventBatchData eventBatchData)
        {
            UInt128 key = 0;

            for (int i = 0; i < eventBatchData.Columns.Count && i < 7; i++)
            {
                var state = eventBatchData.Columns[i].GetColumnState();
                CompareColumnStateBuilder.BuildColumnsKey(ref key, state, i);
            }

            if (eventBatchData.Columns.Count > 7)
            {
                CompareColumnStateBuilder.AddHasTailToKey(ref key);
            }
            return key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareColumn(IColumn column, int x, int y)
        {
            var xval = column.GetValueAt(x, default);
            var yval = column.GetValueAt(y, default);

            return DataValueComparer.CompareTo(xval, yval);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static int CompareTail(ref SortCompareContext context, int x, int y)
        {
            if (x == y)
            {
                return 0;
            }
            var columns = context.columns;

            for (int i = 7; i < columns.Length; i++)
            {
                var xval = columns[i].GetValueAt(x, default);
                var yval = columns[i].GetValueAt(y, default);
                var result = DataValueComparer.CompareTo(xval, yval);
                if (result != 0)
                {
                    return result;
                }
            }
            return 0;
        }

        public delegate int CompareDelegate(ref SortCompareContext context, int x, int y);

        public static Expression<CompareDelegate> Compile(IColumn[] columns)
        {
            var pointersParameter = Expression.Parameter(typeof(SortCompareContext).MakeByRefType(), "context");
                
            var pointersField = typeof(SortCompareContext).GetField(nameof(SortCompareContext.pointers))!;
            var columnsField = typeof(SortCompareContext).GetField(nameof(SortCompareContext.columns))!;

            var fetchPointers = Expression.Field(pointersParameter, pointersField);
            var fetchColumns = Expression.Field(pointersParameter, columnsField);


            var xParameter = Expression.Parameter(typeof(int), "x");
            var yParameter = Expression.Parameter(typeof(int), "y");

            var returnTarget = Expression.Label(typeof(int), "ReturnLabel");
            var compareResultVar = Expression.Variable(typeof(int), "compareResult");
            var bodyExpressions = new List<Expression>();

            for (int i = 0; i < columns.Length && i < 7; i++)
            {
                var column = columns[i];

                if (column.SupportSelfCompareExpression)
                {
                    // Take out index i from the pointers parameter
                    var pointer = Expression.ArrayIndex(fetchPointers, Expression.Constant(i));
                    var compareResult = column.CreateSelfCompareExpression(pointer, xParameter, yParameter);
                    bodyExpressions.Add(Expression.Assign(compareResultVar, compareResult));

                    var ifCompareNotEqual = Expression.IfThen(
                        Expression.NotEqual(compareResultVar, Expression.Constant(0)),
                        Expression.Return(returnTarget, compareResultVar)
                    );
                    bodyExpressions.Add(ifCompareNotEqual);
                }
                else
                {
                    var col = Expression.ArrayIndex(fetchColumns, Expression.Constant(i));
                    var compareResult = Expression.Call(
                        typeof(BatchSortCompiler).GetMethod(nameof(CompareColumn))!,
                        col,
                        xParameter,
                        yParameter
                    );
                    bodyExpressions.Add(Expression.Assign(compareResultVar, compareResult));

                    var ifCompareNotEqual = Expression.IfThen(
                        Expression.NotEqual(compareResultVar, Expression.Constant(0)),
                        Expression.Return(returnTarget, compareResultVar)
                    );
                    bodyExpressions.Add(ifCompareNotEqual);
                }
            }

            if (columns.Length > 7)
            {
                // Add extra code to handle the remainder
                var compareTailCall = Expression.Call(
                    typeof(BatchSortCompiler).GetMethod(nameof(CompareTail))!,
                    pointersParameter,
                    xParameter,
                    yParameter
                );
                bodyExpressions.Add(Expression.Label(returnTarget, compareTailCall));
            }
            else
            {
                bodyExpressions.Add(Expression.Label(returnTarget, Expression.Constant(0)));
            }

            var block = Expression.Block(
                new[] { compareResultVar },
                bodyExpressions
            );

            var lambda = Expression.Lambda<CompareDelegate>(block, pointersParameter, xParameter, yParameter);
            return lambda;
        }

        public static BlockExpression Compile(IColumn[] columns, Expression contextParameter, Expression xParameter, Expression yParameter)
        {
            var pointersField = typeof(SortCompareContext).GetField(nameof(SortCompareContext.pointers))!;
            var columnsField = typeof(SortCompareContext).GetField(nameof(SortCompareContext.columns))!;

            var fetchPointers = Expression.Field(contextParameter, pointersField);
            var fetchColumns = Expression.Field(contextParameter, columnsField);

            var returnTarget = Expression.Label(typeof(int), "ReturnLabel");
            var compareResultVar = Expression.Variable(typeof(int), "compareResult");
            var bodyExpressions = new List<Expression>();

            for (int i = 0; i < columns.Length && i < 7; i++)
            {
                var column = columns[i];

                if (column.SupportSelfCompareExpression)
                {
                    // Take out index i from the pointers parameter
                    var pointer = Expression.ArrayIndex(fetchPointers, Expression.Constant(i));
                    var compareResult = column.CreateSelfCompareExpression(pointer, xParameter, yParameter);
                    bodyExpressions.Add(Expression.Assign(compareResultVar, compareResult));

                    var ifCompareNotEqual = Expression.IfThen(
                        Expression.NotEqual(compareResultVar, Expression.Constant(0)),
                        Expression.Return(returnTarget, compareResultVar)
                    );
                    bodyExpressions.Add(ifCompareNotEqual);
                }
                else
                {
                    var col = Expression.ArrayIndex(fetchColumns, Expression.Constant(i));
                    var compareResult = Expression.Call(
                        typeof(BatchSortCompiler).GetMethod(nameof(CompareColumn))!,
                        col,
                        xParameter,
                        yParameter
                    );
                    bodyExpressions.Add(Expression.Assign(compareResultVar, compareResult));

                    var ifCompareNotEqual = Expression.IfThen(
                        Expression.NotEqual(compareResultVar, Expression.Constant(0)),
                        Expression.Return(returnTarget, compareResultVar)
                    );
                    bodyExpressions.Add(ifCompareNotEqual);
                }
            }

            if (columns.Length > 7)
            {
                // Add extra code to handle the remainder
                var compareTailCall = Expression.Call(
                    typeof(BatchSortCompiler).GetMethod(nameof(CompareTail))!,
                    contextParameter,
                    xParameter,
                    yParameter
                );
                bodyExpressions.Add(Expression.Label(returnTarget, compareTailCall));
            }
            else
            {
                bodyExpressions.Add(Expression.Label(returnTarget, Expression.Constant(0)));
            }

            var block = Expression.Block(
                new[] { compareResultVar },
                bodyExpressions
            );

            return block;
        }
    }
}

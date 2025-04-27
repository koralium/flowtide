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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    internal class SortFieldCompareCompiler
    {

        // These methods are collected through reflection 
        internal static int CompareAscendingNullsFirstImplementation<T1, T2>(T1 a, T2 b)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (a.IsNull)
            {
                if (b.IsNull)
                {
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            else if (b.IsNull)
            {
                return 1;
            }
            return DataValueComparer.CompareTo(a, b);
        }

        internal static int CompareAscendingNullsLastImplementation<T1, T2>(T1 a, T2 b)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (a.IsNull)
            {
                if (b.IsNull)
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
            else if (b.IsNull)
            {
                return -1;
            }
            return DataValueComparer.CompareTo(a, b);
        }

        internal static int CompareDescendingNullsFirstImplementation<T1, T2>(T1 a, T2 b)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (a.IsNull)
            {
                if (b.IsNull)
                {
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            else if (b.IsNull)
            {
                return 1;
            }
            return DataValueComparer.CompareTo(b, a);
        }

        internal static int CompareDescendingNullsLastImplementation<T1, T2>(T1 a, T2 b)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (a.IsNull)
            {
                if (b.IsNull)
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
            else if (b.IsNull)
            {
                return -1;
            }
            return DataValueComparer.CompareTo(b, a);
        }

        private static System.Linq.Expressions.MethodCallExpression CompareAscendingNullsFirst(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo? compareMethod = typeof(SortFieldCompareCompiler).GetMethod(nameof(CompareAscendingNullsFirstImplementation), BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(compareMethod != null);
            var genericMethod = compareMethod.MakeGenericMethod(a.Type, b.Type);
            return System.Linq.Expressions.Expression.Call(genericMethod, a, b);
        }

        private static System.Linq.Expressions.MethodCallExpression CompareAscendingNullsLast(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo? compareMethod = typeof(SortFieldCompareCompiler).GetMethod(nameof(CompareAscendingNullsLastImplementation), BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(compareMethod != null);
            var genericMethod = compareMethod.MakeGenericMethod(a.Type, b.Type);
            return System.Linq.Expressions.Expression.Call(genericMethod, a, b);
        }

        private static System.Linq.Expressions.MethodCallExpression CompareDescendingNullsFirst(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo? compareMethod = typeof(SortFieldCompareCompiler).GetMethod(nameof(CompareDescendingNullsFirstImplementation), BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(compareMethod != null);
            var genericMethod = compareMethod.MakeGenericMethod(a.Type, b.Type);
            return System.Linq.Expressions.Expression.Call(genericMethod, a, b);
        }

        private static System.Linq.Expressions.MethodCallExpression CompareDescendingNullsLast(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo? compareMethod = typeof(SortFieldCompareCompiler).GetMethod(nameof(CompareDescendingNullsLastImplementation), BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(compareMethod != null);
            var genericMethod = compareMethod.MakeGenericMethod(a.Type, b.Type);
            return System.Linq.Expressions.Expression.Call(genericMethod, a, b);
        }

        internal class ColumnRowComparer : IColumnComparer<ColumnRowReference>
        {
            private readonly Func<EventBatchData, int, EventBatchData, int, int> _func;

            public ColumnRowComparer(Func<EventBatchData, int, EventBatchData, int, int> func)
            {
                _func = func;
            }

            public int Compare(in ColumnRowReference x, in ColumnRowReference y)
            {
                return _func(x.referenceBatch, x.RowIndex, y.referenceBatch, y.RowIndex);
            }
        }

        public static IColumnComparer<ColumnRowReference> CreateComparer(List<SortField> sortFields, IFunctionsRegister functionsRegister)
        {
            return new ColumnRowComparer(CreateComparerFunction(sortFields, functionsRegister));
        }

        public static IColumnComparer<ColumnRowReference> CreateComparerAgainstComputedColumns(List<SortField> sortFields, IFunctionsRegister functionsRegister)
        {
            return new ColumnRowComparer(CreateComparerFunctionAgainstComputedColumns(sortFields, functionsRegister));
        }

        public static Func<EventBatchData, int, EventBatchData, int, int> CreateComparerFunction(List<SortField> sortFields, IFunctionsRegister functionsRegister)
        {
            var visitor = new ColumnarExpressionVisitor(functionsRegister);
            ParameterExpression leftBatch = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData));
            ParameterExpression rightBatch = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData));

            ParameterExpression leftIndex = System.Linq.Expressions.Expression.Parameter(typeof(int));
            ParameterExpression rightIndex = System.Linq.Expressions.Expression.Parameter(typeof(int));

            var dataValueContainerLeft = System.Linq.Expressions.Expression.Constant(new DataValueContainer());
            var dataValueContainerRight = System.Linq.Expressions.Expression.Constant(new DataValueContainer());

            var leftParameterInfo = new ColumnParameterInfo(new List<ParameterExpression>() { leftBatch }, new List<ParameterExpression>() { leftIndex }, new List<int>() { 0 }, dataValueContainerLeft);
            var rightParameterInfo = new ColumnParameterInfo(new List<ParameterExpression>() { rightBatch }, new List<ParameterExpression>() { rightIndex }, new List<int>() { 0 }, dataValueContainerRight);

            List<System.Linq.Expressions.Expression> comparisons = new List<System.Linq.Expressions.Expression>();
            for (int i = 0; i < sortFields.Count; i++)
            {
                var sortField = sortFields[i];
                var leftExpression = visitor.Visit(sortField.Expression, leftParameterInfo);
                var rightExpression = visitor.Visit(sortField.Expression, rightParameterInfo);

                Debug.Assert(leftExpression != null);
                Debug.Assert(rightExpression != null);

                MethodCallExpression? compareExpression = null;
                if (sortField.SortDirection == SortDirection.SortDirectionAscNullsFirst)
                {
                    compareExpression = CompareAscendingNullsFirst(leftExpression, rightExpression);
                }
                else if (sortField.SortDirection == SortDirection.SortDirectionAscNullsLast)
                {
                    compareExpression = CompareAscendingNullsLast(leftExpression, rightExpression);
                }
                else if (sortField.SortDirection == SortDirection.SortDirectionDescNullsFirst)
                {
                    compareExpression = CompareDescendingNullsFirst(leftExpression, rightExpression);
                }
                else if (sortField.SortDirection == SortDirection.SortDirectionDescNullsLast)
                {
                    compareExpression = CompareDescendingNullsLast(leftExpression, rightExpression);
                }
                else if (sortField.SortDirection == SortDirection.SortDirectionUnspecified)
                {
                    // Default is ascending with nulls first
                    compareExpression = CompareAscendingNullsFirst(leftExpression, rightExpression);
                }
                else
                {
                    throw new NotSupportedException($"The sort order {sortField.SortDirection} is not supported");
                }
                comparisons.Add(compareExpression);
            }

            if (comparisons.Count == 1)
            {
                var lambda = System.Linq.Expressions.Expression.Lambda<Func<EventBatchData, int, EventBatchData, int, int>>(comparisons[0], leftBatch, leftIndex, rightBatch, rightIndex);
                return lambda.Compile();
            }
            else if (comparisons.Count > 1)
            {
                var tmpVar = System.Linq.Expressions.Expression.Variable(typeof(int));
                var compare = comparisons[comparisons.Count - 1];
                for (int i = comparisons.Count - 2; i >= 0; i--)
                {
                    var res = comparisons[i];
                    var assignOp = System.Linq.Expressions.Expression.Assign(tmpVar, res);

                    var conditionTest = System.Linq.Expressions.Expression.Equal(tmpVar, System.Linq.Expressions.Expression.Constant(0));
                    var condition = System.Linq.Expressions.Expression.Condition(conditionTest, compare, tmpVar);
                    var block = System.Linq.Expressions.Expression.Block(new ParameterExpression[] { tmpVar }, assignOp, condition);
                    compare = block;
                }
                var lambda = System.Linq.Expressions.Expression.Lambda<Func<EventBatchData, int, EventBatchData, int, int>>(compare, leftBatch, leftIndex, rightBatch, rightIndex);
                return lambda.Compile();
            }
            else
            {
                throw new InvalidOperationException("No sort fields specified");
            }
        }

        public static Func<EventBatchData, int, EventBatchData, int, int> CreateComparerFunctionAgainstComputedColumns(List<SortField> sortFields, IFunctionsRegister functionsRegister)
        {
            var visitor = new ColumnarExpressionVisitor(functionsRegister);
            ParameterExpression leftBatch = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData));
            ParameterExpression rightBatch = System.Linq.Expressions.Expression.Parameter(typeof(EventBatchData));

            ParameterExpression leftIndex = System.Linq.Expressions.Expression.Parameter(typeof(int));
            ParameterExpression rightIndex = System.Linq.Expressions.Expression.Parameter(typeof(int));

            var dataValueContainerLeft = System.Linq.Expressions.Expression.Constant(new DataValueContainer());
            var dataValueContainerRight = System.Linq.Expressions.Expression.Constant(new DataValueContainer());

            var leftParameterInfo = new ColumnParameterInfo(new List<ParameterExpression>() { leftBatch }, new List<ParameterExpression>() { leftIndex }, new List<int>() { 0 }, dataValueContainerLeft);
            var rightParameterInfo = new ColumnParameterInfo(new List<ParameterExpression>() { rightBatch }, new List<ParameterExpression>() { rightIndex }, new List<int>() { 0 }, dataValueContainerRight);

            List<System.Linq.Expressions.Expression> comparisons = new List<System.Linq.Expressions.Expression>();
            for (int i = 0; i < sortFields.Count; i++)
            {
                var sortField = sortFields[i];
                var leftExpression = visitor.Visit(sortField.Expression, leftParameterInfo);
                var rightExpression = visitor.Visit(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = i
                    }
                }, rightParameterInfo);

                Debug.Assert(leftExpression != null);
                Debug.Assert(rightExpression != null);

                MethodCallExpression? compareExpression = null;
                if (sortField.SortDirection == SortDirection.SortDirectionAscNullsFirst)
                {
                    compareExpression = CompareAscendingNullsFirst(leftExpression, rightExpression);
                }
                else if (sortField.SortDirection == SortDirection.SortDirectionAscNullsLast)
                {
                    compareExpression = CompareAscendingNullsLast(leftExpression, rightExpression);
                }
                else if (sortField.SortDirection == SortDirection.SortDirectionDescNullsFirst)
                {
                    compareExpression = CompareDescendingNullsFirst(leftExpression, rightExpression);
                }
                else if (sortField.SortDirection == SortDirection.SortDirectionDescNullsLast)
                {
                    compareExpression = CompareDescendingNullsLast(leftExpression, rightExpression);
                }
                else if (sortField.SortDirection == SortDirection.SortDirectionUnspecified)
                {
                    // Default is ascending with nulls first
                    compareExpression = CompareAscendingNullsFirst(leftExpression, rightExpression);
                }
                else
                {
                    throw new NotSupportedException($"The sort order {sortField.SortDirection} is not supported");
                }
                comparisons.Add(compareExpression);
            }

            if (comparisons.Count == 1)
            {
                var lambda = System.Linq.Expressions.Expression.Lambda<Func<EventBatchData, int, EventBatchData, int, int>>(comparisons[0], leftBatch, leftIndex, rightBatch, rightIndex);
                return lambda.Compile();
            }
            else if (comparisons.Count > 1)
            {
                var tmpVar = System.Linq.Expressions.Expression.Variable(typeof(int));
                var compare = comparisons[comparisons.Count - 1];
                for (int i = comparisons.Count - 2; i >= 0; i--)
                {
                    var res = comparisons[i];
                    var assignOp = System.Linq.Expressions.Expression.Assign(tmpVar, res);

                    var conditionTest = System.Linq.Expressions.Expression.Equal(tmpVar, System.Linq.Expressions.Expression.Constant(0));
                    var condition = System.Linq.Expressions.Expression.Condition(conditionTest, compare, tmpVar);
                    var block = System.Linq.Expressions.Expression.Block(new ParameterExpression[] { tmpVar }, assignOp, condition);
                    compare = block;
                }
                var lambda = System.Linq.Expressions.Expression.Lambda<Func<EventBatchData, int, EventBatchData, int, int>>(compare, leftBatch, leftIndex, rightBatch, rightIndex);
                return lambda.Compile();
            }
            else
            {
                throw new InvalidOperationException("No sort fields specified");
            }
        }
    }
}

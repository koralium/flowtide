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
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class SortFieldCompareCreator
    {

        // These methods are collected through reflection 
        internal static int CompareAscendingNullsFirstImplementation(FlxValue a, FlxValue b)
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
            return FlxValueComparer.CompareTo(a, b);
        }

        internal static int CompareAscendingNullsLastImplementation(FlxValue a, FlxValue b)
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
            return FlxValueComparer.CompareTo(a, b);
        }

        internal static int CompareDescendingNullsFirstImplementation(FlxValue a, FlxValue b)
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
            return FlxValueComparer.CompareTo(b, a);
        }

        internal static int CompareDescendingNullsLastImplementation(FlxValue a, FlxValue b)
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
            return FlxValueComparer.CompareTo(b, a);
        }

        private static System.Linq.Expressions.MethodCallExpression CompareAscendingNullsFirst(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo? compareMethod = typeof(SortFieldCompareCreator).GetMethod("CompareAscendingNullsFirstImplementation", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(compareMethod != null);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        private static System.Linq.Expressions.MethodCallExpression CompareAscendingNullsLast(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo? compareMethod = typeof(SortFieldCompareCreator).GetMethod("CompareAscendingNullsLastImplementation", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(compareMethod != null);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        private static System.Linq.Expressions.MethodCallExpression CompareDescendingNullsFirst(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo? compareMethod = typeof(SortFieldCompareCreator).GetMethod("CompareDescendingNullsFirstImplementation", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(compareMethod != null);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        private static System.Linq.Expressions.MethodCallExpression CompareDescendingNullsLast(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo? compareMethod = typeof(SortFieldCompareCreator).GetMethod("CompareDescendingNullsLastImplementation", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            Debug.Assert(compareMethod != null);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        public static Func<T, T, int> CreateComparer<T>(List<SortField> sortFields, FunctionsRegister functionsRegister)
        {
            var visitor = new FlowtideExpressionVisitor(functionsRegister, typeof(T));
            ParameterExpression left = System.Linq.Expressions.Expression.Parameter(typeof(T));
            ParameterExpression right = System.Linq.Expressions.Expression.Parameter(typeof(T));

            var leftParameterInfo = new ParametersInfo(new List<ParameterExpression> { left }, new List<int>());
            var rightParameterInfo = new ParametersInfo(new List<ParameterExpression> { right }, new List<int>());

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
                var lambda = System.Linq.Expressions.Expression.Lambda<Func<T, T, int>>(comparisons[0], left, right);
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
                var lambda = System.Linq.Expressions.Expression.Lambda<Func<T, T, int>>(compare, left, right);
                return lambda.Compile();
            }
            else
            {
                throw new InvalidOperationException("No sort fields specified");
            }
        }
    }
}

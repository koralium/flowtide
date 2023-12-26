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

namespace FlowtideDotNet.Core.Compute.Group
{
    internal static class GroupIndexCreator
    {
        private static System.Linq.Expressions.MethodCallExpression Compare(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo compareMethod = typeof(FlxValueComparer).GetMethod("CompareTo", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }

        public static Func<T, T, int> CreateComparer<T>(IReadOnlyList<int> primaryKeys)
        {
            ParameterExpression left = Expression.Parameter(typeof(T));
            ParameterExpression right = Expression.Parameter(typeof(T));
            var method = typeof(T).GetMethod("GetColumn");

            List<Expression> comparisons = new List<Expression>();
            for (int i = 0; i < primaryKeys.Count; i++)
            {
                var keyLocation = primaryKeys[i];
                // Access the key property in both left and right
                var propertyAccessorLeft = System.Linq.Expressions.Expression.Call(left, method, System.Linq.Expressions.Expression.Constant(keyLocation));
                var propertyAccessorRight = System.Linq.Expressions.Expression.Call(right, method, System.Linq.Expressions.Expression.Constant(keyLocation));
                var comparison = Compare(propertyAccessorLeft, propertyAccessorRight);
                comparisons.Add(comparison);
            }
            
            if (comparisons.Count == 1)
            {
                var lambda = Expression.Lambda<Func<T, T, int>>(comparisons.FirstOrDefault(), left, right);
                return lambda.Compile();
            }
            else if (comparisons.Count > 1)
            {
                var tmpVar = System.Linq.Expressions.Expression.Variable(typeof(int));
                var compare = comparisons.Last();
                for (int i = comparisons.Count - 2; i >= 0; i--)
                {
                    var res = comparisons[i];
                    var assignOp = System.Linq.Expressions.Expression.Assign(tmpVar, res);

                    var conditionTest = System.Linq.Expressions.Expression.Equal(tmpVar, System.Linq.Expressions.Expression.Constant(0));
                    var condition = System.Linq.Expressions.Expression.Condition(conditionTest, compare, tmpVar);
                    var block = System.Linq.Expressions.Expression.Block(new ParameterExpression[] { tmpVar }, assignOp, condition);
                    compare = block;
                }
                var lambda = Expression.Lambda<Func<T, T, int>>(compare, left, right);
                return lambda.Compile();
            }

            throw new NotSupportedException("A grouping must group by atleast one column");
        }
    }
}

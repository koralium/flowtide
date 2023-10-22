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
using FlowtideDotNet.Substrait.Expressions.ScalarFunctions;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Index
{
    /// <summary>
    /// Responsible to create the comparer used when joining one value with another input
    /// </summary>
    internal static class IndexJoinCompiler
    {

        public static System.Linq.Expressions.Expression<Func<JoinStreamEvent, JoinStreamEvent, int>> CreateIndexJoinComparer(BooleanComparison booleanComparison, int leftSize)
        {
            var p1 = Expression.Parameter(typeof(JoinStreamEvent));
            var p2 = Expression.Parameter(typeof(JoinStreamEvent));

            var visitor = new IndexCompilerVisitor();
            var leftExpr = visitor.Visit(booleanComparison.Left, new IndexCompilerVisitor.State() { Parameter = p1, RelativeIndex = 0 });
            var rightExpr = visitor.Visit(booleanComparison.Right, new IndexCompilerVisitor.State() { Parameter = p2, RelativeIndex = leftSize });

            var compareExpr = Compare(leftExpr, rightExpr);
            if (booleanComparison.Type == FlowtideDotNet.Substrait.Expressions.ScalarFunctions.BooleanComparisonType.Equals)
            {
                //var final = Expression.Condition(Expression.LessThanOrEqual(compareExpr, Expression.Constant(0)), Expression.Constant(-1), Expression.Constant(1));
                var lambda = Expression.Lambda<Func<JoinStreamEvent, JoinStreamEvent, int>>(compareExpr, p1, p2);
                return lambda;
            }
            return null;
        }

        public static System.Linq.Expressions.Expression<Func<JoinStreamEvent, JoinStreamEvent, int>> CreateIndexJoinComparerRight(BooleanComparison booleanComparison, int leftSize)
        {
            var p1 = Expression.Parameter(typeof(JoinStreamEvent));
            var p2 = Expression.Parameter(typeof(JoinStreamEvent));

            var visitor = new IndexCompilerVisitor();
            var leftExpr = visitor.Visit(booleanComparison.Left, new IndexCompilerVisitor.State() { Parameter = p2, RelativeIndex = leftSize });
            var rightExpr = visitor.Visit(booleanComparison.Right, new IndexCompilerVisitor.State() { Parameter = p1, RelativeIndex = 0 });

            var compareExpr = Compare(rightExpr, leftExpr);

            // TODO: For future comparisons, flip the comparison tpe
            if (booleanComparison.Type == FlowtideDotNet.Substrait.Expressions.ScalarFunctions.BooleanComparisonType.Equals)
            {
                //var final = Expression.Condition(Expression.LessThanOrEqual(compareExpr, Expression.Constant(0)), Expression.Constant(-1), Expression.Constant(1));
                var lambda = Expression.Lambda<Func<JoinStreamEvent, JoinStreamEvent, int>>(compareExpr, p1, p2);
                return lambda;
            }
            return null;
        }

        public static System.Linq.Expressions.Expression<Func<JoinStreamEvent, JoinStreamEvent, int>> CreateIndexJoinComparer(AndFunction andFunction, int leftSize)
        {
            var p1 = Expression.Parameter(typeof(JoinStreamEvent));
            var p2 = Expression.Parameter(typeof(JoinStreamEvent));
            var tmpVar = System.Linq.Expressions.Expression.Variable(typeof(int));

            var visitor = new IndexCompilerVisitor();

            List<Expression> expressions = new List<Expression>();
            foreach (var arg in andFunction.Arguments)
            {
                if (arg is BooleanComparison booleanComparison)
                {
                    var leftInLeft = new IndexEqualsFunctionEvaluator(0, leftSize).Visit(booleanComparison.Left, null);
                    var leftInRight = new IndexEqualsFunctionEvaluator(0, leftSize).Visit(booleanComparison.Right, null);
                    var rightInLeft = new IndexEqualsFunctionEvaluator(leftSize, int.MaxValue).Visit(booleanComparison.Left, null);
                    var rightInRight = new IndexEqualsFunctionEvaluator(leftSize, int.MaxValue).Visit(booleanComparison.Right, null);

                    if (leftInLeft && rightInRight)
                    {
                        var leftExpr = visitor.Visit(booleanComparison.Left, new IndexCompilerVisitor.State() { Parameter = p1, RelativeIndex = 0 });
                        var rightExpr = visitor.Visit(booleanComparison.Right, new IndexCompilerVisitor.State() { Parameter = p2, RelativeIndex = leftSize });
                        var compareExpr = Compare(leftExpr, rightExpr);
                        expressions.Add(compareExpr);
                    }
                    else if (rightInLeft && leftInRight)
                    {
                        var leftExpr = visitor.Visit(booleanComparison.Left, new IndexCompilerVisitor.State() { Parameter = p2, RelativeIndex = leftSize });
                        var rightExpr = visitor.Visit(booleanComparison.Right, new IndexCompilerVisitor.State() { Parameter = p1, RelativeIndex = 0 });
                        var compareExpr = Compare(leftExpr, rightExpr);
                        expressions.Add(compareExpr);
                    }
                    else if (rightInLeft && !leftInRight)
                    {
                        var leftExpr = visitor.Visit(booleanComparison.Left, new IndexCompilerVisitor.State() { Parameter = p2, RelativeIndex = leftSize });
                        var rightExpr = visitor.Visit(booleanComparison.Right, new IndexCompilerVisitor.State() { Parameter = p1, RelativeIndex = 0 });
                        var compareExpr = Compare(leftExpr, rightExpr);
                        expressions.Add(compareExpr);
                    }
                    
                }
            }

            var compare = expressions.Last();
            for (int i = expressions.Count - 2; i >= 0; i--)
            {
                var res = expressions[i];
                var assignOp = System.Linq.Expressions.Expression.Assign(tmpVar, res);

                var conditionTest = System.Linq.Expressions.Expression.Equal(tmpVar, System.Linq.Expressions.Expression.Constant(0));
                var condition = System.Linq.Expressions.Expression.Condition(conditionTest, compare, tmpVar);
                var block = System.Linq.Expressions.Expression.Block(new ParameterExpression[] { tmpVar }, assignOp, condition);
                compare = block;
            }

            var lambda = Expression.Lambda<Func<JoinStreamEvent, JoinStreamEvent, int>>(compare, p1, p2);
            return lambda;
        }

        private static System.Linq.Expressions.MethodCallExpression Compare(System.Linq.Expressions.Expression a, System.Linq.Expressions.Expression b)
        {
            MethodInfo compareMethod = typeof(FlxValueComparer).GetMethod("CompareTo", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);
            return System.Linq.Expressions.Expression.Call(compareMethod, a, b);
        }
    }
}

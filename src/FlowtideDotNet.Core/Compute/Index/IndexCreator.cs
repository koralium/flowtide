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
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Expressions.ScalarFunctions;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Index
{
    internal static class IndexCreator
    {
        /// <summary>
        /// Checks that the expression is valid to do an index on.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="leftColumnSize"></param>
        /// <returns></returns>
        public static bool IsValidForIndex(
            FlowtideDotNet.Substrait.Expressions.Expression expression, 
            int leftColumnSize, 
            out Func<JoinStreamEvent, JoinStreamEvent, int> leftComp, 
            out Func<JoinStreamEvent, JoinStreamEvent, int> rightComp,
            out Func<JoinStreamEvent, JoinStreamEvent, int> rightJoinLeft)
        {
            // Check if it is a cross join
            if (IsCrossJoin(expression))
            {
                leftComp = (l, r) => 0;
                rightComp = (l, r) => 0;
                rightJoinLeft = (l, r) => 0;
                return true;
            }

            if (expression is BooleanComparison booleanComparison)
            {
                // Check if left side contains left
                var leftInLeft = new IndexEqualsFunctionEvaluator(0, leftColumnSize).Visit(booleanComparison.Left, null);
                var leftInRight = new IndexEqualsFunctionEvaluator(0, leftColumnSize).Visit(booleanComparison.Right, null);
                var rightInLeft = new IndexEqualsFunctionEvaluator(leftColumnSize, int.MaxValue).Visit(booleanComparison.Left, null);
                var rightInRight = new IndexEqualsFunctionEvaluator(leftColumnSize, int.MaxValue).Visit(booleanComparison.Right, null);

                if (leftInLeft && rightInRight)
                {
                    var leftComparer = IndexCompiler.CompileIndexFunction(booleanComparison.Left, 0);
                    var rightComparer = IndexCompiler.CompileIndexFunction(booleanComparison.Right, leftColumnSize);
                    leftComp = leftComparer.Compile();
                    rightComp = rightComparer.Compile();
                    var joiner = IndexJoinCompiler.CreateIndexJoinComparer(booleanComparison, leftColumnSize);
                    rightJoinLeft = joiner.Compile();
                    return true;
                }
                if (leftInRight && rightInLeft)
                {
                    var leftComparer = IndexCompiler.CompileIndexFunction(booleanComparison.Right, 0);
                    var rightComparer = IndexCompiler.CompileIndexFunction(booleanComparison.Left, leftColumnSize);
                    leftComp = leftComparer.Compile();
                    rightComp = rightComparer.Compile();

                    // TODO: Add test case for this
                    var joiner = IndexJoinCompiler.CreateIndexJoinComparerRight(booleanComparison, leftColumnSize);
                    rightJoinLeft = joiner.Compile();
                    return true;
                }
            }
            if (expression is AndFunction andFunction)
            {
                var p1 = Expression.Parameter(typeof(JoinStreamEvent));
                var p2 = Expression.Parameter(typeof(JoinStreamEvent));

                List<Expression> leftIndexExpressions = new List<Expression>();
                List<Expression> rightIndexExpressions = new List<Expression>();
                var tmpVar = System.Linq.Expressions.Expression.Variable(typeof(int));
                // Must go through all comparisons
                foreach (var comp in andFunction.Arguments)
                {
                    if (comp is BooleanComparison comparison)
                    {
                        var leftInLeft = new IndexEqualsFunctionEvaluator(0, leftColumnSize).Visit(comparison.Left, null);
                        var leftInRight = new IndexEqualsFunctionEvaluator(0, leftColumnSize).Visit(comparison.Right, null);
                        var rightInLeft = new IndexEqualsFunctionEvaluator(leftColumnSize, int.MaxValue).Visit(comparison.Left, null);
                        var rightInRight = new IndexEqualsFunctionEvaluator(leftColumnSize, int.MaxValue).Visit(comparison.Right, null);

                        if(leftInLeft && rightInRight)
                        {
                            var leftCompiledIndex = IndexCompiler.Compile(comparison.Left, 0, p1, p2);
                            var rightCompiledIndex = IndexCompiler.Compile(comparison.Right, leftColumnSize, p1, p2);
                            leftIndexExpressions.Add(leftCompiledIndex);
                            rightIndexExpressions.Add(rightCompiledIndex);
                        }
                    }
                }

                var leftCompare = leftIndexExpressions.Last();
                for (int i = leftIndexExpressions.Count - 2; i >= 0; i--)
                {
                    var res = leftIndexExpressions[i];
                    var assignOp = System.Linq.Expressions.Expression.Assign(tmpVar, res);

                    var conditionTest = System.Linq.Expressions.Expression.Equal(tmpVar, System.Linq.Expressions.Expression.Constant(0));
                    var condition = System.Linq.Expressions.Expression.Condition(conditionTest, leftCompare, tmpVar);
                    var block = System.Linq.Expressions.Expression.Block(new ParameterExpression[] { tmpVar }, assignOp, condition);
                    leftCompare = block;
                }

                var rightCompare = rightIndexExpressions.Last();
                for (int i = rightIndexExpressions.Count - 2; i >= 0; i--)
                {
                    var res = rightIndexExpressions[i];
                    var assignOp = System.Linq.Expressions.Expression.Assign(tmpVar, res);

                    var conditionTest = System.Linq.Expressions.Expression.Equal(tmpVar, System.Linq.Expressions.Expression.Constant(0));
                    var condition = System.Linq.Expressions.Expression.Condition(conditionTest, rightCompare, tmpVar);
                    var block = System.Linq.Expressions.Expression.Block(new ParameterExpression[] { tmpVar }, assignOp, condition);
                    rightCompare = block;
                }

                var leftLambda = Expression.Lambda<Func<JoinStreamEvent, JoinStreamEvent, int>>(leftCompare, p1, p2);
                leftComp = leftLambda.Compile();
                var rightLambda = Expression.Lambda<Func<JoinStreamEvent, JoinStreamEvent, int>>(rightCompare, p1, p2);
                rightComp = rightLambda.Compile();

                rightJoinLeft = IndexJoinCompiler.CreateIndexJoinComparer(andFunction, leftColumnSize).Compile();
                return true;
            }
            leftComp = null;
            rightComp = null;
            rightJoinLeft = null;
            return false;
        }

        /// <summary>
        /// Simple function that checks if the join condition is a bool literal with value true.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public static bool IsCrossJoin(FlowtideDotNet.Substrait.Expressions.Expression expression)
        {
            if (expression is BoolLiteral boolLiteral && boolLiteral.Value)
            {
                return true;
            }
            return false;
        }
    }
}

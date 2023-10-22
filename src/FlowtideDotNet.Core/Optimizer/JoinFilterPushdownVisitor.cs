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

using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Expressions.ScalarFunctions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Core.Optimizer.EmitPushdown;

namespace FlowtideDotNet.Core.Optimizer
{
    /// <summary>
    /// Checks if an expression can be pushed down from the join condition.
    /// </summary>
    internal class JoinFilterPushdownVisitor : OptimizerBaseVisitor
    {
        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            // Check root expression
            var visitor = new JoinExpressionVisitor(joinRelation.Left.OutputLength);
            visitor.Visit(joinRelation.Expression, state);
            if (!visitor.unknownCase)
            {
                // Only fields from left is used
                if (visitor.fieldInLeft && !visitor.fieldInRight)
                {
                    joinRelation.Left = new FilterRelation()
                    {
                        Condition = joinRelation.Expression,
                        Input = joinRelation.Left
                    };
                    joinRelation.Expression = new BoolLiteral() { Value = true };
                }
                // Only field in right is used
                else if (!visitor.fieldInLeft && visitor.fieldInRight)
                {
                    joinRelation.Right = new FilterRelation()
                    {
                        Condition = joinRelation.Expression,
                        Input = joinRelation.Right
                    };
                    joinRelation.Expression = new BoolLiteral() { Value = true };
                }
            }

            if (joinRelation.Expression is AndFunction andFunction)
            {
                List<Expression> leftPushDown = new List<Expression>();
                List<Expression> rightPushDown = new List<Expression>();
                for (int i = 0; i < andFunction.Arguments.Count; i++)
                {
                    var expr = andFunction.Arguments[i];
                    var andVisitor = new JoinExpressionVisitor(joinRelation.Left.OutputLength);
                    andVisitor.Visit(expr, state);
                    if (andVisitor.fieldInLeft && !andVisitor.fieldInRight)
                    {
                        leftPushDown.Add(expr);
                        andFunction.Arguments.RemoveAt(i);
                        i--;
                    }
                    // Only field in right is used
                    else if (!andVisitor.fieldInLeft && andVisitor.fieldInRight)
                    {
                        rightPushDown.Add(expr);
                        andFunction.Arguments.RemoveAt(i);
                        i--;
                    }
                    if (andFunction.Arguments.Count == 1)
                    {
                        joinRelation.Expression = andFunction.Arguments[0];
                    }
                    else if(andFunction.Arguments.Count == 0)
                    {
                        joinRelation.Expression = new BoolLiteral() { Value = true };
                    }
                }
                if (leftPushDown.Count > 0)
                {
                    
                    if (leftPushDown.Count == 1)
                    {
                        joinRelation.Left = new FilterRelation()
                        {
                            Condition = leftPushDown[0],
                            Input = joinRelation.Left
                        };
                    }
                    else
                    {
                        joinRelation.Left = new FilterRelation()
                        {
                            Condition = new AndFunction() { Arguments = leftPushDown },
                            Input = joinRelation.Left
                        };
                    }
                }
                if (rightPushDown.Count > 0)
                {
                    // Find used fields
                    var usageVisitor = new ExpressionFieldUsageVisitor(joinRelation.Left.OutputLength);
                    foreach(var expr in rightPushDown)
                    {
                        usageVisitor.Visit(expr, default);
                    }
                    var rightUsageFields = usageVisitor.UsedFieldsRight.Distinct().ToList();

                    // Build lookup table from old to new field id
                    Dictionary<int, int> oldToNew = new Dictionary<int, int>();
                    foreach(var usedField in rightUsageFields)
                    {
                        oldToNew.Add(usedField, usedField - joinRelation.Left.OutputLength);
                    }
                    // Replace old ids with the new ids
                    var replaceVisitor = new ExpressionFieldReplaceVisitor(oldToNew);
                    foreach (var expr in rightPushDown)
                    {
                        replaceVisitor.Visit(expr, default);
                    }
                    if (rightPushDown.Count == 1)
                    {
                        joinRelation.Right = new FilterRelation()
                        {
                            Condition = rightPushDown[0],
                            Input = joinRelation.Right
                        };
                    }
                    else
                    {
                        joinRelation.Right = new FilterRelation()
                        {
                            Condition = new AndFunction() { Arguments = rightPushDown },
                            Input = joinRelation.Right
                        };
                    }
                }
            }

            joinRelation.Left = Visit(joinRelation.Left, state);
            joinRelation.Right = Visit(joinRelation.Right, state);

            return joinRelation;
        }
    }
}

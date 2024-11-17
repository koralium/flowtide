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

using FlowtideDotNet.Core.Optimizer.EmitPushdown;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.JoinProjectionPushdown
{
    /// <summary>
    /// Finds expressions that contain projections can be pushed down, this helps performance of the join queries.
    /// </summary>
    internal class JoinProjectionPushDownVisitor : OptimizerBaseVisitor
    {
        /// <summary>
        /// Goes through an expression and tries to find complex expressions that only use left or right side of the data.
        /// In that case it can be pushed down to a projection infront of a join
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="leftSize"></param>
        /// <param name="rightSize"></param>
        /// <param name="leftSideExpressions"></param>
        /// <param name="rightSideExpressions"></param>
        /// <returns></returns>
        private Expression Check(
            Expression expression,
            int leftSize,
            int rightSize,
            List<Expression> leftSideExpressions,
            List<Expression> rightSideExpressions,
            ref int newIdCounter,
            List<int> leftSideIds,
            List<int> rightSideIds)
        {
            if (expression is DirectFieldReference)
            {
                return expression;
            }
            var visitor = new JoinExpressionVisitor(leftSize);
            visitor.Visit(expression, default!);

            if (visitor.unknownCase || visitor.fieldInLeft && visitor.fieldInRight)
            {
                if (expression is ScalarFunction scalar)
                {
                    for (int i = 0; i < scalar.Arguments.Count; i++)
                    {
                        var arg = scalar.Arguments[i];
                        scalar.Arguments[i] = Check(arg, leftSize, rightSize, leftSideExpressions, rightSideExpressions, ref newIdCounter, leftSideIds, rightSideIds);
                    }
                    return scalar;
                }
                return expression;
            }
            if (visitor.fieldInLeft)
            {
                leftSideExpressions.Add(expression);
                var fieldId = newIdCounter;
                newIdCounter++;
                leftSideIds.Add(fieldId);
                return new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = fieldId
                    }
                };
            }
            else if (visitor.fieldInRight)
            {
                rightSideExpressions.Add(expression);
                var fieldId = newIdCounter;
                newIdCounter++;
                rightSideIds.Add(fieldId);
                return new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = fieldId
                    }
                };
            }
            return expression;
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            if (joinRelation.Expression == null)
            {
                throw new Exceptions.FlowtideException("Join relation must have an expression");
            }
            joinRelation.Left = Visit(joinRelation.Left, state);
            joinRelation.Right = Visit(joinRelation.Right, state);

            int counter = joinRelation.Left.OutputLength + joinRelation.Right.OutputLength;
            int leftSizeBefore = joinRelation.Left.OutputLength;
            int rightSizeBefore = joinRelation.Right.OutputLength;
            List<Expression> leftExpressions = new List<Expression>();
            List<Expression> rightExpressions = new List<Expression>();
            List<int> leftSideEmits = new List<int>();
            List<int> rightSideEmits = new List<int>();
            joinRelation.Expression = Check(
                joinRelation.Expression,
                joinRelation.Left.OutputLength,
                joinRelation.Right.OutputLength,
                leftExpressions,
                rightExpressions,
                ref counter,
                leftSideEmits,
                rightSideEmits);

            if (leftExpressions.Count == 0 && rightExpressions.Count == 0)
            {
                return joinRelation;
            }

            // Create mapping from old emit to new emit
            Dictionary<int, int> oldEmitToNew = new Dictionary<int, int>();
            for (int i = 0; i < joinRelation.Left.OutputLength; i++)
            {
                oldEmitToNew.Add(i, i);
            }
            for (int i = 0; i < leftSideEmits.Count; i++)
            {
                oldEmitToNew.Add(leftSideEmits[i], oldEmitToNew.Count);
            }
            for (int i = 0; i < joinRelation.Right.OutputLength; i++)
            {
                oldEmitToNew.Add(i + joinRelation.Left.OutputLength, oldEmitToNew.Count);
            }
            for (int i = 0; i < rightSideEmits.Count; i++)
            {
                oldEmitToNew.Add(rightSideEmits[i], oldEmitToNew.Count);
            }
            var replaceVisitor = new ExpressionFieldReplaceVisitor(oldEmitToNew);
            replaceVisitor.Visit(joinRelation.Expression, default);

            if (joinRelation.EmitSet)
            {
                for (int i = 0; i < joinRelation.Emit.Count; i++)
                {
                    if (oldEmitToNew.TryGetValue(joinRelation.Emit[i], out var newEmit))
                    {
                        joinRelation.Emit[i] = newEmit;
                    }
                    else
                    {
                        throw new NotImplementedException("Emit optimizer does not support this case yet");
                    }
                }
            }
            else
            {
                // We must create an emit 
                List<int> newEmit = new List<int>();
                for (int i = 0; i < leftSizeBefore + rightSizeBefore; i++)
                {
                    if (oldEmitToNew.TryGetValue(i, out var newId))
                    {
                        newEmit.Add(newId);
                    }
                    else
                    {
                        throw new NotImplementedException("Emit optimizer does not support this case yet");
                    }
                }
                joinRelation.Emit = newEmit;
            }

            if (leftExpressions.Count > 0)
            {
                // FIeld usage on left side does not need to be updated
                joinRelation.Left = new ProjectRelation()
                {
                    Expressions = leftExpressions,
                    Input = joinRelation.Left
                };
            }
            if (rightExpressions.Count > 0)
            {
                // Update field id on right side to remove length of left side
                Dictionary<int, int> rightSideOldToNew = new Dictionary<int, int>();
                for (int i = 0; i < rightSizeBefore; i++)
                {
                    rightSideOldToNew.Add(leftSizeBefore + i, i);
                }
                replaceVisitor = new ExpressionFieldReplaceVisitor(rightSideOldToNew);
                for (int i = 0; i < rightExpressions.Count; i++)
                {
                    replaceVisitor.Visit(rightExpressions[i], default);
                }
                joinRelation.Right = new ProjectRelation()
                {
                    Expressions = rightExpressions,
                    Input = joinRelation.Right
                };
            }


            return joinRelation;
        }
    }
}

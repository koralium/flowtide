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

using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Core.ColumnStore;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal static class PostJoinExtractor
    {
        public static (Func<EventBatchData, int, bool>? leftFunc, Func<EventBatchData, int, bool>? rightFunc) CheckForLeftOrRightPostJoinConditions(MergeJoinRelation mergeJoinRelation, IFunctionsRegister functionsRegister)
        {
            List<Expression> leftExpressions = new List<Expression>();
            List<Expression> rightExpressions = new List<Expression>();
            ExtractLeftAndRightPostJoins(mergeJoinRelation.PostJoinFilter!, mergeJoinRelation.Left.OutputLength, mergeJoinRelation.Right.OutputLength, leftExpressions,rightExpressions);

            Func<EventBatchData, int, bool>? leftFunc = default;
            if (leftExpressions.Count > 0)
            {
                Expression? leftExpression;
                if (leftExpressions.Count > 1)
                {
                    leftExpression = new ScalarFunction()
                    {
                        Arguments = leftExpressions,
                        ExtensionUri = FunctionsBoolean.Uri,
                        ExtensionName = FunctionsBoolean.And
                    };
                }
                else
                {
                    leftExpression = leftExpressions[0];
                }
                leftFunc = ColumnBooleanCompiler.Compile(leftExpression, functionsRegister, 0);
            }
            Func<EventBatchData, int, bool>? rightFunc = default;
            if (rightExpressions.Count > 0)
            {
                Expression? rightExpression;
                if (rightExpressions.Count > 1)
                {
                    rightExpression = new ScalarFunction()
                    {
                        Arguments = rightExpressions,
                        ExtensionUri = FunctionsBoolean.Uri,
                        ExtensionName = FunctionsBoolean.And
                    };
                }
                else
                {
                    rightExpression = rightExpressions[0];
                }
                rightFunc = ColumnBooleanCompiler.Compile(rightExpression, functionsRegister, mergeJoinRelation.Left.OutputLength);
            }
            return (leftFunc, rightFunc);
        }

        public static void ExtractLeftAndRightPostJoins(
            Expression expression,
            int leftSize,
            int rightSize,
            List<Expression> leftSideExpressions,
            List<Expression> rightSideExpressions)
        {
            if (expression is DirectFieldReference directfieldref)
            {
                if (directfieldref.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                {
                    var field = structReferenceSegment.Field;
                    if (field < leftSize)
                    {
                        leftSideExpressions.Add(expression);
                    }
                    else
                    {
                        rightSideExpressions.Add(expression);
                    }
                }
                return;
            }
            var visitor = new JoinExpressionVisitor(leftSize);
            visitor.Visit(expression, default!);

            if (visitor.unknownCase || visitor.fieldInLeft && visitor.fieldInRight)
            {
                if (expression is ScalarFunction scalar && scalar.ExtensionUri == FunctionsBoolean.Uri && scalar.ExtensionName == FunctionsBoolean.And)
                {
                    for (int i = 0; i < scalar.Arguments.Count; i++)
                    {
                        var arg = scalar.Arguments[i];
                        ExtractLeftAndRightPostJoins(arg, leftSize, rightSize, leftSideExpressions, rightSideExpressions);
                    }
                }
                return;
            }
            if (visitor.fieldInLeft)
            {
                leftSideExpressions.Add(expression);
            }
            else if (visitor.fieldInRight)
            {
                rightSideExpressions.Add(expression);
            }
        }
    }
}

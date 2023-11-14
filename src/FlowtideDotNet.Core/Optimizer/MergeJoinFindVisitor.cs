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
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer
{
    internal class MergeJoinFindVisitor : OptimizerBaseVisitor
    {
        private bool Check(JoinRelation joinRelation, Expression expression, out DirectFieldReference? leftKey, out DirectFieldReference? rightKey)
        {
            if (expression is ScalarFunction booleanComparison &&
                booleanComparison.ExtensionUri == FunctionsComparison.Uri &&
                booleanComparison.ExtensionName == FunctionsComparison.Equal)
            {
                var left = booleanComparison.Arguments[0];
                var right = booleanComparison.Arguments[1];
                if (left is DirectFieldReference leftDirectFieldReference &&
                    leftDirectFieldReference.ReferenceSegment is StructReferenceSegment leftStruct &&
                    right is DirectFieldReference rightDirectFieldReference &&
                    rightDirectFieldReference.ReferenceSegment is StructReferenceSegment rightStruct)
                {
                    // Check if left is in left and right in right
                    if (leftStruct.Field < joinRelation.Left.OutputLength &&
                        rightStruct.Field >= joinRelation.Left.OutputLength)
                    {
                        leftKey = leftDirectFieldReference;
                        rightKey = rightDirectFieldReference;
                        return true;
                    }
                    // Check if left is in right and right is in left
                    else if (rightStruct.Field < joinRelation.Left.OutputLength &&
                        leftStruct.Field >= joinRelation.Left.OutputLength)
                    {
                        leftKey = rightDirectFieldReference;
                        rightKey = leftDirectFieldReference;
                        return true;
                    }
                }
            }
            leftKey = null;
            rightKey = null;
            return false;
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            joinRelation.Left = Visit(joinRelation.Left, state);
            joinRelation.Right = Visit(joinRelation.Right, state);
            if (Check(joinRelation, joinRelation.Expression, out var rootLeft, out var rootRight))
            {
                return new MergeJoinRelation()
                {
                    Emit = joinRelation.Emit,
                    Left = joinRelation.Left,
                    Right = joinRelation.Right,
                    LeftKeys = new List<FieldReference>()
                            {
                                rootLeft
                            },
                    RightKeys = new List<FieldReference>()
                            {
                                rootRight
                            },
                    Type = joinRelation.Type
                };
            }
            
            if (joinRelation.Expression is ScalarFunction andFunction &&
                andFunction.ExtensionUri == FunctionsBoolean.Uri &&
                andFunction.ExtensionName == FunctionsBoolean.And)
            {
                List<FieldReference> leftKeys = new List<FieldReference>();
                List<FieldReference> rightKeys = new List<FieldReference>();
                for (int i = 0; i < andFunction.Arguments.Count; i++)
                {
                    if (Check(joinRelation, andFunction.Arguments[i], out var leftkey, out var rightkey))
                    {
                        leftKeys.Add(leftkey);
                        rightKeys.Add(rightkey);
                        andFunction.Arguments.RemoveAt(i);
                        i--;
                    }
                }
                if (andFunction.Arguments.Count == 0)
                {
                    andFunction = null;
                }
                if (leftKeys.Count > 0)
                {
                    return new MergeJoinRelation()
                    {
                        Emit = joinRelation.Emit,
                        Left = joinRelation.Left,
                        Right = joinRelation.Right,
                        LeftKeys = leftKeys,
                        RightKeys = rightKeys,
                        Type = joinRelation.Type,
                        PostJoinFilter = andFunction
                    };
                }
            }
                return base.VisitJoinRelation(joinRelation, state);
        }
    }
}

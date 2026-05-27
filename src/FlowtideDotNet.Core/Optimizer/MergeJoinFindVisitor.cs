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
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Optimizer
{
    internal class MergeJoinFindVisitor : OptimizerBaseVisitor
    {
        internal static bool Check(
            JoinRelation joinRelation, 
            Expression? expression, 
            [NotNullWhen(true)] out DirectFieldReference? leftKey, 
            [NotNullWhen(true)] out DirectFieldReference? rightKey,
            out JoinComparisonType comparisonType)
        {
            if (expression is ScalarFunction booleanComparison &&
                booleanComparison.ExtensionUri == FunctionsComparison.Uri)
            {
                var op = booleanComparison.ExtensionName;
                bool isSupported = false;
                JoinComparisonType baseType = JoinComparisonType.Equal;
                if (op == FunctionsComparison.Equal)
                {
                    isSupported = true;
                    baseType = JoinComparisonType.Equal;
                }
                else if (op == FunctionsComparison.LessThan)
                {
                    isSupported = true;
                    baseType = JoinComparisonType.LessThan;
                }
                else if (op == FunctionsComparison.LessThanOrEqual)
                {
                    isSupported = true;
                    baseType = JoinComparisonType.LessThanOrEqual;
                }
                else if (op == FunctionsComparison.GreaterThan)
                {
                    isSupported = true;
                    baseType = JoinComparisonType.GreaterThan;
                }
                else if (op == FunctionsComparison.GreaterThanOrEqual)
                {
                    isSupported = true;
                    baseType = JoinComparisonType.GreaterThanOrEqual;
                }

                if (isSupported)
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
                            comparisonType = baseType;
                            return true;
                        }
                        // Check if left is in right and right is in left
                        else if (rightStruct.Field < joinRelation.Left.OutputLength &&
                            leftStruct.Field >= joinRelation.Left.OutputLength)
                        {
                            leftKey = rightDirectFieldReference;
                            rightKey = leftDirectFieldReference;
                            // Since operands are swapped, flip the comparison operator:
                            comparisonType = baseType switch
                            {
                                JoinComparisonType.LessThan => JoinComparisonType.GreaterThan,
                                JoinComparisonType.LessThanOrEqual => JoinComparisonType.GreaterThanOrEqual,
                                JoinComparisonType.GreaterThan => JoinComparisonType.LessThan,
                                JoinComparisonType.GreaterThanOrEqual => JoinComparisonType.LessThanOrEqual,
                                _ => JoinComparisonType.Equal
                            };
                            return true;
                        }
                    }
                }
            }
            leftKey = null;
            rightKey = null;
            comparisonType = JoinComparisonType.Equal;
            return false;
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            joinRelation.Left = Visit(joinRelation.Left, state);
            joinRelation.Right = Visit(joinRelation.Right, state);
            if (Check(joinRelation, joinRelation.Expression, out var rootLeft, out var rootRight, out var compType))
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
                    ComparisonTypes = new List<JoinComparisonType>()
                            {
                                compType
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
                List<JoinComparisonType> comparisonTypes = new List<JoinComparisonType>();
                bool extractedRangeCondition = false;
                for (int i = 0; i < andFunction.Arguments.Count; i++)
                {
                    if (Check(joinRelation, andFunction.Arguments[i], out var leftkey, out var rightkey, out var compType2))
                    {
                        if (compType2 != JoinComparisonType.Equal)
                        {
                            if (extractedRangeCondition)
                            {
                                // A range/inequality condition has already been extracted.
                                // Any subsequent range/inequality conditions must remain in the conjunction
                                // and be processed as post-join filters.
                                continue;
                            }
                            extractedRangeCondition = true;
                        }
                        leftKeys.Add(leftkey);
                        rightKeys.Add(rightkey);
                        comparisonTypes.Add(compType2);
                        andFunction.Arguments.RemoveAt(i);
                        i--;
                    }
                }
                if (andFunction.Arguments.Count == 0)
                {
                    andFunction = null!;
                }
                if (leftKeys.Count > 0)
                {
                    List<FieldReference> sortedLeftKeys = new List<FieldReference>();
                    List<FieldReference> sortedRightKeys = new List<FieldReference>();
                    List<JoinComparisonType> sortedComparisonTypes = new List<JoinComparisonType>();
                    
                    // Add equality keys first
                    for (int j = 0; j < leftKeys.Count; j++)
                    {
                        if (comparisonTypes[j] == JoinComparisonType.Equal)
                        {
                            sortedLeftKeys.Add(leftKeys[j]);
                            sortedRightKeys.Add(rightKeys[j]);
                            sortedComparisonTypes.Add(comparisonTypes[j]);
                        }
                    }
                    
                    // Add inequality keys second
                    for (int j = 0; j < leftKeys.Count; j++)
                    {
                        if (comparisonTypes[j] != JoinComparisonType.Equal)
                        {
                            sortedLeftKeys.Add(leftKeys[j]);
                            sortedRightKeys.Add(rightKeys[j]);
                            sortedComparisonTypes.Add(comparisonTypes[j]);
                        }
                    }
                    
                    leftKeys = sortedLeftKeys;
                    rightKeys = sortedRightKeys;
                    comparisonTypes = sortedComparisonTypes;

                    return new MergeJoinRelation()
                    {
                        Emit = joinRelation.Emit,
                        Left = joinRelation.Left,
                        Right = joinRelation.Right,
                        LeftKeys = leftKeys,
                        RightKeys = rightKeys,
                        ComparisonTypes = comparisonTypes,
                        Type = joinRelation.Type,
                        PostJoinFilter = andFunction
                    };
                }
            }
            return base.VisitJoinRelation(joinRelation, state);
        }
    }
}

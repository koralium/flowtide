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

using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.EmitPushdown
{
    internal class EmitPushdownVisitor : OptimizerBaseVisitor
    {
        private Dictionary<int, List<ReferenceRelation>> referenceRelations;
        
        public EmitPushdownVisitor(Dictionary<int, List<ReferenceRelation>> referenceRelations)
        {
            this.referenceRelations = referenceRelations;
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            if (projectRelation.Input is ReferenceRelation referenceRelation)
            {
                return projectRelation;
            }
            if (projectRelation.Input.OutputLength >= projectRelation.OutputLength)
            {
                var input = projectRelation.Input;

                var usageVisitor = new ExpressionFieldUsageVisitor(projectRelation.Input.OutputLength);
                foreach(var expr in projectRelation.Expressions)
                {
                    usageVisitor.Visit(expr, default);
                }

                if (!usageVisitor.CanOptimize)
                {
                    return projectRelation;
                }

                var usedFields = usageVisitor.UsedFieldsLeft.Distinct().ToList();
                
                if (projectRelation.EmitSet)
                {
                    foreach(var field in projectRelation.Emit!)
                    {
                        if (field < input.OutputLength)
                        {
                            // Add all fields that are in the emit that are from the input
                            usedFields.Add(field);
                        }
                    }
                }

                if (usedFields.Count <= input.OutputLength)
                {
                    // Create a new emit for the input
                    // Create a lookup table with old value to new value
                    // Visit all expressions and emits again and remap them to the new value
                    // Remap the expression emits also to reflect the changes
                    Dictionary<int, int> oldToNew = new Dictionary<int, int>();
                    List<int> emit = new List<int>();
                    int count = 0;
                    foreach(var field in usedFields.OrderBy(x => x))
                    {
                        emit.Add(field);
                        oldToNew.Add(field, count);
                        count++;
                    }
                    var replaceVisitor = new ExpressionFieldReplaceVisitor(oldToNew);
                    foreach (var expr in projectRelation.Expressions)
                    {
                        replaceVisitor.Visit(expr, default);
                    }
                    if (projectRelation.EmitSet)
                    {
                        var diff = input.OutputLength - emit.Count;
                        for (int i = 0; i < projectRelation.Emit.Count; i++)
                        {
                            projectRelation.Emit[i] = projectRelation.Emit[i] - diff;
                        }
                    }

                    input.Emit = emit;

                    
                }
            }

            return base.VisitProjectRelation(projectRelation, state);
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
        {
            var inputLength = mergeJoinRelation.Left.OutputLength + mergeJoinRelation.Right.OutputLength;
            
            if (mergeJoinRelation.Left is ReferenceRelation leftReference)
            {
                var projectRel = new ProjectRelation()
                {
                    Expressions = new List<FlowtideDotNet.Substrait.Expressions.Expression>(),
                    Input = leftReference
                };
                mergeJoinRelation.Left = projectRel;
            }
            if (mergeJoinRelation.Right is ReferenceRelation rightReference)
            {
                var projectRel = new ProjectRelation()
                {
                    Expressions = new List<FlowtideDotNet.Substrait.Expressions.Expression>(),
                    Input = rightReference
                };
                mergeJoinRelation.Right = projectRel;
            }
            
            if (inputLength > mergeJoinRelation.OutputLength)
            {
                var usageVisitor = new ExpressionFieldUsageVisitor(mergeJoinRelation.Left.OutputLength);
                // Visit all possible field references
                foreach(var leftFieldKey in mergeJoinRelation.LeftKeys)
                {
                    usageVisitor.Visit(leftFieldKey, default);
                }
                foreach(var rightFieldKey in mergeJoinRelation.RightKeys)
                {
                    usageVisitor.Visit(rightFieldKey, default);
                }
                if (mergeJoinRelation.PostJoinFilter != null)
                {
                    usageVisitor.Visit(mergeJoinRelation.PostJoinFilter, default);
                }


                var leftUsage = usageVisitor.UsedFieldsLeft.ToList();
                var rightUsage = usageVisitor.UsedFieldsRight.ToList();

                if (mergeJoinRelation.EmitSet)
                {
                    foreach (var field in mergeJoinRelation.Emit!)
                    {
                        if (field < mergeJoinRelation.Left.OutputLength)
                        {
                            leftUsage.Add(field);
                        }
                        else
                        {
                            rightUsage.Add(field);
                        }
                    }
                }

                leftUsage = leftUsage.Distinct().OrderBy(x => x).ToList();
                rightUsage = rightUsage.Distinct().OrderBy(x => x).ToList();

                Dictionary<int, int> oldToNew = new Dictionary<int, int>();
                int replacementCounter = 0;
                List<int> leftEmit = new List<int>();
                List<int> rightEmit = new List<int>();

                foreach (var field in leftUsage)
                {
                    leftEmit.Add(field);
                    oldToNew.Add(field, replacementCounter);
                    replacementCounter += 1;
                }

                foreach(var field in rightUsage)
                {
                    var rightIndex = field- mergeJoinRelation.Left.OutputLength;

                    rightEmit.Add(rightIndex);
                    oldToNew.Add(field, replacementCounter);
                    replacementCounter += 1;
                }
                if (leftEmit.Count < mergeJoinRelation.Left.OutputLength)
                {
                    mergeJoinRelation.Left.Emit = leftEmit;
                }
                // Check if right side can be made smaller
                if (rightEmit.Count < mergeJoinRelation.Right.OutputLength)
                {
                    mergeJoinRelation.Right.Emit = rightEmit;
                }

                // Replace all used fields
                var replaceVisitor = new ExpressionFieldReplaceVisitor(oldToNew);
                foreach (var leftFieldKey in mergeJoinRelation.LeftKeys)
                {
                    replaceVisitor.Visit(leftFieldKey, default);
                }
                foreach (var rightFieldKey in mergeJoinRelation.RightKeys)
                {
                    replaceVisitor.Visit(rightFieldKey, default);
                }
                if (mergeJoinRelation.PostJoinFilter != null)
                {
                    replaceVisitor.Visit(mergeJoinRelation.PostJoinFilter, default);
                }

                if (mergeJoinRelation.EmitSet)
                {
                    for (int i = 0; i < mergeJoinRelation.Emit.Count; i++)
                    {
                        if (oldToNew.TryGetValue(mergeJoinRelation.Emit[i], out var newVal))
                        {
                            mergeJoinRelation.Emit[i] = newVal;
                        }
                        else
                        {
                            throw new InvalidOperationException("Error in emit pushdown optimizer");
                        }
                    }
                }
            }
            return base.VisitMergeJoinRelation(mergeJoinRelation, state);
        }
    }
}

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

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            // If emit is set, filter out the struct of the table to only select the fields that are needed
            Dictionary<int, int>? newMappings = default;
            if (readRelation.EmitSet)
            {
                newMappings = new Dictionary<int, int>();

                // Find all fields that are used in the filter
                var usageVisitor = new ExpressionFieldUsageVisitor(readRelation.BaseSchema.Names.Count);
                if (readRelation.Filter != null)
                {
                    usageVisitor.Visit(readRelation.Filter, default);
                }
                var usageList = usageVisitor.UsedFieldsLeft.Distinct().OrderBy(x => x).ToList();

                // Find all fields that needs to be emitted.
                if (readRelation.EmitSet)
                {
                    for (int i = 0; i < readRelation.Emit.Count; i++)
                    {
                        if (!usageList.Contains(readRelation.Emit[i]))
                        {
                            usageList.Add(readRelation.Emit[i]);
                        }
                    }
                }
                usageList = usageList.OrderBy(x => x).ToList();
                for (int i = 0; i < usageList.Count; i++)
                {
                    newMappings.Add(usageList[i], i);
                }

                // Remove all unnused fields in base schema and struct types
                int relativeOffset = 0;
                int lastField = 0;
                for (int i = 0; i < usageList.Count; i++)
                {
                    var emitField = usageList[i] - relativeOffset;
                    for (int k = lastField; k < emitField; k++)
                    {
                        readRelation.BaseSchema.Names.RemoveAt(lastField);
                        readRelation.BaseSchema.Struct.Types.RemoveAt(lastField);
                        relativeOffset++;
                    }
                    lastField = usageList[i] - relativeOffset + 1;
                }
                if (lastField < readRelation.BaseSchema.Names.Count)
                {
                    readRelation.BaseSchema.Names.RemoveRange(lastField, readRelation.BaseSchema.Names.Count - lastField);
                    readRelation.BaseSchema.Struct.Types.RemoveRange(lastField, readRelation.BaseSchema.Struct.Types.Count - lastField);
                }

                if (readRelation.Filter != null)
                {
                    // Replace all the fields in the filter
                    var replaceVisitor = new ExpressionFieldReplaceVisitor(newMappings);
                    replaceVisitor.Visit(readRelation.Filter, default);
                }
                if (readRelation.EmitSet)
                {
                    for (int i = 0; i < readRelation.Emit.Count; i++)
                    {
                        if (newMappings.TryGetValue(readRelation.Emit[i], out var newMapping))
                        {
                            readRelation.Emit[i] = newMapping;
                        }
                        else
                        {
                            throw new InvalidOperationException("Could not find the correct new mapping id");
                        }
                    }
                }
                if (lastField < readRelation.BaseSchema.Names.Count)
                {
                    readRelation.BaseSchema.Names.RemoveRange(lastField, readRelation.BaseSchema.Names.Count - lastField);
                    readRelation.BaseSchema.Struct.Types.RemoveRange(lastField, readRelation.BaseSchema.Struct.Types.Count - lastField);
                }
            }
            return readRelation;
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            if (aggregateRelation.Input is ReferenceRelation referenceRelation)
            {
                return aggregateRelation;
            }
            if (aggregateRelation.Input.OutputLength >= aggregateRelation.OutputLength)
            {
                var input = aggregateRelation.Input;
                var usageVisitor = new ExpressionFieldUsageVisitor(aggregateRelation.Input.OutputLength);
                foreach(var measure in aggregateRelation.Measures)
                {
                    if (measure.Measure.Arguments != null)
                    {
                        foreach (var arg in measure.Measure.Arguments)
                        {
                            usageVisitor.Visit(arg, default);
                        }
                    }
                    if (measure.Filter != null)
                    {
                        usageVisitor.Visit(measure.Filter, default);
                    }
                }
                if (aggregateRelation.Groupings != null)
                {
                    foreach (var grouping in aggregateRelation.Groupings)
                    {
                        foreach (var expr in grouping.GroupingExpressions)
                        {
                            usageVisitor.Visit(expr, default);
                        }
                    }
                }
                
                var usedFields = usageVisitor.UsedFieldsLeft.Distinct().ToList();

                Dictionary<int, int> oldToNew = new Dictionary<int, int>();
                List<int> emit = new List<int>();
                int count = 0;
                foreach (var field in usedFields.OrderBy(x => x))
                {
                    emit.Add(field);
                    oldToNew.Add(field, count);
                    count++;
                }

                var replaceVisitor = new ExpressionFieldReplaceVisitor(oldToNew);
                foreach (var measure in aggregateRelation.Measures)
                {
                    if (measure.Measure.Arguments != null)
                    {
                        foreach (var arg in measure.Measure.Arguments)
                        {
                            replaceVisitor.Visit(arg, default);
                        }
                    }
                    if (measure.Filter != null)
                    {
                        replaceVisitor.Visit(measure.Filter, default);
                    }
                }

                if (aggregateRelation.Groupings != null)
                {
                    foreach (var grouping in aggregateRelation.Groupings)
                    {
                        foreach (var expr in grouping.GroupingExpressions)
                        {
                            replaceVisitor.Visit(expr, default);
                        }
                    }
                }

                input.Emit = emit;
            }
            return base.VisitAggregateRelation(aggregateRelation, state);
        }

        public override Relation VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, object state)
        {
            // Add a project relation infront of iteration reference read, since it cant be emit limitted.
            var projectRelation = new ProjectRelation()
            {
                Expressions = new List<FlowtideDotNet.Substrait.Expressions.Expression>(),
                Emit = iterationReferenceReadRelation.Emit,
                Input = iterationReferenceReadRelation
            };
            iterationReferenceReadRelation.Emit = null;
            return projectRelation;
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            if (projectRelation.Input is ReferenceRelation referenceRelation)
            {
                return projectRelation;
            }
            if (projectRelation.Input is IterationReferenceReadRelation)
            {
                return projectRelation;
            }
            if (projectRelation.Input.OutputLength >= projectRelation.OutputLength)
            {
                var input = projectRelation.Input;

                var usageVisitor = new ExpressionFieldUsageVisitor(projectRelation.Input.OutputLength);
                if (projectRelation.Expressions != null)
                {
                    foreach (var expr in projectRelation.Expressions)
                    {
                        usageVisitor.Visit(expr, default);
                    }
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
                            if (!usedFields.Contains(field))
                            {
                                usedFields.Add(field);
                            }
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

                    Dictionary<int, int> inputEmitToInternal = new Dictionary<int, int>();
                    if (input.EmitSet)
                    {
                        for (int i = 0; i < input.Emit.Count; i++)
                        {
                            inputEmitToInternal.Add(i, input.Emit[i]);
                        }
                    }
                    else
                    {
                        for (int i = 0; i < input.OutputLength; i++)
                        {
                            inputEmitToInternal.Add(i, i);
                        }
                    }

                    int count = 0;
                    foreach(var field in usedFields.OrderBy(x => x))
                    {
                        emit.Add(inputEmitToInternal[field]);
                        oldToNew.Add(field, count);
                        count++;
                    }
                    var replaceVisitor = new ExpressionFieldReplaceVisitor(oldToNew);
                    if (projectRelation.Expressions != null)
                    {
                        foreach (var expr in projectRelation.Expressions)
                        {
                            replaceVisitor.Visit(expr, default);
                        }
                    }
                    
                    if (projectRelation.EmitSet)
                    {
                        var diff = input.OutputLength - emit.Count;
                        for (int i = 0; i < projectRelation.Emit.Count; i++)
                        {
                            if (projectRelation.Emit[i] >= input.OutputLength)
                            {
                                projectRelation.Emit[i] = projectRelation.Emit[i] - diff;
                            }
                            else
                            {
                                if (oldToNew.TryGetValue(projectRelation.Emit[i], out var newMapping))
                                {
                                    projectRelation.Emit[i] = newMapping;
                                }
                                else
                                {
                                    throw new InvalidOperationException("Could not find new mapping during optmization.");
                                }
                            }
                            
                        }
                    }

                    input.Emit = emit;

                    
                }
            }

            return base.VisitProjectRelation(projectRelation, state);
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            var inputLength = joinRelation.Left.OutputLength + joinRelation.Right.OutputLength;

            if (joinRelation.Left is ReferenceRelation leftReference)
            {
                var projectRel = new ProjectRelation()
                {
                    Expressions = new List<FlowtideDotNet.Substrait.Expressions.Expression>(),
                    Input = leftReference
                };
                joinRelation.Left = projectRel;
            }
            if (joinRelation.Right is ReferenceRelation rightReference)
            {
                var projectRel = new ProjectRelation()
                {
                    Expressions = new List<FlowtideDotNet.Substrait.Expressions.Expression>(),
                    Input = rightReference
                };
                joinRelation.Right = projectRel;
            }

            if (inputLength > joinRelation.OutputLength)
            {
                var usageVisitor = new ExpressionFieldUsageVisitor(joinRelation.Left.OutputLength);
                // Visit all possible field references

                if (joinRelation.Expression != null)
                {
                    usageVisitor.Visit(joinRelation.Expression, default);
                }
                if (joinRelation.PostJoinFilter != null)
                {
                    usageVisitor.Visit(joinRelation.PostJoinFilter, default);
                }


                var leftUsage = usageVisitor.UsedFieldsLeft.ToList();
                var rightUsage = usageVisitor.UsedFieldsRight.ToList();

                if (joinRelation.EmitSet)
                {
                    foreach (var field in joinRelation.Emit!)
                    {
                        if (field < joinRelation.Left.OutputLength)
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

                foreach (var field in rightUsage)
                {
                    var rightIndex = field - joinRelation.Left.OutputLength;

                    rightEmit.Add(rightIndex);
                    oldToNew.Add(field, replacementCounter);
                    replacementCounter += 1;
                }
                if (leftEmit.Count < joinRelation.Left.OutputLength)
                {
                    joinRelation.Left.Emit = leftEmit;
                }
                // Check if right side can be made smaller
                if (rightEmit.Count < joinRelation.Right.OutputLength)
                {
                    joinRelation.Right.Emit = rightEmit;
                }

                // Replace all used fields
                var replaceVisitor = new ExpressionFieldReplaceVisitor(oldToNew);
                if (joinRelation.PostJoinFilter != null)
                {
                    replaceVisitor.Visit(joinRelation.PostJoinFilter, default);
                }
                if (joinRelation.Expression != null)
                {
                    replaceVisitor.Visit(joinRelation.Expression, default);
                }

                if (joinRelation.EmitSet)
                {
                    for (int i = 0; i < joinRelation.Emit.Count; i++)
                    {
                        if (oldToNew.TryGetValue(joinRelation.Emit[i], out var newVal))
                        {
                            joinRelation.Emit[i] = newVal;
                        }
                        else
                        {
                            throw new InvalidOperationException("Error in emit pushdown optimizer");
                        }
                    }
                }
            }
            return base.VisitJoinRelation(joinRelation, state);
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

                Dictionary<int, int> leftEmitToInternal = new Dictionary<int, int>();
                if (mergeJoinRelation.Left.EmitSet)
                {
                    for (int i = 0; i < mergeJoinRelation.Left.Emit.Count; i++)
                    {
                        leftEmitToInternal.Add(i, mergeJoinRelation.Left.Emit[i]);
                    }
                }
                else
                {
                    for (int i = 0; i < mergeJoinRelation.Left.OutputLength; i++)
                    {
                        leftEmitToInternal.Add(i, i);
                    }
                }

                Dictionary<int, int> rightEmitToInternal = new Dictionary<int, int>();
                if (mergeJoinRelation.Right.EmitSet)
                {
                    for (int i = 0; i < mergeJoinRelation.Right.Emit.Count; i++)
                    {
                        rightEmitToInternal.Add(i, mergeJoinRelation.Right.Emit[i]);
                    }
                }
                else
                {
                    for (int i = 0; i < mergeJoinRelation.Right.OutputLength; i++)
                    {
                        rightEmitToInternal.Add(i, i);
                    }
                }

                foreach (var field in leftUsage)
                {
                    leftEmit.Add(leftEmitToInternal[field]);
                    oldToNew.Add(field, replacementCounter);
                    replacementCounter += 1;
                }

                foreach(var field in rightUsage)
                {
                    var rightIndex = field- mergeJoinRelation.Left.OutputLength;

                    rightEmit.Add(rightEmitToInternal[rightIndex]);
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

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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class LineageDatasetFieldVisitor : OptimizerBaseVisitor
    {
        private readonly LineageVisitor lineageVisitor;
        private readonly Dictionary<string, LineageInputField> _datasetFields = new Dictionary<string, LineageInputField>();

        public LineageDatasetFieldVisitor(LineageVisitor lineageVisitor)
        {
            this.lineageVisitor = lineageVisitor;
        }

        public static IReadOnlyList<LineageInputField> GetDatasetFields(LineageVisitor lineageVisitor, WriteRelation writeRelation)
        {
            var visitor = new LineageDatasetFieldVisitor(lineageVisitor);
            visitor.Visit(writeRelation, default!);
            return visitor._datasetFields.Values.ToList();
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            var usedFields = LineageExpressionVisitor.GetFieldReferences(filterRelation.Condition);
            
            foreach(var field in usedFields)
            {
                var foundInputs = lineageVisitor.Visit(filterRelation.Input, new LineageVisitorState(field, [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Filter)]));
                foreach(var foundInput in foundInputs.InputFields)
                {
                    var key = $"{foundInput.Namespace}.{foundInput.TableName}.{foundInput.Field}";
                    if (!_datasetFields.ContainsKey(key))
                    {
                        _datasetFields.Add(key, foundInput);
                    }
                }
            }

            return base.VisitFilterRelation(filterRelation, state);
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            if (aggregateRelation.Groupings != null && aggregateRelation.Groupings.Count > 0)
            {
                var grouping = aggregateRelation.Groupings[0];
                var groupLength = grouping.GroupingExpressions.Count;

                if (groupLength > 0)
                {
                    var usedFields = grouping.GroupingExpressions.SelectMany(expr => LineageExpressionVisitor.GetFieldReferences(expr)).ToList();
                    foreach (var field in usedFields)
                    {
                        var foundInputs = lineageVisitor.Visit(aggregateRelation.Input, new LineageVisitorState(field, [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.GroupBy)]));
                        foreach (var foundInput in foundInputs.InputFields)
                        {
                            var key = $"{foundInput.Namespace}.{foundInput.TableName}.{foundInput.Field}";
                            if (!_datasetFields.ContainsKey(key))
                            {
                                _datasetFields.Add(key, foundInput);
                            }
                        }
                    }
                }
            }
            return base.VisitAggregateRelation(aggregateRelation, state);
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
        {
            if (mergeJoinRelation.LeftKeys.Count > 0)
            {
                foreach(var leftKey in mergeJoinRelation.LeftKeys)
                {
                    var usedFields = LineageExpressionVisitor.GetFieldReferences(leftKey);
                    foreach (var field in usedFields)
                    {
                        var foundInputs = lineageVisitor.Visit(mergeJoinRelation.Left, new LineageVisitorState(field, [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]));
                        foreach (var foundInput in foundInputs.InputFields)
                        {
                            var key = $"{foundInput.Namespace}.{foundInput.TableName}.{foundInput.Field}";
                            if (!_datasetFields.ContainsKey(key))
                            {
                                _datasetFields.Add(key, foundInput);
                            }
                        }
                    }
                }
            }
            if (mergeJoinRelation.RightKeys.Count > 0)
            {
                foreach (var rightKey in mergeJoinRelation.RightKeys)
                {
                    var usedFields = LineageExpressionVisitor.GetFieldReferences(rightKey);
                    foreach (var field in usedFields)
                    {
                        if (field.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                        {
                            var foundInputs = lineageVisitor.Visit(mergeJoinRelation.Right, new LineageVisitorState(new DirectFieldReference()
                            {
                                ReferenceSegment = new StructReferenceSegment()
                                {
                                    Field = structReferenceSegment.Field - mergeJoinRelation.Left.OutputLength
                                }
                            }, [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]));
                            foreach (var foundInput in foundInputs.InputFields)
                            {
                                var key = $"{foundInput.Namespace}.{foundInput.TableName}.{foundInput.Field}";
                                if (!_datasetFields.ContainsKey(key))
                                {
                                    _datasetFields.Add(key, foundInput);
                                }
                            }
                        }
                            
                    }
                }
            }
            if (mergeJoinRelation.PostJoinFilter != null)
            {
                var leftSize = mergeJoinRelation.Left.OutputLength;
                var usedFields = LineageExpressionVisitor.GetFieldReferences(mergeJoinRelation.PostJoinFilter);
                foreach (var field in usedFields)
                {
                    if (field.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                    {
                        if (structReferenceSegment.Field < leftSize)
                        {
                            var foundInputs = lineageVisitor.Visit(mergeJoinRelation.Left, new LineageVisitorState(field, [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]));
                            foreach (var foundInput in foundInputs.InputFields)
                            {
                                var key = $"{foundInput.Namespace}.{foundInput.TableName}.{foundInput.Field}";
                                if (!_datasetFields.ContainsKey(key))
                                {
                                    _datasetFields.Add(key, foundInput);
                                }
                            }
                        }
                        else
                        {
                            var foundInputs = lineageVisitor.Visit(mergeJoinRelation.Right, new LineageVisitorState(new DirectFieldReference()
                            {
                                ReferenceSegment = new StructReferenceSegment()
                                {
                                    Field = structReferenceSegment.Field - leftSize
                                }
                            }, [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]));
                            foreach (var foundInput in foundInputs.InputFields)
                            {
                                var key = $"{foundInput.Namespace}.{foundInput.TableName}.{foundInput.Field}";
                                if (!_datasetFields.ContainsKey(key))
                                {
                                    _datasetFields.Add(key, foundInput);
                                }
                            }
                        }
                    }
                }
            }
            return base.VisitMergeJoinRelation(mergeJoinRelation, state);
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            var leftSize = joinRelation.Left.OutputLength;
            if (joinRelation.Expression != null)
            {
                var usedFields = LineageExpressionVisitor.GetFieldReferences(joinRelation.Expression);
                foreach (var field in usedFields)
                {
                    if (field.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                    {
                        if (structReferenceSegment.Field < leftSize)
                        {
                            var foundInputs = lineageVisitor.Visit(joinRelation.Left, new LineageVisitorState(field, [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]));
                            foreach (var foundInput in foundInputs.InputFields)
                            {
                                var key = $"{foundInput.Namespace}.{foundInput.TableName}.{foundInput.Field}";
                                if (!_datasetFields.ContainsKey(key))
                                {
                                    _datasetFields.Add(key, foundInput);
                                }
                            }
                        }
                        else
                        {
                            var foundInputs = lineageVisitor.Visit(joinRelation.Right, new LineageVisitorState(new DirectFieldReference()
                            {
                                ReferenceSegment = new StructReferenceSegment()
                                {
                                    Field = structReferenceSegment.Field - leftSize
                                }
                            }, [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]));
                            foreach (var foundInput in foundInputs.InputFields)
                            {
                                var key = $"{foundInput.Namespace}.{foundInput.TableName}.{foundInput.Field}";
                                if (!_datasetFields.ContainsKey(key))
                                {
                                    _datasetFields.Add(key, foundInput);
                                }
                            }
                        }
                    }
                }
            }
            if (joinRelation.PostJoinFilter != null)
            {
                var usedFields = LineageExpressionVisitor.GetFieldReferences(joinRelation.PostJoinFilter);
                foreach (var field in usedFields)
                {
                    if (field.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                    {
                        if (structReferenceSegment.Field < leftSize)
                        {
                            var foundInputs = lineageVisitor.Visit(joinRelation.Left, new LineageVisitorState(field, [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]));
                            foreach (var foundInput in foundInputs.InputFields)
                            {
                                var key = $"{foundInput.Namespace}.{foundInput.TableName}.{foundInput.Field}";
                                if (!_datasetFields.ContainsKey(key))
                                {
                                    _datasetFields.Add(key, foundInput);
                                }
                            }
                        }
                        else
                        {
                            var foundInputs = lineageVisitor.Visit(joinRelation.Right, new LineageVisitorState(field, [new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Join)]));
                            foreach (var foundInput in foundInputs.InputFields)
                            {
                                var key = $"{foundInput.Namespace}.{foundInput.TableName}.{foundInput.Field}";
                                if (!_datasetFields.ContainsKey(key))
                                {
                                    _datasetFields.Add(key, foundInput);
                                }
                            }
                        }
                    }
                }
            }
            return base.VisitJoinRelation(joinRelation, state);
        }
    }
}

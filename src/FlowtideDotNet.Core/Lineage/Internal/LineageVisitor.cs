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
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class LineageVisitor : RelationVisitor<LineageVisitorResult, LineageVisitorState>
    {
        private readonly IReadOnlyList<Relation> _relations;
        private readonly IDictionary<string, LineageInputTable> inputTables;

        public LineageVisitor(IReadOnlyList<Relation> relations, IDictionary<string, LineageInputTable> inputTables)
        {
            this._relations = relations;
            this.inputTables = inputTables;
        }

        public ColumnLineage HandleWriteRelation(WriteRelation writeRelation)
        {
            Dictionary<string, IReadOnlyList<LineageInputField>> fields = new Dictionary<string, IReadOnlyList<LineageInputField>>();
            for (int i = 0; i < writeRelation.TableSchema.Names.Count; i++)
            {
                var outputFieldName = writeRelation.TableSchema.Names[i];
                var result = Visit(writeRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = i
                    }
                }, [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)]));
                fields.Add(outputFieldName, result.InputFields);
            }

            var datasetFields = LineageDatasetFieldVisitor.GetDatasetFields(this, writeRelation);
            return new ColumnLineage(fields, datasetFields);
        }

        public override LineageVisitorResult VisitAggregateRelation(AggregateRelation aggregateRelation, LineageVisitorState state)
        {
            int groupLength = 0;
            AggregateGrouping? grouping = default;
            if (aggregateRelation.Groupings != null && aggregateRelation.Groupings.Count > 0)
            {
                grouping = aggregateRelation.Groupings[0];
                groupLength = grouping.GroupingExpressions.Count;
            }

            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                int field = 0;
                if (aggregateRelation.EmitSet)
                {
                    field = aggregateRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    field = structReferenceSegment.Field;
                }
                Dictionary<string, LineageInputField> inputFields = new Dictionary<string, LineageInputField>();
                // Check if it is from grouping
                if (field < groupLength)
                {
                    // Grouping will always be not null if the field is referencing a grouping
                    var expr = grouping!.GroupingExpressions[field];
                    // Get which fields are used in the grouping expression
                    var fields = LineageExpressionVisitor.GetFieldReferences(expr);
                    foreach(var exprField in fields)
                    {
                        var result = Visit(aggregateRelation.Input, new LineageVisitorState(exprField, state.Transformations));
                        foreach (var inputField in result.InputFields)
                        {
                            var key = $"{inputField.Namespace}.{inputField.TableName}.{inputField.Field}";
                            if (!inputFields.ContainsKey(key))
                            {
                                inputFields.Add(key, inputField);
                            }
                        }
                    }
                    return new LineageVisitorResult(inputFields.Values.ToList());
                }
                // Measure
                else
                {
                    var measure = aggregateRelation.Measures![field - groupLength];
                    for (int i = 0; i < measure.Measure.Arguments.Count; i++)
                    {
                        // Direct usage from measure arguments
                        var argFields = LineageExpressionVisitor.GetFieldReferences(measure.Measure.Arguments[i]);
                        foreach(var argField in argFields)
                        {
                            var result = Visit(aggregateRelation.Input, new LineageVisitorState(argField, state.AppendTransformation(new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Aggregation))));
                            foreach (var inputField in result.InputFields)
                            {
                                var key = $"{inputField.Namespace}.{inputField.TableName}.{inputField.Field}";
                                if (!inputFields.ContainsKey(key))
                                {
                                    inputFields.Add(key, inputField);
                                }
                            }
                        }
                    }
                    if (measure.Filter != null)
                    {
                        var filterUsedFields = LineageExpressionVisitor.GetFieldReferences(measure.Filter);
                        foreach (var usedField in filterUsedFields)
                        {
                            var result = Visit(aggregateRelation.Input, new LineageVisitorState(usedField, state.AppendTransformation(new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Filter))));
                            foreach (var inputField in result.InputFields)
                            {
                                var key = $"{inputField.Namespace}.{inputField.TableName}.{inputField.Field}";
                                if (!inputFields.ContainsKey(key))
                                {
                                    inputFields.Add(key, inputField);
                                }
                            }
                        }
                    }
                }

                return new LineageVisitorResult(inputFields.Values.ToList());
            }

            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitFilterRelation(FilterRelation filterRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (filterRelation.EmitSet)
                {
                    emitIndex = filterRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }

                return Visit(filterRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = emitIndex
                    }
                }, state.Transformations));
            }

            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitProjectRelation(ProjectRelation projectRelation, LineageVisitorState state)
        {
            var inputLength = projectRelation.Input.OutputLength;

            if (projectRelation.EmitSet)
            {
                if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment referenceSegment)
                {
                    var emitIndex = projectRelation.Emit[referenceSegment.Field];
                    if (emitIndex >= inputLength)
                    {
                        var expr = projectRelation.Expressions[emitIndex - inputLength];

                        if (expr is DirectFieldReference directFieldReference && 
                            directFieldReference.ReferenceSegment is StructReferenceSegment directStructRefSegment)
                        {
                            return Visit(projectRelation.Input, new LineageVisitorState(directFieldReference, state.AppendTransformation(new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity))));
                        }

                        // Column is created from expressions
                        var usedColumns = LineageExpressionVisitor.GetFieldReferences(projectRelation.Expressions[emitIndex - inputLength]);

                        Dictionary<string, LineageInputField> inputFields = new Dictionary<string, LineageInputField>();
                        for (int i = 0; i < usedColumns.Count; i++)
                        {
                            var result = Visit(projectRelation.Input, new LineageVisitorState(usedColumns[i], state.AppendTransformation(new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Transformation))));
                            foreach(var field in result.InputFields)
                            {
                                var key = $"{field.Namespace}.{field.TableName}.{field.Field}";
                                if (!inputFields.TryGetValue(key, out var existing))
                                {
                                    existing = field;
                                    inputFields.Add(key, existing);
                                }
                            }
                        }
                        return new LineageVisitorResult(inputFields.Values.ToList());
                    }
                    else
                    {
                        // Column is created from direct reference
                        return Visit(projectRelation.Input, new LineageVisitorState(new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment()
                            {
                                Field = emitIndex
                            }
                        }, state.AppendTransformation(new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity))));
                    }
                }
                
            }
            else
            {

            }

            return base.VisitProjectRelation(projectRelation, state);
        }

        public override LineageVisitorResult VisitBufferRelation(BufferRelation bufferRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (bufferRelation.EmitSet)
                {
                    emitIndex = bufferRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }

                return Visit(bufferRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, []));
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitReadRelation(ReadRelation readRelation, LineageVisitorState state)
        {
            var key = readRelation.NamedTable.DotSeperated;
            if (!inputTables.TryGetValue(key, out var inputTable))
            {
                return new LineageVisitorResult([]);
            }
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (readRelation.EmitSet)
                {
                    emitIndex = readRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }
                // We have reached the source, we can return the lineage result
                var columnName = readRelation.BaseSchema.Names[emitIndex];
                
                return new LineageVisitorResult([new LineageInputField(inputTable.Namespace, inputTable.TableName, columnName, state.Transformations)]);
            }

            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var inputLength = consistentPartitionWindowRelation.Input.OutputLength;
                var emitIndex = 0;
                if (consistentPartitionWindowRelation.EmitSet)
                {
                    emitIndex = consistentPartitionWindowRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }

                if (emitIndex >= inputLength)
                {
                    // It is a window function so all partitions and order by fields must be added as indirect lineage since they contribute to the window function result
                    Dictionary<string, LineageInputField> inputFields = new Dictionary<string, LineageInputField>();

                    // Go through all partition expressions since they can also reference the input fields and contribute to the lineage result
                    foreach (var partitionExpr in consistentPartitionWindowRelation.PartitionBy)
                    {
                        var partitionFields = LineageExpressionVisitor.GetFieldReferences(partitionExpr);

                        foreach (var field in partitionFields)
                        {
                            var partitionResult = Visit(consistentPartitionWindowRelation.Input, new LineageVisitorState(field, state.AppendTransformation(new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.GroupBy))));

                            foreach (var inputField in partitionResult.InputFields)
                            {
                                var key = $"{inputField.Namespace}.{inputField.TableName}.{inputField.Field}";

                                if (!inputFields.ContainsKey(key))
                                {
                                    inputFields.Add(key, inputField);
                                }
                            }
                        }
                    }

                    foreach (var orderByExpr in consistentPartitionWindowRelation.OrderBy)
                    {
                        var orderFields = LineageExpressionVisitor.GetFieldReferences(orderByExpr.Expression);

                        foreach (var field in orderFields)
                        {
                            var orderResult = Visit(consistentPartitionWindowRelation.Input, new LineageVisitorState(field, state.AppendTransformation(new LineageTransformation(LineageTransformationType.Indirect, LineageTransformationSubtype.Sort))));

                            foreach (var inputField in orderResult.InputFields)
                            {
                                var key = $"{inputField.Namespace}.{inputField.TableName}.{inputField.Field}";

                                if (!inputFields.ContainsKey(key))
                                {
                                    inputFields.Add(key, inputField);
                                }
                            }
                        }
                    }
                    var windowFunc = consistentPartitionWindowRelation.WindowFunctions[inputLength - emitIndex];

                    foreach(var arg in windowFunc.Arguments)
                    {
                        var usedFields = LineageExpressionVisitor.GetFieldReferences(arg);

                        foreach(var field in usedFields)
                        {
                            var fieldResult = Visit(consistentPartitionWindowRelation.Input, new LineageVisitorState(field, state.AppendTransformation(new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Aggregation))));

                            foreach (var inputField in fieldResult.InputFields)
                            {
                                var key = $"{inputField.Namespace}.{inputField.TableName}.{inputField.Field}";

                                if (!inputFields.ContainsKey(key))
                                {
                                    inputFields.Add(key, inputField);
                                }
                            }
                        }
                    }

                    return new LineageVisitorResult(inputFields.Values.ToList());
                }
                else
                {
                    return Visit(consistentPartitionWindowRelation.Input, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                    }, state.Transformations));
                }
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, LineageVisitorState state)
        {
            var leftLength = mergeJoinRelation.Left.OutputLength;

            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (mergeJoinRelation.EmitSet)
                {
                    emitIndex = mergeJoinRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }
                if (emitIndex < leftLength)
                {
                    return Visit(mergeJoinRelation.Left, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                    }, state.Transformations));
                }
                else
                {
                    return Visit(mergeJoinRelation.Right, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex - leftLength }
                    }, state.Transformations));
                }
            }

            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitReferenceRelation(ReferenceRelation referenceRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (referenceRelation.EmitSet)
                {
                    emitIndex = referenceRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }
                var rel = _relations[referenceRelation.RelationId];
                return Visit(rel, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitExchangeRelation(ExchangeRelation exchangeRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (exchangeRelation.EmitSet)
                {
                    emitIndex = exchangeRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }
                return Visit(exchangeRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
            }
            return new LineageVisitorResult([]); 
        }

        public override LineageVisitorResult VisitFetchRelation(FetchRelation fetchRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (fetchRelation.EmitSet)
                {
                    emitIndex = fetchRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }
                return Visit(fetchRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitJoinRelation(JoinRelation joinRelation, LineageVisitorState state)
        {
            var leftLength = joinRelation.Left.OutputLength;

            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (joinRelation.EmitSet)
                {
                    emitIndex = joinRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }
                if (emitIndex < leftLength)
                {
                    return Visit(joinRelation.Left, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                    }, state.Transformations));
                }
                else
                {
                    return Visit(joinRelation.Right, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex - leftLength }
                    }, state.Transformations));
                }
            }

            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitNormalizationRelation(NormalizationRelation normalizationRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (normalizationRelation.EmitSet)
                {
                    emitIndex = normalizationRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }
                return Visit(normalizationRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitSetRelation(SetRelation setRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (setRelation.EmitSet)
                {
                    emitIndex = setRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }
                Dictionary<string, LineageInputField> inputFields = new Dictionary<string, LineageInputField>();
                foreach (var input in setRelation.Inputs)
                {
                    var inputResult = Visit(input, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                    }, state.Transformations));

                    foreach (var resultField in inputResult.InputFields)
                    {
                        var key = $"{resultField.Namespace}.{resultField.TableName}.{resultField.Field}";
                        if (!inputFields.ContainsKey(key))
                        {
                            inputFields.Add(key, resultField);
                        }
                    }
                }
                return new LineageVisitorResult(inputFields.Values.ToList());
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitIterationRelation(IterationRelation iterationRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (iterationRelation.EmitSet)
                {
                    emitIndex = iterationRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }

                Dictionary<string, LineageInputField> inputFields = new Dictionary<string, LineageInputField>();
                if (iterationRelation.Input != null)
                {
                    var inputResult = Visit(iterationRelation.Input, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                    }, state.Transformations));
                    foreach (var resultField in inputResult.InputFields)
                    {
                        var key = $"{resultField.Namespace}.{resultField.TableName}.{resultField.Field}";
                        if (!inputFields.ContainsKey(key))
                        {
                            inputFields.Add(key, resultField);
                        }
                    }
                }
                var loopResult = Visit(iterationRelation.LoopPlan, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
                foreach (var resultField in loopResult.InputFields)
                {
                    var key = $"{resultField.Namespace}.{resultField.TableName}.{resultField.Field}";
                    if (!inputFields.ContainsKey(key))
                    {
                        inputFields.Add(key, resultField);
                    }
                }

                return new LineageVisitorResult(inputFields.Values.ToList());
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, LineageVisitorState state)
        {
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitPullExchangeReferenceRelation(PullExchangeReferenceRelation pullExchangeReferenceRelation, LineageVisitorState state)
        {
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitSortRelation(SortRelation sortRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (sortRelation.EmitSet)
                {
                    emitIndex = sortRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }
                return Visit(sortRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, LineageVisitorState state)
        {
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitSubStreamRootRelation(SubStreamRootRelation subStreamRootRelation, LineageVisitorState state)
        {
            return Visit(subStreamRootRelation.Input, state);
        }

        public override LineageVisitorResult VisitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, LineageVisitorState state)
        {
            // Skip table functions for now
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, LineageVisitorState state)
        {
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitTopNRelation(TopNRelation topNRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = 0;
                if (topNRelation.EmitSet)
                {
                    emitIndex = topNRelation.Emit[structReferenceSegment.Field];
                }
                else
                {
                    emitIndex = structReferenceSegment.Field;
                }
                return Visit(topNRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
            }
            return new LineageVisitorResult([]);
        }
    }
}

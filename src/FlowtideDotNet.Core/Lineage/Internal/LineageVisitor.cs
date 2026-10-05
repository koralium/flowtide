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

using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal class LineageVisitor : RelationVisitor<LineageVisitorResult, LineageVisitorState>
    {
        private readonly LineagePlanIndex _index;
        private readonly IReadOnlyDictionary<string, LineageInputTable> inputTables;

        // Relations being expanded, a repeat means a cycle.
        private readonly HashSet<Relation> _expanding = new HashSet<Relation>(ReferenceEqualityComparer.Instance);
        private readonly Dictionary<ExchangeMemoKey, LineageVisitorResult> _exchangeMemo = new Dictionary<ExchangeMemoKey, LineageVisitorResult>();
        private int _cycleCuts;

        public LineageVisitor(IReadOnlyList<Relation> relations, IReadOnlyDictionary<string, LineageInputTable> inputTables)
            : this(new LineagePlanIndex(relations), inputTables)
        {
        }

        public LineageVisitor(LineagePlanIndex index, IReadOnlyDictionary<string, LineageInputTable> inputTables)
        {
            this._index = index;
            this.inputTables = inputTables;
        }

        internal LineagePlanIndex PlanIndex => _index;

        public ColumnLineage HandleWriteRelation(WriteRelation writeRelation)
        {
            Dictionary<string, ColumnLineageField> fields = new Dictionary<string, ColumnLineageField>();
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

                if (fields.TryGetValue(outputFieldName, out var existing))
                {
                    // Duplicate output name, merge instead of throwing.
                    fields[outputFieldName] = new ColumnLineageField(LineageMerge.MergeInputFields(existing.InputFields, result.InputFields));
                }
                else
                {
                    fields.Add(outputFieldName, new ColumnLineageField(result.InputFields));
                }
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
                var field = aggregateRelation.EmitSet ? aggregateRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;
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
                var emitIndex = filterRelation.EmitSet ? filterRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

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

            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment referenceSegment)
            {
                var emitIndex = projectRelation.EmitSet ? projectRelation.Emit[referenceSegment.Field] : referenceSegment.Field;

                if (emitIndex >= inputLength)
                {
                    var expr = projectRelation.Expressions[emitIndex - inputLength];

                    if (expr is DirectFieldReference directFieldReference)
                    {
                        return Visit(projectRelation.Input, new LineageVisitorState(directFieldReference, state.AppendTransformation(new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity))));
                    }

                    // Column is created from expressions
                    var usedColumns = LineageExpressionVisitor.GetFieldReferences(projectRelation.Expressions[emitIndex - inputLength]);

                    Dictionary<string, LineageInputField> inputFields = new Dictionary<string, LineageInputField>();
                    for (int i = 0; i < usedColumns.Count; i++)
                    {
                        var result = Visit(projectRelation.Input, new LineageVisitorState(usedColumns[i], state.AppendTransformation(new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Transformation))));
                        foreach (var field in result.InputFields)
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

            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitBufferRelation(BufferRelation bufferRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = bufferRelation.EmitSet ? bufferRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

                return Visit(bufferRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitCheckRelation(CheckRelation checkRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = checkRelation.EmitSet ? checkRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

                return Visit(checkRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
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
                var emitIndex = readRelation.EmitSet ? readRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

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
                var emitIndex = consistentPartitionWindowRelation.EmitSet ? consistentPartitionWindowRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

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
                    var windowFunc = consistentPartitionWindowRelation.WindowFunctions[emitIndex - inputLength];

                    for (int i = 0; i < windowFunc.Arguments.Count; i++)
                    {
                        var arg = windowFunc.Arguments[i];
                        var usedFields = LineageExpressionVisitor.GetFieldReferences(arg);

                        for (int f = 0; f < usedFields.Count; f++)
                        {
                            var field = usedFields[f];
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
                var emitIndex = mergeJoinRelation.EmitSet ? mergeJoinRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

                return emitIndex < leftLength
                    ? Visit(mergeJoinRelation.Left, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                    }, state.Transformations))
                    : Visit(mergeJoinRelation.Right, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex - leftLength }
                    }, state.Transformations));
            }

            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitReferenceRelation(ReferenceRelation referenceRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment &&
                _index.TryGetRelation(referenceRelation.RelationId, out var rel))
            {
                var emitIndex = referenceRelation.EmitSet ? referenceRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;
                return VisitGuarded(rel, emitIndex, state.Transformations);
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitRootRelation(RootRelation rootRelation, LineageVisitorState state)
        {
            return Visit(rootRelation.Input, state);
        }

        public override LineageVisitorResult VisitSubstreamExchangeReferenceRelation(SubstreamExchangeReferenceRelation substreamExchangeReferenceRelation, LineageVisitorState state)
        {
            if (_index.TryResolve(substreamExchangeReferenceRelation, out var exchange))
            {
                return VisitExchangeReference(substreamExchangeReferenceRelation, exchange, state);
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitExchangeRelation(ExchangeRelation exchangeRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = exchangeRelation.EmitSet ? exchangeRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

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
                var emitIndex = fetchRelation.EmitSet ? fetchRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

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
                var emitIndex = joinRelation.EmitSet ? joinRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

                return emitIndex < leftLength 
                    ? Visit(joinRelation.Left, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                    }, state.Transformations))
                    : Visit(joinRelation.Right, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex - leftLength }
                    }, state.Transformations));
            }

            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitNormalizationRelation(NormalizationRelation normalizationRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = normalizationRelation.EmitSet ? normalizationRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

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
                var emitIndex = setRelation.EmitSet ? setRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

                Dictionary<string, LineageInputField> inputFields = new Dictionary<string, LineageInputField>();
                for (int i = 0; i < setRelation.Inputs.Count; i++)
                {
                    var input = setRelation.Inputs[i];
                    var inputResult = Visit(input, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                    }, state.Transformations));

                    for (int f = 0; f < inputResult.InputFields.Count; f++)
                    {
                        var resultField = inputResult.InputFields[f];
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
                var emitIndex = iterationRelation.EmitSet ? iterationRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

                Dictionary<string, LineageInputField> inputFields = new Dictionary<string, LineageInputField>();
                if (iterationRelation.Input != null)
                {
                    var inputResult = Visit(iterationRelation.Input, new LineageVisitorState(new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                    }, state.Transformations));
                    for (int f = 0; f < inputResult.InputFields.Count; f++)
                    {
                        var resultField = inputResult.InputFields[f];
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
            if (_index.TryResolve(pullExchangeReferenceRelation, out var exchange))
            {
                return VisitExchangeReference(pullExchangeReferenceRelation, exchange, state);
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitSortRelation(SortRelation sortRelation, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
            {
                var emitIndex = sortRelation.EmitSet ? sortRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

                return Visit(sortRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
            }
            return new LineageVisitorResult([]);
        }

        public override LineageVisitorResult VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, LineageVisitorState state)
        {
            if (_index.TryResolve(standardOutputExchangeReferenceRelation, out var exchange))
            {
                return VisitExchangeReference(standardOutputExchangeReferenceRelation, exchange, state);
            }
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
                var emitIndex = topNRelation.EmitSet ? topNRelation.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;

                return Visit(topNRelation.Input, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = emitIndex }
                }, state.Transformations));
            }
            return new LineageVisitorResult([]);
        }

        private LineageVisitorResult VisitExchangeReference(Relation reference, ExchangeRelation exchange, LineageVisitorState state)
        {
            if (state.DirectFieldReference.ReferenceSegment is not StructReferenceSegment structReferenceSegment)
            {
                return new LineageVisitorResult([]);
            }

            // Reference emit here, the exchange visit applies its own.
            var field = reference.EmitSet ? reference.Emit[structReferenceSegment.Field] : structReferenceSegment.Field;
            var key = new ExchangeMemoKey(exchange, field, state.Transformations);
            if (_exchangeMemo.TryGetValue(key, out var cached))
            {
                return cached;
            }

            var cutsBefore = _cycleCuts;
            var result = VisitGuarded(exchange, field, state.Transformations);
            // Cycle cut results are partial, never cache them.
            if (_cycleCuts == cutsBefore)
            {
                _exchangeMemo[key] = result;
            }
            return result;
        }

        private LineageVisitorResult VisitGuarded(Relation target, int field, IReadOnlyList<LineageTransformation> transformations)
        {
            if (!_expanding.Add(target))
            {
                _cycleCuts++;
                return new LineageVisitorResult([]);
            }
            try
            {
                return Visit(target, new LineageVisitorState(new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = field }
                }, transformations));
            }
            finally
            {
                _expanding.Remove(target);
            }
        }

        // Relations hash structurally, so key by reference.
        private readonly struct ExchangeMemoKey : IEquatable<ExchangeMemoKey>
        {
            private readonly Relation _relation;
            private readonly int _field;
            private readonly IReadOnlyList<LineageTransformation> _transformations;

            public ExchangeMemoKey(Relation relation, int field, IReadOnlyList<LineageTransformation> transformations)
            {
                _relation = relation;
                _field = field;
                _transformations = transformations;
            }

            public bool Equals(ExchangeMemoKey other)
            {
                return ReferenceEquals(_relation, other._relation) &&
                    _field == other._field &&
                    _transformations.SequenceEqual(other._transformations);
            }

            public override bool Equals(object? obj)
            {
                return obj is ExchangeMemoKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                var code = new HashCode();
                code.Add(RuntimeHelpers.GetHashCode(_relation));
                code.Add(_field);
                foreach (var transformation in _transformations)
                {
                    code.Add(transformation);
                }
                return code.ToHashCode();
            }
        }
    }
}

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

using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Optimizer.DistributedMode
{
    /// <summary>
    /// Distributes partitionable operators in a normal plan across substreams.
    ///
    /// This is the distributed version of the parallelize optimization, merge joins, aggregates
    /// and consistent window functions are split into one partition per substream:
    ///
    /// 1. A scatter exchange partitions the operators input on its keys, the exchange and its
    ///    input pipeline is placed in a substream that rotates for each exchange to spread source load.
    /// 2. One copy of the operator runs in every substream, reading its partition from the
    ///    exchange, locally when the exchange runs in the same substream and through cross
    ///    substream communication otherwise.
    /// 3. The results of the copies are gathered back to the substream that runs the sink
    ///    and combined with a union.
    ///
    /// The visitor must run after merge join conversion in the plan optimizer so joins are
    /// in their merge join form. It is deterministic so every substream host computes an
    /// identical plan.
    /// </summary>
    internal sealed class DistributeOperatorsVisitor : OptimizerBaseVisitor
    {
        private readonly Plan _plan;
        private readonly int _substreamCount;
        private readonly Func<int, string> _nameGenerator;
        private int _nextExchangeTargetId;
        private int _exchangeRotation;
        private int _currentSinkSubstreamIndex;

        public DistributeOperatorsVisitor(Plan plan, int substreamCount, Func<int, string> nameGenerator, int firstExchangeTargetId)
        {
            _plan = plan;
            _substreamCount = substreamCount;
            _nameGenerator = nameGenerator;
            _nextExchangeTargetId = firstExchangeTargetId;
        }

        /// <summary>
        /// The unions that combine the partition lanes, used by <see cref="LanePushdownVisitor"/>
        /// to push operators above the unions down into the lanes.
        /// </summary>
        public Dictionary<SetRelation, DistributedLaneUnion> LaneUnions { get; } = new Dictionary<SetRelation, DistributedLaneUnion>(ReferenceEqualityComparer.Instance);

        /// <summary>
        /// Distributes the operators in one sink root tree.
        /// </summary>
        /// <param name="sinkRoot">The root relation of the sink tree.</param>
        /// <param name="sinkSubstreamIndex">The substream index the sink is assigned to.</param>
        public Relation VisitSinkRoot(Relation sinkRoot, int sinkSubstreamIndex)
        {
            _currentSinkSubstreamIndex = sinkSubstreamIndex % _substreamCount;
            return Visit(sinkRoot, null!);
        }

        /// <summary>
        /// The substream names for the partition copies, one copy per substream.
        /// The copy for partition 0 is placed in the sink substream so its output
        /// does not require any cross substream communication.
        /// </summary>
        private string[] GetCopySubstreams()
        {
            var copySubstreams = new string[_substreamCount];
            for (int i = 0; i < _substreamCount; i++)
            {
                copySubstreams[i] = _nameGenerator((_currentSinkSubstreamIndex + i) % _substreamCount);
            }
            return copySubstreams;
        }

        /// <summary>
        /// Adds a scatter exchange as a new top level relation wrapped in the substream it should run in.
        /// Creates one target per partition, standard output targets for copies in the same substream
        /// and substream targets for copies in other substreams, and returns the reference relation
        /// that each partition copy should read from.
        /// </summary>
        private Relation[] AddScatterExchange(Relation input, List<FieldReference> scatterFields, string[] copySubstreams)
        {
            var exchangeSubstream = _nameGenerator(_exchangeRotation % _substreamCount);
            _exchangeRotation++;

            var targets = new List<ExchangeTarget>();
            var exchange = new ExchangeRelation()
            {
                ExchangeKind = new ScatterExchangeKind()
                {
                    Fields = scatterFields
                },
                Input = input,
                Targets = targets,
                PartitionCount = _substreamCount
            };
            var exchangeIndex = _plan.Relations.Count;
            _plan.Relations.Add(new SubStreamRootRelation()
            {
                Name = exchangeSubstream,
                Input = exchange
            });

            var references = new Relation[_substreamCount];
            for (int i = 0; i < _substreamCount; i++)
            {
                if (copySubstreams[i] == exchangeSubstream)
                {
                    targets.Add(new StandardOutputExchangeTarget()
                    {
                        PartitionIds = new List<int>() { i }
                    });
                    references[i] = new StandardOutputExchangeReferenceRelation()
                    {
                        RelationId = exchangeIndex,
                        TargetId = i,
                        ReferenceOutputLength = exchange.OutputLength
                    };
                }
                else
                {
                    var exchangeTargetId = _nextExchangeTargetId++;
                    targets.Add(new SubstreamExchangeTarget()
                    {
                        ExchangeTargetId = exchangeTargetId,
                        PartitionIds = new List<int>() { i },
                        SubstreamName = copySubstreams[i]
                    });
                    references[i] = new SubstreamExchangeReferenceRelation()
                    {
                        SubStreamName = exchangeSubstream,
                        ExchangeTargetId = exchangeTargetId,
                        ReferenceOutputLength = exchange.OutputLength
                    };
                }
            }
            return references;
        }

        /// <summary>
        /// Combines the partition copies into a union in the sink substream.
        /// Copies in other substreams are hoisted to their own top level relations with a
        /// gather exchange that sends all their output to the sink substream.
        /// </summary>
        /// <param name="copies">The partition copies of the distributed operator.</param>
        /// <param name="copySubstreams">The substream each copy runs in.</param>
        /// <param name="partitionKeyColumns">The columns in the copies output that carry the partition key values, or null when unknown.</param>
        private Relation GatherCopies(Relation[] copies, string[] copySubstreams, List<int>? partitionKeyColumns)
        {
            var sinkSubstream = _nameGenerator(_currentSinkSubstreamIndex);
            var unionInputs = new List<Relation>();
            var laneInputs = new List<DistributedLaneInput>();
            for (int i = 0; i < copies.Length; i++)
            {
                if (copySubstreams[i] == sinkSubstream)
                {
                    unionInputs.Add(copies[i]);
                    laneInputs.Add(new DistributedLaneInput());
                    continue;
                }

                // A gather exchange with a single partition, all rows are sent to the sink substream.
                var exchangeTargetId = _nextExchangeTargetId++;
                var gatherExchange = new ExchangeRelation()
                {
                    ExchangeKind = new ScatterExchangeKind()
                    {
                        Fields = new List<FieldReference>()
                        {
                            new DirectFieldReference()
                            {
                                ReferenceSegment = new StructReferenceSegment()
                                {
                                    Field = 0
                                }
                            }
                        }
                    },
                    Input = copies[i],
                    PartitionCount = 1,
                    Targets = new List<ExchangeTarget>()
                    {
                        new SubstreamExchangeTarget()
                        {
                            ExchangeTargetId = exchangeTargetId,
                            // Empty list means all partitions are sent to this target
                            PartitionIds = new List<int>(),
                            SubstreamName = sinkSubstream
                        }
                    }
                };
                _plan.Relations.Add(new SubStreamRootRelation()
                {
                    Name = copySubstreams[i],
                    Input = gatherExchange
                });
                var reference = new SubstreamExchangeReferenceRelation()
                {
                    SubStreamName = copySubstreams[i],
                    ExchangeTargetId = exchangeTargetId,
                    ReferenceOutputLength = copies[i].OutputLength
                };
                unionInputs.Add(reference);
                laneInputs.Add(new DistributedLaneInput()
                {
                    GatherExchange = gatherExchange,
                    Reference = reference
                });
            }

            var union = new SetRelation()
            {
                Inputs = unionInputs,
                Operation = SetOperation.UnionAll
            };
            LaneUnions.Add(union, new DistributedLaneUnion()
            {
                Union = union,
                Inputs = laneInputs,
                PartitionKeyColumns = partitionKeyColumns
            });
            return union;
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
        {
            Debug.Assert(_plan != null);
            if (_substreamCount < 2)
            {
                return mergeJoinRelation;
            }

            var copySubstreams = GetCopySubstreams();

            var rightKeys = mergeJoinRelation.RightKeys.Select(x =>
            {
                if (x is DirectFieldReference directField)
                {
                    if (directField.ReferenceSegment is StructReferenceSegment structRef)
                    {
                        return new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment()
                            {
                                Field = structRef.Field - mergeJoinRelation.Left.OutputLength,
                                Child = structRef.Child
                            }
                        };
                    }
                }
                return x;
            }).ToList();

            var leftReferences = AddScatterExchange(mergeJoinRelation.Left, new List<FieldReference>(mergeJoinRelation.LeftKeys), copySubstreams);
            var rightReferences = AddScatterExchange(mergeJoinRelation.Right, new List<FieldReference>(rightKeys), copySubstreams);

            var copies = new Relation[_substreamCount];
            for (int i = 0; i < _substreamCount; i++)
            {
                copies[i] = new MergeJoinRelation()
                {
                    Left = leftReferences[i],
                    Right = rightReferences[i],
                    LeftKeys = mergeJoinRelation.LeftKeys,
                    RightKeys = mergeJoinRelation.RightKeys,
                    Type = mergeJoinRelation.Type,
                    PostJoinFilter = mergeJoinRelation.PostJoinFilter,
                    Emit = mergeJoinRelation.Emit
                };
            }

            return GatherCopies(copies, copySubstreams, GetJoinPartitionKeyColumns(mergeJoinRelation));
        }

        /// <summary>
        /// Gets the columns in the join output that carry the partition key values, or null
        /// when they cannot be determined. Since the join is an equality join both the left and
        /// the right key columns carry the values the data was partitioned on.
        /// </summary>
        private static List<int>? GetJoinPartitionKeyColumns(MergeJoinRelation mergeJoinRelation)
        {
            var leftKeyColumns = LanePushdownVisitor.MapColumnsThroughEmit(
                LanePushdownVisitor.GetDirectFieldColumns(mergeJoinRelation.LeftKeys),
                mergeJoinRelation.Emit);
            if (leftKeyColumns != null)
            {
                return leftKeyColumns;
            }
            // The right keys are in join output coordinates and carry the same values
            return LanePushdownVisitor.MapColumnsThroughEmit(
                LanePushdownVisitor.GetDirectFieldColumns(mergeJoinRelation.RightKeys),
                mergeJoinRelation.Emit);
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            Debug.Assert(_plan != null);
            if (_substreamCount < 2 || (aggregateRelation.Groupings == null || aggregateRelation.Groupings.Count < 1))
            {
                aggregateRelation.Input = Visit(aggregateRelation.Input, state);
                return aggregateRelation;
            }

            if (aggregateRelation.Measures != null &&
                aggregateRelation.Measures.Any(x => x.Measure.ExtensionUri == FunctionsAggregateGeneric.Uri &&
                    x.Measure.ExtensionName == FunctionsAggregateGeneric.SurrogateKeyInt64))
            {
                // Surrogate key is not supported in distribution
                aggregateRelation.Input = Visit(aggregateRelation.Input, state);
                return aggregateRelation;
            }

            List<FieldReference> fields = new List<FieldReference>();

            foreach (var grouping in aggregateRelation.Groupings)
            {
                foreach (var field in grouping.GroupingExpressions)
                {
                    if (field is DirectFieldReference directFieldReference)
                    {
                        fields.Add(directFieldReference);
                    }
                }
            }

            if (fields.Count == 0)
            {
                aggregateRelation.Input = Visit(aggregateRelation.Input, state);
                return aggregateRelation;
            }

            var groupingColumns = aggregateRelation.Groupings.Count == 1
                ? LanePushdownVisitor.GetDirectFieldColumns(aggregateRelation.Groupings[0].GroupingExpressions)
                : null;
            if (groupingColumns != null && IsCoPartitionedWithJoinBelow(aggregateRelation.Input, groupingColumns))
            {
                // A merge join below is partitioned on columns that the groupings cover, so
                // every group is fully contained in one join partition. Distribute the join
                // instead, the lane pushdown then moves this aggregate into the lanes without
                // another shuffle.
                aggregateRelation.Input = Visit(aggregateRelation.Input, state);
                return aggregateRelation;
            }

            var copySubstreams = GetCopySubstreams();
            var references = AddScatterExchange(aggregateRelation.Input, fields, copySubstreams);

            var copies = new Relation[_substreamCount];
            for (int i = 0; i < _substreamCount; i++)
            {
                copies[i] = new AggregateRelation()
                {
                    Input = references[i],
                    Groupings = aggregateRelation.Groupings,
                    Measures = aggregateRelation.Measures,
                    Emit = aggregateRelation.Emit
                };
            }

            return GatherCopies(copies, copySubstreams, partitionKeyColumns: null);
        }

        public override Relation VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, object state)
        {
            Debug.Assert(_plan != null);
            if (_substreamCount < 2 || (consistentPartitionWindowRelation.PartitionBy.Count < 1))
            {
                consistentPartitionWindowRelation.Input = Visit(consistentPartitionWindowRelation.Input, state);
                return consistentPartitionWindowRelation;
            }

            if (consistentPartitionWindowRelation.WindowFunctions.Any(x => x.ExtensionUri == FunctionsAggregateGeneric.Uri && x.ExtensionName == FunctionsAggregateGeneric.SurrogateKeyInt64))
            {
                // Surrogate key is not supported in distribution
                consistentPartitionWindowRelation.Input = Visit(consistentPartitionWindowRelation.Input, state);
                return consistentPartitionWindowRelation;
            }

            List<FieldReference> fields = new List<FieldReference>();

            foreach (var field in consistentPartitionWindowRelation.PartitionBy)
            {
                if (field is DirectFieldReference directFieldReference)
                {
                    fields.Add(directFieldReference);
                }
            }

            if (fields.Count == 0)
            {
                consistentPartitionWindowRelation.Input = Visit(consistentPartitionWindowRelation.Input, state);
                return consistentPartitionWindowRelation;
            }

            var partitionColumns = LanePushdownVisitor.GetDirectFieldColumns(consistentPartitionWindowRelation.PartitionBy);
            if (partitionColumns != null && IsCoPartitionedWithJoinBelow(consistentPartitionWindowRelation.Input, partitionColumns))
            {
                // A merge join below is partitioned on columns that the window partitions cover,
                // distribute the join instead, the lane pushdown then moves this window into
                // the lanes without another shuffle.
                consistentPartitionWindowRelation.Input = Visit(consistentPartitionWindowRelation.Input, state);
                return consistentPartitionWindowRelation;
            }

            var copySubstreams = GetCopySubstreams();
            var references = AddScatterExchange(consistentPartitionWindowRelation.Input, fields, copySubstreams);

            var copies = new Relation[_substreamCount];
            for (int i = 0; i < _substreamCount; i++)
            {
                copies[i] = new ConsistentPartitionWindowRelation()
                {
                    Input = references[i],
                    PartitionBy = consistentPartitionWindowRelation.PartitionBy,
                    OrderBy = consistentPartitionWindowRelation.OrderBy,
                    WindowFunctions = consistentPartitionWindowRelation.WindowFunctions,
                    Emit = consistentPartitionWindowRelation.Emit
                };
            }

            return GatherCopies(copies, copySubstreams, partitionKeyColumns: null);
        }

        /// <summary>
        /// Checks if the chain below consists of lane safe operators ending in a merge join
        /// whose partition key columns are all covered by the given columns.
        /// The columns are given in the coordinates of the top of the chain and are mapped
        /// down through the filters and projects.
        /// </summary>
        private bool IsCoPartitionedWithJoinBelow(Relation input, List<int> columns)
        {
            List<int>? currentColumns = columns;
            var current = input;
            while (currentColumns != null)
            {
                if (current is FilterRelation filterRelation)
                {
                    currentColumns = MapColumnsDownThroughEmit(currentColumns, filterRelation.Emit);
                    current = filterRelation.Input;
                }
                else if (current is ProjectRelation projectRelation)
                {
                    currentColumns = MapColumnsDownThroughProject(currentColumns, projectRelation);
                    current = projectRelation.Input;
                }
                else if (current is MergeJoinRelation mergeJoinRelation)
                {
                    var keyColumns = GetJoinPartitionKeyColumns(mergeJoinRelation);
                    if (keyColumns == null)
                    {
                        return false;
                    }
                    foreach (var keyColumn in keyColumns)
                    {
                        if (!currentColumns.Contains(keyColumn))
                        {
                            return false;
                        }
                    }
                    return true;
                }
                else
                {
                    return false;
                }
            }
            return false;
        }

        /// <summary>
        /// Maps column indexes from the output space of a relation down to its pre emit space.
        /// </summary>
        private static List<int>? MapColumnsDownThroughEmit(List<int> columns, List<int>? emit)
        {
            if (emit == null)
            {
                return columns;
            }
            var result = new List<int>(columns.Count);
            foreach (var column in columns)
            {
                if (column < 0 || column >= emit.Count)
                {
                    return null;
                }
                result.Add(emit[column]);
            }
            return result;
        }

        /// <summary>
        /// Maps column indexes from the output space of a project down to its input space.
        /// Computed expressions are followed when they are plain direct field references.
        /// </summary>
        private static List<int>? MapColumnsDownThroughProject(List<int> columns, ProjectRelation projectRelation)
        {
            var preEmitColumns = MapColumnsDownThroughEmit(columns, projectRelation.Emit);
            if (preEmitColumns == null)
            {
                return null;
            }
            var inputLength = projectRelation.Input.OutputLength;
            var result = new List<int>(preEmitColumns.Count);
            foreach (var column in preEmitColumns)
            {
                if (column < inputLength)
                {
                    result.Add(column);
                    continue;
                }
                var expressionIndex = column - inputLength;
                if (expressionIndex < projectRelation.Expressions.Count &&
                    projectRelation.Expressions[expressionIndex] is DirectFieldReference directField &&
                    directField.ReferenceSegment is StructReferenceSegment structSegment &&
                    structSegment.Child == null)
                {
                    result.Add(structSegment.Field);
                    continue;
                }
                return null;
            }
            return result;
        }
    }
}

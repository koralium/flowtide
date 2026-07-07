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

namespace FlowtideDotNet.Core.Optimizer.DistributedMode
{
    /// <summary>
    /// Pushes operators above a distributed lane union down into each partition lane, so a pipeline
    /// stays in one lane instead of gathering across substreams first. For example join -&gt; filter
    /// -&gt; project becomes union(project(filter(join_0)), project(filter(join_1)), ...), running the
    /// filter and project per lane and shuffling less data. Filters and projects are always safe (row
    /// local); aggregates and window functions only when their grouping/partition columns cover the
    /// lane partition keys, so every group stays in one lane.
    /// </summary>
    internal sealed class LanePushdownVisitor : OptimizerBaseVisitor
    {
        private readonly Dictionary<SetRelation, DistributedLaneUnion> _laneUnions;

        public LanePushdownVisitor(Dictionary<SetRelation, DistributedLaneUnion> laneUnions)
        {
            _laneUnions = laneUnions;
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            filterRelation.Input = Visit(filterRelation.Input, state);
            if (filterRelation.Input is SetRelation union && _laneUnions.TryGetValue(union, out var lane))
            {
                PushIntoLanes(lane, input => new FilterRelation()
                {
                    Condition = filterRelation.Condition,
                    Input = input,
                    Emit = filterRelation.Emit,
                    Hint = filterRelation.Hint
                });
                // A filter does not move columns except through its emit
                lane.PartitionKeyColumns = MapColumnsThroughEmit(lane.PartitionKeyColumns, filterRelation.Emit);
                return union;
            }
            return filterRelation;
        }

        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            projectRelation.Input = Visit(projectRelation.Input, state);
            if (projectRelation.Input is SetRelation union && _laneUnions.TryGetValue(union, out var lane))
            {
                PushIntoLanes(lane, input => new ProjectRelation()
                {
                    Expressions = projectRelation.Expressions,
                    Input = input,
                    Emit = projectRelation.Emit,
                    Hint = projectRelation.Hint
                });
                // Project appends its expressions after the input columns, input columns keep
                // their indexes in the pre emit space so they can be mapped through the emit
                lane.PartitionKeyColumns = MapColumnsThroughEmit(lane.PartitionKeyColumns, projectRelation.Emit);
                return union;
            }
            return projectRelation;
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            aggregateRelation.Input = Visit(aggregateRelation.Input, state);
            if (aggregateRelation.Input is SetRelation union && _laneUnions.TryGetValue(union, out var lane) &&
                CanPushAggregate(aggregateRelation, lane))
            {
                PushIntoLanes(lane, input => new AggregateRelation()
                {
                    Input = input,
                    Groupings = aggregateRelation.Groupings,
                    Measures = aggregateRelation.Measures,
                    Emit = aggregateRelation.Emit,
                    Hint = aggregateRelation.Hint
                });
                // Tracking the key columns through the aggregate output is not implemented,
                // further stateful operators above are not pushed.
                lane.PartitionKeyColumns = null;
                return union;
            }
            return aggregateRelation;
        }

        public override Relation VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, object state)
        {
            consistentPartitionWindowRelation.Input = Visit(consistentPartitionWindowRelation.Input, state);
            if (consistentPartitionWindowRelation.Input is SetRelation union && _laneUnions.TryGetValue(union, out var lane) &&
                CanPushWindow(consistentPartitionWindowRelation, lane))
            {
                PushIntoLanes(lane, input => new ConsistentPartitionWindowRelation()
                {
                    Input = input,
                    PartitionBy = consistentPartitionWindowRelation.PartitionBy,
                    OrderBy = consistentPartitionWindowRelation.OrderBy,
                    WindowFunctions = consistentPartitionWindowRelation.WindowFunctions,
                    Emit = consistentPartitionWindowRelation.Emit,
                    Hint = consistentPartitionWindowRelation.Hint
                });
                lane.PartitionKeyColumns = null;
                return union;
            }
            return consistentPartitionWindowRelation;
        }

        private static bool CanPushAggregate(AggregateRelation aggregateRelation, DistributedLaneUnion lane)
        {
            if (lane.PartitionKeyColumns == null)
            {
                return false;
            }
            if (aggregateRelation.Groupings == null || aggregateRelation.Groupings.Count != 1)
            {
                return false;
            }
            if (aggregateRelation.Measures != null &&
                aggregateRelation.Measures.Any(x => x.Measure.ExtensionUri == FunctionsAggregateGeneric.Uri &&
                    x.Measure.ExtensionName == FunctionsAggregateGeneric.SurrogateKeyInt64))
            {
                return false;
            }
            var groupingColumns = GetDirectFieldColumns(aggregateRelation.Groupings[0].GroupingExpressions);
            return CoversPartitionKeys(groupingColumns, lane.PartitionKeyColumns);
        }

        private static bool CanPushWindow(ConsistentPartitionWindowRelation windowRelation, DistributedLaneUnion lane)
        {
            if (lane.PartitionKeyColumns == null)
            {
                return false;
            }
            if (windowRelation.WindowFunctions.Any(x => x.ExtensionUri == FunctionsAggregateGeneric.Uri &&
                x.ExtensionName == FunctionsAggregateGeneric.SurrogateKeyInt64))
            {
                return false;
            }
            var partitionColumns = GetDirectFieldColumns(windowRelation.PartitionBy);
            return CoversPartitionKeys(partitionColumns, lane.PartitionKeyColumns);
        }

        /// <summary>
        /// True when the operators grouping columns contain all partition key columns of the lanes,
        /// every group is then fully contained within one lane.
        /// </summary>
        private static bool CoversPartitionKeys(List<int>? groupingColumns, List<int> partitionKeyColumns)
        {
            if (groupingColumns == null)
            {
                return false;
            }
            foreach (var keyColumn in partitionKeyColumns)
            {
                if (!groupingColumns.Contains(keyColumn))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Gets the column indexes of direct field references, returns null if any item is not
        /// a plain direct field reference.
        /// </summary>
        internal static List<int>? GetDirectFieldColumns<T>(IReadOnlyList<T> items)
        {
            var result = new List<int>(items.Count);
            foreach (var item in items)
            {
                if (item is DirectFieldReference directField &&
                    directField.ReferenceSegment is StructReferenceSegment structSegment &&
                    structSegment.Child == null)
                {
                    result.Add(structSegment.Field);
                }
                else
                {
                    return null;
                }
            }
            return result;
        }

        /// <summary>
        /// Maps column indexes from the pre emit space to the output space of a relation.
        /// Returns null when a column is not part of the output.
        /// </summary>
        internal static List<int>? MapColumnsThroughEmit(List<int>? columns, List<int>? emit)
        {
            if (columns == null)
            {
                return null;
            }
            if (emit == null)
            {
                return columns;
            }
            var result = new List<int>(columns.Count);
            foreach (var column in columns)
            {
                var outputIndex = emit.IndexOf(column);
                if (outputIndex < 0)
                {
                    return null;
                }
                result.Add(outputIndex);
            }
            return result;
        }

        /// <summary>
        /// Creates a copy of an operator on top of every lane. Lanes in other substreams get
        /// the copy inside their gather exchange so the operator runs in that substream, the
        /// local lane gets the copy directly.
        /// </summary>
        private static void PushIntoLanes(DistributedLaneUnion lane, Func<Relation, Relation> createCopy)
        {
            for (int i = 0; i < lane.Union.Inputs.Count; i++)
            {
                var laneInput = lane.Inputs[i];
                if (laneInput.GatherExchange != null && laneInput.Reference != null)
                {
                    laneInput.GatherExchange.Input = createCopy(laneInput.GatherExchange.Input);
                    laneInput.Reference.ReferenceOutputLength = laneInput.GatherExchange.Input.OutputLength;
                }
                else
                {
                    lane.Union.Inputs[i] = createCopy(lane.Union.Inputs[i]);
                }
            }
        }
    }
}

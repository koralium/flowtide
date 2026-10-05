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

using FlowtideDotNet.Core.Compute.Columnar.Functions.TableFunctions;
using FlowtideDotNet.Core.Optimizer.EmitPushdown;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.WindowJoin
{
    internal static class TumblingWindowJoinKeyOptimizer
    {
        public static Plan Optimize(Plan plan)
        {
            var visitor = new TumblingWindowJoinKeyVisitor(plan);
            for (int i = 0; i < plan.Relations.Count; i++)
            {
                plan.Relations[i] = visitor.Visit(plan.Relations[i], null!);
            }
            return plan;
        }
    }

    internal class TumblingWindowJoinKeyVisitor : OptimizerBaseVisitor
    {
        private readonly Plan _plan;

        public TumblingWindowJoinKeyVisitor(Plan plan)
        {
            _plan = plan;
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
        {
            mergeJoinRelation.Left = Visit(mergeJoinRelation.Left, state);
            mergeJoinRelation.Right = Visit(mergeJoinRelation.Right, state);
            TryRewrite(mergeJoinRelation);
            return mergeJoinRelation;
        }

        private void TryRewrite(MergeJoinRelation join)
        {
            if (join.Type == JoinType.LeftMark || join.ComparisonTypes == null)
            {
                return;
            }
            int leftLength = join.Left.OutputLength;
            int rightLength = join.Right.OutputLength;

            // Find exactly one range key, every other key must be an equality.
            // Only >= and < can form a half open tumbling window band.
            int rangeIndex = -1;
            for (int i = 0; i < join.LeftKeys.Count; i++)
            {
                var comparison = join.GetComparisonType(i);
                if (comparison == JoinComparisonType.Equal)
                {
                    continue;
                }
                if (comparison == JoinComparisonType.GreaterThanOrEqual || comparison == JoinComparisonType.LessThan)
                {
                    if (rangeIndex >= 0)
                    {
                        return;
                    }
                    rangeIndex = i;
                    continue;
                }
                return;
            }
            if (rangeIndex < 0)
            {
                return;
            }

            if (!TryGetField(join.LeftKeys[rangeIndex], out var timeField) ||
                !TryGetField(join.RightKeys[rangeIndex], out var rangeRightField))
            {
                return;
            }
            if (timeField >= leftLength || rangeRightField < leftLength)
            {
                return;
            }

            // The merge join finder extracts one range condition into the keys,
            // the other half of the band stays in the post join filter
            var rangeComparison = join.GetComparisonType(rangeIndex);
            if (!TryFindBandComplement(join.PostJoinFilter, timeField, leftLength, rangeComparison, out var complementRightField))
            {
                return;
            }

            int windowStartField;
            int windowEndField;
            if (rangeComparison == JoinComparisonType.GreaterThanOrEqual)
            {
                windowStartField = rangeRightField;
                windowEndField = complementRightField;
            }
            else
            {
                windowStartField = complementRightField;
                windowEndField = rangeRightField;
            }

            // Both columns must come untouched from the same tumbling window
            // application, that guarantees window_end = window_start + size which
            // makes the band equivalent to a bucket equality
            var startTrace = TraceToWindowOutput(join.Right, windowStartField - leftLength);
            var endTrace = TraceToWindowOutput(join.Right, windowEndField - leftLength);
            if (startTrace.Relation == null || endTrace.Relation == null)
            {
                return;
            }
            if (!ReferenceEquals(startTrace.Relation, endTrace.Relation))
            {
                return;
            }
            if (startTrace.FunctionColumn != 0 || endTrace.FunctionColumn != 1)
            {
                return;
            }

            var tableFunction = startTrace.Relation.TableFunction;
            if (tableFunction.ExtensionUri != FunctionsDatetime.Uri ||
                tableFunction.ExtensionName != FunctionsDatetime.TumblingWindow ||
                tableFunction.Arguments.Count != 3)
            {
                return;
            }
            long sizeTicks;
            try
            {
                sizeTicks = WindowTickResolver.ResolveTicks(tableFunction.Arguments[1], tableFunction.Arguments[2], "tumbling_window", "size");
            }
            catch (ArgumentException)
            {
                return;
            }

            // Compute the bucket as an extra column on the left input
            var bucketFunction = new ScalarFunction
            {
                ExtensionUri = FunctionsDatetime.Uri,
                ExtensionName = FunctionsDatetime.TumblingWindowStart,
                Arguments = new List<Expression>
                {
                    new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = timeField } },
                    new NumericLiteral() { Value = sizeTicks }
                }
            };
            join.Left = new ProjectRelation()
            {
                Input = join.Left,
                Expressions = new List<Expression>() { bucketFunction }
            };

            // The added bucket column shifts every right side field by one
            var shiftMap = new Dictionary<int, int>();
            for (int i = 0; i < leftLength; i++)
            {
                shiftMap.Add(i, i);
            }
            for (int i = 0; i < rightLength; i++)
            {
                shiftMap.Add(leftLength + i, leftLength + 1 + i);
            }
            var shiftVisitor = new ExpressionFieldReplaceVisitor(shiftMap);
            foreach (var rightKey in join.RightKeys)
            {
                shiftVisitor.Visit(rightKey, default!);
            }
            if (join.PostJoinFilter != null)
            {
                shiftVisitor.Visit(join.PostJoinFilter, default!);
            }
            // Left keys reference fields below leftLength and stay unchanged

            if (join.EmitSet)
            {
                for (int i = 0; i < join.Emit.Count; i++)
                {
                    if (join.Emit[i] >= leftLength)
                    {
                        join.Emit[i] += 1;
                    }
                }
            }
            else
            {
                // Keep the output identical to before, hide the bucket column
                var emit = new List<int>(leftLength + rightLength);
                for (int i = 0; i < leftLength; i++)
                {
                    emit.Add(i);
                }
                for (int i = 0; i < rightLength; i++)
                {
                    emit.Add(leftLength + 1 + i);
                }
                join.Emit = emit;
            }

            // Replace the range key with the bucket equality. The dropped range
            // condition is implied, bucket = window_start bounds t on both sides.
            // The band half in the post join filter stays, it is redundant but harmless.
            join.LeftKeys[rangeIndex] = new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = leftLength } };
            join.RightKeys[rangeIndex] = new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = windowStartField + 1 } };
            join.ComparisonTypes[rangeIndex] = JoinComparisonType.Equal;

            // Key order is the sort order of the join state trees, the window
            // bucket must lead for inserts to land in the current window range
            if (rangeIndex != 0)
            {
                MoveToFront(join.LeftKeys, rangeIndex);
                MoveToFront(join.RightKeys, rangeIndex);
                MoveToFront(join.ComparisonTypes, rangeIndex);
            }
        }

        private static void MoveToFront<T>(List<T> list, int index)
        {
            var item = list[index];
            list.RemoveAt(index);
            list.Insert(0, item);
        }

        private static bool TryGetField(Expression expression, out int field)
        {
            if (expression is DirectFieldReference direct &&
                direct.ReferenceSegment is StructReferenceSegment segment &&
                segment.Child == null)
            {
                field = segment.Field;
                return true;
            }
            field = 0;
            return false;
        }

        // Searches the post join filter for the missing half of the band. For a
        // t >= window_start key the complement is t < window_end and mirrored.
        // Post join filter fields are in the combined left plus right space.
        private static bool TryFindBandComplement(Expression? filter, int timeField, int leftLength, JoinComparisonType rangeComparison, out int rightField)
        {
            rightField = -1;
            if (filter is not ScalarFunction scalar)
            {
                return false;
            }
            if (scalar.ExtensionUri == FunctionsBoolean.Uri && scalar.ExtensionName == FunctionsBoolean.And)
            {
                foreach (var argument in scalar.Arguments)
                {
                    if (TryFindBandComplement(argument, timeField, leftLength, rangeComparison, out rightField))
                    {
                        return true;
                    }
                }
                return false;
            }
            if (scalar.ExtensionUri != FunctionsComparison.Uri || scalar.Arguments.Count != 2)
            {
                return false;
            }
            if (!TryGetField(scalar.Arguments[0], out var first) || !TryGetField(scalar.Arguments[1], out var second))
            {
                return false;
            }

            // Wanted: t < we when the key holds t >= ws, t >= ws when the key holds t < we
            string wantedTimeFirst;
            string wantedTimeSecond;
            if (rangeComparison == JoinComparisonType.GreaterThanOrEqual)
            {
                wantedTimeFirst = FunctionsComparison.LessThan;
                wantedTimeSecond = FunctionsComparison.GreaterThan;
            }
            else
            {
                wantedTimeFirst = FunctionsComparison.GreaterThanOrEqual;
                wantedTimeSecond = FunctionsComparison.LessThanOrEqual;
            }
            if (scalar.ExtensionName == wantedTimeFirst && first == timeField && second >= leftLength)
            {
                rightField = second;
                return true;
            }
            if (scalar.ExtensionName == wantedTimeSecond && second == timeField && first >= leftLength)
            {
                rightField = first;
                return true;
            }
            return false;
        }

        /// <summary>
        /// Follows a right side column down through emit mappings, filters,
        /// projections, grouping keys and plan references until it either reaches
        /// a table function output column untouched or fails.
        /// </summary>
        private (TableFunctionRelation? Relation, int FunctionColumn) TraceToWindowOutput(Relation relation, int index)
        {
            while (true)
            {
                if (index < 0)
                {
                    return (null, 0);
                }
                if (relation.EmitSet)
                {
                    if (index >= relation.Emit.Count)
                    {
                        return (null, 0);
                    }
                    index = relation.Emit[index];
                }
                switch (relation)
                {
                    case FilterRelation filter:
                        relation = filter.Input;
                        continue;
                    case ProjectRelation project:
                        if (index < project.Input.OutputLength)
                        {
                            relation = project.Input;
                            continue;
                        }
                        var expressionIndex = index - project.Input.OutputLength;
                        if (expressionIndex >= project.Expressions.Count)
                        {
                            return (null, 0);
                        }
                        if (!TryGetField(project.Expressions[expressionIndex], out var projectedField))
                        {
                            return (null, 0);
                        }
                        index = projectedField;
                        relation = project.Input;
                        continue;
                    case AggregateRelation aggregate:
                        if (aggregate.Groupings == null || aggregate.Groupings.Count != 1)
                        {
                            return (null, 0);
                        }
                        var groupingExpressions = aggregate.Groupings[0].GroupingExpressions;
                        if (index >= groupingExpressions.Count)
                        {
                            // Measures do not preserve the input values
                            return (null, 0);
                        }
                        if (!TryGetField(groupingExpressions[index], out var groupedField))
                        {
                            return (null, 0);
                        }
                        index = groupedField;
                        relation = aggregate.Input;
                        continue;
                    case TableFunctionRelation tableFunctionRelation:
                        var inputLength = tableFunctionRelation.Input?.OutputLength ?? 0;
                        if (index >= inputLength)
                        {
                            return (tableFunctionRelation, index - inputLength);
                        }
                        return (null, 0);
                    case ReferenceRelation reference:
                        if (reference.RelationId < 0 || reference.RelationId >= _plan.Relations.Count)
                        {
                            return (null, 0);
                        }
                        relation = _plan.Relations[reference.RelationId];
                        continue;
                    default:
                        return (null, 0);
                }
            }
        }
    }
}

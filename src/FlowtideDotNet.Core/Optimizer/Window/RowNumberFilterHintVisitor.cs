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

using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.Window
{
    /// <summary>
    /// Bounds row_number from a filter like WHERE ROW_NUMBER() OVER (...) = 1, so the operator stops at
    /// the bound and skips emitting dropped rows. The filter is kept to carry the semantics.
    /// </summary>
    internal class RowNumberFilterHintVisitor : OptimizerBaseVisitor
    {
        public override Relation VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, object state)
        {
            // Clear stale hints, the current plan shape decides the bound.
            foreach (var windowFunction in consistentPartitionWindowRelation.WindowFunctions)
            {
                windowFunction.Options?.Remove(BulkWindowFunctionOptions.MaxRowNumber);
                windowFunction.Options?.Remove(BulkWindowFunctionOptions.EmitOnlyWithinMaxRowNumber);
            }
            return base.VisitConsistentPartitionWindowRelation(consistentPartitionWindowRelation, state);
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            filterRelation.Input = Visit(filterRelation.Input, state);
            if (filterRelation.Input is ConsistentPartitionWindowRelation windowRelation)
            {
                foreach (var condition in FlattenAnd(filterRelation.Condition))
                {
                    if (TryGetUpperBound(condition, out var fieldIndex, out var bound))
                    {
                        ApplyBound(windowRelation, fieldIndex, bound);
                    }
                }
            }
            return filterRelation;
        }

        private static IEnumerable<Expression> FlattenAnd(Expression condition)
        {
            if (condition is ScalarFunction scalarFunction &&
                scalarFunction.ExtensionUri == FunctionsBoolean.Uri &&
                string.Equals(scalarFunction.ExtensionName, FunctionsBoolean.And, StringComparison.OrdinalIgnoreCase))
            {
                foreach (var argument in scalarFunction.Arguments)
                {
                    foreach (var inner in FlattenAnd(argument))
                    {
                        yield return inner;
                    }
                }
                yield break;
            }
            yield return condition;
        }

        /// <summary>
        /// Matches conditions of the shape field = c, field &lt;= c and field &lt; c (and their mirrored
        /// forms) against a numeric literal, producing the inclusive upper bound they imply.
        /// </summary>
        private static bool TryGetUpperBound(Expression condition, out int fieldIndex, out long bound)
        {
            fieldIndex = -1;
            bound = 0;
            if (condition is not ScalarFunction scalarFunction ||
                scalarFunction.ExtensionUri != FunctionsComparison.Uri ||
                scalarFunction.Arguments.Count != 2)
            {
                return false;
            }

            bool fieldFirst;
            int index;
            decimal literal;
            if (TryGetField(scalarFunction.Arguments[0], out index) && TryGetNumericLiteral(scalarFunction.Arguments[1], out literal))
            {
                fieldFirst = true;
            }
            else if (TryGetField(scalarFunction.Arguments[1], out index) && TryGetNumericLiteral(scalarFunction.Arguments[0], out literal))
            {
                fieldFirst = false;
            }
            else
            {
                return false;
            }

            if (literal != decimal.Truncate(literal) || literal > long.MaxValue || literal < 1)
            {
                return false;
            }
            var literalValue = (long)literal;

            var name = scalarFunction.ExtensionName;
            long? upperBound = default;
            if (string.Equals(name, FunctionsComparison.Equal, StringComparison.OrdinalIgnoreCase))
            {
                upperBound = literalValue;
            }
            else if (string.Equals(name, FunctionsComparison.LessThanOrEqual, StringComparison.OrdinalIgnoreCase))
            {
                upperBound = fieldFirst ? literalValue : default(long?);
            }
            else if (string.Equals(name, FunctionsComparison.LessThan, StringComparison.OrdinalIgnoreCase))
            {
                upperBound = fieldFirst ? literalValue - 1 : default(long?);
            }
            else if (string.Equals(name, FunctionsComparison.GreaterThanOrEqual, StringComparison.OrdinalIgnoreCase))
            {
                upperBound = fieldFirst ? default(long?) : literalValue;
            }
            else if (string.Equals(name, FunctionsComparison.GreaterThan, StringComparison.OrdinalIgnoreCase))
            {
                upperBound = fieldFirst ? default(long?) : literalValue - 1;
            }

            if (upperBound == null || upperBound < 1)
            {
                return false;
            }
            fieldIndex = index;
            bound = upperBound.Value;
            return true;
        }

        private static bool TryGetField(Expression expression, out int fieldIndex)
        {
            if (expression is DirectFieldReference directFieldReference &&
                directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment &&
                structReferenceSegment.Child == null)
            {
                fieldIndex = structReferenceSegment.Field;
                return true;
            }
            fieldIndex = -1;
            return false;
        }

        private static bool TryGetNumericLiteral(Expression expression, out decimal value)
        {
            if (expression is NumericLiteral numericLiteral)
            {
                value = numericLiteral.Value;
                return true;
            }
            value = 0;
            return false;
        }

        private static void ApplyBound(ConsistentPartitionWindowRelation windowRelation, int outputFieldIndex, long bound)
        {
            var underlyingIndex = outputFieldIndex;
            if (windowRelation.EmitSet)
            {
                if (outputFieldIndex < 0 || outputFieldIndex >= windowRelation.Emit.Count)
                {
                    return;
                }
                underlyingIndex = windowRelation.Emit[outputFieldIndex];
            }

            var functionIndex = underlyingIndex - windowRelation.Input.OutputLength;
            if (functionIndex < 0 || functionIndex >= windowRelation.WindowFunctions.Count)
            {
                return;
            }

            var windowFunction = windowRelation.WindowFunctions[functionIndex];
            if (!string.Equals(windowFunction.ExtensionName, FunctionsArithmetic.RowNumber, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            windowFunction.Options ??= new SortedList<string, string>();
            // Rows beyond the bound get a null row number, which no bound comparison matches, so they can
            // be dropped at emission instead of being sent through the filter.
            windowFunction.Options[BulkWindowFunctionOptions.EmitOnlyWithinMaxRowNumber] = "true";
            if (windowFunction.Options.TryGetValue(BulkWindowFunctionOptions.MaxRowNumber, out var existing) &&
                long.TryParse(existing, out var existingBound) &&
                existingBound <= bound)
            {
                return;
            }
            windowFunction.Options[BulkWindowFunctionOptions.MaxRowNumber] = bound.ToString();
        }
    }
}

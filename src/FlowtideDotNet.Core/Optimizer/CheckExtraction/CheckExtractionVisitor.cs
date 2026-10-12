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

using FlowtideDotNet.Core.Optimizer.EmitPushdown;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.CheckExtraction
{
    /// <summary>
    /// Moves check functions into a check relation on the input they are evaluated against.
    /// </summary>
    internal sealed class CheckExtractionVisitor : OptimizerBaseVisitor
    {
        public override Relation VisitProjectRelation(ProjectRelation projectRelation, object state)
        {
            projectRelation.Input = Visit(projectRelation.Input, state);
            var rewriter = new CheckExpressionRewriter();
            for (int i = 0; i < projectRelation.Expressions.Count; i++)
            {
                projectRelation.Expressions[i] = rewriter.Rewrite(projectRelation.Expressions[i]);
            }
            projectRelation.Input = WithChecks(projectRelation.Input, rewriter.Checks);
            return projectRelation;
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            if (!CheckFunctionMatcher.ContainsCheck(filterRelation.Condition))
            {
                filterRelation.Input = Visit(filterRelation.Input, state);
                return filterRelation;
            }
            // Subqueries become mark joins the checks can read
            filterRelation = SubqueryDecorrelationVisitor.DecorrelateCondition(filterRelation);
            filterRelation.Input = Visit(filterRelation.Input, state);
            var split = SplitCondition(filterRelation.Condition);
            Relation input = filterRelation.Input;
            if (split.CheckFree != null)
            {
                input = new FilterRelation()
                {
                    Condition = split.CheckFree,
                    Input = input
                };
            }
            filterRelation.Input = new CheckRelation()
            {
                Input = input,
                Checks = split.Checks
            };
            filterRelation.Condition = split.Rewritten;
            return filterRelation;
        }

        public override Relation VisitReadRelation(ReadRelation readRelation, object state)
        {
            if (!CheckFunctionMatcher.ContainsCheck(readRelation.Filter))
            {
                return readRelation;
            }
            var split = SplitCondition(readRelation.Filter!);

            // Base schema indexing: the emit moves above the checks
            var emit = readRelation.Emit;
            readRelation.Filter = split.CheckFree;
            readRelation.Emit = null;
            return new FilterRelation()
            {
                Condition = split.Rewritten,
                Emit = emit,
                Input = new CheckRelation()
                {
                    Input = readRelation,
                    Checks = split.Checks
                }
            };
        }

        public override Relation VisitNormalizationRelation(NormalizationRelation normalizationRelation, object state)
        {
            normalizationRelation.Input = Visit(normalizationRelation.Input, state);
            if (!CheckFunctionMatcher.ContainsCheck(normalizationRelation.Filter))
            {
                return normalizationRelation;
            }
            var split = SplitCondition(normalizationRelation.Filter!);

            // Input row indexing: the emit moves above the checks
            var emit = normalizationRelation.Emit;
            normalizationRelation.Filter = split.CheckFree;
            normalizationRelation.Emit = null;
            return new FilterRelation()
            {
                Condition = split.Rewritten,
                Emit = emit,
                Input = new CheckRelation()
                {
                    Input = normalizationRelation,
                    Checks = split.Checks
                }
            };
        }

        public override Relation VisitAggregateRelation(AggregateRelation aggregateRelation, object state)
        {
            aggregateRelation.Input = Visit(aggregateRelation.Input, state);
            var rewriter = new CheckExpressionRewriter();
            if (aggregateRelation.Groupings != null)
            {
                foreach (var grouping in aggregateRelation.Groupings)
                {
                    for (int i = 0; i < grouping.GroupingExpressions.Count; i++)
                    {
                        grouping.GroupingExpressions[i] = rewriter.Rewrite(grouping.GroupingExpressions[i]);
                    }
                }
            }
            if (aggregateRelation.Measures != null)
            {
                foreach (var measure in aggregateRelation.Measures)
                {
                    CheckGuardNode? guards = null;
                    if (measure.Filter != null)
                    {
                        measure.Filter = rewriter.Rewrite(measure.Filter);
                        // Measure arguments only run on filtered rows
                        guards = new CheckGuardNode(measure.Filter, CheckGuardKind.IsTrue, null);
                    }
                    var arguments = measure.Measure.Arguments;
                    for (int i = 0; i < arguments.Count; i++)
                    {
                        arguments[i] = rewriter.Rewrite(arguments[i], guards);
                    }
                }
            }
            aggregateRelation.Input = WithChecks(aggregateRelation.Input, rewriter.Checks);
            return aggregateRelation;
        }

        public override Relation VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, object state)
        {
            consistentPartitionWindowRelation.Input = Visit(consistentPartitionWindowRelation.Input, state);
            var rewriter = new CheckExpressionRewriter();
            var partitionBy = consistentPartitionWindowRelation.PartitionBy;
            for (int i = 0; i < partitionBy.Count; i++)
            {
                partitionBy[i] = rewriter.Rewrite(partitionBy[i]);
            }
            foreach (var orderBy in consistentPartitionWindowRelation.OrderBy)
            {
                orderBy.Expression = rewriter.Rewrite(orderBy.Expression);
            }
            foreach (var windowFunction in consistentPartitionWindowRelation.WindowFunctions)
            {
                if (CheckFunctionMatcher.ContainsCheck(GetBoundExpression(windowFunction.LowerBound)) ||
                    CheckFunctionMatcher.ContainsCheck(GetBoundExpression(windowFunction.UpperBound)))
                {
                    throw Unsupported("a window frame bound");
                }
                // The default only runs past the partition edge
                if (IsLeadOrLag(windowFunction) && windowFunction.Arguments.Count > 2 && CheckFunctionMatcher.ContainsCheck(windowFunction.Arguments[2]))
                {
                    throw Unsupported("a LEAD or LAG default argument");
                }
                var arguments = windowFunction.Arguments;
                for (int i = 0; i < arguments.Count; i++)
                {
                    arguments[i] = rewriter.Rewrite(arguments[i]);
                }
            }
            consistentPartitionWindowRelation.Input = WithChecks(consistentPartitionWindowRelation.Input, rewriter.Checks);
            return consistentPartitionWindowRelation;
        }

        public override Relation VisitSortRelation(SortRelation sortRelation, object state)
        {
            sortRelation.Input = Visit(sortRelation.Input, state);
            var rewriter = new CheckExpressionRewriter();
            foreach (var sort in sortRelation.Sorts)
            {
                sort.Expression = rewriter.Rewrite(sort.Expression);
            }
            sortRelation.Input = WithChecks(sortRelation.Input, rewriter.Checks);
            return sortRelation;
        }

        public override Relation VisitTopNRelation(TopNRelation topNRelation, object state)
        {
            topNRelation.Input = Visit(topNRelation.Input, state);
            var rewriter = new CheckExpressionRewriter();
            foreach (var sort in topNRelation.Sorts)
            {
                sort.Expression = rewriter.Rewrite(sort.Expression);
            }
            topNRelation.Input = WithChecks(topNRelation.Input, rewriter.Checks);
            return topNRelation;
        }

        public override Relation VisitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, object state)
        {
            var functionArguments = tableFunctionRelation.TableFunction.Arguments;
            if (tableFunctionRelation.Input == null)
            {
                foreach (var argument in functionArguments)
                {
                    if (CheckFunctionMatcher.ContainsCheck(argument))
                    {
                        throw Unsupported("a table function without an input");
                    }
                }
                if (CheckFunctionMatcher.ContainsCheck(tableFunctionRelation.JoinCondition))
                {
                    throw Unsupported("a table function without an input");
                }
                return tableFunctionRelation;
            }

            tableFunctionRelation.Input = Visit(tableFunctionRelation.Input, state);
            var rewriter = new CheckExpressionRewriter();
            for (int i = 0; i < functionArguments.Count; i++)
            {
                functionArguments[i] = rewriter.Rewrite(functionArguments[i]);
            }
            var inputChecks = new List<CheckDefinition>(rewriter.Checks);
            tableFunctionRelation.JoinCondition = ExtractTwoInputChecks(
                tableFunctionRelation.JoinCondition,
                tableFunctionRelation.Input.OutputLength,
                inputChecks,
                null,
                "a table function join condition");
            tableFunctionRelation.Input = WithChecks(tableFunctionRelation.Input, inputChecks);
            return tableFunctionRelation;
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            joinRelation.Left = Visit(joinRelation.Left, state);
            joinRelation.Right = Visit(joinRelation.Right, state);
            var leftChecks = new List<CheckDefinition>();
            var rightChecks = new List<CheckDefinition>();
            var leftSize = joinRelation.Left.OutputLength;
            joinRelation.Expression = ExtractTwoInputChecks(joinRelation.Expression, leftSize, leftChecks, rightChecks, "a join condition");
            joinRelation.PostJoinFilter = ExtractTwoInputChecks(joinRelation.PostJoinFilter, leftSize, leftChecks, rightChecks, "a join post filter");
            joinRelation.Left = WithChecks(joinRelation.Left, leftChecks);
            joinRelation.Right = WithChecks(joinRelation.Right, rightChecks);
            return joinRelation;
        }

        public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
        {
            mergeJoinRelation.Left = Visit(mergeJoinRelation.Left, state);
            mergeJoinRelation.Right = Visit(mergeJoinRelation.Right, state);
            var leftChecks = new List<CheckDefinition>();
            var rightChecks = new List<CheckDefinition>();
            mergeJoinRelation.PostJoinFilter = ExtractTwoInputChecks(
                mergeJoinRelation.PostJoinFilter,
                mergeJoinRelation.Left.OutputLength,
                leftChecks,
                rightChecks,
                "a join post filter");
            mergeJoinRelation.Left = WithChecks(mergeJoinRelation.Left, leftChecks);
            mergeJoinRelation.Right = WithChecks(mergeJoinRelation.Right, rightChecks);
            return mergeJoinRelation;
        }

        public override Relation VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, object state)
        {
            foreach (var row in virtualTableReadRelation.Values.Expressions)
            {
                foreach (var field in row.Fields)
                {
                    if (CheckFunctionMatcher.ContainsCheck(field))
                    {
                        throw Unsupported("VALUES or a SELECT without FROM");
                    }
                }
            }
            return virtualTableReadRelation;
        }

        public override Relation VisitIterationRelation(IterationRelation iterationRelation, object state)
        {
            if (CheckFunctionMatcher.ContainsCheck(iterationRelation.SkipIterateCondition))
            {
                throw Unsupported("an iteration skip condition");
            }
            return base.VisitIterationRelation(iterationRelation, state);
        }

        public override Relation VisitCheckRelation(CheckRelation checkRelation, object state)
        {
            foreach (var check in checkRelation.Checks)
            {
                if (CheckFunctionMatcher.ContainsCheck(check))
                {
                    throw Unsupported("the expressions of a check relation");
                }
            }
            return base.VisitCheckRelation(checkRelation, state);
        }

        private static Relation WithChecks(Relation input, List<CheckDefinition> checks)
        {
            if (checks.Count == 0)
            {
                return input;
            }
            return new CheckRelation()
            {
                Input = input,
                Checks = checks
            };
        }

        private static Expression? GetBoundExpression(WindowBound? bound)
        {
            return bound switch
            {
                PreceedingRangeWindowBound preceeding => preceeding.Expression,
                FollowingRangeWindowBound following => following.Expression,
                _ => null
            };
        }

        /// <summary>
        /// Rewrites a two input condition and sends each check to the single input it uses.
        /// </summary>
        private static Expression? ExtractTwoInputChecks(
            Expression? condition,
            int leftSize,
            List<CheckDefinition> leftChecks,
            List<CheckDefinition>? rightChecks,
            string context)
        {
            if (condition == null || !CheckFunctionMatcher.ContainsCheck(condition))
            {
                return condition;
            }
            var rewriter = new CheckExpressionRewriter();
            var rewritten = rewriter.Rewrite(condition);
            foreach (var check in rewriter.Checks)
            {
                var usage = new ExpressionFieldUsageVisitor(leftSize);
                foreach (var expression in CheckFunctionMatcher.GetExpressions(check))
                {
                    usage.Visit(expression, default);
                }
                if (!usage.CanOptimize)
                {
                    throw new NotSupportedException($"A check function in {context} uses a field reference that is not a plain column, so its input cannot be determined. Move the check into the SELECT list or WHERE clause.");
                }
                var usesLeft = usage.UsedFieldsLeft.Count > 0;
                var usesRight = usage.UsedFieldsRight.Count > 0;
                if (usesLeft && usesRight)
                {
                    throw new NotSupportedException($"A check function in {context} references both inputs, a check must only use the columns of one input. Move the check into the SELECT list or WHERE clause of one of the inputs.");
                }
                if (!usesRight)
                {
                    leftChecks.Add(check);
                    continue;
                }
                if (rightChecks == null)
                {
                    throw new NotSupportedException($"A check function in {context} references the table function output, only the input columns can be checked there. Move the check into the SELECT list or WHERE clause.");
                }
                var oldToNew = new Dictionary<int, int>();
                foreach (var field in usage.UsedFieldsRight)
                {
                    oldToNew[field] = field - leftSize;
                }
                var replaceVisitor = new ExpressionFieldReplaceVisitor(oldToNew);
                foreach (var expression in CheckFunctionMatcher.GetExpressions(check))
                {
                    replaceVisitor.Visit(expression, default);
                }
                rightChecks.Add(check);
            }
            return rewritten;
        }

        private sealed record ConditionSplit(Expression? CheckFree, Expression Rewritten, List<CheckDefinition> Checks);

        /// <summary>
        /// Splits a filter condition on its top level and into check free and rewritten conjuncts.
        /// </summary>
        private static ConditionSplit SplitCondition(Expression condition)
        {
            var rewriter = new CheckExpressionRewriter();
            if (!IsAnd(condition))
            {
                return new ConditionSplit(null, rewriter.Rewrite(condition), rewriter.Checks);
            }
            var conjuncts = new List<Expression>();
            FlattenAnd(condition, conjuncts);
            var checkFree = new List<Expression>();
            var rewritten = new List<Expression>();
            foreach (var conjunct in conjuncts)
            {
                if (CheckFunctionMatcher.ContainsCheck(conjunct))
                {
                    rewritten.Add(rewriter.Rewrite(conjunct));
                }
                else
                {
                    checkFree.Add(conjunct);
                }
            }
            // And calls keep and semantics for lone conjuncts
            return new ConditionSplit(checkFree.Count > 0 ? And(checkFree) : null, And(rewritten), rewriter.Checks);
        }

        private static bool IsAnd(Expression expression)
        {
            return expression is ScalarFunction scalarFunction &&
                CheckFunctionMatcher.IsFunction(scalarFunction, FunctionsBoolean.Uri, FunctionsBoolean.And);
        }

        private static void FlattenAnd(Expression expression, List<Expression> conjuncts)
        {
            if (expression is ScalarFunction scalarFunction && IsAnd(scalarFunction))
            {
                foreach (var argument in scalarFunction.Arguments)
                {
                    FlattenAnd(argument, conjuncts);
                }
                return;
            }
            conjuncts.Add(expression);
        }

        private static ScalarFunction And(List<Expression> conjuncts)
        {
            return new ScalarFunction()
            {
                ExtensionUri = FunctionsBoolean.Uri,
                ExtensionName = FunctionsBoolean.And,
                Arguments = conjuncts
            };
        }

        private static bool IsLeadOrLag(WindowFunction windowFunction)
        {
            // Case insensitive like the function lookup
            return string.Equals(windowFunction.ExtensionUri, FunctionsArithmetic.Uri, StringComparison.OrdinalIgnoreCase) &&
                (string.Equals(windowFunction.ExtensionName, FunctionsArithmetic.Lead, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(windowFunction.ExtensionName, FunctionsArithmetic.Lag, StringComparison.OrdinalIgnoreCase));
        }

        private static NotSupportedException Unsupported(string context)
        {
            return new NotSupportedException($"Check functions are not supported in {context}. Move the check into the SELECT list or WHERE clause of a query with a single input.");
        }
    }
}

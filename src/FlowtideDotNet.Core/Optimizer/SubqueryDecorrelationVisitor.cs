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
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FlowtideDotNet.Core.Optimizer
{
    internal class SubqueryDecorrelationVisitor : OptimizerBaseVisitor
    {
        public static Plan Optimize(Plan plan)
        {
            var visitor = new SubqueryDecorrelationVisitor();
            return visitor.VisitPlan(plan);
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            // First optimize the input
            filterRelation.Input = Visit(filterRelation.Input, state);

            // Extract subqueries from the filter condition
            var extractor = new SubqueryExtractor();
            extractor.Visit(filterRelation.Condition, null!);

            if (extractor.Subqueries.Count == 0)
            {
                return filterRelation;
            }

            var leftRel = filterRelation.Input;
            var subqueryToMarkColumnIndex = new Dictionary<SetPredicateExpression, int>(ReferenceEqualityComparer.Instance);
            foreach (var subqueryExpr in extractor.Subqueries)
            {
                // Decorrelate this subquery!
                // 1. Remove correlation filters from the subquery relation
                var filterRemover = new SubqueryFilterRemover();
                var rewrittenSubquery = filterRemover.Visit(subqueryExpr.Relation, null!);

                if (rewrittenSubquery is ProjectRelation projectRel)
                {
                    rewrittenSubquery = projectRel.Input;
                }

                // 2. Perform a check to verify that no remaining correlation exists outside the pulled filters
                var correlationChecker = new CorrelationCheckVisitor();
                correlationChecker.Visit(rewrittenSubquery, null!);

                // 3. Rewrite correlation conjuncts to local index offsets
                var leftLength = leftRel.OutputLength;
                var rewriter = new CorrelationConjunctRewriter(leftLength);
                var rewrittenConjuncts = new List<Expression>();
                foreach (var conj in filterRemover.ExtractedCorrelationConjuncts)
                {
                    rewrittenConjuncts.Add(rewriter.Visit(conj, null!));
                }

                var joinCondition = CombineConjuncts(rewrittenConjuncts);

                // 4. Build LeftMark join
                var joinRel = new JoinRelation()
                {
                    Left = leftRel,
                    Right = rewrittenSubquery,
                    Type = JoinType.LeftMark,
                    Expression = joinCondition
                };

                // The mark column is appended at the end of the left relation
                var markColumnIndex = leftLength;
                subqueryToMarkColumnIndex.Add(subqueryExpr, markColumnIndex);

                leftRel = joinRel;
            }

            // 5. Rewrite the filter condition to reference the mark columns
            var conditionRewriter = new SubqueryRewriter(subqueryToMarkColumnIndex);
            var newCondition = conditionRewriter.Visit(filterRelation.Condition, null!);

            // 6. Return the updated filter relation
            var originalLeftLength = filterRelation.Input.OutputLength;
            var newFilter = new FilterRelation()
            {
                Input = leftRel,
                Condition = newCondition
            };

            newFilter.Emit = filterRelation.EmitSet
                ? filterRelation.Emit.ToList()
                : Enumerable.Range(0, originalLeftLength).ToList();

            return newFilter;
        }

        private static void FlattenConjuncts(Expression expr, List<Expression> conjuncts)
        {
            if (expr is ScalarFunction func &&
                func.ExtensionUri == FunctionsBoolean.Uri &&
                func.ExtensionName == FunctionsBoolean.And)
            {
                foreach (var arg in func.Arguments)
                {
                    FlattenConjuncts(arg, conjuncts);
                }
            }
            else
            {
                conjuncts.Add(expr);
            }
        }

        private static Expression? CombineConjuncts(IReadOnlyList<Expression> conjuncts)
        {
            if (conjuncts.Count == 0) return null;
            if (conjuncts.Count == 1) return conjuncts[0];

            var current = conjuncts[0];
            for (int i = 1; i < conjuncts.Count; i++)
            {
                current = new ScalarFunction()
                {
                    ExtensionUri = FunctionsBoolean.Uri,
                    ExtensionName = FunctionsBoolean.And,
                    Arguments = new List<Expression>() { current, conjuncts[i] }
                };
            }
            return current;
        }

        private class SubqueryExtractor : ExpressionVisitor<object, object>
        {
            public List<SetPredicateExpression> Subqueries { get; } = new List<SetPredicateExpression>();

            public override object VisitSetPredicateExpression(SetPredicateExpression setPredicateExpression, object state)
            {
                Subqueries.Add(setPredicateExpression);
                return null!;
            }

            public override object VisitScalarFunction(ScalarFunction scalarFunction, object state)
            {
                if (scalarFunction.Arguments != null)
                {
                    foreach (var arg in scalarFunction.Arguments)
                    {
                        Visit(arg, state);
                    }
                }
                return null!;
            }

            public override object VisitIfThen(IfThenExpression ifThenExpression, object state)
            {
                if (ifThenExpression.Ifs != null)
                {
                    foreach (var ifClause in ifThenExpression.Ifs)
                    {
                        Visit(ifClause.If, state);
                        Visit(ifClause.Then, state);
                    }
                }
                if (ifThenExpression.Else != null)
                {
                    Visit(ifThenExpression.Else, state);
                }
                return null!;
            }

            public override object VisitCastExpression(CastExpression castExpression, object state)
            {
                Visit(castExpression.Expression, state);
                return null!;
            }
        }

        private class SubqueryFilterRemover : OptimizerBaseVisitor
        {
            private readonly List<Expression> extractedCorrelationConjuncts = new List<Expression>();
            private readonly ContainsOuterReferenceVisitor outerRefVisitor = new ContainsOuterReferenceVisitor();

            public IReadOnlyList<Expression> ExtractedCorrelationConjuncts => extractedCorrelationConjuncts;

            public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
            {
                filterRelation.Input = Visit(filterRelation.Input, state);

                var conjuncts = new List<Expression>();
                FlattenConjuncts(filterRelation.Condition, conjuncts);

                var localConjuncts = new List<Expression>();
                foreach (var conj in conjuncts)
                {
                    if (outerRefVisitor.Visit(conj, null!))
                    {
                        extractedCorrelationConjuncts.Add(conj);
                    }
                    else
                    {
                        localConjuncts.Add(conj);
                    }
                }

                if (localConjuncts.Count == 0)
                {
                    return filterRelation.Input;
                }
                else
                {
                    filterRelation.Condition = CombineConjuncts(localConjuncts)!;
                    return filterRelation;
                }
            }
        }

        private class ContainsOuterReferenceVisitor : ExpressionVisitor<bool, object>
        {
            public override bool VisitDirectFieldReference(DirectFieldReference directFieldReference, object state)
            {
                return directFieldReference.Root is OuterReference;
            }

            public override bool VisitScalarFunction(ScalarFunction scalarFunction, object state)
            {
                if (scalarFunction.Arguments != null)
                {
                    foreach (var arg in scalarFunction.Arguments)
                    {
                        if (Visit(arg, state)) return true;
                    }
                }
                return false;
            }

            public override bool VisitIfThen(IfThenExpression ifThenExpression, object state)
            {
                if (ifThenExpression.Ifs != null)
                {
                    foreach (var ifClause in ifThenExpression.Ifs)
                    {
                        if (Visit(ifClause.If, state) || Visit(ifClause.Then, state)) return true;
                    }
                }
                if (ifThenExpression.Else != null)
                {
                    if (Visit(ifThenExpression.Else, state)) return true;
                }
                return false;
            }

            public override bool VisitCastExpression(CastExpression castExpression, object state)
            {
                return Visit(castExpression.Expression, state);
            }
        }

        private class CorrelationCheckVisitor : RelationVisitor<object?, object?>
        {
            private readonly ContainsOuterReferenceVisitor outerRefVisitor = new ContainsOuterReferenceVisitor();

            public override object? VisitFilterRelation(FilterRelation filterRelation, object? state)
            {
                Visit(filterRelation.Input, state);
                return null;
            }

            public override object? VisitJoinRelation(JoinRelation joinRelation, object? state)
            {
                if (joinRelation.Expression != null && outerRefVisitor.Visit(joinRelation.Expression, null!))
                {
                    throw new NotSupportedException("Outer references inside subquery join conditions are not supported for decorrelation.");
                }
                Visit(joinRelation.Left, state);
                Visit(joinRelation.Right, state);
                return null;
            }

            public override object? VisitProjectRelation(ProjectRelation projectRelation, object? state)
            {
                foreach (var expr in projectRelation.Expressions)
                {
                    if (outerRefVisitor.Visit(expr, null!))
                    {
                        throw new NotSupportedException("Outer references inside subquery projection lists are not supported for decorrelation.");
                    }
                }
                Visit(projectRelation.Input, state);
                return null;
            }

            public override object? VisitNormalizationRelation(NormalizationRelation normalizationRelation, object? state)
            {
                Visit(normalizationRelation.Input, state);
                return null;
            }

            public override object? VisitWriteRelation(WriteRelation writeRelation, object? state)
            {
                Visit(writeRelation.Input, state);
                return null;
            }

            public override object? VisitPlanRelation(PlanRelation planRelation, object? state)
            {
                return null;
            }

            public override object? VisitReadRelation(ReadRelation readRelation, object? state)
            {
                return null;
            }

            public override object? VisitReferenceRelation(ReferenceRelation referenceRelation, object? state)
            {
                return null;
            }

            public override object? VisitRootRelation(RootRelation rootRelation, object? state)
            {
                Visit(rootRelation.Input, state);
                return null;
            }

            public override object? VisitSetRelation(SetRelation setRelation, object? state)
            {
                foreach (var input in setRelation.Inputs)
                {
                    Visit(input, state);
                }
                return null;
            }

            public override object? VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object? state)
            {
                Visit(mergeJoinRelation.Left, state);
                Visit(mergeJoinRelation.Right, state);
                return null;
            }

            public override object? VisitAggregateRelation(AggregateRelation aggregateRelation, object? state)
            {
                Visit(aggregateRelation.Input, state);
                return null;
            }

            public override object? VisitIterationRelation(IterationRelation iterationRelation, object? state)
            {
                if (iterationRelation.Input != null)
                {
                    Visit(iterationRelation.Input, state);
                }
                Visit(iterationRelation.LoopPlan, state);
                return null;
            }

            public override object? VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, object? state)
            {
                return null;
            }

            public override object? VisitBufferRelation(BufferRelation bufferRelation, object? state)
            {
                Visit(bufferRelation.Input, state);
                return null;
            }

            public override object? VisitFetchRelation(FetchRelation fetchRelation, object? state)
            {
                Visit(fetchRelation.Input, state);
                return null;
            }

            public override object? VisitSortRelation(SortRelation sortRelation, object? state)
            {
                Visit(sortRelation.Input, state);
                return null;
            }

            public override object? VisitTopNRelation(TopNRelation topNRelation, object? state)
            {
                Visit(topNRelation.Input, state);
                return null;
            }

            public override object? VisitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, object? state)
            {
                if (tableFunctionRelation.Input != null)
                {
                    Visit(tableFunctionRelation.Input, state);
                }
                return null;
            }

            public override object? VisitExchangeRelation(ExchangeRelation exchangeRelation, object? state)
            {
                Visit(exchangeRelation.Input, state);
                return null;
            }

            public override object? VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, object? state)
            {
                return null;
            }

            public override object? VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, object? state)
            {
                return null;
            }

            public override object? VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, object? state)
            {
                Visit(consistentPartitionWindowRelation.Input, state);
                return null;
            }
        }

        private class ExpressionReplacer : ExpressionVisitor<Expression, object>
        {
            public override Expression Visit(Expression expression, object state)
            {
                if (expression == null) return null!;
                return expression.Accept(this, state);
            }

            public override Expression VisitDirectFieldReference(DirectFieldReference directFieldReference, object state)
            {
                return directFieldReference;
            }

            public override Expression VisitStringLiteral(StringLiteral stringLiteral, object state)
            {
                return stringLiteral;
            }

            public override Expression VisitArrayLiteral(ArrayLiteral arrayLiteral, object state)
            {
                for (int i = 0; i < arrayLiteral.Expressions.Count; i++)
                {
                    arrayLiteral.Expressions[i] = Visit(arrayLiteral.Expressions[i], state);
                }
                return arrayLiteral;
            }

            public override Expression VisitNumericLiteral(NumericLiteral numericLiteral, object state)
            {
                return numericLiteral;
            }

            public override Expression VisitNullLiteral(NullLiteral nullLiteral, object state)
            {
                return nullLiteral;
            }

            public override Expression VisitBoolLiteral(BoolLiteral boolLiteral, object state)
            {
                return boolLiteral;
            }

            public override Expression VisitBinaryLiteral(BinaryLiteral binaryLiteral, object state)
            {
                return binaryLiteral;
            }

            public override Expression VisitScalarFunction(ScalarFunction scalarFunction, object state)
            {
                for (int i = 0; i < scalarFunction.Arguments.Count; i++)
                {
                    scalarFunction.Arguments[i] = Visit(scalarFunction.Arguments[i], state);
                }
                return scalarFunction;
            }

            public override Expression VisitIfThen(IfThenExpression ifThenExpression, object state)
            {
                if (ifThenExpression.Else != null)
                {
                    ifThenExpression.Else = Visit(ifThenExpression.Else, state);
                }

                for (int i = 0; i < ifThenExpression.Ifs.Count; i++)
                {
                    ifThenExpression.Ifs[i].If = Visit(ifThenExpression.Ifs[i].If, state);
                    ifThenExpression.Ifs[i].Then = Visit(ifThenExpression.Ifs[i].Then, state);
                }
                return ifThenExpression;
            }

            public override Expression VisitCastExpression(CastExpression castExpression, object state)
            {
                castExpression.Expression = Visit(castExpression.Expression, state);
                return castExpression;
            }

            public override Expression VisitSingularOrList(SingularOrListExpression singularOrList, object state)
            {
                for (int i = 0; i < singularOrList.Options.Count; i++)
                {
                    singularOrList.Options[i] = Visit(singularOrList.Options[i], state);
                }
                singularOrList.Value = Visit(singularOrList.Value, state);
                return singularOrList;
            }

            public override Expression VisitMultiOrList(MultiOrListExpression multiOrList, object state)
            {
                for (int i = 0; i < multiOrList.Value.Count; i++)
                {
                    multiOrList.Value[i] = Visit(multiOrList.Value[i], state);
                }

                for (int i = 0; i < multiOrList.Options.Count; i++)
                {
                    for (int k = 0; k < multiOrList.Options[i].Fields.Count; k++)
                    {
                        multiOrList.Options[i].Fields[k] = Visit(multiOrList.Options[i].Fields[k], state);
                    }
                }
                return multiOrList;
            }

            public override Expression VisitMapNestedExpression(MapNestedExpression mapNestedExpression, object state)
            {
                for (int i = 0; i < mapNestedExpression.KeyValues.Count; i++)
                {
                    mapNestedExpression.KeyValues[i] = new KeyValuePair<Expression, Expression>(
                        Visit(mapNestedExpression.KeyValues[i].Key, state),
                        Visit(mapNestedExpression.KeyValues[i].Value, state)
                    );
                }
                return mapNestedExpression;
            }

            public override Expression VisitListNestedExpression(ListNestedExpression listNestedExpression, object state)
            {
                for (int i = 0; i < listNestedExpression.Values.Count; i++)
                {
                    listNestedExpression.Values[i] = Visit(listNestedExpression.Values[i], state);
                }
                return listNestedExpression;
            }

            public override Expression VisitStructExpression(StructExpression structExpression, object state)
            {
                for (int i = 0; i < structExpression.Fields.Count; i++)
                {
                    structExpression.Fields[i] = Visit(structExpression.Fields[i], state);
                }
                return structExpression;
            }

            public override Expression VisitSetPredicateExpression(SetPredicateExpression setPredicateExpression, object state)
            {
                return setPredicateExpression;
            }
        }

        private class CorrelationConjunctRewriter : ExpressionReplacer
        {
            private readonly int leftLength;

            public CorrelationConjunctRewriter(int leftLength)
            {
                this.leftLength = leftLength;
            }

            public override Expression VisitDirectFieldReference(DirectFieldReference directFieldReference, object state)
            {
                if (directFieldReference.Root is OuterReference outerRef)
                {
                    return new DirectFieldReference()
                    {
                        ReferenceSegment = directFieldReference.ReferenceSegment,
                        Root = outerRef.StepsOut == 1
                            ? new RootReference()
                            : new OuterReference() { StepsOut = outerRef.StepsOut - 1 }
                    };
                }
                else if (directFieldReference.Root is RootReference)
                {
                    if (directFieldReference.ReferenceSegment is StructReferenceSegment structRef)
                    {
                        return new DirectFieldReference()
                        {
                            ReferenceSegment = new StructReferenceSegment() { Field = structRef.Field + leftLength },
                            Root = new RootReference()
                        };
                    }
                    else
                    {
                        throw new NotSupportedException("Only struct reference segments are supported for decorrelation.");
                    }
                }
                return directFieldReference;
            }
        }

        private class SubqueryRewriter : ExpressionReplacer
        {
            private readonly Dictionary<SetPredicateExpression, int> subqueryToMarkColumnIndex;

            public SubqueryRewriter(Dictionary<SetPredicateExpression, int> subqueryToMarkColumnIndex)
            {
                this.subqueryToMarkColumnIndex = subqueryToMarkColumnIndex;
            }

            public override Expression VisitSetPredicateExpression(SetPredicateExpression setPredicateExpression, object state)
            {
                if (subqueryToMarkColumnIndex.TryGetValue(setPredicateExpression, out var index))
                {
                    return new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = index },
                        Root = new RootReference()
                    };
                }
                return setPredicateExpression;
            }
        }
    }
}

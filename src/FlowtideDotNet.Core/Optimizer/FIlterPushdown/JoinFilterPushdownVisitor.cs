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
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Optimizer.FilterPushdown
{
    /// <summary>
    /// Checks if an expression can be pushed down from the join condition.
    /// </summary>
    internal class JoinFilterPushdownVisitor : OptimizerBaseVisitor
    {
        private static void TestPushdownNotNull(JoinRelation joinRelation, Expression expression, List<Expression> leftPushdowns, List<Expression> rightPushdowns)
        {
            if ((joinRelation.Type == JoinType.Inner || joinRelation.Type == JoinType.Left || joinRelation.Type == JoinType.Right) &&
                MergeJoinFindVisitor.Check(joinRelation, expression, out var leftKey, out var rightKey))
            {
                if (joinRelation.Type == JoinType.Inner || joinRelation.Type == JoinType.Right)
                {
                    if (leftKey.ReferenceSegment is StructReferenceSegment leftStruct)
                    {
                        leftPushdowns.Add(new ScalarFunction()
                        {
                            Arguments = new List<Expression>()
                        {
                            new DirectFieldReference()
                            {
                                ReferenceSegment = new StructReferenceSegment()
                                {
                                    Field = leftStruct.Field,
                                    Child = leftStruct.Child
                                }
                            }
                        },
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.IsNotNull
                        });
                    }
                }

                if (joinRelation.Type == JoinType.Inner || joinRelation.Type == JoinType.Left)
                {
                    if (rightKey.ReferenceSegment is StructReferenceSegment rightStruct)
                    {
                        rightPushdowns.Add(new ScalarFunction()
                        {
                            Arguments = new List<Expression>()
                        {
                            new DirectFieldReference()
                            {
                                ReferenceSegment = new StructReferenceSegment()
                                {
                                    Field = rightStruct.Field,
                                    Child = rightStruct.Child
                                }
                            }
                        },
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.IsNotNull
                        });
                    }
                }
            }
        }

        public override Relation VisitJoinRelation(JoinRelation joinRelation, object state)
        {
            // Check root expression
            var visitor = new JoinExpressionVisitor(joinRelation.Left.OutputLength);
            visitor.Visit(joinRelation.Expression!, state);

            List<Expression> leftPushdowns = new List<Expression>();
            List<Expression> rightPushdowns = new List<Expression>();

            if (joinRelation.Expression != null)
            {
                TestPushdownNotNull(joinRelation, joinRelation.Expression, leftPushdowns, rightPushdowns);

                if (joinRelation.Expression is ScalarFunction scalarFunc &&
                    scalarFunc.ExtensionUri == FunctionsBoolean.Uri && scalarFunc.ExtensionName == FunctionsBoolean.And)
                {
                    for (int i = 0; i < scalarFunc.Arguments.Count; i++)
                    {
                        TestPushdownNotNull(joinRelation, scalarFunc.Arguments[i], leftPushdowns, rightPushdowns);
                    }
                }
            }

            if (!visitor.unknownCase)
            {
                // Only fields from left is used
                if (visitor.fieldInLeft && !visitor.fieldInRight && joinRelation.Type == JoinType.Inner)
                {
                    leftPushdowns.Add(new BoolLiteral() { Value = true });
                }
                // Only field in right is used
                else if (!visitor.fieldInLeft && visitor.fieldInRight && joinRelation.Type == JoinType.Inner)
                {
                    rightPushdowns.Add(new BoolLiteral() { Value = true });
                }
            }

            if (joinRelation.Expression is ScalarFunction andFunctionScalar &&
                andFunctionScalar.ExtensionUri == FunctionsBoolean.Uri &&
                andFunctionScalar.ExtensionName == FunctionsBoolean.And)
            {
                for (int i = 0; i < andFunctionScalar.Arguments.Count; i++)
                {
                    var expr = andFunctionScalar.Arguments[i];
                    var andVisitor = new JoinExpressionVisitor(joinRelation.Left.OutputLength);
                    andVisitor.Visit(expr, state);
                    if (andVisitor.fieldInLeft && !andVisitor.fieldInRight && joinRelation.Type == JoinType.Inner)
                    {
                        leftPushdowns.Add(expr);
                        andFunctionScalar.Arguments.RemoveAt(i);
                        i--;
                    }
                    // Only field in right is used
                    else if (!andVisitor.fieldInLeft && andVisitor.fieldInRight && joinRelation.Type == JoinType.Inner)
                    {
                        rightPushdowns.Add(expr);
                        andFunctionScalar.Arguments.RemoveAt(i);
                        i--;
                    }
                    if (andFunctionScalar.Arguments.Count == 1)
                    {
                        joinRelation.Expression = andFunctionScalar.Arguments[0];
                    }
                    else if (andFunctionScalar.Arguments.Count == 0)
                    {
                        joinRelation.Expression = new BoolLiteral() { Value = true };
                    }
                }
            }

            if (leftPushdowns.Count > 0)
            {

                if (leftPushdowns.Count == 1)
                {
                    joinRelation.Left = new FilterRelation()
                    {
                        Condition = leftPushdowns[0],
                        Input = joinRelation.Left
                    };
                }
                else
                {
                    joinRelation.Left = new FilterRelation()
                    {
                        Condition = new ScalarFunction() { ExtensionUri = FunctionsBoolean.Uri, ExtensionName = FunctionsBoolean.And, Arguments = leftPushdowns },
                        Input = joinRelation.Left
                    };
                }
            }
            if (rightPushdowns.Count > 0)
            {
                // Find used fields
                var usageVisitor = new ExpressionFieldUsageVisitor(joinRelation.Left.OutputLength);
                foreach (var expr in rightPushdowns)
                {
                    usageVisitor.Visit(expr, default);
                }
                var rightUsageFields = usageVisitor.UsedFieldsRight.Distinct().ToList();

                // Build lookup table from old to new field id
                Dictionary<int, int> oldToNew = new Dictionary<int, int>();
                foreach (var usedField in rightUsageFields)
                {
                    oldToNew.Add(usedField, usedField - joinRelation.Left.OutputLength);
                }
                // Replace old ids with the new ids
                var replaceVisitor = new ExpressionFieldReplaceVisitor(oldToNew);
                foreach (var expr in rightPushdowns)
                {
                    replaceVisitor.Visit(expr, default);
                }
                if (rightPushdowns.Count == 1)
                {
                    joinRelation.Right = new FilterRelation()
                    {
                        Condition = rightPushdowns[0],
                        Input = joinRelation.Right
                    };
                }
                else
                {
                    joinRelation.Right = new FilterRelation()
                    {
                        Condition = new ScalarFunction() { ExtensionUri = FunctionsBoolean.Uri, ExtensionName = FunctionsBoolean.And, Arguments = rightPushdowns },
                        Input = joinRelation.Right
                    };
                }
            }

            joinRelation.Left = Visit(joinRelation.Left, state);
            joinRelation.Right = Visit(joinRelation.Right, state);

            return joinRelation;
        }

        public override Relation VisitFilterRelation(FilterRelation filterRelation, object state)
        {
            if (filterRelation.Input is JoinRelation joinRelation)
            {
                return TryPushThroughJoin(filterRelation, joinRelation, state);
            }
            if (filterRelation.Input is FilterRelation inputFilter)
            {
                return CombineFilterRelations(filterRelation, inputFilter, state);
            }

            filterRelation.Input = Visit(filterRelation.Input, state);
            return filterRelation;
        }

        private Relation CombineFilterRelations(FilterRelation filter, FilterRelation inputRelation, object state)
        {
            // Check if the input relation has an and condition
            if (inputRelation.Condition is ScalarFunction innerAndScalar &&
                innerAndScalar.ExtensionUri == FunctionsBoolean.Uri &&
                innerAndScalar.ExtensionName == FunctionsBoolean.And)
            {
                // If the filter is also an and condition
                if (filter.Condition is ScalarFunction andScalar &&
                    andScalar.ExtensionUri == FunctionsBoolean.Uri &&
                    andScalar.ExtensionName == FunctionsBoolean.And)
                {
                    // Merge them together
                    foreach (var arg in andScalar.Arguments)
                    {
                        innerAndScalar.Arguments.Add(RemapCondition(arg, inputRelation, inputRelation.Input.OutputLength));
                    }
                    SwitchEmit(filter, inputRelation);
                    return Visit(inputRelation, state);
                }
                else
                {
                    // Append to the input and condition
                    innerAndScalar.Arguments.Add(RemapCondition(filter.Condition, inputRelation, inputRelation.Input.OutputLength));
                    SwitchEmit(filter, inputRelation);
                    return Visit(inputRelation, state);
                }
            }
            else
            {
                if (filter.Condition is ScalarFunction andScalar &&
                    andScalar.ExtensionUri == FunctionsBoolean.Uri &&
                    andScalar.ExtensionName == FunctionsBoolean.And)
                {
                    RemapCondition(andScalar, inputRelation, inputRelation.Input.OutputLength);
                    andScalar.Arguments.Add(inputRelation.Condition);
                    inputRelation.Condition = andScalar;
                    SwitchEmit(filter, inputRelation);
                    return Visit(inputRelation, state);
                }
                else
                {
                    var newAndScalar = new ScalarFunction()
                    {
                        ExtensionUri = FunctionsBoolean.Uri,
                        ExtensionName = FunctionsBoolean.And,
                        Arguments = new List<Expression>()
                        {
                            inputRelation.Condition,
                            RemapCondition(filter.Condition, inputRelation, inputRelation.Input.OutputLength)
                        }
                    };
                    inputRelation.Condition = newAndScalar;
                    SwitchEmit(filter, inputRelation);
                    return Visit(inputRelation, state);
                }
            }
        }

        private Expression RemapCondition(Expression condition, Relation input, int leftSize)
        {
            Dictionary<int, int> mapping = new Dictionary<int, int>();

            if (input.EmitSet)
            {
                for (int i = 0; i < input.Emit.Count; i++)
                {
                    var e = input.Emit[i];
                    if (e >= leftSize)
                    {
                        e = e - leftSize;
                    }
                    mapping.Add(i, e);
                }
            }
            else
            {
                for (int i = 0; i < input.OutputLength; i++)
                {
                    var e = i;
                    if (e >= leftSize)
                    {
                        e = e - leftSize;
                    }
                    mapping.Add(i, e);
                }
            }
            var replacer = new ExpressionFieldReplaceVisitor(mapping);
            replacer.Visit(condition, null);
            return condition;
        }

        private void SwitchEmit(Relation relation, Relation inputRelation)
        {
            if (relation.EmitSet && inputRelation.EmitSet)
            {
                inputRelation.Emit = relation.Emit;
                return;
            }
            else if (relation.EmitSet && !inputRelation.EmitSet)
            {
                inputRelation.Emit = relation.Emit;
            }
        }

        private Relation TryPushThroughJoin(FilterRelation filterRelation, JoinRelation joinRelation, object state)
        {
            // Check if the filter can be pushed down through the join
            var visitor = new JoinExpressionVisitor(joinRelation.Left.OutputLength, joinRelation.Emit);
            visitor.Visit(filterRelation.Condition, state);
            if (!visitor.unknownCase)
            {
                // Only fields from left is used
                if (visitor.fieldInLeft && !visitor.fieldInRight && (joinRelation.Type == JoinType.Inner || joinRelation.Type == JoinType.Left))
                {
                    joinRelation.Left = new FilterRelation()
                    {
                        Condition = RemapCondition(filterRelation.Condition, joinRelation, joinRelation.Left.OutputLength),
                        Input = joinRelation.Left
                    };
                    SwitchEmit(filterRelation, joinRelation);
                    return Visit(joinRelation, state);
                }
                // Only field in right is used
                else if (!visitor.fieldInLeft && visitor.fieldInRight && (joinRelation.Type == JoinType.Inner || joinRelation.Type == JoinType.Right))
                {
                    joinRelation.Right = new FilterRelation()
                    {
                        Condition = RemapCondition(filterRelation.Condition, joinRelation, joinRelation.Left.OutputLength),
                        Input = joinRelation.Right
                    };
                    SwitchEmit(filterRelation, joinRelation);
                    return Visit(joinRelation, state);
                }
            }

            // TODO: Check if it is an AND condition and if it can be split up
            if (filterRelation.Condition is ScalarFunction scalarFunction &&
                scalarFunction.ExtensionUri == FunctionsBoolean.Uri &&
                scalarFunction.ExtensionName == FunctionsBoolean.And)
            {
                for (int i = 0; i < scalarFunction.Arguments.Count; i++)
                {
                    var expr = scalarFunction.Arguments[i];
                    var andVisitor = new JoinExpressionVisitor(joinRelation.Left.OutputLength, joinRelation.Emit);
                    andVisitor.Visit(expr, state);
                    if (andVisitor.fieldInLeft && !andVisitor.fieldInRight && (joinRelation.Type == JoinType.Inner || joinRelation.Type == JoinType.Left))
                    {
                        joinRelation.Left = new FilterRelation()
                        {
                            Condition = RemapCondition(expr, joinRelation, joinRelation.Left.OutputLength),
                            Input = joinRelation.Left
                        };
                        scalarFunction.Arguments.RemoveAt(i);
                        i--;
                    }
                    // Only field in right is used
                    else if (!andVisitor.fieldInLeft && andVisitor.fieldInRight && (joinRelation.Type == JoinType.Inner || joinRelation.Type == JoinType.Right))
                    {
                        joinRelation.Right = new FilterRelation()
                        {
                            Condition = RemapCondition(expr, joinRelation, joinRelation.Left.OutputLength),
                            Input = joinRelation.Right
                        };
                        scalarFunction.Arguments.RemoveAt(i);
                        i--;
                    }
                    if (scalarFunction.Arguments.Count == 1)
                    {
                        filterRelation.Condition = scalarFunction.Arguments[0];
                    }
                    else if (scalarFunction.Arguments.Count == 0)
                    {
                        SwitchEmit(filterRelation, joinRelation);
                        return Visit(joinRelation, state);
                    }
                }
            }

            filterRelation.Input = Visit(filterRelation.Input, state);
            return filterRelation;
        }
    }
}

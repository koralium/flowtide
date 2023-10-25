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
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using SqlParser;
using SqlParser.Ast;
using System.Diagnostics;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal class SqlExpressionVisitor : BaseExpressionVisitor<ExpressionData, EmitData>
    {
        protected override ExpressionData VisitBinaryOperation(SqlParser.Ast.Expression.BinaryOp binaryOp, EmitData state)
        {
            var left = Visit(binaryOp.Left, state);
            var right = Visit(binaryOp.Right, state);

            switch (binaryOp.Op)
            {
                case SqlParser.Ast.BinaryOperator.Eq:
                    var func = new ScalarFunction()
                    {
                        ExtensionUri = FunctionsComparison.Uri,
                        ExtensionName = FunctionsComparison.Equal,
                        Arguments = new List<Expressions.Expression>()
                        {
                            left.Expr,
                            right.Expr
                        }
                    };
                    return new ExpressionData(func, $"{left.Name}_{right.Name}");
                case BinaryOperator.Gt:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.GreaterThan,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        }, $"{left.Name}_{right.Name}"
                        );
                case BinaryOperator.GtEq:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.GreaterThanOrEqual,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        }, $"{left.Name}_{right.Name}"
                        );
                case BinaryOperator.Lt:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.LessThan,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        }, $"{left.Name}_{right.Name}"
                        );
                case BinaryOperator.LtEq:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.LessThanOrEqual,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        }, $"{left.Name}_{right.Name}"
                        );
                case BinaryOperator.NotEq:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.NotEqual,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        }, $"{left.Name}_{right.Name}"
                        );
                case BinaryOperator.And:
                    // Merge and functions together into one big list
                    List<FlowtideDotNet.Substrait.Expressions.Expression> expressions = new List<FlowtideDotNet.Substrait.Expressions.Expression>();
                    if (left.Expr is ScalarFunction leftScalar && 
                        leftScalar.ExtensionUri == FunctionsBoolean.Uri &&
                        leftScalar.ExtensionName == FunctionsBoolean.And)
                    {
                        expressions.AddRange(leftScalar.Arguments);
                    }
                    else
                    {
                        expressions.Add(left.Expr);
                    }
                    if (right.Expr is ScalarFunction rightScalar &&
                        rightScalar.ExtensionUri == FunctionsBoolean.Uri &&
                        rightScalar.ExtensionName == FunctionsBoolean.And)
                    {
                        expressions.AddRange(rightScalar.Arguments);
                    }
                    else
                    {
                        expressions.Add(right.Expr);
                    }

                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            Arguments = expressions,
                            ExtensionName = FunctionsBoolean.And,
                            ExtensionUri = FunctionsBoolean.Uri
                        }, $"{left.Name}_{right.Name}"
                        );
                case BinaryOperator.Or:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsBoolean.Uri,
                            ExtensionName = FunctionsBoolean.Or,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        }, $"{left.Name}_{right.Name}"
                        );
                case BinaryOperator.StringConcat:
                    List<Expressions.Expression> concatExpressions = new List<Expressions.Expression>();
                    if (left.Expr is ScalarFunction leftConcat &&
                        leftConcat.ExtensionUri == FunctionsString.Uri &&
                        leftConcat.ExtensionName == FunctionsString.Concat)
                    {
                        concatExpressions.AddRange(leftConcat.Arguments);
                    }
                    else
                    {
                        concatExpressions.Add(left.Expr);
                    }
                    if (right.Expr is ScalarFunction rightConcat &&
                        rightConcat.ExtensionUri == FunctionsString.Uri &&
                        rightConcat.ExtensionName == FunctionsString.Concat)
                    {
                        concatExpressions.AddRange(rightConcat.Arguments);
                    }
                    else
                    {
                        concatExpressions.Add(right.Expr);
                    }
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsString.Uri,
                            ExtensionName = FunctionsString.Concat,
                            Arguments = concatExpressions
                        }, $"$concat");


                default:
                    throw new NotImplementedException($"Binary operation {binaryOp.Op.ToString()}' is not yet supported in SQL mode.");
            }
        }

        protected override ExpressionData VisitCompoundIdentifier(SqlParser.Ast.Expression.CompoundIdentifier compoundIdentifier, EmitData state)
        {
            var removedQuotaIdentifier = new SqlParser.Ast.Expression.CompoundIdentifier(new Sequence<Ident>(compoundIdentifier.Idents.Select(x => new Ident(x.Value))));
            // First try and get the index directly based on the expression
            if (state.TryGetEmitIndex(removedQuotaIdentifier, out var index))
            {
                var r = new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = index
                    }
                };
                return new ExpressionData(r, state.GetName(index));
            }

            // Otherwise try and find a a part of it.

            return base.VisitCompoundIdentifier(removedQuotaIdentifier, state);
        }

        protected override ExpressionData VisitLiteralValue(SqlParser.Ast.Expression.LiteralValue literalValue, EmitData state)
        {
            if (literalValue.Value is Value.Boolean LiteralBool)
            {
                return new ExpressionData(new BoolLiteral()
                {
                    Value = LiteralBool.Value
                }, $"$bool");
            }
            if (literalValue.Value is Value.DoubleQuotedString valueDoubleQuotedString)
            {
                return new ExpressionData(new StringLiteral()
                {
                    Value = valueDoubleQuotedString.Value
                }, "$string");
            }
            if (literalValue.Value is Value.SingleQuotedString valueSingleQuotedString)
            {
                return new ExpressionData(new StringLiteral()
                {
                    Value = valueSingleQuotedString.Value,
                }, "$string");
            }
            if (literalValue.Value is Value.Number number)
            {
                return new ExpressionData(new NumericLiteral()
                {
                    Value = decimal.Parse(number.Value)
                }, "$number");
            }
            if (literalValue.Value is Value.Null)
            {
                return new ExpressionData(new NullLiteral(), "$null");
            }
            throw new NotImplementedException($"The literal type: '{literalValue.Value.GetType().Name}' is not yet implemented");
        }

        protected override ExpressionData VisitCaseExpression(SqlParser.Ast.Expression.Case caseExpression, EmitData state)
        {
            var ifThen = new IfThenExpression()
            {
                Ifs = new List<IfClause>()
            };
            
            for(int i = 0; i < caseExpression.Conditions.Count; i++)
            {
                var condition = Visit(caseExpression.Conditions[i], state);
                var result = Visit(caseExpression.Results[i], state);
                ifThen.Ifs.Add(new IfClause()
                {
                    If = condition.Expr,
                    Then = result.Expr
                });
            }
            if (caseExpression.ElseResult != null)
            {
                var elseResult = Visit(caseExpression.ElseResult, state);
                ifThen.Else = elseResult.Expr;
            }
            return new ExpressionData(ifThen, "$case");
        }

        protected override ExpressionData VisitFunction(SqlParser.Ast.Expression.Function function, EmitData state)
        {
            var functionName = function.Name.ToSql();
            if (functionName.Equals("coalesce", StringComparison.OrdinalIgnoreCase))
            {
                return VisitCoalesce(function, state);
            }

            return base.VisitFunction(function, state);
        }

        /// <summary>
        /// Coalesce is done with a if then else statement
        /// </summary>
        /// <param name="function"></param>
        /// <param name="state"></param>
        /// <returns></returns>
        private ExpressionData VisitCoalesce(SqlParser.Ast.Expression.Function function, EmitData state)
        {
            var ifThenStatement = new IfThenExpression()
            {
                Ifs = new List<IfClause>()
            };
            {
                Debug.Assert(function.Args != null);
                for (int i = 0; i < function.Args.Count - 1; i++)
                {
                    var arg = function.Args[i];
                    if (arg is FunctionArg.Unnamed unnamed)
                    {
                        if (unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                        {
                            var expr = Visit(funcExpr.Expression, state);
                            ifThenStatement.Ifs.Add(new IfClause()
                            {
                                If = new ScalarFunction() 
                                { 
                                    ExtensionUri = FunctionsComparison.Uri,
                                    ExtensionName = FunctionsComparison.IsNotNull,
                                    Arguments = new List<Expressions.Expression>() { expr.Expr } 
                                },
                                Then = expr.Expr
                            });
                        }
                        else
                        {
                            throw new NotImplementedException("Coalesce does not support the input parameter");
                        }
                    }
                    else
                    {
                        throw new NotImplementedException("Coalesce does not support the input parameter");
                    }
                }
            }
            {
                var lastArg = function.Args[function.Args.Count - 1];
                if (lastArg is FunctionArg.Unnamed unnamed)
                {
                    if (unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                    {
                        var expr = Visit(funcExpr.Expression, state);
                        ifThenStatement.Else = expr.Expr;
                    }
                    else
                    {
                        throw new NotImplementedException("Coalesce does not support the input parameter");
                    }
                }
                else
                {
                    throw new NotImplementedException("Coalesce does not support the input parameter");
                }
            }
            return new ExpressionData(ifThenStatement, "$coalesce");
        }

        protected override ExpressionData VisitIsNotNull(SqlParser.Ast.Expression.IsNotNull isNotNull, EmitData state)
        {
            var expr = Visit(isNotNull.Expression, state);
            return new ExpressionData(new ScalarFunction()
            {
                ExtensionUri = FunctionsComparison.Uri,
                ExtensionName = FunctionsComparison.IsNotNull,
                Arguments = new List<Expressions.Expression>()
                {
                    expr.Expr
                }
            }, "$isnotnull");
        }
    }
}

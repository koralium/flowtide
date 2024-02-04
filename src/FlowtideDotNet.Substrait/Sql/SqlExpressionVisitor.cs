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
using FlowtideDotNet.Substrait.Sql.Internal;
using FlowtideDotNet.Substrait.Type;
using SqlParser;
using SqlParser.Ast;
using System.Diagnostics;
using static SqlParser.Ast.Expression;

namespace FlowtideDotNet.Substrait.Sql
{
    public class SqlExpressionVisitor : BaseExpressionVisitor<ExpressionData, EmitData>
    {
        private readonly SqlFunctionRegister sqlFunctionRegister;

        internal SqlExpressionVisitor(SqlFunctionRegister sqlFunctionRegister)
        {
            this.sqlFunctionRegister = sqlFunctionRegister;
        }

        public override ExpressionData Visit(SqlParser.Ast.Expression expression, EmitData state)
        {
            if (state.TryGetEmitIndex(expression, out var index))
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
            return base.Visit(expression, state);
        }

        protected override ExpressionData VisitBinaryOperation(SqlParser.Ast.Expression.BinaryOp binaryOp, EmitData state)
        {
            var left = Visit(binaryOp.Left, state);
            var right = Visit(binaryOp.Right, state);

            switch (binaryOp.Op)
            {
                case BinaryOperator.Eq:
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
                    List<Expressions.Expression> expressions = new List<Expressions.Expression>();
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
                case BinaryOperator.Plus:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsArithmetic.Uri,
                            ExtensionName = FunctionsArithmetic.Add,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        },
                        $"$add"
                        );
                case BinaryOperator.Minus:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsArithmetic.Uri,
                            ExtensionName = FunctionsArithmetic.Subtract,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        },
                        $"$subtract"
                        );
                case BinaryOperator.Multiply:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsArithmetic.Uri,
                            ExtensionName = FunctionsArithmetic.Multiply,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        }, "$multiply");
                case BinaryOperator.Divide:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsArithmetic.Uri,
                            ExtensionName = FunctionsArithmetic.Divide,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        }, "$divide");
                case BinaryOperator.Modulo:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsArithmetic.Uri,
                            ExtensionName = FunctionsArithmetic.Modulo,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        }, "$modulo");
                case BinaryOperator.Xor:
                    return new ExpressionData(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsBoolean.Uri,
                            ExtensionName = FunctionsBoolean.Xor,
                            Arguments = new List<Expressions.Expression>()
                            {
                                left.Expr,
                                right.Expr
                            }
                        }, $"{left.Name}_{right.Name}"
                        );
                default:
                    throw new NotImplementedException($"Binary operation {binaryOp.Op.ToString()}' is not yet supported in SQL mode.");
            }
        }

        protected override ExpressionData VisitTrim(Trim trim, EmitData state)
        {
            var expr = Visit(trim.Expression, state);

            if (trim.TrimWhere == TrimWhereField.Both || trim.TrimWhere == TrimWhereField.None)
            {
                return new ExpressionData(
                    new ScalarFunction()
                    {
                        ExtensionUri = FunctionsString.Uri,
                        ExtensionName = FunctionsString.Trim,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    },
                    "$trim"
                );
            }
            else if (trim.TrimWhere == TrimWhereField.Trailing)
            {
                return new ExpressionData(
                    new ScalarFunction()
                    {
                        ExtensionUri = FunctionsString.Uri,
                        ExtensionName = FunctionsString.RTrim,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    },
                    "$trim"
                );
            }
            else if (trim.TrimWhere == TrimWhereField.Leading)
            {
                return new ExpressionData(
                    new ScalarFunction()
                    {
                        ExtensionUri = FunctionsString.Uri,
                        ExtensionName = FunctionsString.LTrim,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    },
                    "$trim"
                );
            }
            else
            {
                throw new NotSupportedException();
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
            throw new InvalidOperationException($"Could not find column '{compoundIdentifier.ToSql()}' in the table.");
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
            var ifs = new List<IfClause>();
            Expressions.Expression? elseExpr = null;

            for (int i = 0; i < caseExpression.Conditions.Count; i++)
            {
                var condition = Visit(caseExpression.Conditions[i], state);
                var result = Visit(caseExpression.Results[i], state);
                ifs.Add(new IfClause()
                {
                    If = condition.Expr,
                    Then = result.Expr
                });
            }
            if (caseExpression.ElseResult != null)
            {
                var elseResult = Visit(caseExpression.ElseResult, state);
                elseExpr = elseResult.Expr;
            }

            var ifThen = new IfThenExpression()
            {
                Ifs = ifs,
                Else = elseExpr
            };
            return new ExpressionData(ifThen, "$case");
        }

        protected override ExpressionData VisitFunction(SqlParser.Ast.Expression.Function function, EmitData state)
        {
            var functionName = function.Name.ToSql();
            var functionType = sqlFunctionRegister.GetFunctionType(functionName);

            if (functionType == FunctionType.Scalar)
            {
                var mapper = sqlFunctionRegister.GetScalarMapper(functionName);
                var expr = mapper(function, this, state);
                return new ExpressionData(
                    expr,
                    $"${functionName}"
                    );
            }

            return base.VisitFunction(function, state);
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

        protected override ExpressionData VisitFloor(SqlParser.Ast.Expression.Floor floor, EmitData state)
        {
            var expr = Visit(floor.Expression, state);
            return new ExpressionData(
                new ScalarFunction()
                {
                    ExtensionUri = FunctionsRounding.Uri,
                    ExtensionName = FunctionsRounding.Floor,
                    Arguments = new List<Expressions.Expression>() { expr.Expr }
                }, "$floor");
        }

        protected override ExpressionData VisitCeil(SqlParser.Ast.Expression.Ceil ceil, EmitData state)
        {
            var expr = Visit(ceil.Expression, state);
            return new ExpressionData(
                new ScalarFunction()
                {
                    ExtensionUri = FunctionsRounding.Uri,
                    ExtensionName = FunctionsRounding.Ceil,
                    Arguments = new List<Expressions.Expression>() { expr.Expr }
                }, "ceil");
        }

        protected override ExpressionData VisitUnaryOperation(UnaryOp unaryOp, EmitData state)
        {
            var exprResult = Visit(unaryOp.Expression, state);
            switch (unaryOp.Op)
            {
                case UnaryOperator.Minus:
                    return VisitMinusUnaryOp(exprResult);
                case UnaryOperator.Not:
                    return VisitNotUnaryOp(exprResult);
            }
            return base.VisitUnaryOperation(unaryOp, state);
        }

        private static ExpressionData VisitNotUnaryOp(ExpressionData expressionData)
        {
            return new ExpressionData(
                new ScalarFunction()
                {
                    ExtensionUri = FunctionsBoolean.Uri,
                    ExtensionName = FunctionsBoolean.Not,
                    Arguments = new List<Expressions.Expression>() { expressionData.Expr }
                }, expressionData.Name
                );
        }

        private ExpressionData VisitMinusUnaryOp(ExpressionData expressionData)
        {
            if (expressionData.Expr is NumericLiteral numeric)
            {
                return new ExpressionData(new NumericLiteral()
                {
                    Value = -numeric.Value
                }, $"$minus");
            }
            return new ExpressionData(
                new ScalarFunction()
                {
                    ExtensionUri = FunctionsArithmetic.Uri,
                    ExtensionName = FunctionsArithmetic.Negate,
                    Arguments = new List<Expressions.Expression>() { expressionData.Expr }
                }, "$negate"
                );
        }

        protected override ExpressionData VisitBetween(Between between, EmitData state)
        {
            var expr = Visit(between.Expression, state);
            var low = Visit(between.Low, state);
            var high = Visit(between.High, state);

            return new ExpressionData(
                new ScalarFunction()
                {
                    ExtensionUri = FunctionsComparison.Uri,
                    ExtensionName = FunctionsComparison.Between,
                    Arguments = new List<Expressions.Expression>()
                    {
                        expr.Expr,
                        low.Expr,
                        high.Expr
                    }
                }, "$between"
            );
        }

        protected override ExpressionData VisitIsNull(IsNull isNull, EmitData state)
        {
            var expr = Visit(isNull.Expression, state);
            return new ExpressionData(
                new ScalarFunction()
                {
                    ExtensionUri = FunctionsComparison.Uri,
                    ExtensionName = FunctionsComparison.IsNull,
                    Arguments = new List<Expressions.Expression>()
                    {
                        expr.Expr
                    }
                }, "$isnull"
            );
        }

        protected override ExpressionData VisitInList(InList inList, EmitData state)
        {
            var expr = Visit(inList.Expression, state);
            List<Expressions.Expression> options = new List<Expressions.Expression>();
            foreach(var v in inList.List)
            {
                options.Add(Visit(v, state).Expr);
            }
            
            return new ExpressionData(
                new SingularOrListExpression()
                {
                    Value = expr.Expr,
                    Options = options
                },
                "$inlist"
                );
        }

        protected override ExpressionData VisitCast(Cast cast, EmitData state)
        {
            var expr = Visit(cast.Expression, state);

            SubstraitBaseType? baseType;
            if (cast.DataType is SqlParser.Ast.DataType.StringType)
            {
                baseType = new StringType();
            }
            else if(cast.DataType is SqlParser.Ast.DataType.Int || 
                cast.DataType is SqlParser.Ast.DataType.Integer ||
                cast.DataType is SqlParser.Ast.DataType.SmallInt ||
                cast.DataType is SqlParser.Ast.DataType.TinyInt)
            {
                baseType = new Int64Type();
            }
            else if (cast.DataType is SqlParser.Ast.DataType.Decimal)
            {
                baseType = new DecimalType();
            }
            else if (cast.DataType is SqlParser.Ast.DataType.Boolean)
            {
                baseType = new BoolType();
            }
            else if (cast.DataType is SqlParser.Ast.DataType.Double ||
                cast.DataType is SqlParser.Ast.DataType.Float)
            {
                baseType = new Fp64Type();
            }
            else
            {
                throw new NotImplementedException($"The data type '{cast.DataType.GetType().Name}' is not yet supported in cast for SQL.");
            }

            var castExpression = new Expressions.CastExpression()
            {
                Expression = expr.Expr,
                Type = baseType
            };

            return new ExpressionData(castExpression, expr.Name);
        }

        protected override ExpressionData VisitNested(Nested nested, EmitData state)
        {
            return Visit(nested.Expression, state);
        }

        protected override ExpressionData VisitSubstring(Substring substring, EmitData state)
        {
            Debug.Assert(substring.SubstringFrom != null);
            var expr = Visit(substring.Expression, state);
            var from = Visit(substring.SubstringFrom, state);
            if (substring.SubstringFor == null)
            {
                var substringFunction = new ScalarFunction()
                {
                    ExtensionUri = FunctionsString.Uri,
                    ExtensionName = FunctionsString.Substring,
                    Arguments = new List<Expressions.Expression>()
                    {
                        expr.Expr,
                        from.Expr
                    }
                };
                return new ExpressionData(substringFunction, expr.Name);
            }
            else
            {
                var length = Visit(substring.SubstringFor, state);
                var substringFunction = new ScalarFunction()
                {
                    ExtensionUri = FunctionsString.Uri,
                    ExtensionName = FunctionsString.Substring,
                    Arguments = new List<Expressions.Expression>()
                    {
                        expr.Expr,
                        from.Expr,
                        length.Expr
                    }
                };
                return new ExpressionData(substringFunction, expr.Name);
            }
        }
    }
}

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
using FlowtideDotNet.Substrait.FunctionExtensions;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal static class BuiltInSqlFunctions
    {
        public static void AddBuiltInFunctions(SqlFunctionRegister sqlFunctionRegister)
        {
            sqlFunctionRegister.RegisterScalarFunction("coalesce", (f, visitor, emitData) =>
            {
                return VisitCoalesce(f, visitor, emitData).Expr;
            });
            sqlFunctionRegister.RegisterAggregateFunction("count", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("count must have exactly one argument, and be '*'");
                }
                if (!(f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("count must have exactly one argument, and be '*'");
                }
                return new AggregateFunction()
                {
                    ExtensionUri = FunctionsAggregateGeneric.Uri,
                    ExtensionName = FunctionsAggregateGeneric.Count,
                    Arguments = new List<Expressions.Expression>()
                };
            });
        }

        private static ExpressionData VisitCoalesce(SqlParser.Ast.Expression.Function function, SqlExpressionVisitor visitor, EmitData state)
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
                            var expr = visitor.Visit(funcExpr.Expression, state);
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
                        var expr = visitor.Visit(funcExpr.Expression, state);
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
    }
}

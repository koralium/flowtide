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
using SqlParser.Ast;
using System.Diagnostics;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal static class BuiltInSqlFunctions
    {
        public static void AddBuiltInFunctions(SqlFunctionRegister sqlFunctionRegister)
        {
            sqlFunctionRegister.RegisterScalarFunction("ceiling", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("ceiling must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsRounding.Uri,
                        ExtensionName = FunctionsRounding.Ceil,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("ceiling does not support the input parameter");
                }
            });
            sqlFunctionRegister.RegisterScalarFunction("round", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("round must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsRounding.Uri,
                        ExtensionName = FunctionsRounding.Round,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("round does not support the input parameter");
                }
            });
            sqlFunctionRegister.RegisterScalarFunction("coalesce", (f, visitor, emitData) =>
            {
                return VisitCoalesce(f, visitor, emitData).Expr;
            });

            sqlFunctionRegister.RegisterScalarFunction("is_infinite", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("is_infinite must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsComparison.Uri,
                        ExtensionName = FunctionsComparison.isInfinite,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("is_infinite does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("is_finite", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("is_finite must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsComparison.Uri,
                        ExtensionName = FunctionsComparison.IsFinite,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("is_finite does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("is_nan", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("is_nan must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsComparison.Uri,
                        ExtensionName = FunctionsComparison.IsNan,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("is_nan does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("nullif", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 2)
                {
                    throw new InvalidOperationException("nullif must have exactly two arguments");
                }
                var arguments = new List<Expressions.Expression>();
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    arguments.Add(expr.Expr);
                }
                else
                {
                    throw new NotImplementedException("nullif does not support the input parameter");
                }
                if (f.Args[1] is FunctionArg.Unnamed unnamed2 && unnamed2.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr2)
                {
                    var expr = visitor.Visit(funcExpr2.Expression, emitData);
                    arguments.Add(expr.Expr);
                }
                else
                {
                    throw new NotImplementedException("nullif does not support the input parameter");
                }

                return new ScalarFunction()
                {
                    ExtensionUri = FunctionsComparison.Uri,
                    ExtensionName = FunctionsComparison.NullIf,
                    Arguments = arguments
                };
            });

            sqlFunctionRegister.RegisterScalarFunction("lower", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("lower must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsString.Uri,
                        ExtensionName = FunctionsString.Lower,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("lower does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("upper", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("upper must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsString.Uri,
                        ExtensionName = FunctionsString.Upper,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("upper does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("ltrim", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("ltrim must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsString.Uri,
                        ExtensionName = FunctionsString.LTrim,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("ltrim does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("rtrim", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("rtrim must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsString.Uri,
                        ExtensionName = FunctionsString.RTrim,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("rtrim does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("to_string", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("to_string must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsString.Uri,
                        ExtensionName = FunctionsString.To_String,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("to_string does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("guid", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("guid must have exactly one argument");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsGuid.Uri,
                        ExtensionName = FunctionsGuid.ParseGuid,
                        Arguments = new List<Expressions.Expression>() { expr.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("guid does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("strftime", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 2)
                {
                    throw new InvalidOperationException("strftime must have exactly two arguments");
                }
                if (f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr1 &&
                f.Args[1] is FunctionArg.Unnamed unnamed2 && unnamed2.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr2)
                {
                    var expr1 = visitor.Visit(funcExpr1.Expression, emitData);
                    var expr2 = visitor.Visit(funcExpr2.Expression, emitData);
                    return new ScalarFunction()
                    {
                        ExtensionUri = FunctionsDatetime.Uri,
                        ExtensionName = FunctionsDatetime.Strftime,
                        Arguments = new List<Expressions.Expression>() { expr1.Expr, expr2.Expr }
                    };
                }
                else
                {
                    throw new NotImplementedException("stftime does not support the input parameter");
                }
            });
            
            sqlFunctionRegister.RegisterScalarFunction("gettimestamp", (f, visitor, emitData) =>
            {
                return new ScalarFunction()
                {
                    ExtensionUri = FunctionsDatetime.Uri,
                    ExtensionName = FunctionsDatetime.GetTimestamp,
                    Arguments = new List<Expressions.Expression>()
                };
            });

            sqlFunctionRegister.RegisterScalarFunction("map", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count % 2 != 0)
                {
                    throw new InvalidOperationException("Map must have an even number of arguments, one for key and one for value.");
                }
                MapNestedExpression mapNestedExpression = new MapNestedExpression
                {
                    KeyValues = new List<KeyValuePair<Expressions.Expression, Expressions.Expression>>()
                };
                for (int i = 0; i < f.Args.Count; i += 2)
                {
                    var keyArg = f.Args[i];
                    var valArg = f.Args[i + 1];
                    if (keyArg is FunctionArg.Unnamed keyunnamed && keyunnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression keyFuncExprUnnamed)
                    {
                        var keyExpr = visitor.Visit(keyFuncExprUnnamed.Expression, emitData);
                        
                        if (valArg is FunctionArg.Unnamed valunnamed && valunnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression valFuncExprUnnamed)
                        {
                            var valExpr = visitor.Visit(valFuncExprUnnamed.Expression, emitData);
                            mapNestedExpression.KeyValues.Add(new KeyValuePair<Expressions.Expression, Expressions.Expression>(
                                keyExpr.Expr,
                                valExpr.Expr
                                ));
                        }
                        else
                        {
                            throw new InvalidOperationException("map does not support the input parameter");
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException("map does not support the input parameter");
                    }
                }
                return mapNestedExpression;
            });

            sqlFunctionRegister.RegisterScalarFunction("list", (f, visitor, emitData) =>
            {
                if (f.Args == null)
                {
                    throw new InvalidOperationException("List must have an argument list.");
                }
                ListNestedExpression mapNestedExpression = new ListNestedExpression
                {
                    Values = new List<Expressions.Expression>()
                };
                for (int i = 0; i < f.Args.Count; i += 1)
                {
                    var arg = f.Args[i];
                    if (arg is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExprUnnamed)
                    {
                        var expr = visitor.Visit(funcExprUnnamed.Expression, emitData);
                        mapNestedExpression.Values.Add(expr.Expr);
                    }
                    else
                    {
                        throw new InvalidOperationException("list does not support the input parameter");
                    }
                }
                return mapNestedExpression;
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

            sqlFunctionRegister.RegisterAggregateFunction("sum", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("sum must have exactly one argument, and not be '*'");
                }
                if ((f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("sum must have exactly one argument, and not be '*'");
                }
                if (f.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;
                    return new AggregateFunction()
                    {
                        ExtensionUri = FunctionsArithmetic.Uri,
                        ExtensionName = FunctionsArithmetic.Sum,
                        Arguments = new List<Expressions.Expression>() { argExpr }
                    };
                }
                throw new InvalidOperationException("sum must have exactly one argument, and not be '*'");
            });

            sqlFunctionRegister.RegisterAggregateFunction("sum0", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("sum0 must have exactly one argument, and not be '*'");
                }
                if ((f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("sum0 must have exactly one argument, and not be '*'");
                }
                if (f.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;
                    return new AggregateFunction()
                    {
                        ExtensionUri = FunctionsArithmetic.Uri,
                        ExtensionName = FunctionsArithmetic.Sum0,
                        Arguments = new List<Expressions.Expression>() { argExpr }
                    };
                }
                throw new InvalidOperationException("sum0 must have exactly one argument, and not be '*'");
            });

            RegisterSingleVariableFunction(sqlFunctionRegister, "min", FunctionsArithmetic.Uri, FunctionsArithmetic.Min);
            RegisterSingleVariableFunction(sqlFunctionRegister, "max", FunctionsArithmetic.Uri, FunctionsArithmetic.Max);

            sqlFunctionRegister.RegisterAggregateFunction("list_agg", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException("list_agg must have exactly one argument, and not be '*'");
                }
                if ((f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("list_agg must have exactly one argument, and not be '*'");
                }
                if (f.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;
                    return new AggregateFunction()
                    {
                        ExtensionUri = FunctionsList.Uri,
                        ExtensionName = FunctionsList.ListAgg,
                        Arguments = new List<Expressions.Expression>() { argExpr }
                    };
                }
                throw new InvalidOperationException("list_agg must have exactly one argument, and not be '*'");
            });

            sqlFunctionRegister.RegisterAggregateFunction("string_agg", (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 2)
                {
                    throw new InvalidOperationException("string_agg must have exactly two arguments, and not be '*'");
                }
                if ((f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("string_agg must have exactly two arguments, and not be '*'");
                }
                if ((f.Args[1] is FunctionArg.Unnamed unnamed2 && unnamed2.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("string_agg must have exactly two arguments, and not be '*'");
                }
                if (f.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr &&
                f.Args[1] is FunctionArg.Unnamed arg2 && arg2.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr2)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;
                    var argExpr2 = visitor.Visit(funcExpr2.Expression, emitData).Expr;
                    return new AggregateFunction()
                    {
                        ExtensionUri = FunctionsString.Uri,
                        ExtensionName = FunctionsString.StringAgg,
                        Arguments = new List<Expressions.Expression>() { argExpr, argExpr2 }
                    };
                }
                throw new InvalidOperationException("string_agg must have exactly two arguments, and not be '*'");
            });
        }

        private static void RegisterSingleVariableFunction(
            SqlFunctionRegister sqlFunctionRegister, 
            string functionName,
            string extensionUri,
            string extensionName)
        {
            sqlFunctionRegister.RegisterAggregateFunction(functionName, (f, visitor, emitData) =>
            {
                if (f.Args == null || f.Args.Count != 1)
                {
                    throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
                }
                if ((f.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
                }
                if (f.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;
                    return new AggregateFunction()
                    {
                        ExtensionUri = extensionUri,
                        ExtensionName = extensionName,
                        Arguments = new List<Expressions.Expression>() { argExpr }
                    };
                }
                throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
            });
        }

        private static ExpressionData VisitCoalesce(SqlParser.Ast.Expression.Function function, SqlExpressionVisitor visitor, EmitData state)
        {
            var coalesceFunction = new ScalarFunction()
            {
                ExtensionUri = FunctionsComparison.Uri,
                ExtensionName = FunctionsComparison.Coalesce,
                Arguments = new List<Expressions.Expression>()
            };

            {
                Debug.Assert(function.Args != null);
                for (int i = 0; i < function.Args.Count; i++)
                {
                    var arg = function.Args[i];
                    if (arg is FunctionArg.Unnamed unnamed)
                    {
                        if (unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                        {
                            var expr = visitor.Visit(funcExpr.Expression, state);
                            coalesceFunction.Arguments.Add(expr.Expr);
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
            return new ExpressionData(coalesceFunction, "$coalesce");
        }
    }
}

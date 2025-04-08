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

using FlowtideDotNet.Substrait.Exceptions;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Sql.Internal.TableFunctions;
using FlowtideDotNet.Substrait.Type;
using SqlParser;
using SqlParser.Ast;
using System.Diagnostics;
using static SqlParser.Ast.WindowType;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal static class BuiltInSqlFunctions
    {
        internal static FunctionArgumentList GetFunctionArguments(FunctionArguments arguments)
        {
            if (arguments is FunctionArguments.List listArguments)
            {
                return listArguments.ArgumentList;
            }
            if (arguments is FunctionArguments.None noneArguments)
            {
                return new FunctionArgumentList(new SqlParser.Sequence<FunctionArg>());
            }
            if (arguments is FunctionArguments.Subquery subQueryArgument)
            {
                throw new SubstraitParseException("Subquery is not supported as an argument");
            }
            else
            {
                throw new SubstraitParseException("Unknown function argument type");
            }
        }
        public static void AddBuiltInFunctions(SqlFunctionRegister sqlFunctionRegister)
        {
            sqlFunctionRegister.RegisterScalarFunction("ceiling", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("ceiling must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsRounding.Uri,
                            ExtensionName = FunctionsRounding.Ceil,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        expr.Type
                        );
                }
                else
                {
                    throw new NotImplementedException("ceiling does not support the input parameter");
                }
            });
            sqlFunctionRegister.RegisterScalarFunction("round", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("round must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsRounding.Uri,
                            ExtensionName = FunctionsRounding.Round,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        expr.Type
                        );
                }
                else
                {
                    throw new NotImplementedException("round does not support the input parameter");
                }
            });
            sqlFunctionRegister.RegisterScalarFunction("coalesce", (f, visitor, emitData) =>
            {
                var exprData = VisitCoalesce(f, visitor, emitData);
                return new ScalarResponse(exprData.Expr, exprData.Type);
            });

            sqlFunctionRegister.RegisterScalarFunction("concat", (f, visitor, emitData) =>
            {
                var exprData = VisitConcat(f, visitor, emitData);
                return new ScalarResponse(exprData.Expr, exprData.Type);
            });

            sqlFunctionRegister.RegisterScalarFunction("greatest", (f, visitor, emitData) =>
            {
                var exprData = VisitGreatest(f, visitor, emitData);
                return new ScalarResponse(exprData.Expr, exprData.Type);
            });

            sqlFunctionRegister.RegisterScalarFunction("is_infinite", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("is_infinite must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.isInfinite,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        new BoolType() { Nullable = true }
                        );
                }
                else
                {
                    throw new NotImplementedException("is_infinite does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("is_finite", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("is_finite must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.IsFinite,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        new BoolType() { Nullable = true }
                        );
                }
                else
                {
                    throw new NotImplementedException("is_finite does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("is_nan", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("is_nan must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.IsNan,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        new BoolType() { Nullable = true }
                        );
                }
                else
                {
                    throw new NotImplementedException("is_nan does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("nullif", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 2)
                {
                    throw new InvalidOperationException("nullif must have exactly two arguments");
                }
                SubstraitBaseType returnType = new AnyType();
                var arguments = new List<Expressions.Expression>();
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    arguments.Add(expr.Expr);
                    returnType = expr.Type;
                    returnType.Nullable = true;
                }
                else
                {
                    throw new NotImplementedException("nullif does not support the input parameter");
                }
                if (argList.Args[1] is FunctionArg.Unnamed unnamed2 && unnamed2.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr2)
                {
                    var expr = visitor.Visit(funcExpr2.Expression, emitData);
                    arguments.Add(expr.Expr);
                }
                else
                {
                    throw new NotImplementedException("nullif does not support the input parameter");
                }

                return new ScalarResponse(
                    new ScalarFunction()
                    {
                        ExtensionUri = FunctionsComparison.Uri,
                        ExtensionName = FunctionsComparison.NullIf,
                        Arguments = arguments
                    },
                    returnType
                    );
            });

            sqlFunctionRegister.RegisterScalarFunction("lower", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("lower must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);

                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsString.Uri,
                            ExtensionName = FunctionsString.Lower,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        expr.Type
                        );
                }
                else
                {
                    throw new NotImplementedException("lower does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("upper", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("upper must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);

                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsString.Uri,
                            ExtensionName = FunctionsString.Upper,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        expr.Type
                        );
                }
                else
                {
                    throw new NotImplementedException("upper does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("ltrim", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("ltrim must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);
                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsString.Uri,
                            ExtensionName = FunctionsString.LTrim,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        new StringType() { Nullable = true }
                        );
                }
                else
                {
                    throw new NotImplementedException("ltrim does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("rtrim", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("rtrim must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);

                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsString.Uri,
                            ExtensionName = FunctionsString.RTrim,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        new StringType() { Nullable = true }
                        );
                }
                else
                {
                    throw new NotImplementedException("rtrim does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("to_string", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("to_string must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);

                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsString.Uri,
                            ExtensionName = FunctionsString.To_String,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        new StringType() { Nullable = true }
                        );
                }
                else
                {
                    throw new NotImplementedException("to_string does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("guid", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("guid must have exactly one argument");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var expr = visitor.Visit(funcExpr.Expression, emitData);

                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsGuid.Uri,
                            ExtensionName = FunctionsGuid.ParseGuid,
                            Arguments = new List<Expressions.Expression>() { expr.Expr }
                        },
                        new StringType() { Nullable = true }
                        );
                }
                else
                {
                    throw new NotImplementedException("guid does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("strftime", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 2)
                {
                    throw new InvalidOperationException("strftime must have exactly two arguments");
                }
                if (argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr1 &&
                argList.Args[1] is FunctionArg.Unnamed unnamed2 && unnamed2.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr2)
                {
                    var expr1 = visitor.Visit(funcExpr1.Expression, emitData);
                    var expr2 = visitor.Visit(funcExpr2.Expression, emitData);

                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = FunctionsDatetime.Uri,
                            ExtensionName = FunctionsDatetime.Strftime,
                            Arguments = new List<Expressions.Expression>() { expr1.Expr, expr2.Expr }
                        },
                        new StringType() { Nullable = true }
                        );
                }
                else
                {
                    throw new NotImplementedException("stftime does not support the input parameter");
                }
            });

            sqlFunctionRegister.RegisterScalarFunction("gettimestamp", (f, visitor, emitData) =>
            {
                return new ScalarResponse(
                    new ScalarFunction()
                    {
                        ExtensionUri = FunctionsDatetime.Uri,
                        ExtensionName = FunctionsDatetime.GetTimestamp,
                        Arguments = new List<Expressions.Expression>()
                    },
                    new Int64Type()
                    );
            });

            sqlFunctionRegister.RegisterScalarFunction("map", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count % 2 != 0)
                {
                    throw new InvalidOperationException("Map must have an even number of arguments, one for key and one for value.");
                }
                MapNestedExpression mapNestedExpression = new MapNestedExpression
                {
                    KeyValues = new List<KeyValuePair<Expressions.Expression, Expressions.Expression>>()
                };
                SubstraitBaseType? keyType = new AnyType();
                SubstraitBaseType? valueType = new AnyType();
                for (int i = 0; i < argList.Args.Count; i += 2)
                {
                    var keyArg = argList.Args[i];
                    var valArg = argList.Args[i + 1];
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
                return new ScalarResponse(mapNestedExpression, new MapType(keyType, valueType));
            });

            sqlFunctionRegister.RegisterScalarFunction("list", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null)
                {
                    throw new InvalidOperationException("List must have an argument list.");
                }
                SubstraitBaseType? valueType = default;
                ListNestedExpression mapNestedExpression = new ListNestedExpression
                {
                    Values = new List<Expressions.Expression>()
                };
                for (int i = 0; i < argList.Args.Count; i += 1)
                {
                    var arg = argList.Args[i];
                    if (arg is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExprUnnamed)
                    {
                        var expr = visitor.Visit(funcExprUnnamed.Expression, emitData);

                        if (valueType == null)
                        {
                            valueType = expr.Type;
                        }
                        else if (valueType != expr.Type)
                        {
                            valueType = new AnyType();
                        }

                        mapNestedExpression.Values.Add(expr.Expr);
                    }
                    else
                    {
                        throw new InvalidOperationException("list does not support the input parameter");
                    }
                }
                if (valueType == null)
                {
                    valueType = new AnyType();
                }
                return new ScalarResponse(mapNestedExpression, new ListType(valueType));
            });


            sqlFunctionRegister.RegisterAggregateFunction("count", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("count must have exactly one argument, and be '*'");
                }
                if (!(argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("count must have exactly one argument, and be '*'");
                }
                return new AggregateResponse(
                    new AggregateFunction()
                    {
                        ExtensionUri = FunctionsAggregateGeneric.Uri,
                        ExtensionName = FunctionsAggregateGeneric.Count,
                        Arguments = new List<Expressions.Expression>()
                    },
                    new Int64Type()
                    );
            });

            sqlFunctionRegister.RegisterAggregateFunction("sum", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("sum must have exactly one argument, and not be '*'");
                }
                if ((argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("sum must have exactly one argument, and not be '*'");
                }
                if (argList.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData);

                    SubstraitBaseType returnType = new AnyType();
                    if (argExpr.Type.Type == SubstraitType.Fp64)
                    {
                        returnType = new Fp64Type();
                    }
                    else if (argExpr.Type.Type == SubstraitType.Int64)
                    {
                        returnType = new Int64Type();
                    }
                    else if (argExpr.Type.Type == SubstraitType.Int32)
                    {
                        returnType = new Int64Type();
                    }
                    else if (argExpr.Type.Type == SubstraitType.Fp32)
                    {
                        returnType = new Fp64Type();
                    }

                    return new AggregateResponse(
                        new AggregateFunction()
                        {
                            ExtensionUri = FunctionsArithmetic.Uri,
                            ExtensionName = FunctionsArithmetic.Sum,
                            Arguments = new List<Expressions.Expression>() { argExpr.Expr }
                        },
                        returnType
                        );
                }
                throw new InvalidOperationException("sum must have exactly one argument, and not be '*'");
            });

            sqlFunctionRegister.RegisterAggregateFunction("sum0", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("sum0 must have exactly one argument, and not be '*'");
                }
                if ((argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("sum0 must have exactly one argument, and not be '*'");
                }
                if (argList.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData);

                    SubstraitBaseType returnType = new AnyType();
                    if (argExpr.Type.Type == SubstraitType.Fp64)
                    {
                        returnType = new Fp64Type();
                    }
                    else if (argExpr.Type.Type == SubstraitType.Int64)
                    {
                        returnType = new Int64Type();
                    }
                    else if (argExpr.Type.Type == SubstraitType.Int32)
                    {
                        returnType = new Int64Type();
                    }
                    else if (argExpr.Type.Type == SubstraitType.Fp32)
                    {
                        returnType = new Fp64Type();
                    }

                    return new AggregateResponse(
                        new AggregateFunction()
                        {
                            ExtensionUri = FunctionsArithmetic.Uri,
                            ExtensionName = FunctionsArithmetic.Sum0,
                            Arguments = new List<Expressions.Expression>() { argExpr.Expr }
                        },
                        returnType
                        );
                }
                throw new InvalidOperationException("sum0 must have exactly one argument, and not be '*'");
            });

            RegisterSingleVariableFunction(sqlFunctionRegister, "min", FunctionsArithmetic.Uri, FunctionsArithmetic.Min, new AnyType());
            RegisterSingleVariableFunction(sqlFunctionRegister, "max", FunctionsArithmetic.Uri, FunctionsArithmetic.Max, new AnyType());

            sqlFunctionRegister.RegisterAggregateFunction("list_agg", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("list_agg must have exactly one argument, and not be '*'");
                }
                if ((argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("list_agg must have exactly one argument, and not be '*'");
                }
                if (argList.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData);

                    return new AggregateResponse(
                        new AggregateFunction()
                        {
                            ExtensionUri = FunctionsList.Uri,
                            ExtensionName = FunctionsList.ListAgg,
                            Arguments = new List<Expressions.Expression>() { argExpr.Expr }
                        },
                        new ListType(argExpr.Type)
                        );
                }
                throw new InvalidOperationException("list_agg must have exactly one argument, and not be '*'");
            });

            sqlFunctionRegister.RegisterAggregateFunction("list_union_distinct_agg", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException("list_union_distinct_agg must have exactly one argument, and not be '*'");
                }
                if ((argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("list_union_distinct_agg must have exactly one argument, and not be '*'");
                }
                if (argList.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData);

                    return new AggregateResponse(
                        new AggregateFunction()
                        {
                            ExtensionUri = FunctionsList.Uri,
                            ExtensionName = FunctionsList.ListUnionDistinctAgg,
                            Arguments = new List<Expressions.Expression>() { argExpr.Expr }
                        },
                        new ListType(argExpr.Type)
                        );
                }
                throw new InvalidOperationException("list_union_distinct_agg must have exactly one argument, and not be '*'");
            });

            sqlFunctionRegister.RegisterAggregateFunction("string_agg", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 2)
                {
                    throw new InvalidOperationException("string_agg must have exactly two arguments, and not be '*'");
                }
                if ((argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("string_agg must have exactly two arguments, and not be '*'");
                }
                if ((argList.Args[1] is FunctionArg.Unnamed unnamed2 && unnamed2.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException("string_agg must have exactly two arguments, and not be '*'");
                }
                if (argList.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr &&
                argList.Args[1] is FunctionArg.Unnamed arg2 && arg2.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr2)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;
                    var argExpr2 = visitor.Visit(funcExpr2.Expression, emitData).Expr;

                    return new AggregateResponse(
                        new AggregateFunction()
                        {
                            ExtensionUri = FunctionsString.Uri,
                            ExtensionName = FunctionsString.StringAgg,
                            Arguments = new List<Expressions.Expression>() { argExpr, argExpr2 }
                        },
                        new StringType() { Nullable = true }
                        );
                }
                throw new InvalidOperationException("string_agg must have exactly two arguments, and not be '*'");
            });

            sqlFunctionRegister.RegisterAggregateFunction("surrogate_key_int64", (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args != null && argList.Args.Count != 0)
                {
                    throw new InvalidOperationException("surrogate_key_int64 must have exactly zero arguments.");
                }

                return new AggregateResponse(
                        new AggregateFunction()
                        {
                            ExtensionUri = FunctionsAggregateGeneric.Uri,
                            ExtensionName = FunctionsAggregateGeneric.SurrogateKeyInt64,
                            Arguments = new List<Expressions.Expression>()
                        },
                        new StringType() { Nullable = true }
                        );
            });



            RegisterTwoVariableScalarFunction(sqlFunctionRegister, "power", FunctionsArithmetic.Uri, FunctionsArithmetic.Power);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "sqrt", FunctionsArithmetic.Uri, FunctionsArithmetic.Sqrt);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "exp", FunctionsArithmetic.Uri, FunctionsArithmetic.Exp);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "cos", FunctionsArithmetic.Uri, FunctionsArithmetic.Cos);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "sin", FunctionsArithmetic.Uri, FunctionsArithmetic.Sin);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "tan", FunctionsArithmetic.Uri, FunctionsArithmetic.Tan);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "cosh", FunctionsArithmetic.Uri, FunctionsArithmetic.Cosh);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "sinh", FunctionsArithmetic.Uri, FunctionsArithmetic.Sinh);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "tanh", FunctionsArithmetic.Uri, FunctionsArithmetic.Tanh);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "acos", FunctionsArithmetic.Uri, FunctionsArithmetic.Acos);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "asin", FunctionsArithmetic.Uri, FunctionsArithmetic.Asin);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "atan", FunctionsArithmetic.Uri, FunctionsArithmetic.Atan);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "acosh", FunctionsArithmetic.Uri, FunctionsArithmetic.Acosh);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "asinh", FunctionsArithmetic.Uri, FunctionsArithmetic.Asinh);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "atanh", FunctionsArithmetic.Uri, FunctionsArithmetic.Atanh);
            RegisterTwoVariableScalarFunction(sqlFunctionRegister, "atan2", FunctionsArithmetic.Uri, FunctionsArithmetic.Atan2);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "radians", FunctionsArithmetic.Uri, FunctionsArithmetic.Radians);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "degrees", FunctionsArithmetic.Uri, FunctionsArithmetic.Degrees);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "abs", FunctionsArithmetic.Uri, FunctionsArithmetic.Abs);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "sign", FunctionsArithmetic.Uri, FunctionsArithmetic.Sign);

            RegisterTwoVariableScalarFunction(sqlFunctionRegister, "starts_with", FunctionsString.Uri, FunctionsString.StartsWith);
            RegisterThreeVariableScalarFunction(sqlFunctionRegister, "replace", FunctionsString.Uri, FunctionsString.Replace);

            RegisterOneVariableScalarFunction(sqlFunctionRegister, "string_base64_encode", FunctionsString.Uri, FunctionsString.StringBase64Encode);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "string_base64_decode", FunctionsString.Uri, FunctionsString.StringBase64Decode);

            RegisterOneVariableScalarFunction(sqlFunctionRegister, "len", FunctionsString.Uri, FunctionsString.CharLength);
            RegisterTwoVariableScalarFunction(sqlFunctionRegister, "strpos", FunctionsString.Uri, FunctionsString.StrPos);

            RegisterTwoVariableScalarFunction(sqlFunctionRegister, "string_split", FunctionsString.Uri, FunctionsString.StringSplit);
            RegisterTwoVariableScalarFunction(sqlFunctionRegister, "regexp_string_split", FunctionsString.Uri, FunctionsString.RegexStringSplit);

            RegisterOneVariableScalarFunction(sqlFunctionRegister, "to_json", FunctionsString.Uri, FunctionsString.ToJson);
            RegisterOneVariableScalarFunction(sqlFunctionRegister, "from_json", FunctionsString.Uri, FunctionsString.FromJson);

            RegisterOneVariableScalarFunction(sqlFunctionRegister, "floor_timestamp_day", FunctionsDatetime.Uri, FunctionsDatetime.FloorTimestampDay);

            // Table functions
            UnnestSqlFunction.AddUnnest(sqlFunctionRegister);

            // WindowFunction
            RegisterSingleVariableWindowFunction(sqlFunctionRegister, "sum", FunctionsArithmetic.Uri, FunctionsArithmetic.Sum, new AnyType(), true, false);
            RegisterZeroVariableWindowFunction(sqlFunctionRegister, "row_number", FunctionsArithmetic.Uri, FunctionsArithmetic.RowNumber, new Int64Type(), false, true);
            RegisterZeroVariableWindowFunction(sqlFunctionRegister, "surrogate_key_int64", FunctionsAggregateGeneric.Uri, FunctionsAggregateGeneric.SurrogateKeyInt64, new Int64Type(), false, false);

            sqlFunctionRegister.RegisterWindowFunction("lead",
                (func, visitor, emitData) =>
                {
                    var argList = GetFunctionArguments(func.Args);
                    if (argList.Args == null || argList.Args.Count < 1)
                    {
                        throw new InvalidOperationException($"lead must have exactly at least one argument, and not be '*'");
                    }
                    if ((argList.Args[0] is FunctionArg.Unnamed unnamed0 && unnamed0.FunctionArgExpression is FunctionArgExpression.Wildcard))
                    {
                        throw new InvalidOperationException($"lead must have at least one argument, and not be '*'");
                    }
                    if (argList.Args.Count > 3)
                    {
                        throw new InvalidOperationException($"lead must have at most three arguments, and not be '*'");
                    }

                    if (func.Over is WindowSpecType windowSpecType)
                    {
                        if (windowSpecType.Spec.OrderBy == null)
                        {
                            throw new SubstraitParseException($"'lead' function must have an order by clause");
                        }
                        if (windowSpecType.Spec.WindowFrame != null)
                        {
                            if (windowSpecType.Spec.WindowFrame.Units == WindowFrameUnit.Rows)
                            {
                                throw new SubstraitParseException($"'lead' function does not support ROWS frame");
                            }
                        }
                    }

                    WindowFunction windowFunc = new WindowFunction()
                    {
                        Arguments = new List<Expressions.Expression>(),
                        ExtensionName = FunctionsArithmetic.Lead,
                        ExtensionUri = FunctionsArithmetic.Uri,
                    };

                    SubstraitBaseType? returnType = null;
                    for (int i = 0; i < argList.Args.Count; i++)
                    {
                        var arg = argList.Args[i];
                        if (arg is FunctionArg.Unnamed unnamed)
                        {
                            if (unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                            {
                                var expr = visitor.Visit(funcExpr.Expression, emitData);
                                windowFunc.Arguments.Add(expr.Expr);

                                if (i == 0)
                                {
                                    returnType = expr.Type;
                                }
                                else if (returnType != expr.Type && i == 2)
                                {
                                    returnType = AnyType.Instance;
                                }
                            }
                            else
                            {
                                throw new NotImplementedException("lead does not support the input parameter");
                            }
                        }
                        else
                        {
                            throw new NotImplementedException("lead does not support the input parameter");
                        }
                    }

                    if (returnType == null)
                    {
                        returnType = AnyType.Instance;
                    }

                    return new WindowResponse(windowFunc, returnType);
                });

            // Check functions
            RegisterThreeVariableScalarFunction(sqlFunctionRegister, "check_value", FunctionsCheck.Uri, FunctionsCheck.CheckValue);
            RegisterTwoVariableScalarFunction(sqlFunctionRegister, "check_true", FunctionsCheck.Uri, FunctionsCheck.CheckTrue);
        }

        private static void RegisterSingleVariableFunction(
            SqlFunctionRegister sqlFunctionRegister,
            string functionName,
            string extensionUri,
            string extensionName,
            SubstraitBaseType returnType)
        {
            sqlFunctionRegister.RegisterAggregateFunction(functionName, (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
                }
                if ((argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
                }
                if (argList.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;

                    return new AggregateResponse(
                        new AggregateFunction()
                        {
                            ExtensionUri = extensionUri,
                            ExtensionName = extensionName,
                            Arguments = new List<Expressions.Expression>() { argExpr }
                        },
                        returnType
                        );
                }
                throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
            });
        }

        private static void RegisterOneVariableScalarFunction(
            SqlFunctionRegister sqlFunctionRegister,
            string functionName,
            string extensionUri,
            string extensionName)
        {
            sqlFunctionRegister.RegisterScalarFunction(functionName, (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
                }
                if ((argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
                }
                if (argList.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;

                    // For now, anytype is returned
                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = extensionUri,
                            ExtensionName = extensionName,
                            Arguments = new List<Expressions.Expression>() { argExpr }
                        },
                        new AnyType()
                        );
                }
                throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
            });
        }

        private static void RegisterTwoVariableScalarFunction(
            SqlFunctionRegister sqlFunctionRegister,
            string functionName,
            string extensionUri,
            string extensionName)
        {
            sqlFunctionRegister.RegisterScalarFunction(functionName, (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 2)
                {
                    throw new InvalidOperationException($"{functionName} must have exactly two arguments, and not be '*'");
                }
                if ((argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException($"{functionName} must have exactly two arguments, and not be '*'");
                }
                if ((argList.Args[1] is FunctionArg.Unnamed unnamed2 && unnamed2.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException($"{functionName} must have exactly two arguments, and not be '*'");
                }
                if (argList.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr &&
                argList.Args[1] is FunctionArg.Unnamed arg2 && arg2.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr2)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;
                    var argExpr2 = visitor.Visit(funcExpr2.Expression, emitData).Expr;

                    // For now, anytype is returned
                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = extensionUri,
                            ExtensionName = extensionName,
                            Arguments = new List<Expressions.Expression>() { argExpr, argExpr2 }
                        },
                        new AnyType()
                        );
                }
                throw new InvalidOperationException($"{functionName} must have exactly two arguments, and not be '*'");
            });
        }

        private static void RegisterThreeVariableScalarFunction(
            SqlFunctionRegister sqlFunctionRegister,
            string functionName,
            string extensionUri,
            string extensionName)
        {
            sqlFunctionRegister.RegisterScalarFunction(functionName, (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 3)
                {
                    throw new InvalidOperationException($"{functionName} must have exactly three arguments, and not be '*'");
                }
                if ((argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException($"{functionName} must have exactly three arguments, and not be '*'");
                }
                if ((argList.Args[1] is FunctionArg.Unnamed unnamed2 && unnamed2.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException($"{functionName} must have exactly three arguments, and not be '*'");
                }
                if ((argList.Args[2] is FunctionArg.Unnamed unnamed3 && unnamed3.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException($"{functionName} must have exactly three arguments, and not be '*'");
                }
                if (argList.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr &&
                argList.Args[1] is FunctionArg.Unnamed arg2 && arg2.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr2 &&
                argList.Args[2] is FunctionArg.Unnamed arg3 && arg3.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr3)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;
                    var argExpr2 = visitor.Visit(funcExpr2.Expression, emitData).Expr;
                    var argExpr3 = visitor.Visit(funcExpr3.Expression, emitData).Expr;

                    // For now, anytype is returned
                    return new ScalarResponse(
                        new ScalarFunction()
                        {
                            ExtensionUri = extensionUri,
                            ExtensionName = extensionName,
                            Arguments = new List<Expressions.Expression>() { argExpr, argExpr2, argExpr3 }
                        },
                        new AnyType()
                        );
                }
                throw new InvalidOperationException($"{functionName} must have exactly three arguments, and not be '*'");
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

            SubstraitBaseType? returnType = default;

            {
                var argList = GetFunctionArguments(function.Args);
                Debug.Assert(argList.Args != null);
                for (int i = 0; i < argList.Args.Count; i++)
                {
                    var arg = argList.Args[i];
                    if (arg is FunctionArg.Unnamed unnamed)
                    {
                        if (unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                        {
                            var expr = visitor.Visit(funcExpr.Expression, state);

                            if (returnType == null)
                            {
                                returnType = expr.Type;
                            }
                            else if (returnType != expr.Type)
                            {
                                returnType = new AnyType();
                            }

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

            if (returnType == null)
            {
                throw new InvalidOperationException("Coalesce must have at least one argument");
            }

            return new ExpressionData(coalesceFunction, "$coalesce", returnType);
        }

        private static ExpressionData VisitConcat(SqlParser.Ast.Expression.Function function, SqlExpressionVisitor visitor, EmitData state)
        {
            var coalesceFunction = new ScalarFunction()
            {
                ExtensionUri = FunctionsString.Uri,
                ExtensionName = FunctionsString.Concat,
                Arguments = new List<Expressions.Expression>()
            };

            SubstraitBaseType? returnType = default;

            {
                var argList = GetFunctionArguments(function.Args);
                Debug.Assert(argList.Args != null);
                for (int i = 0; i < argList.Args.Count; i++)
                {
                    var arg = argList.Args[i];
                    if (arg is FunctionArg.Unnamed unnamed)
                    {
                        if (unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                        {
                            var expr = visitor.Visit(funcExpr.Expression, state);

                            if (returnType == null)
                            {
                                returnType = expr.Type;
                            }
                            else if (returnType != expr.Type)
                            {
                                returnType = new AnyType();
                            }

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

            if (returnType == null)
            {
                throw new InvalidOperationException("Coalesce must have at least one argument");
            }

            return new ExpressionData(coalesceFunction, "$concat", returnType);
        }

        private static ExpressionData VisitGreatest(SqlParser.Ast.Expression.Function function, SqlExpressionVisitor visitor, EmitData state)
        {
            var greatestFunction = new ScalarFunction()
            {
                ExtensionUri = FunctionsComparison.Uri,
                ExtensionName = FunctionsComparison.Greatest,
                Arguments = new List<Expressions.Expression>()
            };

            SubstraitBaseType? returnType = default;

            {
                var argList = GetFunctionArguments(function.Args);
                Debug.Assert(argList.Args != null);

                if (argList.Args.Count < 2)
                {
                    throw new SubstraitParseException("Greatest must have at least two arguments");
                }

                for (int i = 0; i < argList.Args.Count; i++)
                {
                    var arg = argList.Args[i];
                    if (arg is FunctionArg.Unnamed unnamed)
                    {
                        if (unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                        {
                            var expr = visitor.Visit(funcExpr.Expression, state);

                            if (returnType == null)
                            {
                                returnType = expr.Type;
                            }
                            else if (returnType != expr.Type)
                            {
                                returnType = new AnyType();
                            }

                            greatestFunction.Arguments.Add(expr.Expr);
                        }
                        else
                        {
                            throw new NotImplementedException("Greatest does not support the input parameter");
                        }
                    }
                    else
                    {
                        throw new NotImplementedException("Greatest does not support the input parameter");
                    }
                }
            }

            if (returnType == null)
            {
                throw new InvalidOperationException("Greatest must have at least two arguments");
            }

            return new ExpressionData(greatestFunction, "$greatest", returnType);
        }

        private static WindowBound? ParseWindowBound(WindowFrameBound? windowFrame)
        {
            if (windowFrame == null)
            {
                return null;
            }
            if (windowFrame is WindowFrameBound.Following startFollowingBound)
            {
                if (startFollowingBound.Expression is SqlParser.Ast.Expression.LiteralValue literalVal &&
                    literalVal.Value is Value.Number startNumberString &&
                    long.TryParse(startNumberString.Value, out var startNumber))
                {
                    return new FollowingRowWindowBound()
                    {
                        Offset = startNumber
                    };
                }
                else if (startFollowingBound.Expression == null)
                {
                    return new UnboundedWindowBound();
                }
                else
                {
                    throw new SubstraitParseException("Window function with ROWS frame must have a literal integer value for the following bound");
                }
            }
            if (windowFrame is WindowFrameBound.Preceding startPreceedingBound)
            {
                if (startPreceedingBound.Expression is SqlParser.Ast.Expression.LiteralValue literalVal &&
                    literalVal.Value is Value.Number startNumberString &&
                    long.TryParse(startNumberString.Value, out var startNumber))
                {
                    return new PreceedingRowWindowBound()
                    {
                        Offset = startNumber
                    };
                }
                else if (startPreceedingBound.Expression == null)
                {
                    return new UnboundedWindowBound();
                }
                else
                {
                    throw new SubstraitParseException("Window function with ROWS frame must have a literal integer value for the preceeding bound");
                }
            }
            if (windowFrame is WindowFrameBound.CurrentRow startCurrentRow)
            {
                return new CurrentRowWindowBound();
            }
            throw new NotImplementedException("Window function with ROWS have an unknown frame");
        }

        private static void RegisterSingleVariableWindowFunction(
            SqlFunctionRegister sqlFunctionRegister,
            string functionName,
            string extensionUri,
            string extensionName,
            SubstraitBaseType returnType,
            bool supportRowBounds,
            bool requireOrderBy)
        {
            sqlFunctionRegister.RegisterWindowFunction(functionName, (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args == null || argList.Args.Count != 1)
                {
                    throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
                }
                if ((argList.Args[0] is FunctionArg.Unnamed unnamed && unnamed.FunctionArgExpression is FunctionArgExpression.Wildcard))
                {
                    throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
                }
                if (argList.Args[0] is FunctionArg.Unnamed arg && arg.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                {
                    var argExpr = visitor.Visit(funcExpr.Expression, emitData).Expr;

                    WindowBound? lowerBound = default;
                    WindowBound? upperBound = default;
                    if (f.Over is WindowSpecType windowSpecType)
                    {
                        if (windowSpecType.Spec.OrderBy == null)
                        {
                            if (requireOrderBy)
                            {
                                throw new SubstraitParseException($"'{functionName}' function must have an order by clause");
                            }
                            // If order by is not set use the entire bounds
                            lowerBound = new UnboundedWindowBound();
                            upperBound = new UnboundedWindowBound();
                        }
                        if (windowSpecType.Spec.WindowFrame != null)
                        {
                            if (windowSpecType.Spec.WindowFrame.Units == WindowFrameUnit.Rows)
                            {
                                if (!supportRowBounds)
                                {
                                    throw new SubstraitParseException($"'{functionName}' function does not support ROWS frame");
                                }
                                lowerBound = ParseWindowBound(windowSpecType.Spec.WindowFrame.StartBound);
                                upperBound = ParseWindowBound(windowSpecType.Spec.WindowFrame.EndBound);
                            }
                        }
                        // If no window frame, and order by is set, the default is unbounded lower and to current row
                        else if (windowSpecType.Spec.OrderBy != null && supportRowBounds)
                        {
                            lowerBound = new UnboundedWindowBound();
                            upperBound = new CurrentRowWindowBound();
                        }
                    }

                    return new WindowResponse(
                        new WindowFunction()
                        {
                            ExtensionUri = extensionUri,
                            ExtensionName = extensionName,
                            Arguments = new List<Expressions.Expression>() { argExpr },
                            LowerBound = lowerBound,
                            UpperBound = upperBound
                        },
                        returnType
                        );
                }
                throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
            });
        }

        private static void RegisterZeroVariableWindowFunction(
            SqlFunctionRegister sqlFunctionRegister,
            string functionName,
            string extensionUri,
            string extensionName,
            SubstraitBaseType returnType,
            bool supportRowBounds,
            bool requireOrderBy)
        {
            sqlFunctionRegister.RegisterWindowFunction(functionName, (f, visitor, emitData) =>
            {
                var argList = GetFunctionArguments(f.Args);
                if (argList.Args != null && argList.Args.Count != 0)
                {
                    throw new InvalidOperationException($"{functionName} must have no arguments");
                }

                WindowBound? lowerBound = default;
                WindowBound? upperBound = default;
                if (f.Over is WindowSpecType windowSpecType)
                {
                    if (windowSpecType.Spec.OrderBy == null)
                    {
                        if (requireOrderBy)
                        {
                            throw new SubstraitParseException($"'{functionName}' function must have an order by clause");
                        }
                        // If order by is not set use the entire bounds
                        lowerBound = new UnboundedWindowBound();
                        upperBound = new UnboundedWindowBound();
                    }
                    if (windowSpecType.Spec.WindowFrame != null)
                    {
                        if (windowSpecType.Spec.WindowFrame.Units == WindowFrameUnit.Rows)
                        {
                            if (!supportRowBounds)
                            {
                                throw new SubstraitParseException($"'{functionName}' function does not support ROWS frame");
                            }
                            lowerBound = ParseWindowBound(windowSpecType.Spec.WindowFrame.StartBound);
                            upperBound = ParseWindowBound(windowSpecType.Spec.WindowFrame.EndBound);
                        }
                    }
                    // If no window frame, and order by is set, the default is unbounded lower and to current row
                    else if (windowSpecType.Spec.OrderBy != null && supportRowBounds)
                    {
                        lowerBound = new UnboundedWindowBound();
                        upperBound = new CurrentRowWindowBound();
                    }
                }

                return new WindowResponse(
                    new WindowFunction()
                    {
                        ExtensionUri = extensionUri,
                        ExtensionName = extensionName,
                        Arguments = new List<Expressions.Expression>(),
                        LowerBound = lowerBound,
                        UpperBound = upperBound
                    },
                    returnType
                    );
                throw new InvalidOperationException($"{functionName} must have exactly one argument, and not be '*'");
            });
        }
    }
}

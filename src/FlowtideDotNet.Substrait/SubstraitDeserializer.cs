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
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Protobuf = Substrait.Protobuf;

namespace FlowtideDotNet.Substrait
{
    public class SubstraitDeserializer
    {
        private sealed class ExpressionDeserializerImpl
        {
            private readonly Dictionary<uint, string> idToFunctionLookup = new Dictionary<uint, string>();
            private readonly Dictionary<uint, string> idToUserDefinedType = new Dictionary<uint, string>();

            public ExpressionDeserializerImpl(Protobuf.Plan plan)
            {
                foreach (var extension in plan.Extensions)
                {
                    if (extension.MappingTypeCase == Protobuf.SimpleExtensionDeclaration.MappingTypeOneofCase.ExtensionType)
                    {
                        idToUserDefinedType.Add(extension.ExtensionType.TypeAnchor, extension.ExtensionType.Name);
                    }
                    else if (extension.MappingTypeCase == Protobuf.SimpleExtensionDeclaration.MappingTypeOneofCase.ExtensionFunction)
                    {
                        var id = extension.ExtensionFunction.FunctionAnchor;
                        var uri = plan.ExtensionUris.First(x => x.ExtensionUriAnchor == extension.ExtensionFunction.ExtensionUriReference).Uri;
                        uri = uri.Substring(uri.LastIndexOf('/'));
                        var colonLocation = extension.ExtensionFunction.Name.IndexOf(':');

                        string? extensionName = default;
                        if (colonLocation == -1)
                        {
                            extensionName = extension.ExtensionFunction.Name;
                        }
                        else
                        {
                            extensionName = extension.ExtensionFunction.Name.Substring(0, colonLocation);
                        }
                        idToFunctionLookup.Add(id, $"{uri}:{extensionName}");
                    }
                    else
                    {
                        throw new NotImplementedException(extension.MappingTypeCase.ToString());
                    }
                       
                }
            }

            internal SubstraitBaseType GetType(Protobuf.Type type, ref Span<string> names)
            {
                switch (type.KindCase)
                {
                    case Protobuf.Type.KindOneofCase.String:

                        return new StringType()
                        {
                            Nullable = type.String.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    case Protobuf.Type.KindOneofCase.I32:
                        return new Int32Type()
                        {
                            Nullable = type.I32.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    case Protobuf.Type.KindOneofCase.Date:
                        return new DateType()
                        {
                            Nullable = type.Date.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    case Protobuf.Type.KindOneofCase.Fp64:
                        return new Fp64Type()
                        {
                            Nullable = type.Fp64.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    case Protobuf.Type.KindOneofCase.I64:
                        return new Int64Type()
                        {
                            Nullable = type.I64.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    case Protobuf.Type.KindOneofCase.Bool:
                        return new BoolType()
                        {
                            Nullable = type.Bool.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    case Protobuf.Type.KindOneofCase.Fp32:
                        return new Fp32Type()
                        {
                            Nullable = type.Fp32.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    case Protobuf.Type.KindOneofCase.UserDefined:
                        if (idToUserDefinedType.TryGetValue(type.UserDefined.TypeReference, out var typeName))
                        {
                            if (typeName.Equals("any", StringComparison.OrdinalIgnoreCase))
                            {
                                return new AnyType();
                            }
                            else
                            {
                                throw new NotImplementedException($"User defined type not implemented {typeName}");
                            }
                        }
                        else
                        {
                            throw new InvalidOperationException($"User defined type not found {type.UserDefined.TypeReference}");
                        }
                    case Protobuf.Type.KindOneofCase.Struct:
                        List<string> structNames = new List<string>();
                        List<SubstraitBaseType> structTypes = new List<SubstraitBaseType>();
                        for (int i = 0; i < type.Struct.Types_.Count; i++)
                        {
                            string? name = default;
                            if (names.Length > 0)
                            {
                                name = names[0];
                                names = names.Slice(1);
                            }
                            else
                            {
                                throw new InvalidOperationException("Not enough names for struct fields");
                            }
                            var structType = GetType(type.Struct.Types_[i], ref names);
                            structNames.Add(name);
                            structTypes.Add(structType);
                        }
                        return new NamedStruct()
                        {
                            Names = structNames,
                            Struct = new Struct()
                            {
                                Types = structTypes
                            }
                        };
                    case Protobuf.Type.KindOneofCase.List:
                        var elementType = GetType(type.List.Type, ref names);
                        return new ListType(elementType)
                        {
                            Nullable = type.List.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    case Protobuf.Type.KindOneofCase.Map:
                        var keyType = GetType(type.Map.Key, ref names);
                        var valueType = GetType(type.Map.Value, ref names);
                        return new MapType(keyType, valueType)
                        {
                            Nullable = type.Map.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    case Protobuf.Type.KindOneofCase.TimestampTz:
                        return new TimestampType()
                        {
                            Nullable = type.TimestampTz.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    case Protobuf.Type.KindOneofCase.Binary:
                        return new BinaryType()
                        {
                            Nullable = type.Binary.Nullability != Protobuf.Type.Types.Nullability.Required
                        };
                    default:
                        throw new NotImplementedException($"Type is not yet implemented {type.KindCase}");
                }
            }

            internal Struct ParseStruct(Protobuf.Type.Types.Struct str)
            {
                var st = new Type.Struct()
                {
                    Types = new List<SubstraitBaseType>()
                };
                foreach (var type in str.Types_)
                {
                    var emptySpan = Span<string>.Empty;
                    st.Types.Add(GetType(type, ref emptySpan));
                }

                return st;
            }

            internal NamedStruct ParseNamedStruct(Protobuf.NamedStruct namedStruct)
            {
                var names = new List<string>();
                Struct? structType = default;
                if (namedStruct.Struct != null)
                {
                    List<SubstraitBaseType> types = new List<SubstraitBaseType>();
                    var namesSpan = namedStruct.Names.ToArray().AsSpan();
                    for (int i = 0; i < namedStruct.Struct.Types_.Count; i++)
                    {
                        var name = namesSpan[0];
                        namesSpan = namesSpan.Slice(1);
                        var type = GetType(namedStruct.Struct.Types_[i], ref namesSpan);
                        names.Add(name);
                        types.Add(type);
                    }
                    structType = new Struct()
                    {
                        Types = types
                    };
                }
                return new NamedStruct()
                {
                    Names = names,
                    Struct = structType
                };
            }

            public TableFunction VisitTableFunction(CustomProtobuf.TableFunction tableFunction)
            {
                if (!idToFunctionLookup.TryGetValue(tableFunction.FunctionReference, out var functionName))
                {
                    throw new NotImplementedException();
                }

                var uri = functionName.Substring(0, functionName.IndexOf(':'));
                var name = functionName.Substring(functionName.IndexOf(':') + 1);

                var tableSchema = ParseNamedStruct(tableFunction.TableSchema);
                var result = new TableFunction()
                {
                    ExtensionName = name,
                    ExtensionUri = uri,
                    TableSchema = tableSchema,
                    Arguments = new List<Expression>()
                };
                if (tableFunction.Arguments != null && tableFunction.Arguments.Count > 0)
                {
                    foreach (var arg in tableFunction.Arguments)
                    {
                        result.Arguments.Add(VisitExpression(arg.Value));
                    }
                }

                return result;
            }

            public Expressions.Expression VisitExpression(Protobuf.Expression expression)
            {
                switch (expression.RexTypeCase)
                {
                    case Protobuf.Expression.RexTypeOneofCase.ScalarFunction:
                        return VisitScalarFunction(expression.ScalarFunction);
                    case Protobuf.Expression.RexTypeOneofCase.Selection:
                        return VisitFieldReference(expression.Selection);
                    case Protobuf.Expression.RexTypeOneofCase.Literal:
                        return VisitLiteral(expression.Literal);
                    case Protobuf.Expression.RexTypeOneofCase.IfThen:
                        return VisitIfThen(expression.IfThen);
                    case Protobuf.Expression.RexTypeOneofCase.Cast:
                        return VisitCast(expression.Cast);
                    case Protobuf.Expression.RexTypeOneofCase.Nested:
                        return VisitNested(expression.Nested);
                    case Protobuf.Expression.RexTypeOneofCase.SingularOrList:
                        return VisitSingularOrList(expression.SingularOrList);
                }
                throw new NotImplementedException();
            }

            public StructExpression VisitStruct(Protobuf.Expression.Types.Nested.Types.Struct structExpr)
            {
                var fields = new List<Expression>();
                foreach(var field in structExpr.Fields)
                {
                    fields.Add(VisitExpression(field));
                }
                return new StructExpression()
                {
                    Fields = fields
                };
            }

            public Expression VisitSingularOrList(Protobuf.Expression.Types.SingularOrList singularOrList)
            {
                var valueExpr = VisitExpression(singularOrList.Value);
                List<Expression> options = new List<Expression>();

                for (int i = 0; i < singularOrList.Options.Count; i++)
                {
                    options.Add(VisitExpression(singularOrList.Options[i]));
                }

                return new SingularOrListExpression()
                {
                    Options = options,
                    Value = valueExpr
                };
            }

            public Expression VisitNested(Protobuf.Expression.Types.Nested nested)
            {
                switch (nested.NestedTypeCase)
                {
                    case Protobuf.Expression.Types.Nested.NestedTypeOneofCase.List:
                        return VisitListNestedExpression(nested.List);
                    case Protobuf.Expression.Types.Nested.NestedTypeOneofCase.Map:
                        return VisitMapNestedExpression(nested.Map);
                    case Protobuf.Expression.Types.Nested.NestedTypeOneofCase.Struct:
                        return VisitStruct(nested.Struct);
                    default:
                        throw new NotImplementedException();
                }
            }

            public MapNestedExpression VisitMapNestedExpression(Protobuf.Expression.Types.Nested.Types.Map map)
            {
                var output = new MapNestedExpression()
                {
                    KeyValues = new List<KeyValuePair<Expression, Expression>>()
                };

                foreach(var val in map.KeyValues)
                {
                    var key = VisitExpression(val.Key);
                    var value = VisitExpression(val.Value);

                    output.KeyValues.Add(new KeyValuePair<Expression, Expression>(key, value));
                }

                return output;
            }

            public Expressions.ListNestedExpression VisitListNestedExpression(Protobuf.Expression.Types.Nested.Types.List list)
            {
                var values = new List<Expression>();

                foreach (var val in list.Values)
                {
                    var expr = VisitExpression(val);
                    if (expr == null)
                    {
                        throw new InvalidOperationException("Expression in list cannot be null");
                    }
                    else
                    {
                        values.Add(expr);
                    }
                }

                var o = new ListNestedExpression()
                {
                    Values = values
                };

                return o;
            }

            public Expressions.AggregateFunction VisitAggregateFunction(Protobuf.AggregateFunction aggregateFunction)
            {
                if (!idToFunctionLookup.TryGetValue(aggregateFunction.FunctionReference, out var functionName))
                {
                    throw new NotImplementedException();
                }

                var uri = functionName.Substring(0, functionName.IndexOf(':'));
                var name = functionName.Substring(functionName.IndexOf(':') + 1);

                var result = new AggregateFunction()
                {
                    ExtensionName = name,
                    ExtensionUri = uri,
                    Arguments = new List<Expression>()
                };
#pragma warning disable CS0612 // Type or member is obsolete
                if (aggregateFunction.Args.Count > 0)
                {
                    foreach (var arg in aggregateFunction.Args)
                    {
                        result.Arguments.Add(VisitExpression(arg));
                    }
                }
                else if (aggregateFunction.Arguments.Count > 0)
                {
                    foreach (var arg in aggregateFunction.Arguments)
                    {
                        result.Arguments.Add(VisitExpression(arg.Value));
                    }
                }
#pragma warning restore CS0612 // Type or member is obsolete
                return result;
            }

            private Expressions.Expression VisitCast(Protobuf.Expression.Types.Cast cast)
            {
                var emptySpan = Span<string>.Empty;
                return new CastExpression()
                {
                    Expression = VisitExpression(cast.Input),
                    Type = GetType(cast.Type, ref emptySpan)
                };
            }

            private SortDirection GetSortDirection(Protobuf.SortField.Types.SortDirection sortDirection)
            {
                switch (sortDirection)
                {
                    case Protobuf.SortField.Types.SortDirection.AscNullsFirst:
                        return SortDirection.SortDirectionAscNullsFirst;
                    case Protobuf.SortField.Types.SortDirection.DescNullsFirst:
                        return SortDirection.SortDirectionDescNullsFirst;
                    case Protobuf.SortField.Types.SortDirection.AscNullsLast:
                        return SortDirection.SortDirectionAscNullsLast;
                    case Protobuf.SortField.Types.SortDirection.DescNullsLast:
                        return SortDirection.SortDirectionDescNullsLast;
                    case Protobuf.SortField.Types.SortDirection.Clustered:
                        return SortDirection.SortDirectionClustered;
                    case Protobuf.SortField.Types.SortDirection.Unspecified:
                        return SortDirection.SortDirectionUnspecified;
                    default:
                        throw new NotImplementedException(sortDirection.ToString());
                }
            }

            public SortField VisitSortField(Protobuf.SortField sortField)
            {
                return new SortField()
                {
                    Expression = VisitExpression(sortField.Expr),
                    SortDirection = GetSortDirection(sortField.Direction),
                };
            }

            public WindowBound? GetWindowBound(Protobuf.Expression.Types.WindowFunction.Types.Bound bound)
            {
                switch (bound.KindCase)
                {
                    case Protobuf.Expression.Types.WindowFunction.Types.Bound.KindOneofCase.CurrentRow:
                        return new CurrentRowWindowBound();
                    case Protobuf.Expression.Types.WindowFunction.Types.Bound.KindOneofCase.Unbounded:
                        return new UnboundedWindowBound();
                    case Protobuf.Expression.Types.WindowFunction.Types.Bound.KindOneofCase.Preceding:
                        return new PreceedingRowWindowBound()
                        {
                            Offset = bound.Preceding.Offset
                        };
                    case Protobuf.Expression.Types.WindowFunction.Types.Bound.KindOneofCase.Following:
                        return new FollowingRowWindowBound()
                        {
                            Offset = bound.Following.Offset
                        };
                    case Protobuf.Expression.Types.WindowFunction.Types.Bound.KindOneofCase.None:
                        return null;
                    default:
                        throw new NotImplementedException(bound.KindCase.ToString());
                }
            }

            public WindowFunction VisitWindowFunction(Protobuf.ConsistentPartitionWindowRel.Types.WindowRelFunction windowRelFunction)
            {
                
                if (!idToFunctionLookup.TryGetValue(windowRelFunction.FunctionReference, out var functionName))
                {
                    throw new NotImplementedException();
                }
                var uri = functionName.Substring(0, functionName.IndexOf(':'));
                var name = functionName.Substring(functionName.IndexOf(':') + 1);
                var result = new WindowFunction()
                {
                    ExtensionName = name,
                    ExtensionUri = uri,
                    Arguments = new List<Expression>(),
                    LowerBound = GetWindowBound(windowRelFunction.LowerBound),
                    UpperBound = GetWindowBound(windowRelFunction.UpperBound)
                };
                foreach(var arg in windowRelFunction.Arguments)
                {
                    result.Arguments.Add(VisitExpression(arg.Value));
                }
                return result;
            }

            private Expressions.Expression VisitIfThen(Protobuf.Expression.Types.IfThen ifThen)
            {
                List<IfClause> ifClauses = new List<IfClause>();

                foreach (var clause in ifThen.Ifs)
                {
                    ifClauses.Add(new IfClause()
                    {
                        If = VisitExpression(clause.If),
                        Then = VisitExpression(clause.Then)
                    });
                }
                return new IfThenExpression()
                {
                    Ifs = ifClauses,
                    Else = VisitExpression(ifThen.Else)
                };
            }

            private static Literal VisitLiteral(Protobuf.Expression.Types.Literal literal)
            {
                switch (literal.LiteralTypeCase)
                {
                    case Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.Boolean:
                        return new BoolLiteral()
                        {
                            Value = literal.Boolean
                        };
                    case Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.Null:
                        return new NullLiteral();
                    case Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.I32:
                        return new NumericLiteral()
                        {
                            Value = literal.I32
                        };
                    case Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.I64:
                        return new NumericLiteral()
                        {
                            Value = literal.I64
                        };
                    case Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.String:
                        return new StringLiteral()
                        {
                            Value = literal.String
                        };
                    case Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.FixedChar:
                        return new StringLiteral()
                        {
                            Value = literal.FixedChar
                        };
                    case Protobuf.Expression.Types.Literal.LiteralTypeOneofCase.Binary:
                        return new BinaryLiteral()
                        {
                            Value = literal.Binary.ToByteArray()
                        };
                    default:
                        throw new NotImplementedException();
                }
            }

            public static FieldReference VisitFieldReference(Protobuf.Expression.Types.FieldReference fieldReference)
            {
                switch (fieldReference.ReferenceTypeCase)
                {
                    case Protobuf.Expression.Types.FieldReference.ReferenceTypeOneofCase.DirectReference:
                        return VisitDirectReference(fieldReference.DirectReference);
                    default:
                        throw new NotImplementedException();
                }
            }

            private static DirectFieldReference VisitDirectReference(Protobuf.Expression.Types.ReferenceSegment referenceSegment)
            {
                switch (referenceSegment.ReferenceTypeCase)
                {
                    case Protobuf.Expression.Types.ReferenceSegment.ReferenceTypeOneofCase.StructField:
                        return VisitStructField(referenceSegment.StructField);
                    default:
                        throw new NotImplementedException();
                }
            }

            private static DirectFieldReference VisitStructField(Protobuf.Expression.Types.ReferenceSegment.Types.StructField structField)
            {
                return new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment()
                    {
                        Field = structField.Field
                    }
                };
            }

            private Expressions.Expression VisitScalarFunction(Protobuf.Expression.Types.ScalarFunction scalarFunction)
            {
                if (!idToFunctionLookup.TryGetValue(scalarFunction.FunctionReference, out var functionName))
                {
                    throw new NotImplementedException();
                }

                var uri = functionName.Substring(0, functionName.IndexOf(':'));
                var name = functionName.Substring(functionName.IndexOf(':') + 1);

                List<Expression> args = new List<Expression>();

#pragma warning disable CS0612 // Type or member is obsolete
                if (scalarFunction.Args != null && scalarFunction.Args.Count > 0)
                {
                    foreach (var arg in scalarFunction.Args)
                    {
                        args.Add(VisitExpression(arg));
                    }
                }
                else if (scalarFunction.Arguments != null && scalarFunction.Arguments.Count > 0)
                {
                    foreach (var arg in scalarFunction.Arguments)
                    {
                        args.Add(VisitExpression(arg.Value));
                    }
                }
#pragma warning restore CS0612 // Type or member is obsolete

                return new ScalarFunction()
                {
                    ExtensionUri = uri,
                    ExtensionName = name,
                    Arguments = args
                };



                throw new NotImplementedException(functionName);
            }
        }

        private sealed class SubstraitDeserializerImpl
        {
            private readonly Protobuf.Plan plan;
            private readonly ExpressionDeserializerImpl expressionDeserializer;
            private List<Relation> _relations;

            public SubstraitDeserializerImpl(Protobuf.Plan plan)
            {
                this.plan = plan;
                expressionDeserializer = new ExpressionDeserializerImpl(plan);
                _relations = new List<Relation>();
            }

            public Plan Convert()
            {
                return VisitPlan(plan);
            }

            private Plan VisitPlan(Protobuf.Plan plan)
            {
                var output = new Plan
                {
                    Relations = _relations
                };
                foreach (var relation in plan.Relations)
                {
                    output.Relations.Add(VisitPlanRel(relation));
                }
                return output;
            }

            private Relation VisitPlanRel(Protobuf.PlanRel planRel)
            {
                switch (planRel.RelTypeCase)
                {
                    case Protobuf.PlanRel.RelTypeOneofCase.Rel:
                        return VisitRel(planRel.Rel);
                    case Protobuf.PlanRel.RelTypeOneofCase.None:
                        throw new NotImplementedException();
                    case Protobuf.PlanRel.RelTypeOneofCase.Root:
                        return VisitRelRoot(planRel.Root);
                }
                throw new NotImplementedException();
            }

            private Relation VisitRelRoot(Protobuf.RelRoot relRoot)
            {
                var input = VisitRel(relRoot.Input);
                var rootRelation = new RootRelation()
                {
                    Input = input,
                    Names = relRoot.Names.ToList()
                };
                return rootRelation;
            }

            private Relation VisitRel(Protobuf.Rel rel)
            {
                switch (rel.RelTypeCase)
                {
                    case Protobuf.Rel.RelTypeOneofCase.Project:
                        return VisitProject(rel.Project);
                    case Protobuf.Rel.RelTypeOneofCase.Read:
                        return VisitRead(rel.Read);
                    case Protobuf.Rel.RelTypeOneofCase.Filter:
                        return VisitFilter(rel.Filter);
                    case Protobuf.Rel.RelTypeOneofCase.Join:
                        return VisitJoin(rel.Join);
                    case Protobuf.Rel.RelTypeOneofCase.Set:
                        return VisitSet(rel.Set);
                    case Protobuf.Rel.RelTypeOneofCase.Aggregate:
                        return VisitAggregate(rel.Aggregate);
                    case Protobuf.Rel.RelTypeOneofCase.ExtensionSingle:
                        return VisitExtensionSingle(rel.ExtensionSingle);
                    case Protobuf.Rel.RelTypeOneofCase.Write:
                        return VisitWrite(rel.Write);
                    case Protobuf.Rel.RelTypeOneofCase.ExtensionLeaf:
                        return VisitExtensionLeaf(rel.ExtensionLeaf);
                    case Protobuf.Rel.RelTypeOneofCase.ExtensionMulti:
                        return VisitExtensionMulti(rel.ExtensionMulti);
                    case Protobuf.Rel.RelTypeOneofCase.Reference:
                        return VisitReference(rel.Reference);
                    case Protobuf.Rel.RelTypeOneofCase.Window:
                        return VisitWindow(rel.Window);
                    case Protobuf.Rel.RelTypeOneofCase.MergeJoin:
                        return VisitMergeJoin(rel.MergeJoin);
                    case Protobuf.Rel.RelTypeOneofCase.Exchange:
                        return VisitExchange(rel.Exchange);
                    case Protobuf.Rel.RelTypeOneofCase.Fetch:
                        return VisitFetch(rel.Fetch);
                    default:
                        throw new NotImplementedException(rel.RelTypeCase.ToString());
                }
            }

            private Relation VisitFetch(Protobuf.FetchRel fetchRel)
            {
                if (fetchRel.Offset < 0 || fetchRel.Count < 0)
                {
                    throw new InvalidOperationException("Offset and count in fetch relation must be non-negative");
                }
                if (fetchRel.Count > int.MaxValue)
                {
                    throw new InvalidOperationException("Count in fetch relation cannot be greater than int max value");
                }
                if (fetchRel.Offset > int.MaxValue)
                {
                    throw new InvalidOperationException("Offset in fetch relation cannot be greater than int max value");
                }
                var output = new FetchRelation()
                {
                    Input = VisitRel(fetchRel.Input),
                    Emit = GetEmit(fetchRel.Common),
                    Offset = (int)fetchRel.Offset,
                    Count = (int)fetchRel.Count
                };
                return output;
            }

            private Relation VisitExchange(Protobuf.ExchangeRel exchange)
            {
                ExchangeKind? exchangeKind = default;

                if (exchange.ExchangeKindCase == Protobuf.ExchangeRel.ExchangeKindOneofCase.ScatterByFields)
                {
                    var scatterExchangeKind = new ScatterExchangeKind()
                    {
                        Fields = new List<FieldReference>()
                    };
                    foreach(var field in exchange.ScatterByFields.Fields)
                    {
                        scatterExchangeKind.Fields.Add(ExpressionDeserializerImpl.VisitFieldReference(field));
                    }
                    exchangeKind = scatterExchangeKind;
                }
                else if (exchange.ExchangeKindCase == Protobuf.ExchangeRel.ExchangeKindOneofCase.Broadcast)
                {
                    exchangeKind = new BroadcastExchangeKind();
                    
                }
                else
                {
                    throw new NotImplementedException();
                }
                List<ExchangeTarget> targets = new List<ExchangeTarget>();
                foreach(var target in exchange.Targets)
                {
                    if (target.Uri == "standard_output")
                    {
                        targets.Add(new StandardOutputExchangeTarget()
                        {
                            PartitionIds = target.PartitionId.ToList()
                        });
                    }
                    else
                    {
                        throw new NotImplementedException($"{target.Uri}");
                    }
                }

                return new ExchangeRelation()
                {
                    Input = VisitRel(exchange.Input),
                    ExchangeKind = exchangeKind,
                    Targets = targets,
                    Emit = GetEmit(exchange.Common),
                    PartitionCount = exchange.PartitionCount == 0 ? null : exchange.PartitionCount
                };
            }

            private JoinType GetJoinType(Protobuf.MergeJoinRel.Types.JoinType joinType)
            {
                switch (joinType)
                {
                    case Protobuf.MergeJoinRel.Types.JoinType.Right:
                        return JoinType.Right;
                    case Protobuf.MergeJoinRel.Types.JoinType.Outer:
                        return JoinType.Outer;
                    case Protobuf.MergeJoinRel.Types.JoinType.Inner:
                        return JoinType.Inner;
                    case Protobuf.MergeJoinRel.Types.JoinType.Left:
                        return JoinType.Left;
                    case Protobuf.MergeJoinRel.Types.JoinType.Unspecified:
                        return JoinType.Unspecified;
                    default:
                        throw new NotSupportedException(joinType.ToString());
                }
            }

            private Relation VisitMergeJoin(Protobuf.MergeJoinRel mergeJoin)
            {
                List<FieldReference> leftKeys = new List<FieldReference>();
                List<FieldReference> rightKeys = new List<FieldReference>();
                foreach (var key in mergeJoin.Keys)
                {
                    leftKeys.Add(ExpressionDeserializerImpl.VisitFieldReference(key.Left));
                    rightKeys.Add(ExpressionDeserializerImpl.VisitFieldReference(key.Right));
                }
                Expression? postJoinFilter = default;
                if (mergeJoin.PostJoinFilter != null)
                {
                    postJoinFilter = expressionDeserializer.VisitExpression(mergeJoin.PostJoinFilter);
                }
                var output = new MergeJoinRelation()
                {
                    Emit = GetEmit(mergeJoin.Common),
                    Left = VisitRel(mergeJoin.Left),
                    Right = VisitRel(mergeJoin.Right),
                    LeftKeys = leftKeys,
                    RightKeys = rightKeys,
                    Type = GetJoinType(mergeJoin.Type),
                    PostJoinFilter = postJoinFilter
                };
                return output;
            }

            private Relation VisitWindow(Protobuf.ConsistentPartitionWindowRel windowRel)
            {
                List<SortField> orderBy = new List<SortField>();

                foreach(var sort in windowRel.Sorts)
                {
                    orderBy.Add(expressionDeserializer.VisitSortField(sort));
                }

                List<Expression> partitionBy = new List<Expression>();

                foreach (var partition in windowRel.PartitionExpressions) 
                {
                    partitionBy.Add(expressionDeserializer.VisitExpression(partition));
                }

                List<WindowFunction> windowFunctions = new List<WindowFunction>();

                foreach(var windowFunction in windowRel.WindowFunctions)
                {
                    windowFunctions.Add(expressionDeserializer.VisitWindowFunction(windowFunction)); 
                }

                var output = new ConsistentPartitionWindowRelation()
                {
                    Input = VisitRel(windowRel.Input),
                    Emit = GetEmit(windowRel.Common),
                    OrderBy = orderBy,
                    PartitionBy = partitionBy,
                    WindowFunctions = windowFunctions
                };

                return output;
            }

            private Relation VisitReference(Protobuf.ReferenceRel referenceRel)
            {
                return new ReferenceRelation()
                {
                    RelationId = referenceRel.SubtreeOrdinal,
                    ReferenceOutputLength = _relations[referenceRel.SubtreeOrdinal].OutputLength
                };
            }

            private Relation VisitExtensionMulti(Protobuf.ExtensionMultiRel extensionMulti)
            {
                var inputs = new List<Relation>();
                foreach (var input in extensionMulti.Inputs)
                {
                    inputs.Add(VisitRel(input));
                }
                var typeName = Google.Protobuf.WellKnownTypes.Any.GetTypeName(extensionMulti.Detail.TypeUrl);
                if (typeName == CustomProtobuf.IterationRelation.Descriptor.FullName)
                {
                    var loopPlan = inputs[0];
                    Relation? inputRel = default;
                    if (inputs.Count > 1)
                    {
                        inputRel = inputs[1];
                    }
                    var iterationRel = extensionMulti.Detail.Unpack<CustomProtobuf.IterationRelation>();
                    var rel = new IterationRelation()
                    {
                        Input = inputRel,
                        LoopPlan = loopPlan,
                        IterationName = iterationRel.IterationName,
                        Emit = GetEmit(extensionMulti.Common)
                    };
                    return rel;
                }
                throw new NotImplementedException();
            }

            private Relation VisitWrite(Protobuf.WriteRel writeRel)
            {
                var input = VisitRel(writeRel.Input);

                var namedStruct = expressionDeserializer.ParseNamedStruct(writeRel.TableSchema);

                List<string> namedTable = new List<string>();
                if (writeRel.NamedTable != null)
                {
                    namedTable.AddRange(writeRel.NamedTable.Names);
                }

                var namedTableObj = new NamedTable()
                {
                    Names = namedTable
                };

                var emitData = GetEmit(writeRel.Common);

                var writeRelation = new WriteRelation()
                {
                    Input = input,
                    NamedObject = namedTableObj,
                    TableSchema = namedStruct,
                    Emit = emitData
                };

                return writeRelation;
            }

            private Relation VisitExtensionLeaf(Protobuf.ExtensionLeafRel extensionLeaf)
            {
                var typeName = Google.Protobuf.WellKnownTypes.Any.GetTypeName(extensionLeaf.Detail.TypeUrl);
                if (typeName == CustomProtobuf.TableFunctionRelation.Descriptor.FullName)
                {
                    var tableFuncRel = extensionLeaf.Detail.Unpack<CustomProtobuf.TableFunctionRelation>();

                    Expression? joinExpr = null;
                    if (tableFuncRel.JoinCondition != null)
                    {
                        joinExpr = expressionDeserializer.VisitExpression(tableFuncRel.JoinCondition);
                    }
                    JoinType joinType = JoinType.Unspecified;
                    switch (tableFuncRel.Type)
                    {
                        case CustomProtobuf.TableFunctionRelation.Types.JoinType.Inner:
                            joinType = JoinType.Inner;
                            break;
                        case CustomProtobuf.TableFunctionRelation.Types.JoinType.Left:
                            joinType = JoinType.Left;
                            break;
                        case CustomProtobuf.TableFunctionRelation.Types.JoinType.Unspecified:
                            joinType = JoinType.Unspecified;
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                    var rel = new TableFunctionRelation()
                    {
                        TableFunction = expressionDeserializer.VisitTableFunction(tableFuncRel.TableFunction),
                        Input = null,
                        JoinCondition = joinExpr,
                        Type = joinType,
                        Emit = GetEmit(extensionLeaf.Common)
                    };
                    return rel;
                }
                else if (typeName == CustomProtobuf.IterationReferenceReadRelation.Descriptor.FullName)
                {
                    var iterationRefRead = extensionLeaf.Detail.Unpack<CustomProtobuf.IterationReferenceReadRelation>();
                    var rel = new IterationReferenceReadRelation()
                    {
                        IterationName = iterationRefRead.IterationName,
                        ReferenceOutputLength = iterationRefRead.OutputLength,
                        Emit = GetEmit(extensionLeaf.Common)
                    };
                    return rel;
                }
                else if (typeName == CustomProtobuf.StandardOutputTargetReferenceRelation.Descriptor.FullName)
                {
                    var standardOutputRef = extensionLeaf.Detail.Unpack<CustomProtobuf.StandardOutputTargetReferenceRelation>();
                    var rel = new StandardOutputExchangeReferenceRelation()
                    {
                        RelationId = standardOutputRef.RelationId,
                        TargetId = standardOutputRef.TargetId,
                        ReferenceOutputLength = _relations[standardOutputRef.RelationId].OutputLength
                    };
                    return rel;
                }

                throw new NotImplementedException();
            }

            private Relation VisitExtensionSingle(Protobuf.ExtensionSingleRel extensionSingle)
            {
                var input = VisitRel(extensionSingle.Input);

                var typeName = Google.Protobuf.WellKnownTypes.Any.GetTypeName(extensionSingle.Detail.TypeUrl);
                if (typeName == CustomProtobuf.SubStreamRootRelation.Descriptor.FullName)
                {
                    var substreamRoot = extensionSingle.Detail.Unpack<CustomProtobuf.SubStreamRootRelation>();
                    var rel = new SubStreamRootRelation()
                    {
                        Input = input,
                        Name = substreamRoot.SubstreamName,
                        Emit = GetEmit(extensionSingle.Common)
                    };
                    return rel;
                }
                else if (typeName == CustomProtobuf.TableFunctionRelation.Descriptor.FullName)
                {
                    var tableFuncRel = extensionSingle.Detail.Unpack<CustomProtobuf.TableFunctionRelation>();
                    var expr = expressionDeserializer.VisitExpression(tableFuncRel.JoinCondition);

                    JoinType joinType = JoinType.Unspecified;

                    switch (tableFuncRel.Type)
                    {
                        case CustomProtobuf.TableFunctionRelation.Types.JoinType.Inner:
                            joinType = JoinType.Inner;
                            break;
                        case CustomProtobuf.TableFunctionRelation.Types.JoinType.Left:
                            joinType = JoinType.Left;
                            break;
                        case CustomProtobuf.TableFunctionRelation.Types.JoinType.Unspecified:
                            joinType = JoinType.Unspecified;
                            break;
                        default:
                            throw new NotImplementedException();
                    }

                    var rel = new TableFunctionRelation()
                    {
                        TableFunction = expressionDeserializer.VisitTableFunction(tableFuncRel.TableFunction),
                        Input = input,
                        JoinCondition = expr,
                        Type = joinType,
                        Emit = GetEmit(extensionSingle.Common)
                    };
                    return rel;
                }
                else if (typeName == CustomProtobuf.TopNRelation.Descriptor.FullName)
                {
                    
                    var topRel = extensionSingle.Detail.Unpack<CustomProtobuf.TopNRelation>();
                    List<SortField> sortFields = new List<SortField>();
                    foreach(var field in topRel.Sorts)
                    {
                        sortFields.Add(expressionDeserializer.VisitSortField(field));
                    }
                    var rel = new TopNRelation()
                    {
                        Input = input,
                        Count = topRel.Count,
                        Offset = topRel.Offset,
                        Emit = GetEmit(extensionSingle.Common),
                        Sorts = sortFields
                    };
                    return rel;

                }
                else if (typeName == CustomProtobuf.BufferRelation.Descriptor.FullName)
                {
                    var bufferRel = extensionSingle.Detail.Unpack<CustomProtobuf.BufferRelation>();

                    return new BufferRelation()
                    {
                        Input = input,
                        Emit = GetEmit(extensionSingle.Common)
                    };
                }
                throw new NotImplementedException();
            }

            private Relation VisitAggregate(Protobuf.AggregateRel aggregateRel)
            {
                var relation = new AggregateRelation()
                {
                    Input = VisitRel(aggregateRel.Input),
                    Groupings = new List<AggregateGrouping>(),
                    Measures = new List<AggregateMeasure>()
                };

                if (aggregateRel.Groupings.Count > 0)
                {
                    foreach (var grouping in aggregateRel.Groupings)
                    {
                        var aggGroup = new AggregateGrouping()
                        {
                            GroupingExpressions = new List<Expression>()
                        };
                        foreach (var expr in grouping.GroupingExpressions)
                        {
                            aggGroup.GroupingExpressions.Add(expressionDeserializer.VisitExpression(expr));
                        }
                        relation.Groupings.Add(aggGroup);
                    }
                }
                if (aggregateRel.Measures.Count > 0)
                {
                    foreach (var measure in aggregateRel.Measures)
                    {
                        Expression? filter = default;
                        if (measure.Filter != null)
                        {
                            filter = expressionDeserializer.VisitExpression(measure.Filter);
                        }
                        var func = expressionDeserializer.VisitAggregateFunction(measure.Measure_);
                        relation.Measures.Add(new AggregateMeasure()
                        {
                            Filter = filter,
                            Measure = func
                        });
                    }
                }

                return relation;
            }

            private static List<int>? GetEmit(Protobuf.RelCommon relCommon)
            {
                if (relCommon == null)
                {
                    return null;
                }
                switch (relCommon.EmitKindCase)
                {
                    case Protobuf.RelCommon.EmitKindOneofCase.Direct:
                        return null;
                    case Protobuf.RelCommon.EmitKindOneofCase.Emit:
                        return relCommon.Emit.OutputMapping.ToList();
                }
                throw new NotImplementedException();
            }

            private Relation VisitSet(Protobuf.SetRel setRel)
            {
                var set = new SetRelation()
                {
                    Inputs = new List<Relation>()
                };

                for (int i = 0; i < setRel.Inputs.Count; i++)
                {
                    set.Inputs.Add(VisitRel(setRel.Inputs[i]));
                }
                set.Operation = SetOperation.UnionAll;
                set.Emit = GetEmit(setRel.Common);
                return set;
            }

            private Relation VisitProject(Protobuf.ProjectRel projectRel)
            {
                var input = VisitRel(projectRel.Input);
                var project = new Relations.ProjectRelation()
                {
                    Input = input,
                    Expressions = new List<Expressions.Expression>(),
                    Emit = GetEmit(projectRel.Common)
                };

                if (projectRel.Expressions.Count > 0)
                {
                    foreach (var expr in projectRel.Expressions)
                    {
                        project.Expressions.Add(expressionDeserializer.VisitExpression(expr));
                    }
                }
                return project;
            }

            
            private Relation VisitRead(Protobuf.ReadRel readRel)
            {
                List<string> names = new List<string>();
                names.AddRange(readRel.BaseSchema.Names);

                Struct? schema = default;
                if (readRel.BaseSchema.Struct != null)
                {
                    schema = expressionDeserializer.ParseStruct(readRel.BaseSchema.Struct);
                }

                var namedStruct = new Type.NamedStruct()
                {
                    Names = names,
                    Struct = schema
                };

                
                if (readRel.ReadTypeCase == Protobuf.ReadRel.ReadTypeOneofCase.NamedTable)
                {
                    List<string> namedTable = new List<string>();
                    namedTable.AddRange(readRel.NamedTable.Names);

                    return new ReadRelation()
                    {
                        BaseSchema = namedStruct,
                        NamedTable = new Type.NamedTable()
                        {
                            Names = namedTable
                        },
                        Emit = GetEmit(readRel.Common)
                    };
                }
                else if (readRel.ReadTypeCase == Protobuf.ReadRel.ReadTypeOneofCase.VirtualTable)
                {
                    List<StructExpression> virtualTableExprs = new List<StructExpression>();
                    foreach (var expr in readRel.VirtualTable.Expressions)
                    {
                        virtualTableExprs.Add(expressionDeserializer.VisitStruct(expr));
                    }
                    VirtualTable virtualTable = new VirtualTable()
                    {
                        Expressions = virtualTableExprs
                    };
                    return new VirtualTableReadRelation()
                    {
                        BaseSchema = namedStruct,
                        Emit = GetEmit(readRel.Common),
                        Values = virtualTable
                    };
                }
                else
                {
                    throw new NotImplementedException("Read relation must have either named table or virtual table expressions");
                }
            }

            private Relation VisitFilter(Protobuf.FilterRel filterRel)
            {
                var input = VisitRel(filterRel.Input);

                if (filterRel.Condition == null)
                {
                    throw new InvalidOperationException("Filter must have a condition");
                }

                var filter = new FilterRelation()
                {
                    Input = input,
                    Condition = expressionDeserializer.VisitExpression(filterRel.Condition),
                    Emit = GetEmit(filterRel.Common)
                };

                return filter;
            }

            private Relation VisitJoin(Protobuf.JoinRel joinRel)
            {
                var inputLeft = VisitRel(joinRel.Left);
                var inputRight = VisitRel(joinRel.Right);
                var joinType = JoinType.Unspecified;
                switch (joinRel.Type)
                {
                    case Protobuf.JoinRel.Types.JoinType.Right:
                        joinType = JoinType.Right;
                        break;
                    case Protobuf.JoinRel.Types.JoinType.Inner:
                        joinType = JoinType.Inner;
                        break;
                    case Protobuf.JoinRel.Types.JoinType.Single:
                        joinType = JoinType.Single;
                        break;
                    case Protobuf.JoinRel.Types.JoinType.Left:
                        joinType = JoinType.Left;
                        break;
                    case Protobuf.JoinRel.Types.JoinType.Anti:
                        joinType = JoinType.Anti;
                        break;
                    case Protobuf.JoinRel.Types.JoinType.Semi:
                        joinType = JoinType.Semi;
                        break;
                    default:
                        throw new NotSupportedException("Join type not supported");
                }
                var joinRelation = new JoinRelation()
                {
                    Left = inputLeft,
                    Right = inputRight,
                    Emit = GetEmit(joinRel.Common),
                    Type = joinType
                };
                if (joinRel.Expression != null)
                {
                    joinRelation.Expression = expressionDeserializer.VisitExpression(joinRel.Expression);
                }
                if (joinRel.PostJoinFilter != null)
                {
                    joinRelation.PostJoinFilter = expressionDeserializer.VisitExpression(joinRel.PostJoinFilter);
                }
                return joinRelation;
            }
        }

        public Plan Deserialize(string json)
        {
            var typeRegistry = Google.Protobuf.Reflection.TypeRegistry.FromMessages(
                    CustomProtobuf.IterationReferenceReadRelation.Descriptor,
                    CustomProtobuf.IterationRelation.Descriptor);
            var parser = new Google.Protobuf.JsonParser(new Google.Protobuf.JsonParser.Settings(300, typeRegistry));
            var plan = parser.Parse<Protobuf.Plan>(json);
            return Deserialize(plan);
        }

        public static Plan DeserializeFromJson(string json)
        {
            var deserializer = new SubstraitDeserializer();
            return deserializer.Deserialize(json);
        }

        public Plan Deserialize(Protobuf.Plan plan)
        {
            var impl = new SubstraitDeserializerImpl(plan);
            return impl.Convert();
        }
    }
}
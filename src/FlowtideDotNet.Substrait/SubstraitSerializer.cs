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
using Google.Protobuf;
using Substrait.Protobuf;
using Protobuf = Substrait.Protobuf;

namespace FlowtideDotNet.Substrait
{
    internal class SubstraitSerializer
    {
        private sealed class SerializerVisitorState
        {
            public int uriCounter = 0;
            public int extensionCounter = 0;
            public Dictionary<string, int> _typeExtensions = new Dictionary<string, int>();
            public Dictionary<string, uint> _functionsExtensions = new Dictionary<string, uint>();

            public SerializerVisitorState(Protobuf.Plan root)
            {
                Root = root;
            }

            public uint GetFunctionExtensionAnchor(string uri, string name)
            {
                var key = $"{uri}:{name}";
                if (_functionsExtensions.TryGetValue(key, out var id))
                {
                    return id;
                }
                var anchor = uriCounter++;
                Root.ExtensionUris.Add(new Protobuf.SimpleExtensionURI
                {
                    Uri = uri,
                    ExtensionUriAnchor = (uint)anchor
                });
                var functionAnchor = (uint)extensionCounter++;
                Root.Extensions.Add(new Protobuf.SimpleExtensionDeclaration()
                {
                    ExtensionFunction = new Protobuf.SimpleExtensionDeclaration.Types.ExtensionFunction()
                    {
                        ExtensionUriReference = (uint)anchor,
                        FunctionAnchor = functionAnchor,
                        Name = name
                    }
                });
                _functionsExtensions.Add(key, functionAnchor);
                return functionAnchor;
            }

            private uint GetAnyTypeId()
            {
                if (!_typeExtensions.TryGetValue("any", out var id))
                {
                    var anchor = uriCounter++;
                    Root.ExtensionUris.Add(new Protobuf.SimpleExtensionURI
                    {
                        Uri = $"/any_type.yaml",
                        ExtensionUriAnchor = (uint)anchor
                    });
                    var typeAnchor = (uint)extensionCounter++;
                    Root.Extensions.Add(new Protobuf.SimpleExtensionDeclaration()
                    {
                        ExtensionType = new Protobuf.SimpleExtensionDeclaration.Types.ExtensionType()
                        {
                            ExtensionUriReference = (uint)anchor,
                            Name = "any",
                            TypeAnchor = typeAnchor
                        }
                    });
                    id = (int)typeAnchor;
                    _typeExtensions.Add("any", id);
                }
                return (uint)id;
            }

            public Protobuf.Type GetType(SubstraitBaseType type, List<string>? names = default)
            {
                Protobuf.Type.Types.Nullability nullable = type.Nullable ? Protobuf.Type.Types.Nullability.Nullable : Protobuf.Type.Types.Nullability.Required;
                switch (type.Type)
                {
                    case Type.SubstraitType.Int64:
                        return new Protobuf.Type()
                        {
                            I64 = new Protobuf.Type.Types.I64()
                            {
                                Nullability = nullable
                            }
                        };
                    case SubstraitType.Int32:
                        return new Protobuf.Type()
                        {
                            I32 = new Protobuf.Type.Types.I32()
                            {
                                Nullability = nullable
                            }
                        };
                    case SubstraitType.Any:
                        var anyTypeId = GetAnyTypeId();
                        return new Protobuf.Type()
                        {
                            UserDefined = new Protobuf.Type.Types.UserDefined()
                            {
                                Nullability = nullable,
                                TypeReference = anyTypeId
                            }
                        };
                    case SubstraitType.Bool:
                        return new Protobuf.Type()
                        {
                            Bool = new Protobuf.Type.Types.Boolean()
                            {
                                Nullability = nullable
                            }
                        };
                    case SubstraitType.Binary:
                        return new Protobuf.Type()
                        {
                            Binary = new Protobuf.Type.Types.Binary()
                            {
                                Nullability = nullable
                            }
                        };
                    case SubstraitType.Date:
                        return new Protobuf.Type()
                        {
                            Date = new Protobuf.Type.Types.Date()
                            {
                                Nullability = nullable
                            }
                        };
                    case SubstraitType.Decimal:
                        return new Protobuf.Type()
                        {
                            Decimal = new Protobuf.Type.Types.Decimal()
                            {
                                Nullability = nullable
                            }
                        };
                    case SubstraitType.Fp32:
                        return new Protobuf.Type()
                        {
                            Fp32 = new Protobuf.Type.Types.FP32()
                            {
                                Nullability = nullable
                            }
                        };
                    case SubstraitType.Fp64:
                        return new Protobuf.Type()
                        {
                            Fp64 = new Protobuf.Type.Types.FP64()
                            {
                                Nullability = nullable
                            }
                        };
                    case SubstraitType.String:
                        return new Protobuf.Type()
                        {
                            String = new Protobuf.Type.Types.String()
                            {
                                Nullability = nullable
                            }
                        };
                    case SubstraitType.Struct:
                        if (names == null)
                        {
                            throw new NotSupportedException("names list must be provided with serializing named structs");
                        }
                        var structType = new Protobuf.Type.Types.Struct();
                        if (type is Type.NamedStruct namedStruct)
                        {
                            if (namedStruct.Struct != null)
                            {
                                for (int i = 0; i < namedStruct.Names.Count; i++)
                                {
                                    names.Add(namedStruct.Names[i]);
                                    structType.Types_.Add(GetType(namedStruct.Struct.Types[i], names));
                                }
                                return new Protobuf.Type()
                                {
                                    Struct = structType
                                };
                            }
                            else
                            {
                                throw new NotSupportedException("Inner structs must have data types");
                            }
                        }
                        else
                        {
                            throw new InvalidOperationException("Struct must be NamedStruct");
                        }
                    case SubstraitType.List:
                        if (type is ListType listType)
                        {
                            return new Protobuf.Type()
                            {
                                List = new Protobuf.Type.Types.List()
                                {
                                    Type = GetType(listType.ValueType, names),
                                    Nullability = nullable
                                }
                            };
                        }
                        else
                        {
                            throw new InvalidOperationException("List type must be ListType");
                        }
                    case SubstraitType.Map:
                        if (type is MapType mapType)
                        {
                            var keyType = GetType(mapType.KeyType, names);
                            var valueType = GetType(mapType.ValueType, names);
                            return new Protobuf.Type()
                            {
                                Map = new Protobuf.Type.Types.Map()
                                {
                                    Key = keyType,
                                    Value = valueType,
                                    Nullability = nullable
                                }
                            };
                        }
                        else
                        {
                            throw new InvalidOperationException("Map type must be MapType");
                        }
                    case SubstraitType.Null:
                        var anyTypeIdForNull = GetAnyTypeId();
                        return new Protobuf.Type()
                        {
                            UserDefined = new Protobuf.Type.Types.UserDefined()
                            {
                                Nullability = Protobuf.Type.Types.Nullability.Nullable,
                                TypeReference = anyTypeIdForNull
                            }
                        };
                    case SubstraitType.TimestampTz:
                        return new Protobuf.Type()
                        {
                            TimestampTz = new Protobuf.Type.Types.TimestampTZ()
                            {
                                Nullability = nullable
                            }
                        };
                    default:
                        throw new NotImplementedException(type.Type.ToString());
                }
            }

            public Protobuf.Plan Root { get; }
        }

        private sealed class SerializerExpressionVisitor : Substrait.Expressions.ExpressionVisitor<Protobuf.Expression, SerializerVisitorState>
        {
            public override Protobuf.Expression? VisitScalarFunction(ScalarFunction scalarFunction, SerializerVisitorState state)
            {
                var anchor = state.GetFunctionExtensionAnchor(scalarFunction.ExtensionUri, scalarFunction.ExtensionName);

                var scalar = new Protobuf.Expression.Types.ScalarFunction()
                {
                    FunctionReference = anchor,
                };
                foreach (var arg in scalarFunction.Arguments)
                {
                    scalar.Arguments.Add(new Protobuf.FunctionArgument()
                    {
                        Value = Visit(arg, state)
                    });
                }

                return new Protobuf.Expression()
                {
                    ScalarFunction = scalar
                };
            }

            public override Protobuf.Expression? VisitBoolLiteral(BoolLiteral boolLiteral, SerializerVisitorState state)
            {
                return new Protobuf.Expression()
                {
                    Literal = new Protobuf.Expression.Types.Literal()
                    {
                        Boolean = boolLiteral.Value
                    }
                };
            }

            public override Protobuf.Expression? VisitStringLiteral(StringLiteral stringLiteral, SerializerVisitorState state)
            {
                return new Protobuf.Expression()
                {
                    Literal = new Protobuf.Expression.Types.Literal()
                    {
                        String = stringLiteral.Value
                    }
                };
            }

            public override Protobuf.Expression? VisitNumericLiteral(NumericLiteral numericLiteral, SerializerVisitorState state)
            {
                if (numericLiteral.Value % 1 == 0)
                {
                    return new Protobuf.Expression()
                    {
                        Literal = new Protobuf.Expression.Types.Literal()
                        {
                            I64 = (long)numericLiteral.Value
                        }
                    };
                }

                return new Protobuf.Expression()
                {
                    Literal = new Protobuf.Expression.Types.Literal()
                    {
                        Fp64 = (double)numericLiteral.Value
                    }
                };
            }

            public override Protobuf.Expression? VisitNullLiteral(NullLiteral nullLiteral, SerializerVisitorState state)
            {
                return new Protobuf.Expression
                {
                    Literal = new Protobuf.Expression.Types.Literal()
                    {
                        Null = new Protobuf.Type()
                    }
                };
            }

            public override Protobuf.Expression? VisitIfThen(IfThenExpression ifThenExpression, SerializerVisitorState state)
            {
                var ifThen = new Protobuf.Expression.Types.IfThen();
                foreach (var ifStatement in ifThenExpression.Ifs)
                {
                    ifThen.Ifs.Add(new Protobuf.Expression.Types.IfThen.Types.IfClause()
                    {
                        If = Visit(ifStatement.If, state),
                        Then = Visit(ifStatement.Then, state)
                    });
                }
                if (ifThenExpression.Else != null)
                {
                    ifThen.Else = Visit(ifThenExpression.Else, state);
                }
                return new Protobuf.Expression()
                {
                    IfThen = ifThen
                };
            }

            public override Protobuf.Expression? VisitListNestedExpression(ListNestedExpression listNestedExpression, SerializerVisitorState state)
            {
                var list = new Protobuf.Expression.Types.Nested.Types.List();

                foreach(var item in listNestedExpression.Values)
                {
                    var itemExpr = Visit(item, state);
                    if (itemExpr == null)
                    {
                        throw new InvalidOperationException("Array literal contained expression that could not be parsed.");
                    }
                    list.Values.Add(itemExpr);
                }
                
                return new Protobuf.Expression()
                {
                    Nested = new Protobuf.Expression.Types.Nested()
                    {
                        List = list
                    }
                };
            }

            public override Protobuf.Expression? VisitArrayLiteral(ArrayLiteral arrayLiteral, SerializerVisitorState state)
            {
                var list = new Protobuf.Expression.Types.Literal.Types.List();

                foreach (var item in arrayLiteral.Expressions)
                {
                    var itemExpr = Visit(item, state);
                    if (itemExpr == null)
                    {
                        throw new InvalidOperationException("Array literal contained expression that could not be parsed.");
                    }
                    if (itemExpr.Literal != null)
                    {
                        list.Values.Add(itemExpr.Literal);
                    }
                    else
                    {
                        throw new InvalidOperationException("Array literal can only contain literals");
                    }
                }

                var expr = new Protobuf.Expression()
                {
                    Literal = new Protobuf.Expression.Types.Literal()
                    {
                        List = list
                    }
                };
                return expr;
            }

            public override Protobuf.Expression? VisitDirectFieldReference(DirectFieldReference directFieldReference, SerializerVisitorState state)
            {
                if (directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                {
                    var expr = new Protobuf.Expression()
                    {
                        Selection = new Protobuf.Expression.Types.FieldReference()
                        {
                            DirectReference = new Protobuf.Expression.Types.ReferenceSegment()
                            {
                                StructField = new Protobuf.Expression.Types.ReferenceSegment.Types.StructField()
                                {
                                    Field = structReferenceSegment.Field
                                }
                            }
                        }
                    };

                    return expr;
                }

                throw new NotImplementedException();
            }

            public override Protobuf.Expression? VisitSingularOrList(SingularOrListExpression singularOrList, SerializerVisitorState state)
            {
                var list = new Protobuf.Expression.Types.SingularOrList()
                {
                    Value = Visit(singularOrList.Value, state)
                };

                foreach (var opt in singularOrList.Options)
                {
                    list.Options.Add(Visit(opt, state));
                }
                return new Protobuf.Expression()
                {
                    SingularOrList = list
                };
            }

            public override Protobuf.Expression? VisitMultiOrList(MultiOrListExpression multiOrList, SerializerVisitorState state)
            {
                var list = new Protobuf.Expression.Types.MultiOrList();

                foreach (var val in multiOrList.Value)
                {
                    list.Value.Add(Visit(val, state));
                }
                foreach (var opt in multiOrList.Options)
                {
                    var record = new Protobuf.Expression.Types.MultiOrList.Types.Record();
                    foreach (var optVal in opt.Fields)
                    {
                        record.Fields.Add(Visit(optVal, state));
                    }
                    list.Options.Add(record);
                }
                return new Protobuf.Expression()
                {
                    MultiOrList = list
                };
            }

            public override Protobuf.Expression? VisitCastExpression(CastExpression castExpression, SerializerVisitorState state)
            {
                return new Protobuf.Expression()
                {
                    Cast = new Protobuf.Expression.Types.Cast()
                    {
                        Input = Visit(castExpression.Expression, state),
                        FailureBehavior = Protobuf.Expression.Types.Cast.Types.FailureBehavior.ReturnNull,
                        Type = state.GetType(castExpression.Type)
                    }
                };
            }

            public override Protobuf.Expression? VisitStructExpression(StructExpression structExpression, SerializerVisitorState state)
            {
                var s = new Protobuf.Expression.Types.Nested.Types.Struct();
                foreach (var field in structExpression.Fields)
                {
                    s.Fields.Add(Visit(field, state));
                }
                return new Protobuf.Expression()
                {
                    Nested = new Protobuf.Expression.Types.Nested()
                    {
                        Struct = s
                    }
                };
            }

            public override Protobuf.Expression? VisitBinaryLiteral(BinaryLiteral binaryLiteral, SerializerVisitorState state)
            {
                var literal = new Protobuf.Expression.Types.Literal();
                literal.Binary = ByteString.CopyFrom(binaryLiteral.Value);
                return new Protobuf.Expression()
                {
                    Literal = literal
                };
            }

            public override Protobuf.Expression? VisitMapNestedExpression(MapNestedExpression mapNestedExpression, SerializerVisitorState state)
            {
                var output = new Protobuf.Expression()
                {
                    Nested = new Protobuf.Expression.Types.Nested()
                    {
                        Map = new Protobuf.Expression.Types.Nested.Types.Map()
                    }
                };
                for (int i = 0; i < mapNestedExpression.KeyValues.Count; i++)
                {
                    var key = Visit(mapNestedExpression.KeyValues[i].Key, state);
                    var value = Visit(mapNestedExpression.KeyValues[i].Value, state);

                    output.Nested.Map.KeyValues.Add(new Protobuf.Expression.Types.Nested.Types.Map.Types.KeyValue()
                    {
                        Key = key,
                        Value = value
                    });
                }

                return output;
            }
        }

        private sealed class SerializerVisitor : RelationVisitor<Protobuf.Rel, SerializerVisitorState>
        {
            public SerializerVisitor()
            {
            }

            public override Protobuf.Rel VisitReadRelation(ReadRelation readRelation, SerializerVisitorState state)
            {
                var readRel = new Protobuf.ReadRel();

                if (readRelation.NamedTable != null)
                {
                    readRel.NamedTable = new Protobuf.ReadRel.Types.NamedTable();
                    readRel.NamedTable.Names.AddRange(readRelation.NamedTable.Names);
                }
                if (readRelation.BaseSchema != null)
                {
                    readRel.BaseSchema = SerializeNamedStruct(readRelation.BaseSchema, state);
                }
                if (readRelation.Filter != null)
                {
                    var exprVisitor = new SerializerExpressionVisitor();
                    readRel.Filter = exprVisitor.Visit(readRelation.Filter, state);
                }
                if (readRelation.EmitSet)
                {
                    readRel.Common = new Protobuf.RelCommon();
                    readRel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    readRel.Common.Emit.OutputMapping.AddRange(readRelation.Emit);
                }

                return new Protobuf.Rel()
                {
                    Read = readRel
                };
            }

            public override Protobuf.Rel VisitProjectRelation(ProjectRelation projectRelation, SerializerVisitorState state)
            {
                var projectRel = new Protobuf.ProjectRel();

                if (projectRelation.Expressions != null)
                {
                    var exprVisitor = new SerializerExpressionVisitor();
                    foreach (var expr in projectRelation.Expressions)
                    {
                        projectRel.Expressions.Add(exprVisitor.Visit(expr, state));
                    }
                }
                if (projectRelation.EmitSet)
                {
                    projectRel.Common = new Protobuf.RelCommon();
                    projectRel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    projectRel.Common.Emit.OutputMapping.AddRange(projectRelation.Emit);
                }
                projectRel.Input = Visit(projectRelation.Input, state);

                return new Protobuf.Rel()
                {
                    Project = projectRel
                };
            }

            public override Protobuf.Rel VisitFilterRelation(FilterRelation filterRelation, SerializerVisitorState state)
            {
                var filterRel = new Protobuf.FilterRel();

                if (filterRelation.Condition != null)
                {
                    var exprVisitor = new SerializerExpressionVisitor();
                    filterRel.Condition = exprVisitor.Visit(filterRelation.Condition, state);
                }
                if (filterRelation.EmitSet)
                {
                    filterRel.Common = new Protobuf.RelCommon();
                    filterRel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    filterRel.Common.Emit.OutputMapping.AddRange(filterRelation.Emit);
                }

                filterRel.Input = Visit(filterRelation.Input, state);

                return new Protobuf.Rel()
                {
                    Filter = filterRel
                };
            }

            public override Protobuf.Rel VisitAggregateRelation(AggregateRelation aggregateRelation, SerializerVisitorState state)
            {
                var aggRel = new Protobuf.AggregateRel();

                if (aggregateRelation.Groupings != null)
                {
                    var exprVisitor = new SerializerExpressionVisitor();

                    foreach (var grouping in aggregateRelation.Groupings)
                    {
                        var grp = new Protobuf.AggregateRel.Types.Grouping();
                        foreach (var groupExpr in grouping.GroupingExpressions)
                        {
                            grp.GroupingExpressions.Add(exprVisitor.Visit(groupExpr, state));
                        }
                        aggRel.Groupings.Add(grp);
                    }
                }

                if (aggregateRelation.Measures != null)
                {
                    var exprVisitor = new SerializerExpressionVisitor();
                    foreach (var measure in aggregateRelation.Measures)
                    {
                        var m = new Protobuf.AggregateRel.Types.Measure();
                        if (measure.Filter != null)
                        {
                            m.Filter = exprVisitor.Visit(measure.Filter, state);
                        }
                        if (measure.Measure != null)
                        {
                            m.Measure_ = new Protobuf.AggregateFunction()
                            {
                                FunctionReference = state.GetFunctionExtensionAnchor(measure.Measure.ExtensionUri, measure.Measure.ExtensionName)
                            };
                            if (measure.Measure.Arguments != null)
                            {
                                foreach (var arg in measure.Measure.Arguments)
                                {
                                    m.Measure_.Arguments.Add(new Protobuf.FunctionArgument()
                                    {
                                        Value = exprVisitor.Visit(arg, state)
                                    });
                                }
                            }
                        }
                        aggRel.Measures.Add(m);
                    }
                }

                if (aggregateRelation.EmitSet)
                {
                    aggRel.Common = new Protobuf.RelCommon();
                    aggRel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    aggRel.Common.Emit.OutputMapping.AddRange(aggregateRelation.Emit);
                }

                aggRel.Input = Visit(aggregateRelation.Input, state);

                return new Protobuf.Rel()
                {
                    Aggregate = aggRel
                };
            }

            public override Protobuf.Rel VisitIterationRelation(IterationRelation iterationRelation, SerializerVisitorState state)
            {
                FlowtideDotNet.Substrait.CustomProtobuf.IterationRelation iterRel = new FlowtideDotNet.Substrait.CustomProtobuf.IterationRelation();
                iterRel.IterationName = iterationRelation.IterationName;

                var rel = new Protobuf.ExtensionMultiRel()
                {
                    Detail = Google.Protobuf.WellKnownTypes.Any.Pack(iterRel)
                };

                rel.Inputs.Add(Visit(iterationRelation.LoopPlan, state));
                if (iterationRelation.Input != null)
                {
                    rel.Inputs.Add(Visit(iterationRelation.Input, state));
                }

                if (iterationRelation.EmitSet)
                {
                    rel.Common = new Protobuf.RelCommon();
                    rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    rel.Common.Emit.OutputMapping.AddRange(iterationRelation.Emit);
                }

                return new Protobuf.Rel()
                {
                    ExtensionMulti = rel
                };
            }

            public override Protobuf.Rel VisitIterationReferenceReadRelation(IterationReferenceReadRelation iterationReferenceReadRelation, SerializerVisitorState state)
            {
                CustomProtobuf.IterationReferenceReadRelation iterRel = new CustomProtobuf.IterationReferenceReadRelation();
                iterRel.IterationName = iterationReferenceReadRelation.IterationName;
                iterRel.OutputLength = iterationReferenceReadRelation.ReferenceOutputLength;

                var rel = new Protobuf.ExtensionLeafRel();
                rel.Detail = Google.Protobuf.WellKnownTypes.Any.Pack(iterRel);

                if (iterationReferenceReadRelation.EmitSet)
                {
                    rel.Common = new Protobuf.RelCommon();
                    rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    rel.Common.Emit.OutputMapping.AddRange(iterationReferenceReadRelation.Emit);
                }

                return new Protobuf.Rel()
                {
                    ExtensionLeaf = rel
                };
            }

            public override Protobuf.Rel VisitJoinRelation(JoinRelation joinRelation, SerializerVisitorState state)
            {
                var joinRel = new Protobuf.JoinRel();

                var exprVisitor = new SerializerExpressionVisitor();
                if (joinRelation.Expression != null)
                {
                    joinRel.Expression = exprVisitor.Visit(joinRelation.Expression, state);
                }

                if (joinRelation.PostJoinFilter != null)
                {
                    joinRel.Expression = exprVisitor.Visit(joinRelation.PostJoinFilter, state);
                }

                if (joinRelation.EmitSet)
                {
                    joinRel.Common = new Protobuf.RelCommon();
                    joinRel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    joinRel.Common.Emit.OutputMapping.AddRange(joinRelation.Emit);
                }

                switch (joinRelation.Type)
                {
                    case JoinType.Anti:
                        joinRel.Type = Protobuf.JoinRel.Types.JoinType.Anti;
                        break;
                    case JoinType.Semi:
                        joinRel.Type = Protobuf.JoinRel.Types.JoinType.Semi;
                        break;
                    case JoinType.Inner:
                        joinRel.Type = Protobuf.JoinRel.Types.JoinType.Inner;
                        break;
                    case JoinType.Unspecified:
                        joinRel.Type = Protobuf.JoinRel.Types.JoinType.Unspecified;
                        break;
                    case JoinType.Left:
                        joinRel.Type = Protobuf.JoinRel.Types.JoinType.Left;
                        break;
                    case JoinType.Outer:
                        joinRel.Type = Protobuf.JoinRel.Types.JoinType.Outer;
                        break;
                    case JoinType.Right:
                        joinRel.Type = Protobuf.JoinRel.Types.JoinType.Right;
                        break;
                    case JoinType.Single:
                        joinRel.Type = Protobuf.JoinRel.Types.JoinType.Single;
                        break;
                }

                joinRel.Left = Visit(joinRelation.Left, state);
                joinRel.Right = Visit(joinRelation.Right, state);

                return new Protobuf.Rel()
                {
                    Join = joinRel
                };
            }

            private Protobuf.Expression.Types.WindowFunction.Types.Bound GetWindowBound(WindowBound windowBound)
            {
                switch (windowBound.Type)
                {
                    case WindowBoundType.CurrentRow:
                        return new Protobuf.Expression.Types.WindowFunction.Types.Bound()
                        {
                            CurrentRow = new Protobuf.Expression.Types.WindowFunction.Types.Bound.Types.CurrentRow()
                        };
                    case WindowBoundType.Unbounded:
                        return new Protobuf.Expression.Types.WindowFunction.Types.Bound()
                        {
                            Unbounded = new Protobuf.Expression.Types.WindowFunction.Types.Bound.Types.Unbounded()
                        };
                    case WindowBoundType.PreceedingRow:
                        if (windowBound is PreceedingRowWindowBound rangeWindowBound)
                        {
                            return new Protobuf.Expression.Types.WindowFunction.Types.Bound()
                            {
                                Preceding = new Protobuf.Expression.Types.WindowFunction.Types.Bound.Types.Preceding()
                                {
                                    Offset = rangeWindowBound.Offset
                                }
                            };
                        }
                        throw new InvalidOperationException("PreceedingRange must be PreceedingRowWindowBound");
                    case WindowBoundType.FollowingRow:
                        if (windowBound is FollowingRowWindowBound followingRowWindowBound)
                        {
                            return new Protobuf.Expression.Types.WindowFunction.Types.Bound()
                            {
                                Following = new Protobuf.Expression.Types.WindowFunction.Types.Bound.Types.Following()
                                {
                                    Offset = followingRowWindowBound.Offset
                                }
                            };
                        }
                        throw new InvalidOperationException("FollowingRange must be FollowingRowWindowBound");
                    default:
                        throw new InvalidOperationException("Window bound type not implemented");
                }
            }

            private Protobuf.ConsistentPartitionWindowRel.Types.WindowRelFunction GetWindowRelFunction(WindowFunction windowFunction, SerializerVisitorState state)
            {
                var exprVisitor = new SerializerExpressionVisitor();

                var output = new ConsistentPartitionWindowRel.Types.WindowRelFunction()
                {
                };

                foreach(var arg in windowFunction.Arguments)
                {
                    output.Arguments.Add(new FunctionArgument()
                    {
                        Value = exprVisitor.Visit(arg, state)
                    });
                }

                output.FunctionReference = state.GetFunctionExtensionAnchor(windowFunction.ExtensionUri, windowFunction.ExtensionName);
                output.Invocation = Protobuf.AggregateFunction.Types.AggregationInvocation.Unspecified;
                
                if (windowFunction.LowerBound != null)
                {
                    output.LowerBound = GetWindowBound(windowFunction.LowerBound);
                }
                if (windowFunction.UpperBound != null)
                {
                    output.UpperBound = GetWindowBound(windowFunction.UpperBound);
                }
                output.Phase = AggregationPhase.Unspecified;

                return output;
            }

            public override Rel VisitConsistentPartitionWindowRelation(ConsistentPartitionWindowRelation consistentPartitionWindowRelation, SerializerVisitorState state)
            {
                var rel = new ConsistentPartitionWindowRel();

                var exprVisitor = new SerializerExpressionVisitor();

                if (consistentPartitionWindowRelation.PartitionBy != null)
                {
                    foreach (var expr in consistentPartitionWindowRelation.PartitionBy)
                    {
                        rel.PartitionExpressions.Add(exprVisitor.Visit(expr, state));
                    }
                }

                if (consistentPartitionWindowRelation.OrderBy != null)
                {
                    foreach (var order in consistentPartitionWindowRelation.OrderBy)
                    {
                        rel.Sorts.Add(GetSortField(order, state));
                    }
                }

                foreach(var func in consistentPartitionWindowRelation.WindowFunctions)
                {
                    rel.WindowFunctions.Add(GetWindowRelFunction(func, state));
                }

                rel.Input = Visit(consistentPartitionWindowRelation.Input, state);

                if (consistentPartitionWindowRelation.EmitSet)
                {
                    rel.Common = new Protobuf.RelCommon();
                    rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    rel.Common.Emit.OutputMapping.AddRange(consistentPartitionWindowRelation.Emit);
                }

                return new Rel()
                {
                    Window = rel
                };
            }

            public override Rel VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, SerializerVisitorState state)
            {
                var rel = new MergeJoinRel();

                var exprVisitor = new SerializerExpressionVisitor();

                for (int i = 0; i < mergeJoinRelation.LeftKeys.Count; i++)
                {
                    var leftKey = mergeJoinRelation.LeftKeys[i];
                    var rightKey = mergeJoinRelation.RightKeys[i];
                    if (leftKey is DirectFieldReference directFieldReferenceLeft &&
                        directFieldReferenceLeft.ReferenceSegment is StructReferenceSegment structReferenceSegmentLeft &&
                        rightKey is DirectFieldReference directFieldReferenceRight &&
                        directFieldReferenceRight.ReferenceSegment is StructReferenceSegment structReferenceSegmentRight)
                    {
                        rel.Keys.Add(new ComparisonJoinKey()
                        {
                            Comparison = new ComparisonJoinKey.Types.ComparisonType()
                            {
                                Simple = ComparisonJoinKey.Types.SimpleComparisonType.Eq
                            },
                            Left = new Protobuf.Expression.Types.FieldReference()
                            {
                                DirectReference = new Protobuf.Expression.Types.ReferenceSegment()
                                {
                                    StructField = new Protobuf.Expression.Types.ReferenceSegment.Types.StructField()
                                    {
                                        Field = structReferenceSegmentLeft.Field
                                    }
                                }
                            },
                            Right = new Protobuf.Expression.Types.FieldReference()
                            {
                                DirectReference = new Protobuf.Expression.Types.ReferenceSegment()
                                {
                                    StructField = new Protobuf.Expression.Types.ReferenceSegment.Types.StructField()
                                    {
                                        Field = structReferenceSegmentRight.Field
                                    }
                                }
                            }
                        });
                    }
                    else
                    {
                        throw new NotImplementedException("Only direct field reference is implemented");
                    }
                }

                if (mergeJoinRelation.PostJoinFilter != null)
                {
                    rel.PostJoinFilter = exprVisitor.Visit(mergeJoinRelation.PostJoinFilter, state);
                }

                switch (mergeJoinRelation.Type)
                {
                    case JoinType.Anti:
                        throw new NotSupportedException("Anti not supported in merge join");
                    case JoinType.Semi:
                        throw new NotSupportedException("Semi not supported in merge join");
                    case JoinType.Inner:
                        rel.Type = Protobuf.MergeJoinRel.Types.JoinType.Inner;
                        break;
                    case JoinType.Unspecified:
                        rel.Type = Protobuf.MergeJoinRel.Types.JoinType.Unspecified;
                        break;
                    case JoinType.Left:
                        rel.Type = Protobuf.MergeJoinRel.Types.JoinType.Left;
                        break;
                    case JoinType.Outer:
                        rel.Type = Protobuf.MergeJoinRel.Types.JoinType.Outer;
                        break;
                    case JoinType.Right:
                        rel.Type = Protobuf.MergeJoinRel.Types.JoinType.Right;
                        break;
                    case JoinType.Single:
                        throw new NotSupportedException("Single not supported in merge join");
                }

                if (mergeJoinRelation.EmitSet)
                {
                    rel.Common = new Protobuf.RelCommon();
                    rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    rel.Common.Emit.OutputMapping.AddRange(mergeJoinRelation.Emit);
                }

                rel.Left = Visit(mergeJoinRelation.Left, state);
                rel.Right = Visit(mergeJoinRelation.Right, state);

                return new Rel()
                {
                    MergeJoin = rel
                };
            }

            public override Rel VisitNormalizationRelation(NormalizationRelation normalizationRelation, SerializerVisitorState state)
            {
                var rel = new Protobuf.ExtensionSingleRel();
                var customRel = new CustomProtobuf.NormalizationRelation();

                var exprVisitor = new SerializerExpressionVisitor();

                if (normalizationRelation.Filter != null)
                {
                    customRel.Filter = exprVisitor.Visit(normalizationRelation.Filter, state);
                }
                foreach (var k in normalizationRelation.KeyIndex)
                {
                    customRel.KeyIndex.Add(k);
                }
                rel.Detail = Google.Protobuf.WellKnownTypes.Any.Pack(customRel);

                if (normalizationRelation.EmitSet)
                {
                    rel.Common = new Protobuf.RelCommon();
                    rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    rel.Common.Emit.OutputMapping.AddRange(normalizationRelation.Emit);
                }

                rel.Input = Visit(normalizationRelation.Input, state);

                return new Protobuf.Rel()
                {
                    ExtensionSingle = rel
                };
            }

            public override Rel VisitReferenceRelation(ReferenceRelation referenceRelation, SerializerVisitorState state)
            {
                var refRel = new ReferenceRel()
                {
                    SubtreeOrdinal = referenceRelation.RelationId
                };

                return new Rel()
                {
                    Reference = refRel
                };
            }

            public override Rel VisitBufferRelation(BufferRelation bufferRelation, SerializerVisitorState state)
            {
                var rel = new Protobuf.ExtensionSingleRel();
                var bufRel = new CustomProtobuf.BufferRelation();
                rel.Detail = Google.Protobuf.WellKnownTypes.Any.Pack(bufRel);

                if (bufferRelation.EmitSet)
                {
                    rel.Common = new Protobuf.RelCommon();
                    rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    rel.Common.Emit.OutputMapping.AddRange(bufferRelation.Emit);
                }
                rel.Input = Visit(bufferRelation.Input, state);

                return new Protobuf.Rel()
                {
                    ExtensionSingle = rel
                };
            }

            public override Rel VisitSetRelation(SetRelation setRelation, SerializerVisitorState state)
            {
                var rel = new Protobuf.SetRel();
                switch (setRelation.Operation)
                {
                    case SetOperation.UnionDistinct:
                        rel.Op = Protobuf.SetRel.Types.SetOp.UnionDistinct;
                        break;
                    case SetOperation.IntersectionPrimary:
                        rel.Op = Protobuf.SetRel.Types.SetOp.IntersectionPrimary;
                        break;
                    case SetOperation.MinusPrimary:
                        rel.Op = Protobuf.SetRel.Types.SetOp.MinusPrimary;
                        break;
                    case SetOperation.IntersectionMultiset:
                        rel.Op = Protobuf.SetRel.Types.SetOp.IntersectionMultiset;
                        break;
                    case SetOperation.MinusMultiset:
                        rel.Op = Protobuf.SetRel.Types.SetOp.MinusMultiset;
                        break;
                    case SetOperation.UnionAll:
                        rel.Op = Protobuf.SetRel.Types.SetOp.UnionAll;
                        break;
                    case SetOperation.Unspecified:
                        rel.Op = Protobuf.SetRel.Types.SetOp.Unspecified;
                        break;
                }

                if (setRelation.EmitSet)
                {
                    rel.Common = new Protobuf.RelCommon();
                    rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    rel.Common.Emit.OutputMapping.AddRange(setRelation.Emit);
                }

                foreach (var input in setRelation.Inputs)
                {
                    rel.Inputs.Add(Visit(input, state));
                }

                return new Rel()
                {
                    Set = rel
                };
            }

            public override Rel VisitVirtualTableReadRelation(VirtualTableReadRelation virtualTableReadRelation, SerializerVisitorState state)
            {
                var rel = new Protobuf.ReadRel();
                rel.VirtualTable = new ReadRel.Types.VirtualTable();

                var exprVisitor = new SerializerExpressionVisitor();
                foreach (var val in virtualTableReadRelation.Values.Expressions)
                {
                    var expr = exprVisitor.Visit(val, state);
                    if (expr?.Nested?.Struct != null)
                    {
                        rel.VirtualTable.Expressions.Add(expr.Nested.Struct);
                    }
                }
                rel.BaseSchema = SerializeNamedStruct(virtualTableReadRelation.BaseSchema, state);
                if (virtualTableReadRelation.EmitSet)
                {
                    rel.Common = new Protobuf.RelCommon();
                    rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    rel.Common.Emit.OutputMapping.AddRange(virtualTableReadRelation.Emit);
                }
                return new Rel()
                {
                    Read = rel
                };
            }

            public override Protobuf.Rel VisitWriteRelation(WriteRelation writeRelation, SerializerVisitorState state)
            {
                var writeRel = new Protobuf.WriteRel();

                if (writeRelation.TableSchema != null)
                {
                    writeRel.TableSchema = SerializeNamedStruct(writeRelation.TableSchema, state);
                }
                if (writeRelation.NamedObject != null)
                {
                    writeRel.NamedTable = new Protobuf.NamedObjectWrite();
                    writeRel.NamedTable.Names.AddRange(writeRelation.NamedObject.Names);
                }
                writeRel.Input = Visit(writeRelation.Input, state);

                return new Protobuf.Rel()
                {
                    Write = writeRel
                };
            }

            public override Rel VisitExchangeRelation(ExchangeRelation exchangeRelation, SerializerVisitorState state)
            {
                var output = new ExchangeRel()
                {
                    PartitionCount = exchangeRelation.PartitionCount.HasValue ? exchangeRelation.PartitionCount.Value : 0,
                };
                if (exchangeRelation.ExchangeKind.Type == ExchangeKindType.Scatter)
                {
                    output.ScatterByFields = new ExchangeRel.Types.ScatterFields();

                    if (exchangeRelation.ExchangeKind is ScatterExchangeKind scatterExchangeKind)
                    {
                        var exprVisitor = new SerializerExpressionVisitor();
                        foreach (var field in scatterExchangeKind.Fields)
                        {
                            var fieldExpr = exprVisitor.Visit(field, state);

                            if (fieldExpr == null)
                            {
                                throw new InvalidOperationException("Scatter field could not be serialized");
                            }

                            output.ScatterByFields.Fields.Add(fieldExpr.Selection);
                        }
                    }
                }
                else if (exchangeRelation.ExchangeKind.Type == ExchangeKindType.Broadcast)
                {
                    output.Broadcast = new ExchangeRel.Types.Broadcast();
                }
                else
                {
                    throw new NotImplementedException("Unsupported exchange kind type");
                }

                output.Input = Visit(exchangeRelation.Input, state);

                foreach(var target in exchangeRelation.Targets)
                {
                    var protoTarget = new ExchangeRel.Types.ExchangeTarget();
                    foreach(var partitionId in target.PartitionIds)
                    {
                        protoTarget.PartitionId.Add(partitionId);
                    }
                    switch (target.Type)
                    {
                        case ExchangeTargetType.StandardOutput:
                            protoTarget.Uri = "standard_output";
                            break;
                        case ExchangeTargetType.PullBucket:
                            // TODO: Fix later on when distributed mode is on.
                            throw new NotImplementedException();
                    }
                    output.Targets.Add(protoTarget);
                }
                
                if (exchangeRelation.EmitSet)
                {
                    output.Common = new Protobuf.RelCommon();
                    output.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    output.Common.Emit.OutputMapping.AddRange(exchangeRelation.Emit);
                }

                return new Rel()
                {
                    Exchange = output
                };
            }

            public override Rel VisitStandardOutputExchangeReferenceRelation(StandardOutputExchangeReferenceRelation standardOutputExchangeReferenceRelation, SerializerVisitorState state)
            {

                FlowtideDotNet.Substrait.CustomProtobuf.StandardOutputTargetReferenceRelation target = new FlowtideDotNet.Substrait.CustomProtobuf.StandardOutputTargetReferenceRelation();
                target.RelationId = standardOutputExchangeReferenceRelation.RelationId;
                target.TargetId = standardOutputExchangeReferenceRelation.TargetId;
                

                var rel = new Protobuf.ExtensionLeafRel()
                {
                    Detail = Google.Protobuf.WellKnownTypes.Any.Pack(target)
                };
                

                return new Rel()
                {
                    ExtensionLeaf = rel
                };
            }

            private Protobuf.SortField.Types.SortDirection GetSortDirection(SortDirection sortDirection)
            {
                switch (sortDirection)
                {
                    case SortDirection.SortDirectionUnspecified:
                        return Protobuf.SortField.Types.SortDirection.Unspecified;
                    case SortDirection.SortDirectionAscNullsFirst:
                        return Protobuf.SortField.Types.SortDirection.AscNullsFirst;
                    case SortDirection.SortDirectionAscNullsLast:
                        return Protobuf.SortField.Types.SortDirection.AscNullsLast;
                    case SortDirection.SortDirectionDescNullsFirst:
                        return Protobuf.SortField.Types.SortDirection.DescNullsFirst;
                    case SortDirection.SortDirectionDescNullsLast:
                        return Protobuf.SortField.Types.SortDirection.DescNullsLast;
                    case SortDirection.SortDirectionClustered:
                        return Protobuf.SortField.Types.SortDirection.Clustered;
                    default:
                        throw new NotImplementedException();
                }
            }

            private Protobuf.SortField GetSortField(Expressions.SortField sortField, SerializerVisitorState state)
            {
                var exprVisitor = new SerializerExpressionVisitor();
                return new Protobuf.SortField()
                {
                    Direction = GetSortDirection(sortField.SortDirection),
                    Expr = exprVisitor.Visit(sortField.Expression, state)
                };
            }

            public override Rel VisitTopNRelation(TopNRelation topNRelation, SerializerVisitorState state)
            {
                var rel = new Protobuf.ExtensionSingleRel();
                var topRel = new CustomProtobuf.TopNRelation();
                topRel.Offset = topNRelation.Offset;
                topRel.Count = topNRelation.Count;

                foreach (var sortField in topNRelation.Sorts)
                {
                    topRel.Sorts.Add(GetSortField(sortField, state));
                }

                rel.Detail = Google.Protobuf.WellKnownTypes.Any.Pack(topRel);

                if (topNRelation.EmitSet)
                {
                    rel.Common = new Protobuf.RelCommon();
                    rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    rel.Common.Emit.OutputMapping.AddRange(topNRelation.Emit);
                }
                rel.Input = Visit(topNRelation.Input, state);

                return new Protobuf.Rel()
                {
                    ExtensionSingle = rel
                };
            }

            

            private static Protobuf.NamedStruct SerializeNamedStruct(Type.NamedStruct namedStruct, SerializerVisitorState state)
            {
                var protoNamedStruct = new Protobuf.NamedStruct();
                List<string> names = new List<string>();
                List<Protobuf.Type> types = new List<Protobuf.Type>();
                if (namedStruct.Struct != null)
                {
                    for (int i = 0; i < namedStruct.Names.Count; i++)
                    {
                        var name = namedStruct.Names[i];
                        names.Add(name);
                        types.Add(state.GetType(namedStruct.Struct.Types[i], names));
                    }
                    protoNamedStruct.Struct = new Protobuf.Type.Types.Struct();
                    protoNamedStruct.Struct.Types_.AddRange(types);
                }
                else
                {
                    names.AddRange(namedStruct.Names);
                }
                protoNamedStruct.Names.AddRange(names); 
                return protoNamedStruct;
            }

            private static Protobuf.Type.Types.Struct SerializeStruct(Type.Struct structType, SerializerVisitorState state)
            {
                var protoStruct = new Protobuf.Type.Types.Struct();
                foreach (var t in structType.Types)
                {
                    protoStruct.Types_.Add(state.GetType(t));
                }
                return protoStruct;
            }

            private static CustomProtobuf.TableFunction CreateTableFunctionProtoDefintion(TableFunction tableFunction, SerializerVisitorState state)
            {
                var protoDef = new CustomProtobuf.TableFunction();

                // Serialize the table function
                protoDef.FunctionReference = state.GetFunctionExtensionAnchor(tableFunction.ExtensionUri, tableFunction.ExtensionName);

                // Serialize the table schema if it exists
                if (tableFunction.TableSchema != null)
                {
                    protoDef.TableSchema = SerializeNamedStruct(tableFunction.TableSchema, state);
                }

                // Serialize function arguments
                if (tableFunction.Arguments != null)
                {
                    var exprVisitor = new SerializerExpressionVisitor();
                    foreach (var arg in tableFunction.Arguments)
                    {
                        protoDef.Arguments.Add(new Protobuf.FunctionArgument()
                        {
                            Value = exprVisitor.Visit(arg, state)
                        });
                    }
                }

                return protoDef;
            }

            private static CustomProtobuf.TableFunctionRelation CreateTableFunctionRelationProtoDefintion(TableFunctionRelation tableFunctionRelation, SerializerVisitorState state)
            {
                var protoDef = new CustomProtobuf.TableFunctionRelation
                {
                    // Serialize the table function
                    TableFunction = CreateTableFunctionProtoDefintion(tableFunctionRelation.TableFunction, state)
                };

                // Set join type
                switch (tableFunctionRelation.Type)
                {
                    case JoinType.Left:
                        protoDef.Type = CustomProtobuf.TableFunctionRelation.Types.JoinType.Left;
                        break;
                    case JoinType.Inner:
                        protoDef.Type = CustomProtobuf.TableFunctionRelation.Types.JoinType.Inner;
                        break;
                    default:
                        protoDef.Type = CustomProtobuf.TableFunctionRelation.Types.JoinType.Unspecified;
                        break;
                }

                if (tableFunctionRelation.JoinCondition != null)
                {
                    var exprVisitor = new SerializerExpressionVisitor();
                    protoDef.JoinCondition = exprVisitor.Visit(tableFunctionRelation.JoinCondition, state);
                }
                return protoDef;
            }

            public override Rel VisitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, SerializerVisitorState state)
            {
                var protoDef = CreateTableFunctionRelationProtoDefintion(tableFunctionRelation, state);

                // Check if it has an input, then we will use extension single rel, otherwise leaf.
                if (tableFunctionRelation.Input != null)
                {
                    var rel = new Protobuf.ExtensionSingleRel();
                    rel.Input = Visit(tableFunctionRelation.Input, state);
                    rel.Detail = Google.Protobuf.WellKnownTypes.Any.Pack(protoDef);

                    // Emit info
                    if (tableFunctionRelation.EmitSet)
                    {
                        rel.Common = new Protobuf.RelCommon();
                        rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                        rel.Common.Emit.OutputMapping.AddRange(tableFunctionRelation.Emit);
                    }

                    return new Rel()
                    {
                        ExtensionSingle = rel
                    };
                }
                else
                {
                    var rel = new Protobuf.ExtensionLeafRel();
                    rel.Detail = Google.Protobuf.WellKnownTypes.Any.Pack(protoDef);
                    // Emit info
                    if (tableFunctionRelation.EmitSet)
                    {
                        rel.Common = new Protobuf.RelCommon();
                        rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                        rel.Common.Emit.OutputMapping.AddRange(tableFunctionRelation.Emit);
                    }

                    return new Rel()
                    {
                        ExtensionLeaf = rel
                    };
                }
            }

            public override Rel VisitSubStreamRootRelation(SubStreamRootRelation subStreamRootRelation, SerializerVisitorState state)
            {
                var rel = new Protobuf.ExtensionSingleRel();

                var subStreamRel = new CustomProtobuf.SubStreamRootRelation();
                subStreamRel.SubstreamName = subStreamRootRelation.Name;

                rel.Detail = Google.Protobuf.WellKnownTypes.Any.Pack(subStreamRel);
                if (subStreamRootRelation.EmitSet)
                {
                    rel.Common = new Protobuf.RelCommon();
                    rel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    rel.Common.Emit.OutputMapping.AddRange(subStreamRootRelation.Emit);
                }

                rel.Input = Visit(subStreamRootRelation.Input, state);

                return new Rel()
                {
                    ExtensionSingle = rel
                };
            }

            public override Rel VisitFetchRelation(FetchRelation fetchRelation, SerializerVisitorState state)
            {
                var fetchRel = new Protobuf.FetchRel();

                fetchRel.Offset = fetchRelation.Offset;
                fetchRel.Count = fetchRelation.Count;
                if (fetchRelation.EmitSet)
                {
                    fetchRel.Common = new Protobuf.RelCommon();
                    fetchRel.Common.Emit = new Protobuf.RelCommon.Types.Emit();
                    fetchRel.Common.Emit.OutputMapping.AddRange(fetchRelation.Emit);
                }

                fetchRel.Input = Visit(fetchRelation.Input, state);
                return new Rel()
                {
                    Fetch = fetchRel
                };
            }
        }

        public static Protobuf.Plan Serialize(Plan plan)
        {
            var rootPlan = new Protobuf.Plan();

            var state = new SerializerVisitorState(rootPlan);
            var visitor = new SerializerVisitor();
            foreach (var relation in plan.Relations)
            {
                rootPlan.Relations.Add(new Protobuf.PlanRel()
                {
                    Rel = visitor.Visit(relation, state)
                });
            }
            return rootPlan;
        }

        public static string SerializeToJson(Plan plan)
        {
            var protoPlan = Serialize(plan);
            var typeRegistry = Google.Protobuf.Reflection.TypeRegistry.FromMessages(
                CustomProtobuf.IterationReferenceReadRelation.Descriptor,
                CustomProtobuf.IterationRelation.Descriptor,
                CustomProtobuf.NormalizationRelation.Descriptor,
                CustomProtobuf.TopNRelation.Descriptor,
                CustomProtobuf.StandardOutputTargetReferenceRelation.Descriptor);
            var settings = new Google.Protobuf.JsonFormatter.Settings(true, typeRegistry)
                .WithIndentation();
            var formatter = new Google.Protobuf.JsonFormatter(settings);
            return formatter.Format(protoPlan);
        }
    }
}

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

using Protobuf = Substrait.Protobuf;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Expressions.IfThen;
using Google.Protobuf;
using Substrait.Protobuf;

namespace FlowtideDotNet.Substrait
{
    internal class SubstraitSerializer
    {
        private class SerializerVisitorState
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

            public Protobuf.Rel? Previous { get; }
            public Protobuf.Plan Root { get; }
        }

        private class SerializerExpressionVisitor : Substrait.Expressions.ExpressionVisitor<Protobuf.Expression, SerializerVisitorState>
        {

            

            public override Protobuf.Expression? VisitScalarFunction(ScalarFunction scalarFunction, SerializerVisitorState state)
            {
                var functionName = $"{scalarFunction.ExtensionUri}:{scalarFunction.ExtensionName}";
                var anchor = state.GetFunctionExtensionAnchor(scalarFunction.ExtensionUri, scalarFunction.ExtensionName);

                var scalar = new Protobuf.Expression.Types.ScalarFunction()
                {
                    FunctionReference = anchor,
                };
                foreach(var arg in scalarFunction.Arguments)
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
                            I64 = (int)numericLiteral.Value
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
                foreach(var ifStatement in ifThenExpression.Ifs)
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

            public override Protobuf.Expression? VisitArrayLiteral(ArrayLiteral arrayLiteral, SerializerVisitorState state)
            {
                var list = new Protobuf.Expression.Types.Literal.Types.List();

                foreach(var item in arrayLiteral.Expressions)
                {
                    var itemExpr = Visit(item, state);
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

                foreach(var opt in singularOrList.Options)
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
                
                foreach(var val in multiOrList.Value)
                {
                    list.Value.Add(Visit(val, state));
                }
                foreach (var opt in multiOrList.Options)
                {
                    var record = new Protobuf.Expression.Types.MultiOrList.Types.Record();
                    foreach(var optVal in opt.Fields)
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
        }

        private class SerializerVisitor : RelationVisitor<Protobuf.Rel, SerializerVisitorState>
        {
            

            public SerializerVisitor()
            {
            }

            public override Protobuf.Rel VisitPlanRelation(PlanRelation planRelation, SerializerVisitorState state)
            {
                return base.VisitPlanRelation(planRelation, state);
            }

            public override Protobuf.Rel VisitRootRelation(RootRelation rootRelation, SerializerVisitorState state)
            {
                return base.VisitRootRelation(rootRelation, state);
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
                    readRel.BaseSchema = new Protobuf.NamedStruct();
                    readRel.BaseSchema.Names.AddRange(readRelation.BaseSchema.Names);
                    if (readRelation.BaseSchema.Struct != null)
                    {
                        var anyTypeAnchor = GetAnyTypeId(state);
                        readRel.BaseSchema.Struct = new Protobuf.Type.Types.Struct();
                        foreach(var type in readRelation.BaseSchema.Struct.Types)
                        {
                            readRel.BaseSchema.Struct.Types_.Add(new Protobuf.Type()
                            {
                                UserDefined = new Protobuf.Type.Types.UserDefined()
                                {
                                    TypeReference = anyTypeAnchor
                                }
                            });
                        }
                    }
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
                    
                    foreach(var grouping in aggregateRelation.Groupings)
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
                            var functionAnchor = (measure.Measure.ExtensionUri, measure.Measure.ExtensionName, state);
                            m.Measure_ = new Protobuf.AggregateFunction()
                            {
                                FunctionReference = state.GetFunctionExtensionAnchor(measure.Measure.ExtensionUri, measure.Measure.ExtensionName)
                            };
                            if (measure.Measure.Arguments != null)
                            {
                                foreach(var arg in measure.Measure.Arguments)
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
                    Detail = new Google.Protobuf.WellKnownTypes.Any()
                    {
                        TypeUrl = "flowtide/IterationRelation",
                        Value = iterRel.ToByteString()
                    }
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

                var rel = new Protobuf.ExtensionLeafRel();
                rel.Detail = new Google.Protobuf.WellKnownTypes.Any()
                {
                    TypeUrl = "flowtide/IterationReferenceReadRelation",
                    Value = iterRel.ToByteString()
                };

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
                        joinRel.Type  = Protobuf.JoinRel.Types.JoinType.Anti;
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

            public override Rel VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, SerializerVisitorState state)
            {
                var rel = new MergeJoinRel();

                var exprVisitor = new SerializerExpressionVisitor();

                foreach(var leftKey in mergeJoinRelation.LeftKeys)
                {
                    if (leftKey is DirectFieldReference directFieldReference &&
                        directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                    {
                        rel.LeftKeys.Add(new Protobuf.Expression.Types.FieldReference()
                        {
                            DirectReference = new Protobuf.Expression.Types.ReferenceSegment()
                            {
                                StructField = new Protobuf.Expression.Types.ReferenceSegment.Types.StructField()
                                {
                                    Field = structReferenceSegment.Field
                                }
                            }
                        });
                    }
                    else
                    {
                        throw new NotImplementedException("Only direct field reference is implemented");
                    }
                }
                foreach (var rightKey in mergeJoinRelation.RightKeys)
                {
                    if (rightKey is DirectFieldReference directFieldReference &&
                        directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                    {
                        rel.RightKeys.Add(new Protobuf.Expression.Types.FieldReference()
                        {
                            DirectReference = new Protobuf.Expression.Types.ReferenceSegment()
                            {
                                StructField = new Protobuf.Expression.Types.ReferenceSegment.Types.StructField()
                                {
                                    Field = structReferenceSegment.Field
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
                        break;
                    case JoinType.Semi:
                        throw new NotSupportedException("Semi not supported in merge join");
                        break;
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
                        break;
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
                foreach(var k in normalizationRelation.KeyIndex)
                {
                    customRel.KeyIndex.Add(k);
                }
                rel.Detail = new Google.Protobuf.WellKnownTypes.Any()
                {
                    TypeUrl = "flowtide/NormalizationRelation",
                    Value = customRel.ToByteString()
                };

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
                var refRel = new CustomProtobuf.ReferenceRelation()
                {
                    ReferenceId = referenceRelation.RelationId
                };
                var rel = new Protobuf.ExtensionLeafRel()
                {
                    Detail = new Google.Protobuf.WellKnownTypes.Any()
                    {
                        TypeUrl = "flowtide/ReferenceRelation",
                        Value = refRel.ToByteString()
                    }
                };
                return new Rel()
                {
                    ExtensionLeaf = rel
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

                foreach(var val in virtualTableReadRelation.Values.JsonValues)
                {
                    var s = new Protobuf.Expression.Types.Literal.Types.Struct();
                    s.Fields.Add(new Protobuf.Expression.Types.Literal
                    {
                        String = val
                    });
                    rel.VirtualTable.Values.Add(s);
                }
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

            public override Rel VisitUnwrapRelation(UnwrapRelation unwrapRelation, SerializerVisitorState state)
            {
                throw new NotImplementedException("Unwrap cant be serialized yet");
            }

            private uint GetAnyTypeId(SerializerVisitorState state)
            {
                if (!state._typeExtensions.TryGetValue("any", out var id))
                {
                    var anchor = state.uriCounter++;
                    state.Root.ExtensionUris.Add(new Protobuf.SimpleExtensionURI
                    {
                        Uri = $"/any_type.yaml",
                        ExtensionUriAnchor = (uint)anchor
                    });
                    var typeAnchor = (uint)state.extensionCounter++;
                    state.Root.Extensions.Add(new Protobuf.SimpleExtensionDeclaration()
                    {
                        ExtensionType = new Protobuf.SimpleExtensionDeclaration.Types.ExtensionType()
                        {
                            ExtensionUriReference = (uint)anchor,
                            Name = "any",
                            TypeAnchor = typeAnchor
                        }
                    });
                    id = (int)typeAnchor;
                    state._typeExtensions.Add("any", id);
                }
                return (uint)id;
            }

            public override Protobuf.Rel VisitWriteRelation(WriteRelation writeRelation, SerializerVisitorState state)
            {
                var writeRel = new Protobuf.WriteRel();
                
                if (writeRelation.TableSchema != null)
                {
                    
                    writeRel.TableSchema = new Protobuf.NamedStruct();
                    writeRel.TableSchema.Names.AddRange(writeRelation.TableSchema.Names);
                    if(writeRelation.TableSchema.Struct != null)
                    {
                        var anyTypeAnchor = GetAnyTypeId(state);
                        writeRel.TableSchema.Struct = new Protobuf.Type.Types.Struct();
                        foreach (var t in writeRelation.TableSchema.Struct.Types)
                        {
                            writeRel.TableSchema.Struct.Types_.Add(new Protobuf.Type()
                            {
                                UserDefined = new Protobuf.Type.Types.UserDefined()
                                {
                                    TypeReference = anyTypeAnchor
                                }
                            });
                        }
                    }
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
        }

        public Protobuf.Plan Serialize(Plan plan)
        {
            var rootPlan = new Protobuf.Plan();

            var visitor = new SerializerVisitor();
            foreach (var relation in plan.Relations)
            {
                rootPlan.Relations.Add(new Protobuf.PlanRel()
                {
                    Rel = visitor.Visit(relation, new SerializerVisitorState(rootPlan))
                });
            }
            return rootPlan;
        }

        public string SerializeToJson(Plan plan)
        {
            var protoPlan = Serialize(plan);
            var typeRegistry = Google.Protobuf.Reflection.TypeRegistry.FromMessages(
                CustomProtobuf.IterationReferenceReadRelation.Descriptor,
                CustomProtobuf.IterationRelation.Descriptor,
                CustomProtobuf.NormalizationRelation.Descriptor,
                CustomProtobuf.ReferenceRelation.Descriptor);
            var settings = new Google.Protobuf.JsonFormatter.Settings(true, typeRegistry)
                .WithIndentation();
            var formatter = new Google.Protobuf.JsonFormatter(settings);
            return formatter.Format(protoPlan);
        }
    }
}

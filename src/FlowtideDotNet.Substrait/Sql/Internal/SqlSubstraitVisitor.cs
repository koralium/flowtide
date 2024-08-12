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
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using SqlParser;
using SqlParser.Ast;
using SqlParser.Tokens;
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal class SqlSubstraitVisitor : SqlBaseVisitor<RelationData?, object?>
    {
        private readonly TablesMetadata tablesMetadata;
        private readonly SqlPlanBuilder sqlPlanBuilder;
        private readonly SqlFunctionRegister sqlFunctionRegister;
        private readonly Dictionary<string, CTEContainer> cteContainers;
        private readonly Dictionary<string, ExchangeContainer> exchangeRelations;
        private readonly Dictionary<string, ViewContainer> viewRelations;
        private string? subStreamName;
        private int exchangeTargetIdCounter;
        private readonly List<Relation> subRelations;

        public SqlSubstraitVisitor(SqlPlanBuilder sqlPlanBuilder, SqlFunctionRegister sqlFunctionRegister)
        {
            this.sqlPlanBuilder = sqlPlanBuilder;
            this.sqlFunctionRegister = sqlFunctionRegister;
            tablesMetadata = sqlPlanBuilder._tablesMetadata;
            cteContainers = new Dictionary<string, CTEContainer>(StringComparer.OrdinalIgnoreCase);
            exchangeRelations = new Dictionary<string, ExchangeContainer>(StringComparer.OrdinalIgnoreCase);
            viewRelations = new Dictionary<string, ViewContainer>(StringComparer.OrdinalIgnoreCase);
            subRelations = new List<Relation>();
        }

        public List<Relation> GetRelations(Sequence<Statement> statements)
        {
            subRelations.Clear();
            foreach(var statement in statements)
            {
                var relData = Visit(statement, default);
                if (relData == null)
                {
                    continue;
                }
                subRelations.Add(relData.Relation);
            }
            
            return subRelations;
        }

        protected override RelationData? VisitInsertStatement(Statement.Insert insert, object? state)
        {
            var source = Visit(insert.Source, state);

            Debug.Assert(source != null);

            NamedStruct? tableSchema = null;

            if (insert.Columns != null && insert.Columns.Count > 0)
            {
                tableSchema = new NamedStruct()
                {
                    Names = insert.Columns.Select(x => x.Value).ToList(),
                    Struct = new FlowtideDotNet.Substrait.Type.Struct()
                    {
                        Types = insert.Columns.Select(x => new AnyType() { Nullable = true } as SubstraitBaseType).ToList()
                    }
                };
            }
            else
            {
                var names = source.EmitData.GetNames();
                tableSchema = new NamedStruct()
                {
                    Names = names.ToList(),
                    Struct = new FlowtideDotNet.Substrait.Type.Struct()
                    {
                        Types = names.Select(x => new AnyType() { Nullable = true } as SubstraitBaseType).ToList()
                    }
                };
            }

            var writeRelation = new WriteRelation()
            {
                Input = source.Relation,
                NamedObject = new FlowtideDotNet.Substrait.Type.NamedTable() { Names = insert.Name.Values.Select(x => x.Value).ToList() },
                TableSchema = tableSchema
            };

            Relation relation = writeRelation;
            if (subStreamName != null)
            {
                relation = new SubStreamRootRelation()
                {
                    Input = writeRelation,
                    Name = subStreamName
                };
            }

            return new RelationData(relation, source.EmitData);
        }

        protected override RelationData? VisitCreateView(Statement.CreateView createView, object? state)
        {
            var relationData = Visit(createView.Query, state);
            Debug.Assert(relationData != null);

            bool isBuffered = false;
            bool isDistributed = false;
            Expressions.FieldReference? scatterField = default;
            int? partitionCount = default;
            if (createView.WithOptions != null)
            {
                foreach(var opt in createView.WithOptions)
                {
                    var upperName = opt.Name.ToString().ToUpper();
                    if (upperName == SqlTextResources.Buffered)
                    {
                        var val = opt.Value.ToSql();
                        if (string.Equals(val, bool.TrueString, StringComparison.OrdinalIgnoreCase))
                        {
                            isBuffered = true;
                        }
                    }
                    else if (upperName == SqlTextResources.Distributed)
                    {
                        var val = opt.Value.ToSql();
                        if (string.Equals(val, bool.TrueString, StringComparison.OrdinalIgnoreCase))
                        {
                            isDistributed = true;
                        }
                    }
                    else if (upperName == SqlTextResources.ScatterBy)
                    {
                        if (opt.Value is Value.StringBasedValue stringBasedVal)
                        {
                            var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);
                            // Do a lookup on the partition by field
                            var exprData = exprVisitor.Visit(new Expression.Identifier(new Ident(stringBasedVal.Value)), relationData.EmitData);
                            if (exprData.Expr is Expressions.FieldReference fieldReference)
                            {
                                scatterField = fieldReference;
                            }
                            else
                            {
                                throw new SubstraitParseException("SCATTER_BY expects a field reference.");
                            }
                        }
                        else
                        {
                            throw new SubstraitParseException("SCATTER_BY expects a string based value with qoutes.");
                        }
                    }
                    else if (upperName == SqlTextResources.PartitionCount)
                    {
                        if (int.TryParse(opt.Value.ToSql(), out var partitionCountValue))
                        {
                            partitionCount = partitionCountValue;
                        }
                        else
                        {
                            throw new SubstraitParseException($"Invalid partition count expected a number got '{opt.Value.ToSql()}'");
                        }
                    }
                    else
                    {
                        throw new SubstraitParseException($"Unknown option '{opt.Name}' in create view statement");
                    }
                }
            }

            var relation = relationData.Relation;

            var viewName = createView.Name.ToSql();

            if (isBuffered)
            {
                relation = new BufferRelation()
                {
                    Input = relation
                };
            }
            if (isDistributed)
            {
                ExchangeKind? exchangeKind;
                if (scatterField != null)
                {
                    exchangeKind = new ScatterExchangeKind()
                    {
                        Fields = [scatterField]
                    };
                }
                else
                {
                    exchangeKind = new BroadcastExchangeKind();
                }
                var exchangeRelation = new ExchangeRelation()
                {
                    Input = relation,
                    ExchangeKind = exchangeKind,
                    PartitionCount = partitionCount,
                    Targets = new List<ExchangeTarget>()
                };

                // Add the exchange relation to a lookup table so usage of the view can add to the targets.
                exchangeRelations.Add(createView.Name.ToSql(),new ExchangeContainer(relationData.EmitData, subRelations.Count, relation.OutputLength, exchangeRelation, subStreamName));

                if (subStreamName == null)
                {
                    relation = exchangeRelation;
                }
                else
                {
                    // Add a sub stream root relation to mark in the plan that this is a sub stream.
                    relation = new SubStreamRootRelation()
                    {
                        Input = exchangeRelation,
                        Name = subStreamName
                    };
                }
                
                subRelations.Add(relation);
            }
            else
            {
                if (scatterField != null)
                {
                    throw new SubstraitParseException("SCATTER_BY can only be used on a distributed view");
                }
                if (partitionCount != null)
                {
                    throw new SubstraitParseException("PARTITION_COUNT can only be used on a distributed view");
                }
                viewRelations.Add(viewName, new ViewContainer(relationData.EmitData, subRelations.Count, relation.OutputLength));
                subRelations.Add(relation);
            }
            
            return default;
        }

        protected override RelationData? VisitCreateTable(Statement.CreateTable createTable, object? state)
        {
            var tableName = string.Join(".", createTable.Name.Values.Select(x=> x.Value));
            var columnNames = createTable.Columns.Select(x => x.Name.Value).ToList();

            NamedStruct schema = new NamedStruct()
            {
                Names = columnNames,
                Struct = new Struct()
                {
                    Types = createTable.Columns.Select(x => SqlToSubstraitType.GetType(x.DataType)).ToList()
                }
            };

            tablesMetadata.AddTable(tableName, schema);
            return null;
        }

        protected override RelationData? VisitQuery(Query query, object? state)
        {
            if (query.With != null)
            {
                
                
                foreach (var with in query.With.CteTables)
                {
                    var emitDataExtractor = new EmitDataExtractorVisitor(tablesMetadata, sqlFunctionRegister);
                    var cteEmitData = emitDataExtractor.Visit(with.Query, state);
                    if (cteEmitData == null)
                    {
                        throw new InvalidOperationException("Could not extract emit information from with query");
                    }
                    var alias = with.Alias.Name.ToSql();
                    var container = new CTEContainer(alias, cteEmitData, cteEmitData.GetNames().Count);
                    
                    cteContainers.Add(alias, container);
                    var p = Visit(with.Query, state)!.Relation;
                    
                    // Check if this is recursive CTE
                    if (container.UsageCounter > 0)
                    {
                        p = new IterationRelation()
                        {
                            LoopPlan = p,
                            IterationName = alias
                        };
                    }
                    var plan = new Plan()
                    {
                        Relations = new List<Relation>()
                        {
                            p
                        }
                    };
                    // Remove from containers since it will be added as a view now.
                    cteContainers.Remove(alias);
                    // With queries should be registered as views in the plan
                    // So they can be reused multiple times in the query
                    sqlPlanBuilder._planModifier.AddPlanAsView(alias, plan);
                    tablesMetadata.AddTable(alias, cteEmitData.GetNamedStruct());
                }
            }
            var node = Visit(query.Body, state);

            if (node == null)
            {
                throw new InvalidOperationException("Could not create a plan from the query");
            }
            if (query.OrderBy != null)
            {
                var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);
                List<Expressions.SortField> sortFields = new List<Expressions.SortField>();
                foreach (var o in query.OrderBy)
                {
                    var expr = exprVisitor.Visit(o.Expression, node.EmitData);
                    var sortDirection = GetSortDirection(o);

                    sortFields.Add(new Expressions.SortField()
                    {
                        Expression = expr.Expr,
                        SortDirection = sortDirection
                    });
                }

                if (node.Relation is FetchRelation fetch)
                {
                    var rel = new TopNRelation()
                    {
                        Input = fetch.Input,
                        Sorts = sortFields,
                        Count = fetch.Count,
                        Offset = fetch.Offset
                    };
                    // Add the order by before the fetch, since the fetch can come from the TOP N in the select.
                    node = new RelationData(rel, node.EmitData);
                }
            }
            return node;
        }

        private static Expressions.SortDirection GetSortDirection(OrderByExpression o)
        {
            Expressions.SortDirection sortDirection;

            // Find the sort direction of this field
            if (o.Asc != null)
            {
                if (o.Asc.Value)
                {
                    if (o.NullsFirst != null)
                    {
                        if (o.NullsFirst.Value)
                        {
                            sortDirection = Expressions.SortDirection.SortDirectionAscNullsFirst;
                        }
                        else
                        {
                            sortDirection = Expressions.SortDirection.SortDirectionAscNullsLast;
                        }
                    }
                    else
                    {
                        sortDirection = Expressions.SortDirection.SortDirectionAscNullsFirst;
                    }
                }
                else
                {
                    if (o.NullsFirst != null)
                    {
                        if (o.NullsFirst.Value)
                        {
                            sortDirection = Expressions.SortDirection.SortDirectionDescNullsFirst;
                        }
                        else
                        {
                            sortDirection = Expressions.SortDirection.SortDirectionDescNullsLast;
                        }
                    }
                    else
                    {
                        sortDirection = Expressions.SortDirection.SortDirectionDescNullsLast;
                    }
                }
            }
            else
            {
                if (o.NullsFirst != null)
                {
                    if (o.NullsFirst.Value)
                    {
                        sortDirection = Expressions.SortDirection.SortDirectionAscNullsFirst;
                    }
                    else
                    {
                        sortDirection = Expressions.SortDirection.SortDirectionAscNullsLast;
                    }
                }
                else
                {
                    sortDirection = Expressions.SortDirection.SortDirectionAscNullsFirst;
                }
            }   
            return sortDirection;
        }

        protected override RelationData? VisitSelect(Select select, object? state)
        {
            RelationData? outNode = default;
            if (select.From != null)
            {
                if (select.From.Count != 1)
                {
                    throw new InvalidOperationException("Only a single table in the FROM statement is supported");
                }
                var fromTable = select.From.First();

                outNode = Visit(fromTable, state);
            }

            if (outNode == null)
            {
                throw new NotImplementedException("Only queries that does 'FROM' with potential joins is supported at this time");
            }
            
            if (select.Selection != null)
            {
                var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);
                var expr = exprVisitor.Visit(select.Selection, outNode.EmitData);
                outNode = new RelationData(new FilterRelation()
                {
                    Input = outNode.Relation,
                    Condition = expr.Expr
                }, outNode.EmitData);
            }


            ContainsAggregateVisitor containsAggregateVisitor = new ContainsAggregateVisitor(sqlFunctionRegister);
            bool containsAggregate = select.GroupBy != null;
            if (select.Having != null)
            {
                containsAggregate |= containsAggregateVisitor.Visit(select.Having, default);
            }

            if (select.Projection != null)
            {
                foreach (var item in select.Projection)
                {
                    containsAggregate |= containsAggregateVisitor.VisitSelectItem(item);
                }
            }

            if (containsAggregate)
            {
                outNode = VisitSelectAggregate(select, containsAggregateVisitor, outNode);
            }

            if (select.Projection != null)
            {
                outNode = VisitProjection(select.Projection, outNode);
            }

            if (select.Distinct)
            {
                if (outNode == null)
                {
                    throw new InvalidOperationException("DISTINCT statement is not supported without a FROM statement");
                }
                var setRelation = new SetRelation()
                {
                    Operation = SetOperation.UnionDistinct,
                    Inputs = new List<Relation>()
                    {
                        outNode.Relation
                    }
                };
                outNode = new RelationData(setRelation, outNode.EmitData);
            }

            if (select.Top != null)
            {
                if (outNode == null)
                {
                    throw new InvalidOperationException("TOP statement is not supported without a FROM statement");
                }
                var literal = select.Top.Quantity?.AsLiteral()?.Value?.AsNumber();
                if (literal == null)
                {
                    throw new NotSupportedException("Only numeric literal values are supported in the TOP statement");
                }
                outNode = new RelationData(new FetchRelation()
                {
                    Input = outNode.Relation,
                    Count = int.Parse(literal.Value)
                }, outNode.EmitData);
            }

            return outNode;
        }

        private RelationData VisitSelectAggregate(Select select, ContainsAggregateVisitor containsAggregateVisitor, RelationData parent)
        {
            var aggRel = new AggregateRelation()
            {
                Input = parent.Relation
            };
            aggRel.Measures = new List<AggregateMeasure>();
            Relation outputRelation = aggRel;

            EmitData aggEmitData = new EmitData();

            int emitcount = 0;

            if (select.GroupBy != null)
            {
                aggRel.Groupings = new List<AggregateGrouping>();
                var grouping = new AggregateGrouping()
                {
                    GroupingExpressions = new List<Expressions.Expression>()
                };
                aggRel.Groupings.Add(grouping);
                foreach (var group in select.GroupBy)
                {
                    var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);
                    var result = exprVisitor.Visit(group, parent.EmitData);
                    grouping.GroupingExpressions.Add(result.Expr);
                    aggEmitData.Add(group, emitcount, result.Name, result.Type);
                    emitcount++;
                }
            }

            foreach (var foundMeasure in containsAggregateVisitor.AggregateFunctions)
            {
                var mapper = sqlFunctionRegister.GetAggregateMapper(foundMeasure.Name);
                var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);

                var aggregateResponse = mapper(foundMeasure, exprVisitor, parent.EmitData);
                aggRel.Measures.Add(new AggregateMeasure()
                {
                    Measure = aggregateResponse.AggregateFunction
                });
                aggEmitData.Add(foundMeasure, emitcount, $"$expr{emitcount}", aggregateResponse.Type);
                emitcount++;
            }

            if (select.Having != null)
            {
                var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);
                outputRelation = new FilterRelation()
                {
                    Condition = exprVisitor.Visit(select.Having, aggEmitData).Expr,
                    Input = aggRel
                };
            }

            return new RelationData(outputRelation, aggEmitData);
        }

        private RelationData? VisitProjection(SqlParser.Sequence<SelectItem> selects, RelationData parent)
        {
            EmitData projectEmitData = new EmitData();
            List<FlowtideDotNet.Substrait.Expressions.Expression> expressions = new List<FlowtideDotNet.Substrait.Expressions.Expression>();
            List<int> emitList = new List<int>();
            int emitCounter = parent.Relation.OutputLength;
            int outputCounter = 0;
            foreach (var s in selects)
            {
                var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);
                if (s is SelectItem.ExpressionWithAlias exprAlias)
                {
                    var condition = exprVisitor.Visit(exprAlias.Expression, parent.EmitData);
                    expressions.Add(condition.Expr);
                    projectEmitData.Add(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(exprAlias.Alias) })), outputCounter, exprAlias.Alias, condition.Type);
                    outputCounter++;
                    emitList.Add(emitCounter);
                    emitCounter++;
                }
                else if (s is SelectItem.UnnamedExpression unnamedExpr)
                {
                    var condition = exprVisitor.Visit(unnamedExpr.Expression, parent.EmitData);
                    expressions.Add(condition.Expr);
                    projectEmitData.Add(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(condition.Name) })), outputCounter, condition.Name, condition.Type);
                    outputCounter++;
                    emitList.Add(emitCounter);
                    emitCounter++;
                }
                else if (s is SelectItem.Wildcard)
                {
                    var parentExpressions = parent.EmitData.GetExpressions();
                    AddExpressionsFromWildcard(parentExpressions, expressions, projectEmitData, emitList, ref outputCounter, ref emitCounter);
                }
                else if (s is SelectItem.QualifiedWildcard qualifiedWildcard)
                {
                    var parentExpressions = parent.EmitData.GetExpressions(qualifiedWildcard.Name);
                    AddExpressionsFromWildcard(parentExpressions, expressions, projectEmitData, emitList, ref outputCounter, ref emitCounter);
                }
                else
                {
                    throw new InvalidOperationException("Unsupported select item");
                }
            }
            var projectRel = new ProjectRelation()
            {
                Input = parent.Relation,
                Expressions = expressions,
                Emit = emitList
            };
            return new RelationData(projectRel, projectEmitData);
        }

        private static void AddExpressionsFromWildcard(
            IReadOnlyList<EmitData.ExpressionInformation> parentExpressions,
            List<Expressions.Expression> expressions,
            EmitData projectEmitData,
            List<int> emitList,
            ref int outputCounter,
            ref int emitCounter)
        {
            for (int i = 0; i < parentExpressions.Count; i++)
            {
                var expr = new Expressions.DirectFieldReference()
                {
                    ReferenceSegment = new Expressions.StructReferenceSegment()
                    {
                        Field = parentExpressions[i].Index
                    }
                };
                expressions.Add(expr);
                projectEmitData.Add(parentExpressions[i].Expression[0], outputCounter, parentExpressions[i].Name, parentExpressions[i].Type);
                outputCounter++;
                emitList.Add(emitCounter);
                emitCounter++;
            }
        }

        protected override RelationData? VisitTableWithJoins(TableWithJoins tableWithJoins, object? state)
        {
            ArgumentNullException.ThrowIfNull(tableWithJoins.Relation);

            RelationData? parent = null;
            if (IsTableFunction(tableWithJoins.Relation))
            {
                parent = VisitTableFunctionRoot(tableWithJoins.Relation);
            }
            else
            {
                parent = Visit(tableWithJoins.Relation, state);
            }
            
            Debug.Assert(parent != null);
            if (tableWithJoins.Joins != null)
            {
                foreach (var join in tableWithJoins.Joins)
                {
                    if (IsTableFunction(join.Relation))
                    {
                        return VisitTableFunctionJoin(join, parent);
                    }
                    parent = VisitJoin(join, parent, state);
                    Debug.Assert(parent != null);
                }
            }
            return parent;
        }

        private static bool IsTableFunction(TableFactor? tableFactor)
            => tableFactor is TableFactor.Table table && table.Args != null;

        private static void GetTableFunctionNameAndArgs(TableFactor tableFactor, out string name, out Sequence<FunctionArg> args)
        {
            if (tableFactor is TableFactor.Table table &&
                table.Args != null)
            {
                name = string.Join('.', table.Name.Values.Select(x => x.Value));
                args = table.Args;
                return;
            }
            throw new InvalidOperationException("Table factor is not a table function");
        }

        private RelationData VisitTableFunctionRoot(TableFactor tableFactor)
        {
            GetTableFunctionNameAndArgs(tableFactor, out var name, out var args);
            var tableFunctionMapper = sqlFunctionRegister.GetTableMapper(name);
            var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);

            var tableFunction = tableFunctionMapper(
                new SqlTableFunctionArgument(args, tableFactor.Alias?.Name.Value, exprVisitor, new EmitData())
                );
            var rel = new TableFunctionRelation()
            {
                TableFunction = tableFunction
            };

            EmitData emitData = new EmitData();
            for (int i = 0; i < tableFunction.TableSchema.Names.Count; i++)
            {
                SubstraitBaseType type = new AnyType();
                if (tableFunction.TableSchema.Struct != null)
                {
                    type = tableFunction.TableSchema.Struct.Types[i];
                }
                emitData.Add(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(tableFunction.TableSchema.Names[i]) })), i, tableFunction.TableSchema.Names[i], type);
            }

            return new RelationData(rel, emitData);
        }

        private RelationData VisitTableFunctionJoin(Join join, RelationData parent)
        {
            ArgumentNullException.ThrowIfNull(join.Relation);

            GetTableFunctionNameAndArgs(join.Relation, out var name, out var args);

            var tableFunctionMapper = sqlFunctionRegister.GetTableMapper(name);
            var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);

            var tableFunction = tableFunctionMapper(
                new SqlTableFunctionArgument(args, join.Relation?.Alias?.Name.Value, exprVisitor, parent.EmitData)
                );

            EmitData tableFuncEmitData = new EmitData();
            for (int i = 0; i < tableFunction.TableSchema.Names.Count; i++)
            {
                SubstraitBaseType type = new AnyType();
                if (tableFunction.TableSchema.Struct != null)
                {
                    type = tableFunction.TableSchema.Struct.Types[i];
                }
                tableFuncEmitData.Add(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(tableFunction.TableSchema.Names[i]) })), tableFuncEmitData.Count, tableFunction.TableSchema.Names[i], type);
            }

            // Create the emit data for the table function with the parent data
            EmitData joinEmitData = new EmitData();
            joinEmitData.Add(parent.EmitData, 0);
            joinEmitData.Add(tableFuncEmitData, parent.Relation.OutputLength);
            var rel = new TableFunctionRelation()
            {
                TableFunction = tableFunction,
                Input = parent.Relation
            };

            if (join.JoinOperator is JoinOperator.LeftOuter leftOuter)
            {
                rel.Type = JoinType.Left;

                if (leftOuter.JoinConstraint is JoinConstraint.On on)
                {
                    var condition = exprVisitor.Visit(on.Expression, joinEmitData);
                    rel.JoinCondition = condition.Expr;
                }
            }
            else if (join.JoinOperator is JoinOperator.Inner inner)
            {
                rel.Type = JoinType.Inner;

                if (inner.JoinConstraint is JoinConstraint.On on)
                {
                    var condition = exprVisitor.Visit(on.Expression, joinEmitData);
                    rel.JoinCondition = condition.Expr;
                }
            }
            else
            {
                throw new NotImplementedException($"Join type '{join.JoinOperator!.GetType().Name}' is not yet supported for table function with joins in SQL mode.");
            }
            return new RelationData(rel, joinEmitData);
        }

        private bool TryVisitExchangeRelationAsTable(TableFactor.Table table, [NotNullWhen(true)] out RelationData? relationData)
        {
            var tableName = string.Join('.', table.Name.Values.Select(x => x.Value));

            if (exchangeRelations.TryGetValue(tableName, out var exchangeRelationsContainer))
            {
                EmitData emitData = exchangeRelationsContainer.EmitData;
                if (table.Alias != null)
                {
                    emitData = exchangeRelationsContainer.EmitData.CloneWithAlias(table.Alias.Name.Value);
                }
                // Try and find a partition_id hint
                int? partitionId = default;
                if (table.WithHints != null)
                {
                    foreach (var hint in table.WithHints)
                    {
                        if (hint is Expression.BinaryOp binaryOp &&
                            binaryOp.Left is Expression.Identifier hintIdentifier &&
                            hintIdentifier.Ident.Value.Equals("PARTITION_ID", StringComparison.OrdinalIgnoreCase) &&
                            binaryOp.Right is Expression.LiteralValue literalValue &&
                            literalValue.Value is Value.Number number &&
                            int.TryParse(number.Value, out var parsedPartitionId)
                            )
                        {
                            partitionId = parsedPartitionId;
                        }
                        else
                        {
                            throw new InvalidOperationException($"Unknown distributed view select hint: '{hint.ToSql()}'");
                        }
                    }
                }

                var partitionIds = new List<int>();
                if (partitionId != null)
                {
                    if (exchangeRelationsContainer.ExchangeRelation.PartitionCount == null)
                    {
                        throw new InvalidOperationException("Cannot use PARTITION_ID on a distributed view without PARTITION_COUNT hint.");
                    }
                    partitionIds.Add(partitionId.Value);
                }

                if (subStreamName != exchangeRelationsContainer.SubStreamName)
                {
                    if (exchangeRelationsContainer.SubStreamName == null)
                    {
                        throw new InvalidOperationException("Trying to access an exchange relation that is not in a substream from a different stream.");
                    }
                    var exchangeTargetId = exchangeTargetIdCounter++;
                    var exchangeRelReference = new PullExchangeReferenceRelation()
                    {
                        SubStreamName = exchangeRelationsContainer.SubStreamName,
                        ExchangeTargetId = exchangeTargetId,
                        ReferenceOutputLength = exchangeRelationsContainer.OutputLength,
                    };

                    exchangeRelationsContainer.ExchangeRelation.Targets.Add(new PullBucketExchangeTarget()
                    {
                        ExchangeTargetId = exchangeTargetId,
                        PartitionIds = partitionIds
                    });
                    relationData = new RelationData(exchangeRelReference, emitData);
                }
                else // In the same substream
                {
                    var targetId = exchangeRelationsContainer.ExchangeRelation.Targets.Count;
                    exchangeRelationsContainer.ExchangeRelation.Targets.Add(new StandardOutputExchangeTarget()
                    {
                        PartitionIds = partitionIds
                    });
                    relationData = new RelationData(new StandardOutputExchangeReferenceRelation()
                    {
                        RelationId = exchangeRelationsContainer.RelationId,
                        TargetId = targetId,
                        ReferenceOutputLength = exchangeRelationsContainer.OutputLength
                    }, emitData);
                }
                return true;
            }
            relationData = default;
            return false;
        }

        protected override RelationData? VisitTable(TableFactor.Table table, object? state)
        {
            if (TryVisitExchangeRelationAsTable(table, out var exchangeRelationData))
            {
                return exchangeRelationData;
            }

            if (table.WithHints != null)
            {
                throw new InvalidOperationException("Hints are not supported on tables at this point.");
            }
            
            var tableName = string.Join('.', table.Name.Values.Select(x => x.Value));

            if (viewRelations.TryGetValue(tableName, out var viewContainer))
            {
                var emitData = viewContainer.EmitData;
                if (table.Alias != null)
                {
                    emitData = emitData.CloneWithAlias(table.Alias.Name.Value);
                }
                return new RelationData(new ReferenceRelation()
                {
                    ReferenceOutputLength = viewContainer.OutputLength,
                    RelationId = viewContainer.RelationId
                }, emitData);
            }

            if (cteContainers.TryGetValue(tableName, out var cteContainer))
            {
                var emitData = cteContainer.EmitData;
                cteContainer.UsageCounter++;
                if (table.Alias != null)
                {
                    emitData = emitData.CloneWithAlias(table.Alias.Name.Value);
                }
                return new RelationData(new IterationReferenceReadRelation()
                {
                    IterationName = cteContainer.Alias,
                    ReferenceOutputLength = cteContainer.OutputLength
                }, emitData);
            }

            if (tablesMetadata.TryGetTable(tableName, out var t))
            {
                var emitData = new EmitData();

                for (int i = 0; i < t.Schema.Names.Count; i++)
                {
                    SubstraitBaseType type = new AnyType();
                    if (t.Schema.Struct != null)
                    {
                        type = t.Schema.Struct.Types[i];
                    }
                    emitData.Add(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(t.Schema.Names[i]) })), i, t.Schema.Names[i], type);
                }

                if (table.Alias != null)
                {
                    for (int i = 0; i < t.Schema.Names.Count; i++)
                    {
                        emitData.AddWithAlias(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(table.Alias.Name), new Ident(t.Schema.Names[i]) })), i);
                    }
                }

                if (t.Schema.Struct == null)
                {
                    t.Schema.Struct = new Struct()
                    {
                        Types = t.Schema.Names.Select(x => new AnyType() as SubstraitBaseType).ToList()
                    };
                }
                var readRelation = new ReadRelation()
                {
                    NamedTable = new FlowtideDotNet.Substrait.Type.NamedTable()
                    {
                        Names = table.Name.Values.Select(x => x.Value).ToList()
                    },
                    BaseSchema = t.Schema
                };
                return new RelationData(readRelation, emitData);
            }
            else
            {
                throw new InvalidOperationException($"Table '{tableName}' does not exist");
            }

        }

        private RelationData? VisitJoin(Join join, RelationData left, object? state)
        {
            Debug.Assert(join.Relation != null);
            var right = Visit(join.Relation, state);
            Debug.Assert(right != null);

            var joinRelation = new JoinRelation()
            {
                Left = left.Relation,
                Right = right.Relation
            };
            EmitData joinEmitData = new EmitData();
            joinEmitData.Add(left.EmitData, 0);
            joinEmitData.Add(right.EmitData, left.Relation.OutputLength);


            if (join.JoinOperator is JoinOperator.LeftOuter leftOuter)
            {
                joinRelation.Type = JoinType.Left;

                if (leftOuter.JoinConstraint is JoinConstraint.On on)
                {
                    var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);
                    var condition = exprVisitor.Visit(on.Expression, joinEmitData);
                    joinRelation.Expression = condition.Expr;
                }
                else
                {
                    throw new InvalidOperationException("Left joins must have an 'ON' expression.");
                }
            }
            else if (join.JoinOperator is JoinOperator.Inner inner)
            {
                joinRelation.Type = JoinType.Inner;
                if (inner.JoinConstraint is JoinConstraint.On on)
                {
                    var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);
                    var condition = exprVisitor.Visit(on.Expression, joinEmitData);
                    joinRelation.Expression = condition.Expr;
                }
                else
                {
                    throw new InvalidOperationException("Left joins must have an 'ON' expression.");
                }
            }
            else
            {
                throw new NotImplementedException($"Join type '{join.JoinOperator!.GetType().Name}' is not yet supported in SQL mode.");
            }

            return new RelationData(joinRelation, joinEmitData);
        }

        protected override RelationData? VisitDerivedTable(TableFactor.Derived derived, object? state)
        {
            var relationData = VisitQuery(derived.SubQuery, state);
            Debug.Assert(relationData != null);
            if (derived.Alias != null)
            {
                // Append the alias to the emit data so its possible to find columns from the emit data
                var newEmitData = relationData.EmitData.CloneWithAlias(derived.Alias.Name.Value);
                return new RelationData(relationData.Relation, newEmitData);
            }

            return relationData;
        }

        protected override RelationData? VisitSetOperation(SetExpression.SetOperation setOperation, object? state)
        {
            if (setOperation.Op != SetOperator.Union)
            {
                throw new NotImplementedException($"The set operation {setOperation.Op.ToString()} is not yet supported in SQL.");
            }
            if (setOperation.SetQuantifier != SetQuantifier.All && setOperation.SetQuantifier != SetQuantifier.None)
            {
                throw new NotImplementedException("Only union all is supported in SQL at this time.");
            }

            var left = Visit(setOperation.Left, state);
            var right = Visit(setOperation.Right, state);

            Debug.Assert(left != null);
            Debug.Assert(right != null);

            var setRelation = new SetRelation()
            {
                Inputs = new List<Relation>()
                {
                    left.Relation,
                    right.Relation
                },
                Operation = SetOperation.UnionAll
            };

            return new RelationData(setRelation, left.EmitData);
        }

        protected override RelationData? VisitBeginSubStream(BeginSubStream beginSubStream)
        {
            subStreamName = string.Join(".", beginSubStream.Name.Values.Select(x => x.Value));
            return null;
        }
    }
}

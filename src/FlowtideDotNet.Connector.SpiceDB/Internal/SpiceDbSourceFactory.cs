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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Substrait.Exceptions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using Grpc.Core;
using SqlParser.Ast;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SpiceDB.Internal
{
    internal class SpiceDbSourceFactory : AbstractConnectorSourceFactory, IConnectorTableProviderFactory, ITableProvider
    {
        private readonly string sourceTableName;
        private readonly SpiceDbSourceOptions options;

        public SpiceDbSourceFactory(string sourceTableName, SpiceDbSourceOptions options)
        {
            this.sourceTableName = sourceTableName;
            this.options = options;
        }

        public override bool CanHandle(ReadRelation readRelation)
        {
            if (readRelation.NamedTable.DotSeperated.Equals(sourceTableName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
            return false;
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            // Make sure that all primary key columns are in the query, if not add them.
            List<int> indices = new List<int>();
            var userTypeIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("subject_type", StringComparison.OrdinalIgnoreCase));
            if (userTypeIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("subject_type");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                userTypeIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(userTypeIndex);
            var userIdIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("subject_id", StringComparison.OrdinalIgnoreCase));
            if (userIdIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("subject_id");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                userIdIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(userIdIndex);

            var userRelationIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("subject_relation", StringComparison.OrdinalIgnoreCase));
            if (userRelationIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("subject_relation");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                userRelationIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(userRelationIndex);

            var relationIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("relation", StringComparison.OrdinalIgnoreCase));
            if (relationIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("relation");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                relationIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(relationIndex);

            var objectTypeIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("resource_type", StringComparison.OrdinalIgnoreCase));
            if (objectTypeIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("resource_type");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                objectTypeIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(objectTypeIndex);
            var objectIdIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("resource_id", StringComparison.OrdinalIgnoreCase));
            if (objectIdIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("resource_id");
                readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                objectIdIndex = readRelation.BaseSchema.Names.Count - 1;
            }
            indices.Add(objectIdIndex);

            return new NormalizationRelation()
            {
                Input = readRelation,
                Filter = readRelation.Filter,
                KeyIndex = indices,
                Emit = readRelation.Emit
            };
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new ColumnSpiceDbSource(options, readRelation, dataflowBlockOptions);
        }

        public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            var fullName = string.Join(".", tableName);

            if (fullName.Equals(sourceTableName, StringComparison.OrdinalIgnoreCase))
            {
                tableMetadata = new TableMetadata(string.Join(".", tableName), new NamedStruct()
                {
                    Names = ["subject_type", "subject_id", "subject_relation", "relation", "resource_type", "resource_id"],
                    Struct = new Struct()
                    {
                        Types = new List<Substrait.Type.SubstraitBaseType>()
                        {
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType(),
                            new StringType()
                        }
                    }
                });
                return true;
            }
            tableMetadata = default;
            return false;
        }

        public bool TryHandleTableFunction(IReadOnlyList<string> functionName, TableProviderTableFunctionArguments sqlTableFunction, [NotNullWhen(true)] out TableProviderTableFunctionResult? relation)
        {
            var fullName = string.Join(".", functionName);
            if (fullName.Equals($"{sourceTableName}.materialize_permission", StringComparison.OrdinalIgnoreCase))
            {
                Metadata? metadata = default;
                if (options.GetMetadata != null)
                {
                    metadata = options.GetMetadata();
                }
                else
                {
                    metadata = new Metadata();
                }

                if (sqlTableFunction.JoinType != null)
                {
                    throw new SubstraitParseException("The table function cannot be used in a join.");
                }

                if (sqlTableFunction.Arguments.Count < 2)
                {
                    throw new SubstraitParseException("The table function must have at least two arguments: the resource type and the relation to resolve.");
                }

                string? type = null;
                {
                    if (sqlTableFunction.Arguments[0] is FunctionArg.Unnamed unnamed &&
                        unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                    {
                        var exprResult = sqlTableFunction.ExpressionVisitor.Visit(funcExpr.Expression, sqlTableFunction.EmitData);
                        if (exprResult.Expr is StringLiteral stringLiteral)
                        {
                            type = stringLiteral.Value;
                        }
                        else
                        {
                            throw new SubstraitParseException("The first argument of the table function must be a string literal representing the resource type to resolve.");
                        }
                    }
                    else
                    {
                        throw new SubstraitParseException("The first argument of the table function must be a string literal representing the resource type to resolve.");
                    }
                }
                string? relationInput = null;
                {
                    if (sqlTableFunction.Arguments[1] is FunctionArg.Unnamed unnamed &&
                        unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                    {
                        var exprResult = sqlTableFunction.ExpressionVisitor.Visit(funcExpr.Expression, sqlTableFunction.EmitData);
                        if (exprResult.Expr is StringLiteral stringLiteral)
                        {
                            relationInput = stringLiteral.Value;
                        }
                        else
                        {
                            throw new SubstraitParseException("The second argument of the table function must be a string literal representing the permission to resolve.");
                        }
                    }
                    else
                    {
                        throw new SubstraitParseException("The second argument of the table function must be a string literal representing the permission to resolve.");
                    }
                }

                bool recurseAtStopType = false;
                if (sqlTableFunction.Arguments.Count >= 3)
                {
                    if (sqlTableFunction.Arguments[2] is FunctionArg.Unnamed unnamed &&
                        unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                    {
                        var exprResult = sqlTableFunction.ExpressionVisitor.Visit(funcExpr.Expression, sqlTableFunction.EmitData);
                        if (exprResult.Expr is BoolLiteral boolLiteral)
                        {
                            recurseAtStopType = boolLiteral.Value;
                        }
                        else
                        {
                            throw new SubstraitParseException("The third argument of the table function must be a boolean literal representing whether to recurse at stop types.");
                        }
                    }
                    else
                    {
                        throw new SubstraitParseException("The third argument of the table function must be a boolean literal representing whether to recurse at stop types.");
                    }
                }

                List<string> stopTypes = new List<string>();
                if (sqlTableFunction.Arguments.Count >= 4)
                {
                    for (int i = 3; i < sqlTableFunction.Arguments.Count; i++)
                    {
                        if (sqlTableFunction.Arguments[i] is FunctionArg.Unnamed unnamed &&
                            unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcExpr)
                        {
                            var exprResult = sqlTableFunction.ExpressionVisitor.Visit(funcExpr.Expression, sqlTableFunction.EmitData);
                            if (exprResult.Expr is StringLiteral stringLiteral)
                            {
                                stopTypes.Add(stringLiteral.Value);
                            }
                            else
                            {
                                throw new SubstraitParseException("Each stop type argument (from the fourth argument onward) must be a string literal representing a stop type.");
                            }
                        }
                        else
                        {
                            throw new SubstraitParseException("Each stop type argument (from the fourth argument onward) must be provided as an unnamed string literal representing a stop type.");
                        }
                    }
                }

                // We can do this synchronously right now, it is only run during query plan creation which is done only at start up.
                var plan = SpiceDbToFlowtide.ConvertAsync(options.Channel, metadata, type, relationInput, sourceTableName, recurseAtStopType, stopTypes.ToArray()).GetAwaiter().GetResult();

                Relation? mainRelation = default;
                List<Relation> subRelations = new List<Relation>();

                for (int i = 0; i < plan.Relations.Count; i++)
                {
                    if (plan.Relations[i] is RootRelation rootRel)
                    {
                        mainRelation = rootRel.Input;
                    }
                    else
                    {
                        subRelations.Add(plan.Relations[i]);
                    }
                }
                EmitData emitData = new EmitData();
                if (mainRelation == null)
                {
                    throw new InvalidOperationException("Could not create a query plan for spicedb permission materialization.");
                }

                List<string> names = ["subject_type", "subject_id", "subject_relation", "relation", "resource_type", "resource_id"];
                for (int i = 0; i < names.Count; i++)
                {
                    emitData.AddSourceColumn(names[i], new StringType());
                }

                relation = new TableProviderTableFunctionResult(emitData, subRelations, mainRelation);
                return true;
            }

            relation = default;
            return false;
        }

        public ITableProvider Create()
        {
            return this;
        }

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
        {
            return new TableLineageMetadata("spicedb", "relationships", new NamedStruct()
            {
                Names = ["subject_type", "subject_id", "subject_relation", "relation", "resource_type", "resource_id"],
                Struct = new Struct()
                {
                    Types = new List<Substrait.Type.SubstraitBaseType>()
                    {
                        new StringType(),
                        new StringType(),
                        new StringType(),
                        new StringType(),
                        new StringType(),
                        new StringType()
                    }
                }
            });
        }
    }
}

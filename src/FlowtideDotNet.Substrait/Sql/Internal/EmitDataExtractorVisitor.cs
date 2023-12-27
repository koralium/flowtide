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

using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    /// <summary>
    /// Visitor that tries to identify what data will be returned by a query.
    /// </summary>
    internal class EmitDataExtractorVisitor : SqlBaseVisitor<EmitData?, object?>
    {
        private readonly TablesMetadata tablesMetadata;
        private readonly SqlFunctionRegister sqlFunctionRegister;

        public EmitDataExtractorVisitor(TablesMetadata tablesMetadata, SqlFunctionRegister sqlFunctionRegister)
        {

            this.tablesMetadata = tablesMetadata;
            this.sqlFunctionRegister = sqlFunctionRegister;
        }

        protected override EmitData? VisitQuery(Query query, object? state)
        {
            return Visit(query.Body, state);
        }

        protected override EmitData? VisitSetOperation(SetExpression.SetOperation setOperation, object? state)
        {
            var left = Visit(setOperation.Left, state);
            var right = Visit(setOperation.Right, state);

            if (left != null && right != null)
            {
                EmitData emitData = new EmitData();
                var leftNames = left.GetNames();
                var rightNames = right.GetNames();
                for (int i = 0; i < leftNames.Count; i++)
                {
                    var alias = default(string);
                    if (!leftNames[i].StartsWith('$'))
                    {
                        alias = leftNames[i];
                    }
                    else if (!rightNames[i].StartsWith('$'))
                    {
                        alias = rightNames[i];
                    }
                    else
                    {
                        alias = leftNames[i];
                    }
                    emitData.Add(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(alias) })), i, alias);
                }
                return emitData;
            }
            else if (left != null)
            {
                return left;
            }
            else if (right != null)
            {
                return right;
            }
            else
            {
                return null;
            }
        }

        protected override EmitData VisitSelect(Select select, object? state)
        {
            EmitData? parent = default;
            if (select.From != null)
            {
                if (select.From.Count != 1)
                {
                    throw new InvalidOperationException("Only a single table in the FROM statement is supported");
                }
                var fromTable = select.From[0];

                parent = Visit(fromTable, state);
            }

            if (select.Projection != null)
            {
                parent = VisitProjection(select.Projection, parent);
            }

            if (parent == null)
            {
                throw new InvalidOperationException("Could not extract emit data from query");
            }

            return parent;
        }

        private EmitData? VisitProjection(SqlParser.Sequence<SelectItem> selects, EmitData? parent)
        {
            EmitData projectEmitData = new EmitData();
            int outputCounter = 0;
            foreach (var s in selects)
            {
                var exprVisitor = new SqlExpressionVisitor(sqlFunctionRegister);
                if (s is SelectItem.ExpressionWithAlias exprAlias)
                {
                    projectEmitData.Add(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(exprAlias.Alias) })), outputCounter, exprAlias.Alias);
                    outputCounter++;
                }
                if (s is SelectItem.UnnamedExpression unnamedExpr)
                {
                    var conditionName = $"$expr{outputCounter}";
                    if (parent != null)
                    {
                        var condition = exprVisitor.Visit(unnamedExpr.Expression, parent);
                        conditionName = condition.Name;
                    }
                    
                    projectEmitData.Add(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(conditionName) })), outputCounter, conditionName);
                    outputCounter++;
                }
            }

            return projectEmitData;
        }

        protected override EmitData? VisitTable(TableFactor.Table table, object? state)
        {
            var tableName = string.Join('.', table.Name.Values.Select(x => x.Value));

            if (tablesMetadata.TryGetTable(tableName, out var t))
            {
                var emitData = new EmitData();

                for (int i = 0; i < t.Columns.Count; i++)
                {
                    emitData.Add(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(t.Columns[i]) })), i, t.Columns[i]);
                }

                if (table.Alias != null)
                {
                    for (int i = 0; i < t.Columns.Count; i++)
                    {
                        emitData.AddWithAlias(new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { new Ident(table.Alias.Name), new Ident(t.Columns[i]) })), i);
                    }
                }

                return emitData;
            }
            return null;
        }

        protected override EmitData? VisitTableWithJoins(TableWithJoins tableWithJoins, object? state)
        {
            var parent = Visit(tableWithJoins.Relation!, state);
            if (tableWithJoins.Joins != null)
            {
                foreach (var join in tableWithJoins.Joins)
                {
                    parent = VisitJoin(join, parent, state);
                }
            }
            return parent;
        }

        private EmitData? VisitJoin(Join join, EmitData? left, object? state)
        {
            Debug.Assert(join.Relation != null);
            var right = Visit(join.Relation, state);
            
            if (left == null || right == null)
            {
                return null;
            }

            EmitData joinEmitData = new EmitData();
            joinEmitData.Add(left, 0);
            joinEmitData.Add(right, right.GetNames().Count);

            return joinEmitData;
        }

        protected override EmitData? VisitCreateTable(Statement.CreateTable createTable, object? state)
        {
            return null;
        }
    }
}

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
using SqlParser;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Sql
{
    /// <summary>
    /// Carries all SQL-parser context needed by an <see cref="ITableProvider"/> implementation to
    /// resolve a table-valued function call into a Substrait <see cref="Relation"/>.
    /// </summary>
    /// <remarks>
    /// An instance is created by the SQL visitor for every table-valued function reference it
    /// encounters — either as a standalone <c>FROM</c> source or as the right-hand side of a
    /// <c>JOIN</c>. When used in a <c>JOIN</c>, <see cref="ParentRelation"/>,
    /// <see cref="JoinType"/>, and <see cref="JoinCondition"/> are populated so the
    /// implementation can build a joined relation directly; all three are <see langword="null"/>
    /// when the function is used as a standalone <c>FROM</c> source.
    /// <para>
    /// Implementations of <see cref="ITableProvider.TryHandleTableFunction"/> receive this
    /// object and use <see cref="Arguments"/>, <see cref="ExpressionVisitor"/>, and
    /// <see cref="EmitData"/> to evaluate the individual SQL arguments and resolve column
    /// references. <see cref="JoinType"/> should be checked first when the function does not
    /// support being placed inside a <c>JOIN</c>.
    /// </para>
    /// </remarks>
    public class TableProviderTableFunctionArguments
    {
        /// <summary>
        /// Gets the raw SQL function arguments as parsed from the query.
        /// </summary>
        /// <remarks>
        /// Use <see cref="ExpressionVisitor"/> together with <see cref="EmitData"/> to convert
        /// each element to a Substrait <see cref="Expressions.Expression"/>. The argument count
        /// and types are determined by the specific table function being implemented.
        /// </remarks>
        public Sequence<SqlParser.Ast.FunctionArg> Arguments { get; }

        /// <summary>
        /// Gets the optional alias declared for the table function in the SQL query, or
        /// <see langword="null"/> if no alias was specified.
        /// </summary>
        /// <remarks>
        /// For example, given <c>FROM spicedb.func(...) AS f</c>, this property returns
        /// <c>"f"</c>. Column references qualified with the alias are resolved through
        /// <see cref="EmitData"/>.
        /// </remarks>
        public string? TableAlias { get; }

        /// <summary>
        /// Gets the visitor used to convert raw SQL <see cref="SqlParser.Ast.Expression"/> nodes
        /// into Substrait <see cref="ExpressionData"/> values.
        /// </summary>
        /// <remarks>
        /// Pass each argument expression together with <see cref="EmitData"/> to
        /// <see cref="SqlExpressionVisitor.Visit"/> to obtain the corresponding Substrait
        /// expression and its inferred type.
        /// </remarks>
        public SqlExpressionVisitor ExpressionVisitor { get; }

        /// <summary>
        /// Gets the column metadata that tracks the names, types, and ordinal positions visible
        /// in the current query scope at the point where the table function is referenced.
        /// </summary>
        /// <remarks>
        /// When the function appears as a standalone <c>FROM</c> source this is an empty
        /// <see cref="EmitData"/> instance. When called from a <c>JOIN</c> it contains the
        /// columns contributed by the left-hand <see cref="ParentRelation"/>, allowing argument
        /// expressions to reference columns from the left side of the join.
        /// </remarks>
        public EmitData EmitData { get; }

        /// <summary>
        /// Gets the left-hand Substrait <see cref="Relation"/> when this function is the
        /// right-hand side of a <c>JOIN</c>, or <see langword="null"/> when the function
        /// appears as a standalone <c>FROM</c> source.
        /// </summary>
        public Relation? ParentRelation { get; }

        /// <summary>
        /// Gets the type of the enclosing SQL <c>JOIN</c> (such as <c>INNER</c> or
        /// <c>LEFT OUTER</c>), or <see langword="null"/> when the function appears as a
        /// standalone <c>FROM</c> source.
        /// </summary>
        /// <remarks>
        /// Implementations that do not support being placed inside a <c>JOIN</c> should check
        /// this property and throw when it is not <see langword="null"/>.
        /// </remarks>
        public JoinType? JoinType { get; }

        /// <summary>
        /// Gets the raw SQL <c>ON</c> condition expression of the enclosing <c>JOIN</c>, or
        /// <see langword="null"/> when the function appears as a standalone <c>FROM</c> source
        /// or when no explicit <c>ON</c> clause was provided.
        /// </summary>
        /// <remarks>
        /// Use <see cref="ExpressionVisitor"/> together with <see cref="EmitData"/> to convert
        /// this expression into a Substrait filter condition when building a joined relation.
        /// </remarks>
        public Expression? JoinCondition { get; }

        internal TableProviderTableFunctionArguments(
            Sequence<SqlParser.Ast.FunctionArg> arguments, 
            string? tableAlias, 
            SqlExpressionVisitor expressionVisitor, 
            EmitData emitData,
            Relation? parentRelation,
            JoinType? joinType,
            Expression? joinCondition)
        {
            Arguments = arguments;
            TableAlias = tableAlias;
            ExpressionVisitor = expressionVisitor;
            EmitData = emitData;
            JoinType = joinType;
            JoinCondition = joinCondition;
        }
    }
}

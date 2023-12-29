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

using SqlParser;
using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Statement;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    /// <summary>
    /// All code taken from https://github.com/TylerBrinks/SqlParser-cs/blob/main/src/SqlParser/Parser.cs
    /// and modified only to remove the requirement of data type when creating a table.
    /// </summary>
    internal class FlowtideDialect : MsSqlDialect
    {
        public override Statement? ParseStatement(Parser parser)
        {
            if (parser.ParseKeyword(Keyword.CREATE))
            {   
                if (parser.ParseKeyword(Keyword.TABLE))
                {
                    return ParseCreateTable(parser, false, false, false, false);
                }
                else
                {
                    parser.PrevToken();
                    return base.ParseStatement(parser);
                }
            }
            
            return base.ParseStatement(parser);
        }

        public CreateTable ParseCreateTable(Parser parser, bool orReplace, bool temporary, bool? global, bool transient)
        {
            var ifNotExists = parser.ParseIfNotExists();
            var tableName = parser.ParseObjectName();

            // Clickhouse has `ON CLUSTER 'cluster'` syntax for DDLs
            string? onCluster = null;
            if (parser.ParseKeywordSequence(Keyword.ON, Keyword.CLUSTER))
            {
                var token = parser.NextToken();
                onCluster = token switch
                {
                    SingleQuotedString s => s.Value,
                    Word w => w.Value,
                    _ => throw Parser.Expected("identifier or cluster literal", token)
                };
            }

            var like = Parser.ParseInit<ObjectName?>(parser.ParseKeyword(Keyword.LIKE) || parser.ParseKeyword(Keyword.ILIKE), parser.ParseObjectName);

            var clone = Parser.ParseInit<ObjectName?>(parser.ParseKeyword(Keyword.CLONE), parser.ParseObjectName);

            var (columns, constraints) = ParseColumns(parser);

            // SQLite supports `WITHOUT ROWID` at the end of `CREATE TABLE`
            var withoutRowId = parser.ParseKeywordSequence(Keyword.WITHOUT, Keyword.ROWID);

            var hiveDistribution = parser.ParseHiveDistribution();
            var hiveFormats = parser.ParseHiveFormats();

            // PostgreSQL supports `WITH ( options )`, before `AS`
            var withOptions = parser.ParseOptions(Keyword.WITH);
            var tableProperties = parser.ParseOptions(Keyword.TBLPROPERTIES);

            var engine = Parser.ParseInit(parser.ParseKeyword(Keyword.ENGINE), () =>
            {
                parser.ExpectToken<Equal>();
                var token = parser.NextToken();

                if (token is Word w)
                {
                    return w.Value;
                }

                throw Parser.Expected("identifier", token);
            });

            var orderBy = Parser.ParseInit(parser.ParseKeywordSequence(Keyword.ORDER, Keyword.BY), () =>
            {
                if (!parser.ConsumeToken<LeftParen>())
                {
                    return new Sequence<Ident> { parser.ParseIdentifier() };
                }

                var cols = Parser.ParseInit(parser.PeekToken() is not RightParen, () => parser.ParseCommaSeparated(parser.ParseIdentifier));
                parser.ExpectRightParen();
                return cols;

            });

            // Parse optional `AS ( query )`
            var query = Parser.ParseInit<Query>(parser.ParseKeyword(Keyword.AS), () => parser.ParseQuery());

            var defaultCharset = Parser.ParseInit(parser.ParseKeywordSequence(Keyword.DEFAULT, Keyword.CHARSET), () =>
            {
                parser.ExpectToken<Equal>();
                var token = parser.NextToken();

                if (token is Word w)
                {
                    return w.Value;
                }

                throw Parser.Expected("identifier", token);
            });

            var collation = Parser.ParseInit(parser.ParseKeyword(Keyword.COLLATE), () =>
            {
                parser.ExpectToken<Equal>();
                var token = parser.NextToken();

                if (token is Word w)
                {
                    return w.Value;
                }

                throw Parser.Expected("identifier", token);
            });

            var onCommit = OnCommit.None;

            if (parser.ParseKeywordSequence(Keyword.ON, Keyword.COMMIT, Keyword.DELETE, Keyword.ROWS))
            {
                onCommit = OnCommit.DeleteRows;
            }
            else if (parser.ParseKeywordSequence(Keyword.ON, Keyword.COMMIT, Keyword.PRESERVE, Keyword.ROWS))
            {
                onCommit = OnCommit.PreserveRows;
            }
            else if (parser.ParseKeywordSequence(Keyword.ON, Keyword.COMMIT, Keyword.DROP))
            {
                onCommit = OnCommit.Drop;
            }

            return new CreateTable(tableName, columns)
            {
                Temporary = temporary,
                Constraints = constraints.Any() ? constraints : null,
                WithOptions = withOptions.Any() ? withOptions : null,
                TableProperties = tableProperties.Any() ? tableProperties : null,
                OrReplace = orReplace,
                IfNotExists = ifNotExists,
                Transient = transient,
                HiveDistribution = hiveDistribution,
                HiveFormats = hiveFormats,
                Global = global,
                Query = query,
                WithoutRowId = withoutRowId,
                Like = like,
                CloneClause = clone,
                Engine = engine,
                OrderBy = orderBy,
                DefaultCharset = defaultCharset,
                Collation = collation,
                OnCommit = onCommit,
                OnCluster = onCluster
            };
        }

        public (Sequence<ColumnDef>, Sequence<TableConstraint>) ParseColumns(Parser parser)
        {
            var columns = new Sequence<ColumnDef>();
            var constraints = new Sequence<TableConstraint>();

            if (!parser.ConsumeToken<LeftParen>() || parser.ConsumeToken<RightParen>())
            {
                return (columns, constraints);
            }

            while (true)
            {
                var constraint = parser.ParseOptionalTableConstraint();
                if (constraint != null)
                {
                    constraints.Add(constraint);
                }
                else if (parser.PeekToken() is Word)
                {
                    columns.Add(ParseColumnDef(parser));
                }
                else
                {
                    Parser.ThrowExpected("column name or constraint definition", parser.PeekToken());
                }

                var commaFound = parser.ConsumeToken<Comma>();

                if (parser.ConsumeToken<RightParen>())
                {
                    // allow a trailing comma, even though it's not in standard
                    break;
                }

                if (!commaFound)
                {
                    Parser.ThrowExpected("',' or ')' after column definition", parser.PeekToken());
                }
            }

            return (columns, constraints);
        }

        public ColumnDef ParseColumnDef(Parser parser)
        {
            var name = parser.ParseIdentifier();
            var peekedToken = parser.PeekToken();
            DataType? dataType = default;
            if (peekedToken is Comma || peekedToken is RightParen)
            {
                dataType = new DataType.Custom(new ObjectName(new Ident("any")));
            }
            else
            {
                dataType = parser.ParseDataType();
            }
            var collation = Parser.ParseInit(parser.ParseKeyword(Keyword.COLLATE), parser.ParseObjectName);
            Sequence<ColumnOptionDef>? options = null;

            while (true)
            {
                ColumnOption? opt;
                if (parser.ParseKeyword(Keyword.CONSTRAINT))
                {
                    var ident = parser.ParseIdentifier();
                    if ((opt = parser.ParseOptionalColumnOption()) != null)
                    {
                        options ??= new Sequence<ColumnOptionDef>();
                        options.Add(new ColumnOptionDef(opt, ident));
                    }
                    else
                    {
                        throw Parser.Expected("constraint details after CONSTRAINT <name>", parser.PeekToken());
                    }
                }
                else if ((opt = parser.ParseOptionalColumnOption()) is { })
                {
                    options ??= new Sequence<ColumnOptionDef>();
                    options.Add(new ColumnOptionDef(opt));
                }
                else
                {
                    break;
                }
            }

            return new ColumnDef(name, dataType, collation, options);
        }

    }
}

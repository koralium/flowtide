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
using System.Text;
using static SqlParser.Ast.Statement;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    public record class StructSqlDataType : DataType
    {
        public List<string> Names { get; }

        public List<DataType> Types { get; }

        public StructSqlDataType(List<string> names, List<DataType> types)
        {
            Names = names;
            Types = types;
        }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(ToString());
        }

        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append("STRUCT<");

            for (int i = 0; i < Names.Count; i++)
            {
                stringBuilder.Append(Names[i]);
                stringBuilder.Append(" ");
                stringBuilder.Append(Types[i].ToSql());
                if (i < Names.Count - 1)
                {
                    stringBuilder.Append(", ");
                }
            }

            stringBuilder.Append(">");
            return stringBuilder.ToString();
        }
    }

    public record class MapSqlDataType : DataType
    {
        public DataType KeyType { get; }

        public DataType ValueType { get; }

        public MapSqlDataType(DataType keyType, DataType valueType)
        {
            KeyType = keyType;
            ValueType = valueType;
        }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(ToString());
        }

        public override string ToString()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append("MAP<");

            stringBuilder.Append(KeyType.ToSql());
            stringBuilder.Append(", ");
            stringBuilder.Append(ValueType.ToSql());

            stringBuilder.Append(">");
            return stringBuilder.ToString();
        }
    }

    /// <summary>
    /// All code taken from https://github.com/TylerBrinks/SqlParser-cs/blob/main/src/SqlParser/Parser.cs
    /// and modified only to remove the requirement of data type when creating a table.
    /// </summary>
    internal class FlowtideDialect : MsSqlDialect
    {
        
        private static bool TryParseSubstream(Parser parser)
        {
            var token = parser.PeekToken();
            if (token is Word word &&
                    string.Equals(word.Value, SqlTextResources.Substream, StringComparison.OrdinalIgnoreCase))
            {
                parser.NextToken();
                return true;
            }
            return false;
        }

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
                }
            }
            else if (TryParseSubstream(parser))
            {
                var objName = parser.ParseObjectName();
                return new BeginSubStream(objName);
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

        private DataType ParseDataType(Parser parser)
        {
            var nextToken = parser.NextToken();

            if (nextToken is Word word)
            {
                switch (word.Keyword)
                {
                    case Keyword.BOOLEAN:
                        return new DataType.Boolean();
                    case Keyword.FLOAT:
                        return new DataType.Float();
                    case Keyword.DOUBLE:
                        return new DataType.Double();
                    case Keyword.TINYINT:
                        return new DataType.TinyInt();
                    case Keyword.SMALLINT:
                        return new DataType.SmallInt();
                    case Keyword.MEDIUMINT:
                        return new DataType.MediumInt();
                    case Keyword.INT:
                        return new DataType.Int();
                    case Keyword.INTEGER:
                        return new DataType.Integer();
                    case Keyword.BIGINT:
                        return new DataType.BigInt();
                    case Keyword.VARCHAR:
                        return new DataType.Varchar();
                    case Keyword.NVARCHAR:
                        return new DataType.Nvarchar();
                    case Keyword.CHARACTER:
                        return new DataType.Character();
                    case Keyword.CHAR:
                        return new DataType.Char();
                    case Keyword.CLOB:
                        return new DataType.Clob();
                    case Keyword.BINARY:
                        return new DataType.Binary();
                    case Keyword.VARBINARY:
                        return new DataType.Varbinary();
                    case Keyword.BLOB:
                        return new DataType.Blob();
                    case Keyword.UUID:
                        return new DataType.Uuid();
                    case Keyword.DATE:
                        return new DataType.Date();
                    case Keyword.DATETIME:
                        return new DataType.Datetime();
                    case Keyword.TIMESTAMP:
                        return new DataType.Timestamp(TimezoneInfo.None);
                    case Keyword.TIMESTAMPTZ:
                        return new DataType.Timestamp(TimezoneInfo.Tz);
                    case Keyword.TIME:
                        return new DataType.Time(TimezoneInfo.None);
                    case Keyword.TIMETZ:
                        return new DataType.Time(TimezoneInfo.Tz);
                    case Keyword.INTERVAL:
                        return new DataType.Interval();
                    case Keyword.JSON:
                        return new DataType.Json();
                    case Keyword.REGCLASS:
                        return new DataType.Regclass();
                    case Keyword.STRING:
                        return new DataType.StringType();
                    case Keyword.TEXT:
                        return new DataType.Text();
                    case Keyword.BYTEA:
                        return new DataType.Bytea();
                    case Keyword.NUMERIC:
                        return new DataType.Numeric(new ExactNumberInfo.None());
                    case Keyword.DECIMAL:
                        return ParseDecimal();
                    case Keyword.DEC:
                        return new DataType.Dec(new ExactNumberInfo.None());
                    case Keyword.BIGNUMERIC:
                        return new DataType.BigNumeric(new ExactNumberInfo.None());;
                }

                if (word.Value.Equals("any", StringComparison.OrdinalIgnoreCase))
                {
                    return new DataType.Custom(new ObjectName(new Ident("any")));
                }

                if (word.Value.Equals("STRUCT", StringComparison.OrdinalIgnoreCase))
                {
                    parser.ExpectToken<LessThan>();
                    List<string> names = new List<string>();
                    List<DataType> types = new List<DataType>();
                    while (!parser.PeekTokenIs<GreaterThan>())
                    {
                        var fieldNameToken = parser.NextToken();

                        if (fieldNameToken is not Word fieldNameWord)
                        {
                            throw new ParserException("Expected field name", fieldNameToken.Location);
                        }
                        var fieldName = fieldNameWord.Value;

                        var fieldType = ParseDataType(parser);

                        names.Add(fieldName);
                        types.Add(fieldType);

                        if (parser.PeekTokenIs<Comma>())
                        {
                            parser.NextToken();
                        }
                    }

                    parser.ExpectToken<GreaterThan>();
                    return new StructSqlDataType(names, types);
                }
                if (word.Value.Equals("LIST", StringComparison.OrdinalIgnoreCase))
                {
                    parser.ExpectToken<LessThan>();
                    DataType dataType = ParseDataType(parser);
                    parser.ExpectToken<GreaterThan>();
                    return new DataType.Array(dataType);
                }
                if (word.Value.Equals("ARRAY", StringComparison.OrdinalIgnoreCase))
                {
                    parser.ExpectToken<LessThan>();
                    DataType dataType = ParseDataType(parser);
                    parser.ExpectToken<GreaterThan>();
                    return new DataType.Array(dataType);
                }
                if (word.Value.Equals("MAP", StringComparison.OrdinalIgnoreCase))
                {
                    parser.ExpectToken<LessThan>();
                    DataType keyType = ParseDataType(parser);
                    parser.ExpectToken<Comma>();
                    DataType valueType = ParseDataType(parser);
                    parser.ExpectToken<GreaterThan>();
                    return new MapSqlDataType(keyType, valueType);
                }
            }
            
            DataType ParseDecimal()
            {
                if (!parser.PeekTokenIs<LeftParen>())
                {
                    return new DataType.Decimal(new ExactNumberInfo.None());
                }

                parser.NextToken();
                var precision = parser.ParseNumberValue();
                parser.ExpectToken<Comma>();
                var scale = parser.ParseNumberValue();
                parser.ExpectToken<RightParen>();

                return new DataType.Decimal(new ExactNumberInfo.PrecisionAndScale((ulong)int.Parse(precision.AsNumber().Value), (ulong)int.Parse(scale.AsNumber().Value)));
            }


            throw new ParserException($"Unexpected data type {nextToken.ToString()}", nextToken.Location);
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
                dataType = ParseDataType(parser);
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

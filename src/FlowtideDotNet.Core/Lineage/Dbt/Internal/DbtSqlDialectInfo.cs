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

using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Substrait.Type;
using System.Globalization;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    internal sealed class DbtSqlDialectInfo
    {
        private static readonly DbtSqlDialectInfo s_postgres = new DbtSqlDialectInfo(DbtSqlDialect.Postgres, "postgres");
        private static readonly DbtSqlDialectInfo s_tsql = new DbtSqlDialectInfo(DbtSqlDialect.TSql, "tsql");
        private static readonly DbtSqlDialectInfo s_snowflake = new DbtSqlDialectInfo(DbtSqlDialect.Snowflake, "snowflake");

        private DbtSqlDialectInfo(DbtSqlDialect dialect, string adapterType)
        {
            Dialect = dialect;
            AdapterType = adapterType;
        }

        public DbtSqlDialect Dialect { get; }

        // Always a sqlglot dialect, never "sqlserver".
        public string AdapterType { get; }

        public static DbtSqlDialectInfo For(DbtSqlDialect dialect)
        {
            return dialect switch
            {
                DbtSqlDialect.Postgres => s_postgres,
                DbtSqlDialect.TSql => s_tsql,
                DbtSqlDialect.Snowflake => s_snowflake,
                _ => throw new ArgumentOutOfRangeException(nameof(dialect), dialect, "Unknown dbt SQL dialect.")
            };
        }

        public string Quote(string identifier)
        {
            return "\"" + identifier.Replace("\"", "\"\"") + "\"";
        }

        public string QuoteRelation(DbtTableIdentity identity)
        {
            return string.Join(".", identity.NonEmptyParts().Select(Quote));
        }

        // Null keeps the key, consumers read it as unknown.
        public string? MapDataType(SubstraitBaseType type)
        {
            if (type is DecimalType decimalType)
            {
                var name = Dialect switch
                {
                    DbtSqlDialect.Postgres => "numeric",
                    DbtSqlDialect.Snowflake => "number",
                    _ => "decimal"
                };
                if (decimalType.Precision is not int precision)
                {
                    return name;
                }
                if (decimalType.Scale is not int scale)
                {
                    return string.Create(CultureInfo.InvariantCulture, $"{name}({precision})");
                }
                return string.Create(CultureInfo.InvariantCulture, $"{name}({precision},{scale})");
            }

            return (LineageSchemaConverter.ToTypeName(type), Dialect) switch
            {
                ("string", DbtSqlDialect.Postgres) => "text",
                ("string", DbtSqlDialect.TSql) => "nvarchar(max)",
                ("string", DbtSqlDialect.Snowflake) => "varchar",
                ("int", DbtSqlDialect.TSql) => "int",
                ("int", _) => "integer",
                ("bigint", _) => "bigint",
                ("boolean", DbtSqlDialect.TSql) => "bit",
                ("boolean", _) => "boolean",
                ("date", _) => "date",
                ("timestamp", DbtSqlDialect.Postgres) => "timestamptz",
                ("timestamp", DbtSqlDialect.TSql) => "datetimeoffset",
                ("timestamp", DbtSqlDialect.Snowflake) => "timestamp_tz",
                ("float", DbtSqlDialect.Snowflake) => "float",
                ("float", _) => "real",
                ("double", DbtSqlDialect.Postgres) => "double precision",
                ("double", DbtSqlDialect.TSql) => "float",
                ("double", DbtSqlDialect.Snowflake) => "double",
                ("binary", DbtSqlDialect.Postgres) => "bytea",
                ("binary", DbtSqlDialect.TSql) => "varbinary(max)",
                ("binary", DbtSqlDialect.Snowflake) => "binary",
                ("array", DbtSqlDialect.Snowflake) => "array",
                ("map" or "struct", DbtSqlDialect.Snowflake) => "object",
                ("array" or "map" or "struct", DbtSqlDialect.Postgres) => "jsonb",
                ("array" or "map" or "struct", DbtSqlDialect.TSql) => "nvarchar(max)",
                _ => null
            };
        }
    }
}

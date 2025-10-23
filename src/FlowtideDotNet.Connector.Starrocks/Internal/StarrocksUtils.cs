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

using FlowtideDotNet.Connector.StarRocks.Internal.HttpApi;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Connector.StarRocks.Internal
{
    internal class StarRocksTableKey
    {
        public StarRocksTableKey(string schema, string tableName)
        {
            Schema = schema;
            TableName = tableName;
        }

        public string Schema { get; }
        public string TableName { get; }

        public override bool Equals(object? obj)
        {
            return obj is StarRocksTableKey key &&
                   Schema.Equals(key.Schema, StringComparison.OrdinalIgnoreCase) &&
                   TableName.Equals(key.TableName, StringComparison.OrdinalIgnoreCase);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(Schema), StringComparer.OrdinalIgnoreCase.GetHashCode(TableName));
        }
    }

    internal class TableInfo
    {
        public required List<string> ColumnNames { get; set; }

        public required List<string> PrimaryKeys { get; set; }
    }
    internal static class StarRocksUtils
    {
        private const string StarrocksColumnKeyPrimary = "PRI";

        

        private static SubstraitBaseType StarrocksTypeToSubstrait(string typeName)
        {
            if (typeName == "int")
            {
                return new Int64Type();
            }
            else if (typeName == "datetime")
            {
                return new TimestampType();
            }
            else if (typeName == "varchar")
            {
                return new StringType();
            }

            return new AnyType();
        }

        public static async Task<Dictionary<StarRocksTableKey, TableMetadata>> GetTables(StarRocksSinkOptions options)
        {
            var client = new StarRocksHttpClient(options);
            var result = await client.Query(@"
                SELECT t.table_schema, t.table_name, c.column_name, c.data_type, c.column_key FROM information_schema.columns c
                INNER JOIN information_schema.tables t
                ON c.`TABLE_SCHEMA` = t.`TABLE_SCHEMA` AND c.`TABLE_NAME` = t.`TABLE_NAME`
                where c.`TABLE_SCHEMA` NOT IN ('information_schema', '_statistics_', 'sys') AND t.`TABLE_TYPE` = 'BASE TABLE';
            ");
            var schemaIndex = result.FieldIndex("table_schema");
            var tableIndex = result.FieldIndex("table_name");
            var columnNameIndex = result.FieldIndex("column_name");
            var dataTypeIndex = result.FieldIndex("data_type");
            var columnKeyIndex = result.FieldIndex("column_key");
            

            Dictionary<StarRocksTableKey, NamedStruct> tablesInfo = new Dictionary<StarRocksTableKey, NamedStruct>();

            await foreach (var row in result.Rows)
            {
                string? schema = default;
                if (row[schemaIndex] is string schemaString)
                {
                    schema = schemaString;
                }
                else
                {
                    throw new InvalidOperationException("Schema name is not a string.");
                }
                string? table = default;
                if (row[tableIndex] is string tableString)
                {
                    table = tableString;
                }
                else
                {
                    throw new InvalidOperationException("Table name is not a string.");
                }
                string? columnName = default;
                if (row[columnNameIndex] is string columnNameString)
                {
                    columnName = columnNameString;
                }
                else
                {
                    throw new InvalidOperationException("Column name is not a string.");
                }
                string? dataType = default;
                if (row[dataTypeIndex] is string dataTypeString)
                {
                    dataType = dataTypeString;
                }
                else
                {
                    throw new InvalidOperationException("Data type is not a string.");
                }
                string? columnKey = default;
                if (row[columnKeyIndex] is string columnKeyString)
                {
                    columnKey = columnKeyString;
                }
                else
                {
                    throw new InvalidOperationException("Column key is not a string.");
                }

                if (!tablesInfo.TryGetValue(new StarRocksTableKey(schema, table), out var namedStruct))
                {
                    namedStruct = new NamedStruct()
                    {
                        Names = new List<string>(),
                        Struct = new Struct()
                        {
                            Types = new List<SubstraitBaseType>()
                        }
                    };
                    tablesInfo[new StarRocksTableKey(schema, table)] = namedStruct;
                }

                if (namedStruct.Struct == null)
                {
                    throw new InvalidOperationException("Struct is null.");
                }

                namedStruct.Names.Add(columnName);
                namedStruct.Struct.Types.Add(StarrocksTypeToSubstrait(dataType));
            }
            Dictionary<StarRocksTableKey, TableMetadata> tables = new Dictionary<StarRocksTableKey, TableMetadata>();

            foreach (var kv in tablesInfo)
            {
                tables.Add(kv.Key, new TableMetadata($"{kv.Key.Schema}.{kv.Key.TableName}", kv.Value));
            }

            return tables;

        }

        public static async Task<TableInfo> GetTableInfo(StarRocksSinkOptions options, List<string> names)
        {
            GetCatalogSchemaTable(names, out var catalog, out var schema, out var table);
            var client = new StarRocksHttpClient(options);
            var result = await client.Query($"SELECT column_name, data_type, column_key FROM information_schema.columns WHERE table_name = '{table}' AND table_schema = '{schema}';");

            var columnNameIndex = result.FieldIndex("column_name");
            var columnKeyIndex = result.FieldIndex("column_key");
            var dataTypeIndex = result.FieldIndex("data_type");

            List<string> columnNames = new List<string>();
            List<string> primaryKeyColumns = new List<string>();
            await foreach (var row in result.Rows)
            {
                string? columnName = default;
                if (row[columnNameIndex] is string columnNameString)
                {
                    columnName = columnNameString;
                }
                else
                {
                    throw new InvalidOperationException("Column name is not a string.");
                }
                columnNames.Add(columnName);
                if (row[columnKeyIndex] is string columnKeyString && columnKeyString == StarrocksColumnKeyPrimary)
                {
                    primaryKeyColumns.Add(columnName);
                }
            }

            return new TableInfo()
            {
                ColumnNames = columnNames,
                PrimaryKeys = primaryKeyColumns
            };
        }

        private static void GetCatalogSchemaTable(List<string> names, out string catalog, out string schema, out string table)
        {
            if (names.Count == 3)
            {
                catalog = names[0];
                schema = names[1];
                table = names[2];
            }
            else if (names.Count == 2)
            {
                catalog = "default_catalog";
                schema = names[0];
                table = names[1];
            }
            else if (names.Count == 1)
            {
                catalog = "default_catalog";
                schema = "default_schema";
                table = names[0];
            }
            else
            {
                throw new NotSupportedException($"Table name with {names.Count} parts is not supported.");
            }
        }

    }
}

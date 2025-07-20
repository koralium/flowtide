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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Substrait.Relations;
using Npgsql;
using Npgsql.Schema;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Postgresql.Internal
{
    internal static class PostgresClientUtils
    {
        public static async Task<NpgsqlLogSequenceNumber> GetCurrentWalLocation(NpgsqlConnection connection)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT pg_current_wal_lsn()";

            var result = await cmd.ExecuteScalarAsync();
            if (result is NpgsqlLogSequenceNumber lsn)
            {
                return lsn;
            }
            else if (result is string strLsn)
            {
                return NpgsqlLogSequenceNumber.Parse(strLsn);
            }
            else
            {
                throw new InvalidOperationException("Unexpected result type from pg_current_wal_lsn()");
            }
        }

        public static string CreateSelectStatementTop1(ReadRelation readRelation, int topLength = 1)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append($"SELECT ");

            stringBuilder.Append(string.Join(", ", readRelation.BaseSchema.Names.Select(x => $"\"{x}\"")));

            stringBuilder.Append(" FROM ");
            stringBuilder.Append(string.Join(".", readRelation.NamedTable.Names.Select(x => $"\"{x}\"")));

            stringBuilder.Append(" LIMIT 1");

            return stringBuilder.ToString();
        }

        public static async Task<List<NpgsqlDbColumn>> GetTableSchema(NpgsqlConnection connection, ReadRelation readRelation)
        {
            var command = connection.CreateCommand();
            command.CommandText = CreateSelectStatementTop1(readRelation);

            using var reader = await command.ExecuteReaderAsync();
            return (await reader.GetColumnSchemaAsync()).ToList();
        }

        public static async Task<IReadOnlyList<string>> GetReplicationColumns(NpgsqlConnection connection, IReadOnlyList<string> tableName)
        {
            var replicationIdentity = await GetReplicationIdentity(connection, tableName);

            switch (replicationIdentity)
            {
                case ReplicationIdentity.Default:
                    return await GetPrimaryKeys(connection, tableName);
                case ReplicationIdentity.Index:
                    throw new InvalidOperationException("Postgres source does not support replication identity 'index'. Please use 'default' or 'full'.");
                case ReplicationIdentity.Nothing:
                    throw new InvalidOperationException("Postgres source does not support replication identity 'nothing'. Please use 'default' or 'full'.");
                case ReplicationIdentity.Full:
                    return await GetColumnNames(connection, tableName);
                default:
                    throw new InvalidOperationException($"Unknown replication identity: {replicationIdentity}");
            }
        }

        public static async Task<ReplicationIdentity> GetReplicationIdentity(NpgsqlConnection connection, IReadOnlyList<string> tableName)
        {
            var cmd = @"
                SELECT relreplident
                FROM pg_class
                WHERE oid = 'tablename'::regclass;
            ";

            using var command = connection.CreateCommand();

            command.CommandText = cmd.Replace("tablename", tableName[0]);
            var result = await command.ExecuteScalarAsync();

            if (result is null)
            {
                throw new InvalidOperationException($"Table {tableName[0]} does not exist or has no replication identity set.");
            }

            if (result is char identity)
            {
                return identity switch
                {
                    'd' => ReplicationIdentity.Default,
                    'n' => ReplicationIdentity.Nothing,
                    'f' => ReplicationIdentity.Full,
                    'i' => ReplicationIdentity.Index,
                    _ => throw new InvalidOperationException($"Unknown replication identity: {identity}")
                };
            }
            else
            {
                throw new InvalidOperationException($"Unexpected result type for replication identity: {result.GetType()}");
            }
        }

        public static async Task<IReadOnlyList<string>> GetColumnNames(NpgsqlConnection connection, IReadOnlyList<string> tableName)
        {
            var cmd = @"
            SELECT
                column_name
            FROM
                information_schema.columns
            WHERE
                table_name = 'table_name';";

            using var command = connection.CreateCommand();
            command.CommandText = cmd.Replace("table_name", tableName[0]);
            using var reader = await command.ExecuteReaderAsync();

            var columnNames = new List<string>();

            while (reader.Read())
            {

                var columnName = reader.GetString(0);
                columnNames.Add(columnName);
            }

            if (columnNames.Count == 0)
            {
                throw new InvalidOperationException($"No columns found for table {tableName[0]}");
            }

            return columnNames;
        }

        public static async Task<IReadOnlyList<string>> GetPrimaryKeys(NpgsqlConnection connection, IReadOnlyList<string> tableName)
        {
            var cmd = @"
                SELECT a.attname
                FROM   pg_index i
                JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                     AND a.attnum = ANY(i.indkey)
                WHERE  i.indrelid = 'tablename'::regclass
                AND    i.indisprimary;";

            using var command = connection.CreateCommand();
            command.CommandText = cmd.Replace("tablename", tableName[0]);
            using var reader = await command.ExecuteReaderAsync();

            var primaryKeys = new List<string>();

            while (await reader.ReadAsync())
            {
                primaryKeys.Add(reader.GetString(0));
            }

            if (primaryKeys.Count == 0)
            {
                throw new InvalidOperationException($"No primary key found for table {tableName[0]}");
            }

            return primaryKeys;
        }

        public static List<Action<NpgsqlDataReader, IColumn>> GetColumnEventCreator(List<NpgsqlDbColumn> dbColumns)
        {
            List<Action<NpgsqlDataReader, IColumn>> output = new List<Action<NpgsqlDataReader, IColumn>>();

            for (int i = 0; i < dbColumns.Count; i++)
            {
                var type = dbColumns[i].DataType;
                int index = i;

                if (type == null)
                {
                    throw new InvalidOperationException($"Column {dbColumns[i].ColumnName} has no data type defined.");
                }
                if (type.Equals(typeof(string)))
                {
                    output.Add((reader, column) =>
                    {
                        if (reader.IsDBNull(index))
                        {
                            column.Add(NullValue.Instance);
                            return;
                        }
                        column.Add(new StringValue(reader.GetString(index)));
                    });
                }
                else if (type.Equals(typeof(int)))
                {
                    output.Add((reader, column) =>
                    {
                        if (reader.IsDBNull(index))
                        {
                            column.Add(NullValue.Instance);
                            return;
                        }
                        column.Add(new Int64Value(reader.GetInt32(index)));
                    });
                }
                else if (type.Equals(typeof(long)))
                {
                    output.Add((reader, column) =>
                    {
                        if (reader.IsDBNull(index))
                        {
                            column.Add(NullValue.Instance);
                            return;
                        }
                        column.Add(new Int64Value(reader.GetInt64(index)));
                    });
                }
                else
                {
                    throw new NotSupportedException($"Unsupported data type: {type.Name} for column {dbColumns[i].ColumnName}");
                }
            }
            return output;
        }

        public static string CreateInitialSelectStatement(ReadRelation readRelation, IReadOnlyList<string> primaryKeys, int batchSize, bool includePkParameters, string? filter)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append("SELECT ");

            stringBuilder.Append(string.Join(", ", readRelation.BaseSchema.Names.Select(x => $"\"{x}\"")));

            stringBuilder.Append(" FROM ");
            stringBuilder.AppendLine(string.Join(".", readRelation.NamedTable.Names.Select(x => $"\"{x}\"")));

            string? filters = filter;
            if (includePkParameters)
            {
                var pkFilters = GetInitialLoadWhereStatement(primaryKeys);

                if (filters != null)
                {
                    filters = $"({filters}) AND ({pkFilters})";
                }
                else
                {
                    filters = pkFilters;
                }

            }

            if (filters != null)
            {
                stringBuilder.AppendLine($"WHERE {filters}");
            }

            string orderBys = string.Join(", ", primaryKeys);
            stringBuilder.AppendLine(" ORDER BY " + orderBys);
            stringBuilder.AppendLine(" OFFSET 0 ROWS FETCH NEXT " + batchSize + " ROWS ONLY");

            return stringBuilder.ToString();
        }

        public static string? GetInitialLoadWhereStatement(IReadOnlyList<string> primaryKeys)
        {
            List<string> primaryKeyComparators = new List<string>();
            for (int i = 0; i < primaryKeys.Count; i++)
            {
                string pkName = primaryKeys[i];
                string pkValue = $"@{pkName}";

                List<string> comparators = new List<string>();
                comparators.Add("(" + pkName + " > " + pkValue + ")");

                for (int k = i - 1; k >= 0; k--)
                {
                    string innerPkName = primaryKeys[k];
                    String innerPkValue = $"@{innerPkName}";
                    comparators.Add("(" + innerPkName + " = " + innerPkValue + ")");
                }

                if (comparators.Count == 1)
                {
                    primaryKeyComparators.Add(string.Join(" AND ", comparators));
                }
                else
                {
                    primaryKeyComparators.Add("(" + string.Join(" AND ", comparators) + ")");
                }
            }

            string output = "(" + string.Join(" OR ", primaryKeyComparators) + ")";
            return output;
        }

        public static async Task<bool> CheckIfPublicationExists(NpgsqlConnection connection, string publication)
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = @publication)";
            command.Parameters.AddWithValue("publication", publication);

            var result = await command.ExecuteScalarAsync();

            return (bool)result;
        }

        public static async Task CreatePublication(NpgsqlConnection connection, string publication, IReadOnlyList<string> tables)
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"CREATE PUBLICATION {publication} FOR TABLE {string.Join(", ", tables.Select(t => $"\"{t}\""))}";
            await command.ExecuteNonQueryAsync();
        }

        public static async Task<Dictionary<uint, string>> GetDataTypeIdToTypeList(NpgsqlConnection connection)
        {
            var cmd = @"
            SELECT oid, typname AS schema
            FROM pg_type
            ORDER BY oid;";

            using var command = connection.CreateCommand();
            command.CommandText = cmd;
            using var reader = await command.ExecuteReaderAsync();
            Dictionary<uint, string> dataTypeIdToType = new Dictionary<uint, string>();

            while (await reader.ReadAsync()) {

                uint oid = (uint)reader.GetValue(0);
                string typeName = reader.GetString(1);

                dataTypeIdToType[oid] = typeName;
            }

            if (dataTypeIdToType.Count == 0)
            {
                throw new InvalidOperationException("No data types found in the database.");
            }

            return dataTypeIdToType;
        }
    }
}

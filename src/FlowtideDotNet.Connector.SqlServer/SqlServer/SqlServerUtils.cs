﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlexBuffers;
using FlowtideDotNet.Connector.SqlServer.SqlServer;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Exceptions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Data.SqlClient;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Substrait.Tests.SqlServer
{
    internal static partial class SqlServerUtils
    {
        public static List<Action<SqlDataReader, IColumn>> GetColumnEventCreator(ReadOnlyCollection<DbColumn> dbColumns)
        {
            List<Action<SqlDataReader, IColumn>> output = new List<Action<SqlDataReader, IColumn>>();
            for (int i = 0; i < dbColumns.Count; i++)
            {
                var column = dbColumns[i];
                int index = i;
                switch (column.DataTypeName)
                {
                    case "nchar":
                    case "char":
                    case "varchar":
                    case "nvarchar":
                    case "ntext":
                    case "text":
                    case "xml":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }
                            column.Add(new StringValue(reader.GetString(index)));
                        });
                        break;
                    case "tinyint":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }
                            column.Add(new Int64Value(reader.GetByte(index)));
                        });
                        break;
                    case "smallint":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }

                            column.Add(new Int64Value(reader.GetInt16(index)));
                        });
                        break;
                    case "int":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }

                            column.Add(new Int64Value(reader.GetInt32(index)));
                        });
                        break;
                    case "money":
                    case "decimal":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }

                            column.Add(new DecimalValue(reader.GetDecimal(index)));
                        });
                        break;
                    case "date":
                    case "datetime":
                    case "smalldatetime":
                    case "datetime2":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }
                            var dateTime = reader.GetDateTime(index);
                            column.Add(new TimestampTzValue(dateTime));
                        });
                        break;
                    case "datetimeoffset":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }
                            var dateTimeOffset = reader.GetDateTimeOffset(index);
                            column.Add(new TimestampTzValue(dateTimeOffset));
                        });
                        break;
                    case "time":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }
                            var time = reader.GetTimeSpan(index);
                            column.Add(new Int64Value(time.Ticks));
                        });
                        break;
                    case "bit":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }
                            var boolean = reader.GetBoolean(index);
                            column.Add(new BoolValue(boolean));
                        });
                        break;
                    case "bigint":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }

                            column.Add(new Int64Value(reader.GetInt64(index)));
                        });
                        break;
                    case "real":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }

                            column.Add(new DoubleValue(reader.GetFloat(index)));
                        });
                        break;
                    case "float":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }

                            column.Add(new DoubleValue(reader.GetDouble(index)));
                        });
                        break;
                    case "uniqueidentifier":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }

                            var guid = reader.GetGuid(index);
                            column.Add(new StringValue(guid.ToString()));
                        });
                        break;
                    case "binary":
                    case "varbinary":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }

                            var binary = reader.GetSqlBinary(index);
                            column.Add(new BinaryValue(binary.Value));
                        });
                        break;
                    case "image":
                        output.Add((reader, column) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                column.Add(NullValue.Instance);
                                return;
                            }

                            var binary = reader.GetSqlBinary(index);
                            column.Add(new BinaryValue(binary.Value));
                        });
                        break;
                    default:
                        throw new NotImplementedException(column.DataTypeName);
                }
            }
            return output;
        }

        public static SubstraitBaseType GetSubstraitType(string dataTypeName)
        {
            switch (dataTypeName.ToLower())
            {
                case "nchar":
                case "char":
                case "varchar":
                case "nvarchar":
                case "ntext":
                case "text":
                case "xml":
                    return new StringType();
                case "tinyint":
                case "smallint":
                case "int":
                    return new Int64Type();
                case "money":
                case "decimal":
                    return new DecimalType();
                case "date":
                case "datetime":
                case "smalldatetime":
                case "datetime2":
                    return new TimestampType();
                case "time":
                    return new Int64Type();
                case "bit":
                    return new BoolType();
                case "bigint":
                    return new Int64Type();
                case "real":
                    return new Fp64Type();
                case "float":
                    return new Fp64Type();
                case "uniqueidentifier":
                    return new StringType();
                case "binary":
                case "varbinary":
                    return new BinaryType();
                case "image":
                    return new BinaryType();
                default:
                    return new AnyType();
            }
        }

        public static Func<SqlDataReader, RowEvent> GetStreamEventCreator(ReadOnlyCollection<DbColumn> dbColumns)
        {
            List<Action<SqlDataReader, IFlexBufferVectorBuilder>> columns = new List<Action<SqlDataReader, IFlexBufferVectorBuilder>>();
            for (int i = 0; i < dbColumns.Count; i++)
            {
                var column = dbColumns[i];
                int index = i;
                switch (column.DataTypeName)
                {
                    case "nchar":
                    case "char":
                    case "varchar":
                    case "nvarchar":
                    case "ntext":
                    case "text":
                    case "xml":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }
                            builder.Add(reader.GetString(index));
                        });
                        break;
                    case "tinyint":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }
                            builder.Add(reader.GetByte(index));
                        });
                        break;
                    case "smallint":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }

                            builder.Add(reader.GetInt16(index));
                        });
                        break;
                    case "int":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }

                            builder.Add(reader.GetInt32(index));
                        });
                        break;
                    case "money":
                    case "decimal":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }

                            builder.Add(reader.GetDecimal(index));
                        });
                        break;
                    case "date":
                    case "datetime":
                    case "smalldatetime":
                    case "datetime2":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }
                            var dateTime = reader.GetDateTime(index);
                            var ms = dateTime.Subtract(DateTime.UnixEpoch).Ticks;
                            builder.Add(ms);
                        });
                        break;
                    case "time":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }
                            var time = reader.GetTimeSpan(index);
                            builder.Add(time.Ticks);
                        });
                        break;
                    case "bit":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }
                            var boolean = reader.GetBoolean(index);
                            builder.Add(boolean);
                        });
                        break;
                    case "bigint":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }

                            builder.Add(reader.GetInt64(index));
                        });
                        break;
                    case "real":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }

                            builder.Add(reader.GetFloat(index));
                        });
                        break;
                    case "float":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }

                            builder.Add(reader.GetDouble(index));
                        });
                        break;
                    case "uniqueidentifier":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }

                            var guid = reader.GetGuid(index);
                            builder.Add(guid.ToString());
                        });
                        break;
                    case "binary":
                    case "varbinary":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }

                            var binary = reader.GetSqlBinary(index);
                            builder.Add(binary.Value);
                        });
                        break;
                    case "image":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }

                            var binary = reader.GetSqlBinary(index);
                            builder.Add(binary.Value);
                        });
                        break;
                    default:
                        throw new NotImplementedException(column.DataTypeName);
                }
            }
            return (reader) =>
            {
                return RowEvent.Create(1, 0, builder =>
                {
                    for (int i = 0; i < columns.Count; i++)
                    {
                        columns[i](reader, builder);
                    }
                });
            };
        }

        public static string CreateSelectStatementTop1(ReadRelation readRelation, int topLength = 1)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append($"SELECT TOP ({topLength}) ");

            stringBuilder.Append(string.Join(", ", readRelation.BaseSchema.Names.Select(x => $"[{x}]")));

            stringBuilder.Append(" FROM ");
            stringBuilder.Append(string.Join(".", readRelation.NamedTable.Names.Select(x => $"[{x}]")));

            return stringBuilder.ToString();
        }

        public static string CreateInitialSelectStatement(ReadRelation readRelation)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append("SELECT ");

            stringBuilder.Append(string.Join(", ", readRelation.BaseSchema.Names.Select(x => $"[{x}]")));

            stringBuilder.Append(" FROM ");
            stringBuilder.Append(string.Join(".", readRelation.NamedTable.Names.Select(x => $"[{x}]")));

            return stringBuilder.ToString();
        }

        public static string CreateInitialPartitionedSelectStatement(ReadRelation readRelation, PartitionMetadata partitionMetadata, List<string> primaryKeys, int batchSize, bool includePkParameters, string? filter)
        {
            var stringBuilder = new StringBuilder();
            var cols = string.Join(", ", readRelation.BaseSchema.Names.Select(x => $"[{x}]"));
            var table = string.Join(".", readRelation.NamedTable.Names.Select(x => $"[{x}]"));

            stringBuilder.AppendLine($"SELECT {cols}");
            stringBuilder.AppendLine($"FROM {table}");

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
                stringBuilder.AppendLine($"WHERE $PARTITION.{partitionMetadata.PartitionFunction}({partitionMetadata.PartitionColumn}) = @PartitionId AND {filters}");
            }
            else
            {
                stringBuilder.AppendLine($"WHERE $PARTITION.{partitionMetadata.PartitionFunction}({partitionMetadata.PartitionColumn}) = @PartitionId");
            }

            stringBuilder.AppendLine($"ORDER BY {string.Join(", ", primaryKeys)}");
            stringBuilder.AppendLine($"OFFSET 0 ROWS FETCH NEXT {batchSize} ROWS ONLY");

            return stringBuilder.ToString();

        }

        public static string CreateInitialSelectStatement(ReadRelation readRelation, List<string> primaryKeys, int batchSize, bool includePkParameters, string? filter)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append("SELECT ");

            stringBuilder.Append(string.Join(", ", readRelation.BaseSchema.Names.Select(x => $"[{x}]")));

            stringBuilder.Append(" FROM ");
            stringBuilder.AppendLine(string.Join(".", readRelation.NamedTable.Names.Select(x => $"[{x}]")));

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

        public static async Task<ReadOnlyCollection<DbColumn>> GetWriteTableSchema(SqlConnection connection, WriteRelation writeRelation)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append("SELECT TOP (1) ");

            stringBuilder.Append(string.Join(", ", writeRelation.TableSchema.Names.Select(x => $"[{x}]")));

            stringBuilder.Append(" FROM ");
            stringBuilder.Append(string.Join(".", writeRelation.NamedObject.Names.Select(x => $"[{x}]")));

            var cmd = stringBuilder.ToString();
            using var command = connection.CreateCommand();
            command.CommandText = cmd;
            using var reader = await command.ExecuteReaderAsync();
            var columnSchema = await reader.GetColumnSchemaAsync();
            return columnSchema;
        }

        public static string CreateChangesSelectStatement(ReadRelation readRelation, List<string> primaryKeys)
        {
            StringBuilder stringBuilder = new StringBuilder();

            List<string> primaryKeyEquals = new List<string>();
            List<string> columnSelects = new List<string>();
            if (readRelation.BaseSchema.Struct == null)
            {
                throw new FlowtideException("Struct must be defined in the base schema for SQL Server.");
            }
            for (int i = 0; i < readRelation.BaseSchema.Struct.Types.Count; i++)
            {
                if (primaryKeys.Contains(readRelation.BaseSchema.Names[i], StringComparer.OrdinalIgnoreCase))
                {
                    columnSelects.Add($"c.[{readRelation.BaseSchema.Names[i]}]");
                    primaryKeyEquals.Add($"c.{readRelation.BaseSchema.Names[i]} = t.{readRelation.BaseSchema.Names[i]}");
                }
                else
                {
                    columnSelects.Add($"t.[{readRelation.BaseSchema.Names[i]}]");
                }
            }

            var joinCondition = string.Join(" AND ", primaryKeyEquals);

            stringBuilder.Append("SELECT ");

            stringBuilder.Append(string.Join(", ", columnSelects));

            stringBuilder.Append(", c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION from CHANGETABLE(CHANGES ");
            stringBuilder.Append(string.Join(".", readRelation.NamedTable.Names.Select(x => $"[{x}]")));
            stringBuilder.AppendLine(", @ChangeVersion) as c");
            stringBuilder.Append("LEFT JOIN ");
            stringBuilder.Append(string.Join(".", readRelation.NamedTable.Names.Select(x => $"[{x}]")));
            stringBuilder.AppendLine(" t");
            stringBuilder.Append("ON ");
            stringBuilder.AppendLine(joinCondition);
            stringBuilder.AppendLine("ORDER BY c.SYS_CHANGE_VERSION");

            return stringBuilder.ToString();
        }

        public static string CreateMergeIntoProcedure(string tmpTableName, string destinationTableName, HashSet<string> primaryKeys, DataTable dataTable)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("MERGE INTO ");
            stringBuilder.Append(destinationTableName);
            stringBuilder.AppendLine(" AS tgt");

            stringBuilder.Append("USING ");
            stringBuilder.Append(tmpTableName);
            stringBuilder.AppendLine(" AS src");

            stringBuilder.Append("ON ");

            // Add primary keys as the on condition in the merge statement
            List<string> pkComparisons = new List<string>();
            foreach (var pkColumn in primaryKeys)
            {
                pkComparisons.Add($"src.[{pkColumn}] = tgt.[{pkColumn}]");
            }

            stringBuilder.AppendLine(string.Join(" AND ", pkComparisons));

            List<string> updateNotEquals = new List<string>();
            for (int i = 1; i < dataTable.Columns.Count; i++)
            {
                var col = dataTable.Columns[i];
                if (primaryKeys.Contains(col.ColumnName))
                {
                    continue;
                }

                updateNotEquals.Add($"src.[{col.ColumnName}] != tgt.[{col.ColumnName}]");
            }

            // Add update statement
            // This statement only updates rows if there is a difference
            // Check that there are columns except primary keys
            if (primaryKeys.Count != (dataTable.Columns.Count - 1))
            {
                stringBuilder.AppendLine($"WHEN MATCHED AND src.[md_operation] = 'I' THEN");
                stringBuilder.Append("UPDATE SET ");

                List<string> updateSets = new List<string>();
                for (int i = 1; i < dataTable.Columns.Count; i++)
                {
                    var col = dataTable.Columns[i];
                    if (primaryKeys.Contains(col.ColumnName))
                    {
                        continue;
                    }

                    updateSets.Add($"tgt.[{col.ColumnName}] = src.[{col.ColumnName}]");
                }
                stringBuilder.AppendLine(string.Join(", ", updateSets));
            }


            // Add delete statement
            stringBuilder.AppendLine($"WHEN MATCHED AND src.[md_operation] = 'D' THEN");
            stringBuilder.AppendLine("DELETE");

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND src.[md_operation] = 'I' THEN");

            List<string> columnNames = new List<string>();
            for (int i = 1; i < dataTable.Columns.Count; i++)
            {
                var col = dataTable.Columns[i];
                columnNames.Add(col.ColumnName);
            }

            stringBuilder.AppendLine($"INSERT ({string.Join(", ", columnNames.Select(x => $"[{x}]"))})");
            stringBuilder.AppendLine($"VALUES ({string.Join(", ", columnNames.Select(x => $"[{x}]"))});");

            stringBuilder.Append("DELETE FROM ");
            stringBuilder.Append(tmpTableName);
            stringBuilder.AppendLine(";");

            return stringBuilder.ToString();
        }

        public static async Task<bool> IsChangeTrackingEnabled(SqlConnection sqlConnection, string tableFullName)
        {
            var splitName = tableFullName.Split('.');

            if (splitName.Length != 3)
            {
                throw new InvalidOperationException("Table name must contain database.schema.tablename");
            }
            var db = splitName[0];
            var schema = splitName[1];
            var table = splitName[2];

            await sqlConnection.ChangeDatabaseAsync(db);

            string query = "SELECT sys.schemas.name as schema_name, sys.tables.name as table_name\n" +
                "FROM sys.change_tracking_tables\n" +
                "JOIN sys.tables ON sys.tables.object_id = sys.change_tracking_tables.object_id\n" +
                "JOIN sys.schemas ON sys.schemas.schema_id = sys.tables.schema_id\n" +
                "WHERE sys.tables.name = @tableName AND sys.schemas.name = @schema;";

            using var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = query;
            cmd.Parameters.Add(new SqlParameter("tableName", table));
            cmd.Parameters.Add(new SqlParameter("schema", schema));

            using var reader = await cmd.ExecuteReaderAsync();

            return await reader.ReadAsync();
        }

        public static async Task<long> GetLatestChangeVersion(SqlConnection sqlConnection, IReadOnlyList<string> table)
        {
            if (table.Count == 3)
            {
                var originalDatabase = sqlConnection.Database;
                try
                {
                    await sqlConnection.ChangeDatabaseAsync(table[0]);
                }
                finally
                {
                    await sqlConnection.ChangeDatabaseAsync(originalDatabase);
                }
            }
            using var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = "SELECT CHANGE_TRACKING_CURRENT_VERSION()";

            using var reader = await cmd.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                return reader.GetInt64(0);
            }
            else
            {
                throw new InvalidOperationException("Could not get change tracking version from sql server.");
            }
        }

        public static async Task<long> GetServerTimestamp(SqlConnection sqlConnection)
        {
            using var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = "SELECT SYSDATETIMEOFFSET()";
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var dateTimeOffset = reader.GetDateTimeOffset(0);
                return dateTimeOffset.ToUniversalTime().Ticks;
            }
            else
            {
                throw new InvalidOperationException("Could not get server timestamp from sql server.");
            }
        }

        public static string GetCreateTemporaryTableQuery(ReadOnlyCollection<DbColumn> columns, string tmpTableName)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("CREATE TABLE ");
            stringBuilder.AppendLine(tmpTableName);

            List<string> columnsData = new List<string>();
            columnsData.Add("md_operation varchar(2)");

            foreach (var column in columns)
            {
                var columnType = column.DataTypeName;
                if (columnType == "varchar" || columnType == "nvarchar" || columnType == "char" || columnType == "nchar" || columnType == "binary" || columnType == "varbinary")
                {
                    if (column.ColumnSize > 8000)
                    {
                        columnType = $"{columnType}(MAX)";
                    }
                    else
                    {
                        columnType = $"{columnType}({column.ColumnSize})";
                    }
                }
                else if (columnType == "decimal")
                {
                    columnType = $"{columnType}({column.NumericPrecision}, {column.NumericScale})";
                }

                columnsData.Add($"[{column.ColumnName}] {columnType}");
            }
            stringBuilder.AppendLine("(");
            stringBuilder.AppendLine(string.Join(",\r\n", columnsData));
            stringBuilder.AppendLine(")");
            return stringBuilder.ToString();
        }

        public static async Task CreateTemporaryTable(SqlConnection connection, ReadOnlyCollection<DbColumn> columns, string tmpTableName)
        {
            var query = GetCreateTemporaryTableQuery(columns, tmpTableName);

            using var command = connection.CreateCommand();
            command.CommandText = query;
            await command.ExecuteNonQueryAsync();
        }

        public static async Task<List<string>> GetFullTableNames(SqlConnection sqlConnection)
        {
            // Returns table names in {database}.{schema}.{table} format
            using var dbNameSelectCommand = sqlConnection.CreateCommand();
            dbNameSelectCommand.CommandText = "SELECT DB_NAME()";
            var dbName = (string?)await dbNameSelectCommand.ExecuteScalarAsync();

            if (dbName == null)
            {
                throw new InvalidOperationException("Could not get database name from sql server.");
            }

            string query = "SELECT sys.schemas.name as schema_name, sys.tables.name as table_name\n" +
                "FROM sys.tables\n" +
                "JOIN sys.schemas ON sys.schemas.schema_id = sys.tables.schema_id;";

            using var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = query;

            using var reader = await cmd.ExecuteReaderAsync();
            var list = new List<string>();

            while (await reader.ReadAsync())
            {
                list.Add($"{dbName}.{reader.GetString(0)}.{reader.GetString(1)}");
            }

            return list;
        }

        public static async Task<List<string>> GetPrimaryKeys(SqlConnection connection, string tableFullName)
        {
            var splitName = tableFullName.Split('.');

            if (splitName.Length != 3)
            {
                throw new InvalidOperationException("Table name must contain database.schema.tablename");
            }
            var db = splitName[0];
            var schema = splitName[1];
            var table = splitName[2];

            await connection.ChangeDatabaseAsync(db);

            var cmd = @"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
            AND TABLE_NAME = @tableName AND TABLE_SCHEMA = @schema ORDER BY ORDINAL_POSITION";

            using var command = connection.CreateCommand();
            command.CommandText = cmd;
            command.Parameters.Add(new SqlParameter("tableName", table));
            command.Parameters.Add(new SqlParameter("schema", schema));
            using var reader = await command.ExecuteReaderAsync();

            List<string> output = new List<string>();
            while (await reader.ReadAsync())
            {
                output.Add(reader.GetString(0));
            }
            return output;
        }

        public static async Task<List<int>> GetPrimaryKeyOrdinals(SqlConnection connection, string tableFullName)
        {
            var splitName = tableFullName.Split('.');

            if (splitName.Length != 3)
            {
                throw new InvalidOperationException("Table name must contain database.schema.tablename");
            }
            var db = splitName[0];
            var schema = splitName[1];
            var table = splitName[2];

            await connection.ChangeDatabaseAsync(db);

            var cmd = @"
            SELECT ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
            AND TABLE_NAME = @tableName AND TABLE_SCHEMA = @schema";

            using var command = connection.CreateCommand();
            command.CommandText = cmd;
            command.Parameters.Add(new SqlParameter("tableName", table));
            command.Parameters.Add(new SqlParameter("schema", schema));
            using var reader = await command.ExecuteReaderAsync();

            var output = new List<int>();
            while (await reader.ReadAsync())
            {
                output.Add(reader.GetInt32(0));
            }

            return output;
        }

        public static async Task<List<string>> GetColumns(SqlConnection connection, string tableFullName)
        {
            var splitName = tableFullName.Split('.');
            string? db = null;
            string? schema = null;
            string? table = null;
            if (splitName.Length == 3)
            {
                db = splitName[0];
                schema = splitName[1];
                table = splitName[2];
            }
            else if (splitName.Length == 2)
            {
                schema = splitName[0];
                table = splitName[1];
            }
            else if (splitName.Length == 1)
            {
                schema = "dbo";
                table = splitName[0];
            }
            else
            {
                throw new InvalidOperationException("Table name must be in one of the following formats: database.schema.tablename, schema.tablename, or tablename (defaulting to schema 'dbo').");
            }
            if (db != null)
            {
                await connection.ChangeDatabaseAsync(db);
            }
            
            var cmd = @"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = @tableName AND TABLE_SCHEMA = @schema
            ORDER BY ORDINAL_POSITION";
            using var command = connection.CreateCommand();
            command.CommandText = cmd;
            command.Parameters.Add(new SqlParameter("tableName", table));
            command.Parameters.Add(new SqlParameter("schema", schema));
            using var reader = await command.ExecuteReaderAsync();
            var output = new List<string>();
            while (await reader.ReadAsync())
            {
                output.Add(reader.GetString(0));
            }
            return output;
        }

        public static async Task<List<int>> GetColumnOrdinals(SqlConnection connection, string tableFullName)
        {

            var splitName = tableFullName.Split('.');

            if (splitName.Length != 3)
            {
                throw new InvalidOperationException("Table name must contain database.schema.tablename");
            }

            var db = splitName[0];
            var schema = splitName[1];
            var table = splitName[2];

            await connection.ChangeDatabaseAsync(db);

            var cmd = @"
            SELECT ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = @tableName AND TABLE_SCHEMA = @schema";

            using var command = connection.CreateCommand();
            command.CommandText = cmd;
            command.Parameters.Add(new SqlParameter("tableName", table));
            command.Parameters.Add(new SqlParameter("schema", schema));
            using var reader = await command.ExecuteReaderAsync();

            var output = new List<int>();
            while (await reader.ReadAsync())
            {
                output.Add(reader.GetInt32(0));
            }

            return output;
        }

        public static Action<DataRow, bool, EventBatchData, int> GetDataRowFromColumnsFunc(
            IReadOnlyCollection<DbColumn> columns,
            IReadOnlyList<int> primaryKeys,
            DataValueContainer dataValueContainer,
            bool includeOperation)
        {
            var columnList = columns.ToList();
            var mapFuncs = GetColumnsToDataTableValueMaps(columnList, dataValueContainer);
            var columnNames = columnList.Select(x => x.ColumnName).ToList();
            return (row, isDeleted, batch, index) =>
            {
                if (isDeleted)
                {
                    if (includeOperation)
                    {
                        row["md_operation"] = "D";
                    }
                    
                    // Set only primaryKeys
                    for (int i = 0; i < columnNames.Count; i++)
                    {
                        if (primaryKeys.Contains(i))
                        {
                            row[columnNames[i]] = mapFuncs[i](batch, index);
                        }
                        else
                        {
                            row[columnNames[i]] = DBNull.Value;
                        }
                    }
                    return;
                }

                if (includeOperation)
                {
                    row["md_operation"] = "I";
                }
                
                for (int i = 0; i < columnNames.Count; i++)
                {
                    var val = mapFuncs[i](batch, index);
                    if (val == null)
                    {
                        val = DBNull.Value;
                    }
                    else
                    {
                        row[columnNames[i]] = val;
                    }
                }
            };
        }

        public static Action<DataTable, bool, RowEvent> GetDataRowMapFunc(IReadOnlyCollection<DbColumn> columns, IReadOnlyList<int> primaryKeys)
        {
            var columnList = columns.ToList();
            var mapFuncs = GetDataTableValueMaps(columnList);
            var columnNames = columnList.Select(x => x.ColumnName).ToList();
            return (table, isDeleted, e) =>
            {
                var row = table.NewRow();

                if (isDeleted)
                {
                    row["md_operation"] = "D";
                    // Set only primaryKeys
                    for (int i = 0; i < columnNames.Count; i++)
                    {
                        if (primaryKeys.Contains(i))
                        {
                            row[columnNames[i]] = mapFuncs[i](e);
                        }
                        else
                        {
                            row[columnNames[i]] = DBNull.Value;
                        }
                    }
                    table.Rows.Add(row);
                    return;
                }

                row["md_operation"] = "I";
                for (int i = 0; i < columnNames.Count; i++)
                {
                    var val = mapFuncs[i](e);
                    if (val == null)
                    {
                        val = DBNull.Value;
                    }
                    else
                    {
                        row[columnNames[i]] = val;
                    }
                }
                table.Rows.Add(row);
            };
        }

        public static async Task<(List<RowEvent>, Dictionary<string, object>)> InitialSelect(
            ReadRelation readRelation,
            SqlConnection sqlConnection,
            List<string> primaryKeys,
            int batchSize,
            Dictionary<string, object> pkValues,
            Func<SqlDataReader, RowEvent> transformFunction,
            string? filter,
            CancellationToken cancellationToken)
        {
            using var command = sqlConnection.CreateCommand();

            if (pkValues.Count == 0)
            {
                command.CommandText = CreateInitialSelectStatement(readRelation, primaryKeys, batchSize, false, filter);
            }
            else
            {
                command.CommandText = CreateInitialSelectStatement(readRelation, primaryKeys, batchSize, true, filter);
                foreach (var pk in pkValues)
                {
                    command.Parameters.Add(new SqlParameter(pk.Key, pk.Value));
                }
            }

            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            List<int> primaryKeyOrdinals = new List<int>();
            foreach (var pk in primaryKeys)
            {
                primaryKeyOrdinals.Add(reader.GetOrdinal(pk));
            }
            List<RowEvent> outdata = new List<RowEvent>();
            while (await reader.ReadAsync())
            {
                outdata.Add(transformFunction(reader));

                pkValues.Clear();
                for (int i = 0; i < primaryKeyOrdinals.Count; i++)
                {
                    pkValues.Add(primaryKeys[i], reader.GetValue(primaryKeyOrdinals[i]));
                }
            }

            return (outdata, pkValues);
        }

        public static async Task<IEnumerable<int>> GetPartitionIds(SqlConnection connection, ReadRelation relation)
        {
            var schema = relation.NamedTable.Names[1];
            var tableName = relation.NamedTable.Names[2];

            var cmd = @"SELECT 
                    p.partition_number AS partition_id
                FROM sys.partitions p
                JOIN sys.tables t ON p.object_id = t.object_id
                JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.name = @tableName
                  AND i.index_id < 2
                  AND s.name = @schema
                ORDER BY partition_id;";

            using var command = connection.CreateCommand();
            command.CommandText = cmd;
            command.Parameters.Add(new SqlParameter("tableName", tableName));
            command.Parameters.Add(new SqlParameter("schema", schema));
            using var reader = await command.ExecuteReaderAsync();

            var output = new List<int>();
            while (await reader.ReadAsync())
            {
                output.Add(reader.GetInt32(0));
            }

            return output;
        }

        public static async Task<PartitionMetadata?> GetPartitionMetadata(SqlConnection connection, ReadRelation relation)
        {
            GetSchemaAndName(relation, out var schema, out var tableName);

            var cmd = @"SELECT
                    ps.name AS partition_scheme,
                    pf.name AS partition_function,
                    c.name AS partition_column,
                    t.name AS table_name,
                    s.name AS schema_name,
                  ic.partition_ordinal
                FROM sys.indexes i  
                JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
                JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                JOIN sys.tables t ON i.object_id = t.object_id
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE i.index_id < 2 -- clustered index or heap
                  AND t.name = @tableName
                  AND s.name = @schema
                  AND ic.partition_ordinal = 1;";

            using var command = connection.CreateCommand();
            command.CommandText = cmd;
            command.Parameters.Add(new SqlParameter("tableName", tableName));
            command.Parameters.Add(new SqlParameter("schema", schema));
            using var reader = await command.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {

                return new PartitionMetadata(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetString(4),
                    reader.GetByte(5));
            }

            return null;
        }

        public static List<Func<EventBatchData, int, object?>> GetColumnsToDataTableValueMaps(List<DbColumn> columns, DataValueContainer dataValueContainer)
        {
            List<Func<EventBatchData, int, object?>> output = new List<Func<EventBatchData, int, object?>>();
            for (int i = 0; i < columns.Count; i++)
            {
                output.Add(GetColumnToDataTableValueMap(columns[i], dataValueContainer, i));
            }
            return output;

        }

        public static List<Func<RowEvent, object?>> GetDataTableValueMaps(List<DbColumn> columns)
        {
            List<Func<RowEvent, object?>> output = new List<Func<RowEvent, object?>>();

            for (int i = 0; i < columns.Count; i++)
            {
                output.Add(GetDataTableValueMap(columns[i], i));
            }
            return output;
        }

        public static Func<EventBatchData, int, object?> GetColumnToDataTableValueMap(DbColumn dbColumn, DataValueContainer dataValueContainer, int columnIndex)
        {
            var t = dbColumn.DataType;

            if (t == null)
            {
                throw new FlowtideException("Could not get data type from SQL Server");
            }

            if (t.Equals(typeof(string)))
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    if (dataValueContainer.Type == ArrowTypeId.String)
                    {
                        return dataValueContainer.AsString.ToString();
                    }
                    // Json as fallback
                    using MemoryStream stream = new();
                    using Utf8JsonWriter utf8JsonWriter = new Utf8JsonWriter(stream);
                    batch.Columns[columnIndex].WriteToJson(in utf8JsonWriter, index);
                    return Encoding.UTF8.GetString(stream.ToArray());
                };
            }
            if (t.Equals(typeof(int)))
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    return dataValueContainer.AsLong;
                };
            }
            if (t.Equals(typeof(DateTime)))
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    if (dataValueContainer.Type == ArrowTypeId.Int64)
                    {
                        return DateTimeOffset.UnixEpoch.AddTicks(dataValueContainer.AsLong).DateTime;
                    }
                    if (dataValueContainer.Type == ArrowTypeId.Timestamp)
                    {
                        return dataValueContainer.AsTimestamp.ToDateTimeOffset().DateTime;
                    }
                    throw new NotSupportedException($"The data type {dataValueContainer.Type} cant be used as datetime");
                };
            }
            if (t.Equals(typeof(DateTimeOffset)))
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    if (dataValueContainer.Type == ArrowTypeId.Int64)
                    {
                        return DateTimeOffset.UnixEpoch.AddTicks(dataValueContainer.AsLong);
                    }
                    if (dataValueContainer.Type == ArrowTypeId.Timestamp)
                    {
                        return dataValueContainer.AsTimestamp.ToDateTimeOffset();
                    }
                    throw new NotSupportedException($"The data type {dataValueContainer.Type} cant be used as datetime");
                };
            }
            if (t.Equals(typeof(double))) // float
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    return dataValueContainer.AsDouble;
                };
            }
            if (t.Equals(typeof(float))) // real
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    return dataValueContainer.AsDouble;
                };
            }
            if (t.Equals(typeof(decimal)))
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    if (dataValueContainer.Type == ArrowTypeId.Double)
                    {
                        return (decimal)dataValueContainer.AsDouble;
                    }
                    if (dataValueContainer.Type == ArrowTypeId.Int64)
                    {
                        return (decimal)dataValueContainer.AsLong;
                    }
                    return dataValueContainer.AsDecimal;
                };
            }
            if (t.Equals(typeof(bool)))
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    if (dataValueContainer.Type == ArrowTypeId.Boolean)
                    {
                        return dataValueContainer.AsBool;
                    }
                    if (dataValueContainer.Type == ArrowTypeId.Int64)
                    {
                        return dataValueContainer.AsLong > 0;
                    }
                    if (dataValueContainer.Type == ArrowTypeId.String)
                    {
                        var stringVal = dataValueContainer.AsString.ToString();
                        if (stringVal.Equals("true", StringComparison.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                        if (stringVal.Equals("false", StringComparison.OrdinalIgnoreCase))
                        {
                            return false;
                        }
                    }
                    throw new NotSupportedException("Bool can only support bool or int values");
                };
            }
            if (t.Equals(typeof(short)))
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    return dataValueContainer.AsLong;
                };
            }
            if (t.Equals(typeof(long)))
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    return dataValueContainer.AsLong;
                };
            }
            if (t.Equals(typeof(Guid)))
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    if (dataValueContainer.Type == ArrowTypeId.String)
                    {
                        return new Guid(dataValueContainer.AsString.ToString());
                    }
                    else
                    {
                        var blob = dataValueContainer.AsBinary;
                        return new Guid(blob);
                    }
                };
            }
            if (t.Equals(typeof(byte))) // tiny int
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }

                    return dataValueContainer.AsLong;
                };
            }
            if (t.Equals(typeof(byte[]))) // binary
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }
                    return dataValueContainer.AsBinary.ToArray();
                };
            }
            if (t.Equals(typeof(TimeSpan))) // time(7)
            {
                return (batch, index) =>
                {
                    batch.Columns[columnIndex].GetValueAt(index, dataValueContainer, default);
                    if (dataValueContainer.IsNull)
                    {
                        return null;
                    }

                    return TimeSpan.FromTicks(dataValueContainer.AsLong);
                };
            }

            throw new NotImplementedException();
        }

        public static Func<RowEvent, object?> GetDataTableValueMap(DbColumn dbColumn, int index)
        {
            var t = dbColumn.DataType;

            if (t == null)
            {
                throw new FlowtideException("Could not get data type from SQL Server");
            }

            if (t.Equals(typeof(string)))
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    if (c.ValueType == FlexBuffers.Type.String)
                    {
                        return c.AsString;
                    }
                    return c.ToJson;
                };
            }
            if (t.Equals(typeof(int)))
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    return c.AsLong;
                };
            }
            if (t.Equals(typeof(DateTime)))
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    return DateTimeOffset.UnixEpoch.AddTicks(c.AsLong).DateTime;
                };
            }
            if (t.Equals(typeof(double))) // float
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }

                    return c.AsDouble;
                };
            }
            if (t.Equals(typeof(float))) // real
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }

                    return c.AsDouble;
                };
            }
            if (t.Equals(typeof(decimal)))
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    if (c.ValueType == FlexBuffers.Type.Float)
                    {
                        return (decimal)c.AsDouble;
                    }
                    if (c.ValueType == FlexBuffers.Type.Int)
                    {
                        return (decimal)c.AsLong;
                    }
                    return c.AsDecimal;
                };
            }
            if (t.Equals(typeof(bool)))
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    if (c.ValueType == FlexBuffers.Type.Bool)
                    {
                        return c.AsBool;
                    }
                    if (c.ValueType == FlexBuffers.Type.Int)
                    {
                        return c.AsLong > 0;
                    }
                    if (c.ValueType == FlexBuffers.Type.String)
                    {
                        var stringVal = c.AsString;
                        if (stringVal.Equals("true", StringComparison.OrdinalIgnoreCase))
                        {
                            return true;
                        }
                        if (stringVal.Equals("false", StringComparison.OrdinalIgnoreCase))
                        {
                            return false;
                        }
                    }
                    throw new NotSupportedException("Bool can only support bool or int values");
                };
            }
            if (t.Equals(typeof(short)))
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    return c.AsLong;
                };
            }
            if (t.Equals(typeof(long)))
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    return c.AsLong;
                };
            }
            if (t.Equals(typeof(Guid)))
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    if (c.ValueType == FlexBuffers.Type.String)
                    {
                        return new Guid(c.AsString);
                    }
                    else
                    {
                        var blob = c.AsBlob;
                        return new Guid(blob);
                    }
                };
            }
            if (t.Equals(typeof(byte))) // tiny int
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }

                    return c.AsLong;
                };
            }
            if (t.Equals(typeof(byte[]))) // binary
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }

                    return c.AsBlob.ToArray();
                };
            }
            if (t.Equals(typeof(TimeSpan))) // time(7)
            {
                return (e) =>
                {
                    var c = e.GetColumn(index);
                    if (c.IsNull)
                    {
                        return null;
                    }

                    return TimeSpan.FromTicks(c.AsLong);
                };
            }

            throw new NotImplementedException();
        }

        public static string? GetInitialLoadWhereStatement(List<string> primaryKeys)
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

        public static async Task<bool> IsView(SqlConnection sqlConnection, string tableFullName)
        {
            var splitName = tableFullName.Split('.');

            if (splitName.Length != 3)
            {
                throw new InvalidOperationException("Table name must contain database.schema.tablename");
            }

            var db = splitName[0];
            var schema = splitName[1];
            var table = splitName[2];

            await sqlConnection.ChangeDatabaseAsync(db);

            var cmd = @"
            SELECT type_desc 
            FROM Sys.objects o
            JOIN Sys.schemas s ON s.schema_id = o.schema_id
            WHERE o.name = @name AND s.name = @schema";

            using var command = sqlConnection.CreateCommand();
            command.CommandText = cmd;
            command.Parameters.AddWithValue("name", table);
            command.Parameters.AddWithValue("schema", schema);

            var value = await command.ExecuteScalarAsync();
            return (string?)value == "VIEW";
        }

        public static async Task<int> GetRowCount(SqlConnection sqlConnection, string tableFullName)
        {
            var splitName = tableFullName.Split('.');

            if (splitName.Length != 3)
            {
                throw new InvalidOperationException("Table name must contain database.schema.tablename");
            }

            var db = splitName[0];
            var schema = splitName[1];
            var table = splitName[2];

            using var command = sqlConnection.CreateCommand();
            command.CommandText = $@"SELECT COUNT(*) FROM [{db}].[{schema}].[{table}]";
            var count = await command.ExecuteScalarAsync();
            return (int?)count ?? -1;
        }

        private static void GetSchemaAndName(ReadRelation relation, out string schema, out string tableName)
        {
            if (relation.NamedTable.Names.Count > 3)
            {
                throw new InvalidOperationException("Incorrect number of sql table name parts");
            }
            else if (relation.NamedTable.Names.Count == 3)
            {
                schema = relation.NamedTable.Names[1];
                tableName = relation.NamedTable.Names[2];
            }
            else if (relation.NamedTable.Names.Count == 2)
            {
                schema = relation.NamedTable.Names[0];
                tableName = relation.NamedTable.Names[1];
            }
            else
            {
                schema = "dbo";
                tableName = relation.NamedTable.Names[0];
            }
        }
    }
}

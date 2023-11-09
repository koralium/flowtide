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

using FlowtideDotNet.Core;
using FlexBuffers;
using Microsoft.Data.SqlClient;
using FlowtideDotNet.Substrait.Relations;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Text;
using Microsoft.VisualBasic;

namespace FlowtideDotNet.Substrait.Tests.SqlServer
{
    internal static class SqlServerUtils
    {
        public static Func<SqlDataReader, StreamEvent> GetStreamEventCreator(ReadRelation readRelation)
        {
            List<Action<SqlDataReader, IFlexBufferVectorBuilder>> columns = new List<Action<SqlDataReader, IFlexBufferVectorBuilder>>();
            for (int i = 0; i < readRelation.BaseSchema.Struct.Types.Count; i++)
            {
                var type = readRelation.BaseSchema.Struct.Types[i];

                int index = i;
                switch (type.Type)
                {
                    case Type.SubstraitType.String:
                        columns.Add((reader, builder) =>
                        {
                            builder.Add(reader.GetString(index));
                        });
                        break;
                    case Type.SubstraitType.Int32:
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
                    case Type.SubstraitType.Int64:
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
                    case Type.SubstraitType.Date:
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }
                            var dateTime = reader.GetDateTime(index);
                            var unixTime = ((DateTimeOffset)dateTime).ToUnixTimeMilliseconds() * 1000;
                            builder.Add(unixTime);
                        });
                        break;
                    default:
                        throw new NotImplementedException($"{type.Type}");
                }
            }
            return (reader) =>
            {
                return StreamEvent.Create(1, 0, builder =>
                {
                    for (int i = 0; i < columns.Count; i++)
                    {
                        columns[i](reader, builder);
                    }
                });
            };
        }

        public static Func<SqlDataReader, StreamEvent> GetStreamEventCreator(ReadOnlyCollection<DbColumn> dbColumns)
        {
            List<Action<SqlDataReader, IFlexBufferVectorBuilder>> columns = new List<Action<SqlDataReader, IFlexBufferVectorBuilder>>();
            for (int i = 0; i < dbColumns.Count; i++)
            {
                var column = dbColumns[i];

                int index = i;
                switch (column.DataTypeName)
                {
                    case "char":
                    case "varchar":
                    case "nvarchar":
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
                    case "decimal":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }

                            builder.Add((double)reader.GetDecimal(index));
                        });
                        break;
                    case "date":
                    case "datetime2":
                        columns.Add((reader, builder) =>
                        {
                            if (reader.IsDBNull(index))
                            {
                                builder.AddNull();
                                return;
                            }
                            var dateTime = reader.GetDateTime(index);
                            var unixTime = ((DateTimeOffset)dateTime).ToUnixTimeMilliseconds() * 1000;
                            builder.Add(unixTime);
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
                    default:
                        throw new NotImplementedException(column.DataTypeName);
                }
            }
            return (reader) =>
            {
                return StreamEvent.Create(1, 0, builder =>
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
            stringBuilder.Append(readRelation.NamedTable.DotSeperated);

            return stringBuilder.ToString();
        }

        public static string CreateInitialSelectStatement(ReadRelation readRelation)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append("SELECT ");

            stringBuilder.Append(string.Join(", ", readRelation.BaseSchema.Names.Select(x => $"[{x}]")));

            stringBuilder.Append(" FROM ");
            stringBuilder.Append(readRelation.NamedTable.DotSeperated);

            return stringBuilder.ToString();
        }

        public static string CreateInitialSelectStatement(ReadRelation readRelation, List<string> primaryKeys, int batchSize, bool includePkParameters, string? filter)
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.Append("SELECT ");

            stringBuilder.Append(string.Join(", ", readRelation.BaseSchema.Names.Select(x => $"[{x}]")));

            stringBuilder.Append(" FROM ");
            stringBuilder.AppendLine(readRelation.NamedTable.DotSeperated);

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
            stringBuilder.Append(writeRelation.NamedObject.DotSeperated);

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
            stringBuilder.Append(readRelation.NamedTable.DotSeperated);
            stringBuilder.AppendLine(", @ChangeVersion) as c");
            stringBuilder.Append("LEFT JOIN ");
            stringBuilder.Append(readRelation.NamedTable.DotSeperated);
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
                pkComparisons.Add($"src.{pkColumn} = tgt.{pkColumn}");
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

                updateNotEquals.Add($"src.{col.ColumnName} != tgt.{col.ColumnName}");
            }

            // Add update statement
            // This statement only updates rows if there is a difference
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

                updateSets.Add($"tgt.{col.ColumnName} = src.{col.ColumnName}");
            }
            stringBuilder.AppendLine(string.Join(", ", updateSets));

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

                stringBuilder.AppendLine($"INSERT ({string.Join(", ", columnNames)})");
            stringBuilder.AppendLine($"VALUES ({string.Join(", ", columnNames)});");

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

        public static async Task<long> GetLatestChangeVersion(SqlConnection sqlConnection)
        {
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
                if (columnType == "varchar" || columnType == "nvarchar" || columnType == "char")
                {
                    columnType = $"{columnType}({column.ColumnSize})";
                }
                if (columnType == "decimal")
                {
                    columnType = $"{columnType}({column.NumericPrecision}, {column.NumericScale})";
                }
                columnsData.Add($"{column.ColumnName} {columnType}");
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
            AND TABLE_NAME = @tableName AND TABLE_SCHEMA = @schema";

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

        public static Action<DataTable, bool, StreamEvent> GetDataRowMapFunc(IReadOnlyCollection<DbColumn> columns, IReadOnlyList<int> primaryKeys)
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
                            row[columnNames[i]] = null;
                        }
                    }
                    table.Rows.Add(row);
                    return;
                }

                row["md_operation"] = "I";
                for (int i = 0; i < columnNames.Count; i++)
                {
                    row[columnNames[i]] = mapFuncs[i](e);
                }
                table.Rows.Add(row);
            };
        }

        public static async Task<(List<StreamEvent>, Dictionary<string, object>)> InitialSelect(
            ReadRelation readRelation, 
            SqlConnection sqlConnection, 
            List<string> primaryKeys, 
            int batchSize, 
            Dictionary<string, object> pkValues,
            Func<SqlDataReader, StreamEvent> transformFunction,
            string? filter)
        {
            using var command = sqlConnection.CreateCommand();

            if (pkValues.Count == 0)
            {
                command.CommandText = CreateInitialSelectStatement(readRelation, primaryKeys, batchSize, false, filter);
            }
            else
            {
                command.CommandText = CreateInitialSelectStatement(readRelation, primaryKeys, batchSize, true, filter);
                foreach(var pk in pkValues)
                {
                    command.Parameters.Add(new SqlParameter(pk.Key, pk.Value));
                }
            }
            
            using var reader = await command.ExecuteReaderAsync();

            List<int> primaryKeyOrdinals = new List<int>();
            foreach (var pk in primaryKeys)
            {
                primaryKeyOrdinals.Add(reader.GetOrdinal(pk));
            }
            List<StreamEvent> outdata = new List<StreamEvent>();
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

        public static List<Func<StreamEvent, object>> GetDataTableValueMaps(List<DbColumn> columns)
        {
            List<Func<StreamEvent, object>> output = new List<Func<StreamEvent, object>>();

            for (int i = 0; i < columns.Count; i++)
            {
                output.Add(GetDataTableValueMap(columns[i], i));
            }
            return output;
        }

        public static Func<StreamEvent, object?> GetDataTableValueMap(DbColumn dbColumn, int index)
        {
            var t = dbColumn.DataType;
            
            if (t.Equals(typeof(string)))
            {
                return (e) =>
                {
                    var c = e.Vector.Get(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    return c.AsString;
                };
            }
            if (t.Equals(typeof(int)))
            {
                return (e) =>
                {
                    var c = e.Vector.Get(index);
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
                    var c = e.Vector.Get(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    return DateTimeOffset.FromUnixTimeMilliseconds(c.AsLong / 1000);
                };
            }
            if (t.Equals(typeof(decimal)))
            {
                return (e) =>
                {
                    var c = e.Vector.Get(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    return c.AsDouble;
                };
            }
            if (t.Equals(typeof(bool)))
            {
                return (e) =>
                {
                    var c = e.Vector.Get(index);
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
                    var c = e.Vector.Get(index);
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
                    var c = e.Vector.Get(index);
                    if (c.IsNull)
                    {
                        return null;
                    }
                    return c.AsLong;
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
    }
}

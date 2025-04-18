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

using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Data.SqlClient;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.SqlServer
{
    public class SqlServerTableProvider : ITableProvider
    {
        private readonly Func<string> connectionStringFunc;
        private readonly bool mustBeInConnectionStringDb;

        public SqlServerTableProvider(Func<string> connectionStringFunc, bool mustBeInConnectionStringDb)
        {
            this.connectionStringFunc = connectionStringFunc;
            this.mustBeInConnectionStringDb = mustBeInConnectionStringDb;
        }

        public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            var fullName = string.Join(".", tableName);
            var tableNameSplitted = fullName.Split(".");
            string? schema = "dbo";
            string? name = null;
            string? tableCatalog = default;
            if (tableNameSplitted.Length == 3)
            {
                tableCatalog = tableNameSplitted[0];
                if (tableNameSplitted[1].Length > 0)
                {
                    schema = tableNameSplitted[1];
                }
                name = tableNameSplitted[2];
            }
            else
            {
                tableMetadata = default;
                return false;
            }

            var connStr = connectionStringFunc();

            if (mustBeInConnectionStringDb)
            {
                var connStrBuilder = new SqlConnectionStringBuilder(connStr);

                if (tableCatalog != null && connStrBuilder.InitialCatalog != tableCatalog)
                {
                    tableMetadata = default;
                    return false;
                }
            }

            using var conn = new SqlConnection(connStr);
            conn.Open();
            try
            {
                conn.ChangeDatabase(tableCatalog);
            }
            catch (DbException dbException)
            {
                if (dbException.Message.Contains("not able to access the database"))
                {
                    tableMetadata = default;
                    return false;
                }
            }

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tableName AND TABLE_SCHEMA = @tableSchema  AND TABLE_CATALOG = @catalog";
            cmd.Parameters.Add(new SqlParameter("@tableName", name));
            cmd.Parameters.Add(new SqlParameter("@tableSchema", schema));
            cmd.Parameters.Add(new SqlParameter("@catalog", tableCatalog));

            using var reader = cmd.ExecuteReader();
            List<string> columnOutput = new List<string>();
            List<SubstraitBaseType> columnTypes = new List<SubstraitBaseType>();

            var columnNameOrdinal = reader.GetOrdinal("COLUMN_NAME");
            var dataTypeOrdinal = reader.GetOrdinal("DATA_TYPE");
            while (reader.Read())
            {
                columnOutput.Add(reader.GetString(columnNameOrdinal));
                var dataTypeName = reader.GetString(dataTypeOrdinal);
                columnTypes.Add(SqlServerUtils.GetSubstraitType(dataTypeName));
            }

            if (columnOutput.Count == 0)
            {
                tableMetadata = default;
                return false;
            }
            tableMetadata = new TableMetadata(fullName, new NamedStruct()
            {
                Names = columnOutput,
                Struct = new Struct()
                {
                    Types = columnTypes
                }
            });
            return true;
        }
    }
}

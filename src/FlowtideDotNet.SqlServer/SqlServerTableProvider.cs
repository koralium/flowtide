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
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.SqlServer
{
    public class SqlServerTableProvider : ITableProvider
    {
        private readonly Func<string> connectionStringFunc;

        public SqlServerTableProvider(Func<string> connectionStringFunc)
        {
            this.connectionStringFunc = connectionStringFunc;
        }
        public bool TryGetTableInformation(string tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            var tableNameSplitted = tableName.Split(".");
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

            using var conn = new SqlConnection(connectionStringFunc());
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tableName AND TABLE_SCHEMA = @tableSchema  AND TABLE_CATALOG = @catalog";
            cmd.Parameters.Add(new SqlParameter("@tableName", name));
            cmd.Parameters.Add(new SqlParameter("@tableSchema", schema));
            cmd.Parameters.Add(new SqlParameter("@catalog", tableCatalog));

            using var reader = cmd.ExecuteReader();
            List<string> columnOutput = new List<string>();
            
            var columnNameOrdinal = reader.GetOrdinal("COLUMN_NAME");
            while (reader.Read())
            {
                columnOutput.Add(reader.GetString(columnNameOrdinal));
            }

            if (columnOutput.Count == 0)
            {
                tableMetadata = default;
                return false;
            }
            tableMetadata = new TableMetadata(tableName, columnOutput);
            return true;
        }
    }
}

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

using FlowtideDotNet.Connector.PostgreSQL.Internal;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using Npgsql;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Connector.PostgreSQL
{
    public class PostgresTableProvider : ITableProvider
    {
        private readonly Func<string> _connectionStringFunc;

        public PostgresTableProvider(Func<string> connectionStringFunc)
        {
            _connectionStringFunc = connectionStringFunc;
        }

        public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            string schema;
            string table;
            try
            {
                (schema, table) = PostgresUtils.ResolveSchemaAndTable(tableName);
            }
            catch
            {
                tableMetadata = default;
                return false;
            }

            using var connection = new NpgsqlConnection(_connectionStringFunc());
            connection.Open();

            var columns = PostgresUtils.GetColumns(connection, schema, table, default).GetAwaiter().GetResult();
            if (columns.Count == 0)
            {
                tableMetadata = default;
                return false;
            }

            var names = new List<string>();
            var types = new List<SubstraitBaseType>();
            foreach (var (name, udtName) in columns)
            {
                names.Add(name);
                types.Add(PostgresUtils.GetSubstraitType(udtName));
            }

            var fullName = string.Join(".", tableName);
            tableMetadata = new TableMetadata(fullName, new NamedStruct()
            {
                Names = names,
                Struct = new Struct()
                {
                    Types = types
                }
            });
            return true;
        }

        public bool TryHandleTableFunction(IReadOnlyList<string> tableName, TableProviderTableFunctionArguments sqlTableFunction, [NotNullWhen(true)] out TableProviderTableFunctionResult? relation)
        {
            relation = null;
            return false;
        }
    }
}

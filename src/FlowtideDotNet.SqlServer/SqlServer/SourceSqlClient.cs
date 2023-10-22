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

using Microsoft.Data.SqlClient;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Substrait.Tests.SqlServer
{
    internal class SourceSqlClient
    {
        private readonly string connectionString;
        private SqlConnection _sqlConnection;
        private bool _connected;

        public SourceSqlClient(string connectionString, ReadRelation readRelation)
        {
            this.connectionString = connectionString;
            _sqlConnection = new SqlConnection(connectionString);
            _connected = false;

            
        }

        public async Task OpenAsync()
        {
            await _sqlConnection.OpenAsync();
            _connected = true;
        }

        public async Task<bool> IsChangeTrackingEnabledAsync(string schema, string table)
        {
            if (!_connected)
            {
                throw new InvalidOperationException("Not connected");
            }
            using var cmd = _sqlConnection.CreateCommand();
            cmd.CommandText = @"
            SELECT sys.schemas.name as schema_name, sys.tables.name as table_name
            FROM sys.change_tracking_tables
            JOIN sys.tables ON sys.tables.object_id = sys.change_tracking_tables.object_id
            JOIN sys.schemas ON sys.schemas.schema_id = sys.tables.schema_id
            WHERE sys.tables.name = @TABLE_NAME AND sys.schemas.name = @SCHEMA_NAME;
            ";
            cmd.Parameters.Add(new SqlParameter("@TABLE_NAME", table));
            cmd.Parameters.Add(new SqlParameter("@SCHEMA_NAME", schema));

            using var reader = await cmd.ExecuteReaderAsync();

            return await reader.ReadAsync();
        }

        public async Task<long> GetLatestChangeTrackingVersion()
        {
            if (!_connected)
            {
                throw new InvalidOperationException("Not connected");
            }
            using var cmd = _sqlConnection.CreateCommand();
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
    }
}

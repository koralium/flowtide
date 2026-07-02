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
using Testcontainers.MsSql;

namespace FlowtideDotNet.SqlServer.Tests.e2e
{
    public class SqlServerEndToEndFixture : IAsyncLifetime
    {
        MsSqlContainer _msSqlContainer;
        public SqlServerEndToEndFixture()
        {
            _msSqlContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
                .Build();
        }

        public string ConnectionString
        {
            get
            {
                var connectionStringBuilder = new SqlConnectionStringBuilder(_msSqlContainer.GetConnectionString());
                connectionStringBuilder.InitialCatalog = "test-db";
                var tpchConnectionString = connectionStringBuilder.ToString();
                return tpchConnectionString;

            }
        }

        public async Task DisposeAsync()
        {
            await _msSqlContainer.DisposeAsync();
        }

        public async Task InitializeAsync()
        {
            await _msSqlContainer.StartAsync();

            await RunCommand("CREATE DATABASE [test-db]", false);
            await RunCommand("ALTER DATABASE [test-db] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)");
        }

        public async Task RunCommand(string command, bool changeDatabase = true)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_msSqlContainer.GetConnectionString()))
            {
                await sqlConnection.OpenAsync();
                if (changeDatabase)
                {
                    await sqlConnection.ChangeDatabaseAsync("test-db");
                }
                using var cmd = sqlConnection.CreateCommand();
                cmd.CommandText = command;
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task<T> ExecuteReader<T>(string command, Func<SqlDataReader, T> func)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_msSqlContainer.GetConnectionString()))
            {
                await sqlConnection.OpenAsync();
                using var cmd = sqlConnection.CreateCommand();
                cmd.CommandText = command;
                return func(await cmd.ExecuteReaderAsync());
            }
        }
    }
}

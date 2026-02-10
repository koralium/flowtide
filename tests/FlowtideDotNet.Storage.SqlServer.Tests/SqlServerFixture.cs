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

namespace FlowtideDotNet.Storage.SqlServer.Tests
{
    public class SqlServerFixture : IAsyncLifetime
    {
        private readonly MsSqlContainer _msSqlContainer;
        public SqlServerFixture()
        {
            _msSqlContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04").Build();
        }

        public string ConnectionString => _msSqlContainer.GetConnectionString();

        public async Task DisposeAsync()
        {
            await _msSqlContainer.DisposeAsync();
        }

        private async Task RunCommand(string command)
        {
            using (SqlConnection sqlConnection = new SqlConnection(_msSqlContainer.GetConnectionString()))
            {
                await sqlConnection.OpenAsync();
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

        public async Task StopAsync()
        {
            await _msSqlContainer.StopAsync();
        }

        public async Task StartAsync()
        {
            await _msSqlContainer.StartAsync();
        }

        public async Task InitializeAsync()
        {
            await _msSqlContainer.StartAsync();

            await RunCommand("CREATE DATABASE StorageTestDb");
            await RunCommand(await File.ReadAllTextAsync("create_tables.sql"));
            await RunCommand("CREATE SCHEMA [test]");
            await RunCommand(await File.ReadAllTextAsync("create_alt_schema_tables.sql"));
        }
    }
}

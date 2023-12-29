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

using FlowtideDotNet.Core.Tests.SmokeTests;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;

namespace FlowtideDotNet.SqlServer.Tests.Acceptance
{
    public class SqlServerFixture : IAsyncLifetime
    {
        MsSqlContainer _msSqlContainer;
        private TpchDbContext dbContext;
        public SqlServerFixture()
        {
            _msSqlContainer = new MsSqlBuilder()
                .Build();
        }

        public string ConnectionString
        {
            get
            {
                var connectionStringBuilder = new SqlConnectionStringBuilder(_msSqlContainer.GetConnectionString());
                connectionStringBuilder.InitialCatalog = "tpch";
                var tpchConnectionString = connectionStringBuilder.ToString();
                return  tpchConnectionString;

            }
        }

        public TpchDbContext DbContext
        {
            get
            {
                var optionsBuilder = new DbContextOptionsBuilder<TpchDbContext>();
                optionsBuilder.UseSqlServer(ConnectionString, b => b.MigrationsAssembly("FlowtideDotNet.SqlServer.Tests"));
                dbContext = new TpchDbContext(optionsBuilder.Options);
                return dbContext;
            }
        }

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
            
            var optionsBuilder = new DbContextOptionsBuilder<TpchDbContext>();

            var connstr = _msSqlContainer.GetConnectionString();

            await RunCommand("CREATE DATABASE tpch");
            await RunCommand("ALTER DATABASE tpch SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)");

            var connectionStringBuilder = new SqlConnectionStringBuilder(connstr);
            connectionStringBuilder.InitialCatalog = "tpch";
            var tpchConnectionString = connectionStringBuilder.ToString();

            optionsBuilder.UseSqlServer(tpchConnectionString, b => b.MigrationsAssembly("FlowtideDotNet.SqlServer.Tests"));
            dbContext = new TpchDbContext(optionsBuilder.Options);
            
            await dbContext.Database.MigrateAsync();
            await RunCommand("ALTER TABLE tpch.dbo.lineitems ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await RunCommand("ALTER TABLE tpch.dbo.orders ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await RunCommand("ALTER TABLE tpch.dbo.shipmodes ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");

            await RunCommand("CREATE TABLE tpch.dbo.notracking (id int PRIMARY KEY)");

            //await TpchData.InsertIntoDbContext(dbContext);
        }
    }
}

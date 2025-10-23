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

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using MySql.Data.MySqlClient;

namespace FlowtideDotNet.Connector.StarRocks.Tests
{
    public class StarRocksFixture : IAsyncLifetime
    {
        private IContainer? container;
        private string password = "";
        public async Task DisposeAsync()
        {
            if (container != null)
            {
                await container.DisposeAsync();
            }

        }

        private sealed class WaitUntil : IWaitUntil
        {
            private static readonly IEnumerable<string> Pattern = ["Enjoy the journey to StarRocks blazing-fast lake-house engine!"];

            public async Task<bool> UntilAsync(IContainer container)
            {
                string item = (await container.GetLogsAsync(default(DateTime), default(DateTime), timestampsEnabled: false).ConfigureAwait(continueOnCapturedContext: false)).Item1;
                return Pattern.Any(new Func<string, bool>(item.Contains));
            }
        }

        public async Task InitializeAsync()
        {
            container = new ContainerBuilder()
                .WithImage("starrocks/allin1-ubuntu")
                .WithPortBinding(9030, true)
                .WithPortBinding(8030, true)
                .WithPortBinding(8040, true)
                .WithWaitStrategy(Wait.ForUnixContainer().AddCustomWaitStrategy(new WaitUntil()))
                .Build();
            await container.StartAsync();

            await RunQuery(@"
                CREATE DATABASE test
                DEFAULT CHARACTER SET = 'utf8mb4';
            ");

            await RunQuery(@"
                CREATE TABLE testtable (  
                    id int NOT NULL,
                    create_time DATETIME COMMENT 'Create Time',
                    name VARCHAR(255)
                )
                PRIMARY KEY (id)
                 COMMENT '';
            ", "test");
        }

        public string Uri => GetUri();

        public string BackendUrl => GetBackendUri();

        public string GetUri()
        {
            if (container == null)
            {
                throw new ArgumentNullException(nameof(container));
            }
            return $"http://localhost:{container.GetMappedPublicPort(8030)}";
        }

        public string GetBackendUri()
        {
            if (container == null)
            {
                throw new ArgumentNullException(nameof(container));
            }
            return $"http://localhost:{container.GetMappedPublicPort(8040)}";
        }


        public MySqlConnection GetMySqlConnection(string database)
        {
            if (container == null)
            {
                throw new ArgumentNullException(nameof(container));
            }

            var connStrBuilder = new MySql.Data.MySqlClient.MySqlConnectionStringBuilder();
            connStrBuilder.Server = "localhost";
            connStrBuilder.Port = container.GetMappedPublicPort(9030);
            connStrBuilder.UserID = "root";
            connStrBuilder.Password = password;
            connStrBuilder.Database = database;

            var connectionString = connStrBuilder.ToString();

            return new MySql.Data.MySqlClient.MySqlConnection(connectionString);
        }

        public async Task RunQuery(string query, string? database = default)
        {
            if (container == null)
            {
                throw new ArgumentNullException(nameof(container));
            }
            var connStrBuilder = new MySql.Data.MySqlClient.MySqlConnectionStringBuilder();
            connStrBuilder.Server = "localhost";
            connStrBuilder.Port = container.GetMappedPublicPort(9030);
            connStrBuilder.UserID = "root";
            connStrBuilder.Password = password;
            connStrBuilder.Database = database;
           
            var connectionString = connStrBuilder.ToString();

            using var connection = new MySql.Data.MySqlClient.MySqlConnection(connectionString);
            await connection.OpenAsync();
            var cmd = connection.CreateCommand();
            cmd.CommandText = query;
            await cmd.ExecuteNonQueryAsync();
        }

        public async Task<int> GetTableCount(string database, string tableName)
        {
            using var connection = GetMySqlConnection(database);
            await connection.OpenAsync();
            var cmd = connection.CreateCommand();
            while (true)
            {
                cmd.CommandText = $"SELECT COUNT(*) FROM {database}.{tableName}";
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    var count = reader.GetInt32(0);
                    return count;
                }

            }
        }
    }
}

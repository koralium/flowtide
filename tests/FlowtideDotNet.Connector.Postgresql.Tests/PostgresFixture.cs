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
using DotNet.Testcontainers.Containers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Postgresql.Tests
{
    public class PostgresFixture : IAsyncLifetime
    {
        private readonly IContainer _postgresContainer;

        public PostgresFixture()
        {
            _postgresContainer = new DotNet.Testcontainers.Builders.ContainerBuilder()
                .WithImage("postgres:16")
                .WithNetworkAliases("postgres")
                .WithPortBinding(5432, 5432)
                .WithEnvironment("POSTGRES_USER", "postgres")
                .WithEnvironment("POSTGRES_PASSWORD", "password")
                .WithEnvironment("POSTGRES_DB", "testdb")
                .WithCommand("-c", "fsync=off")
                .WithCommand("-c", "full_page_writes=off")
                .WithCommand("-c", "synchronous_commit=off")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("database system is ready to accept connections"))
                .Build();
        }

        public string GetConnectionString()
        {
            return $"Host=localhost;Port=5432;Database=testdb;Username=postgres;Password=password;";
        }

        public async Task DisposeAsync()
        {
            await _postgresContainer.DisposeAsync();
        }

        public async Task InitializeAsync()
        {
            await _postgresContainer.StartAsync();
        }

        public async Task ExecuteNonQueryCommand(string command)
        {
            using var conn = new Npgsql.NpgsqlConnection(GetConnectionString());
            await conn.OpenAsync();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = command;

            await cmd.ExecuteNonQueryAsync();
        }
    }
}

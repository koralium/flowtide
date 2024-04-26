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

using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Substrait.Protobuf.RelCommon.Types;

namespace FlowtideDotNet.Connector.Kafka.Tests.Debezium.Postgres
{
    public class PostgresDebeziumTests : IClassFixture<DebeziumPostgresFixture>
    {
        private readonly DebeziumPostgresFixture debeziumPostgresFixture;

        public PostgresDebeziumTests(DebeziumPostgresFixture debeziumPostgresFixture)
        {
            this.debeziumPostgresFixture = debeziumPostgresFixture;
        }

        [Fact]
        public async Task TestPostgres()
        {
            await debeziumPostgresFixture.ListenOnTables("inventory", "dbserver1");
            var stream = new DebeziumPostgresStream(debeziumPostgresFixture, "TestPostgres");

            await stream.StartStream(@"
                CREATE TABLE dbserver1.inventory.customers (
                  id,
                  first_name,
                  last_name,
                  email
                );

                INSERT INTO outputtable
                SELECT 
                  first_name,
                  last_name, 
                  email
                FROM dbserver1.inventory.customers;
            ");

            await stream.WaitForUpdate();
            var connectionString = "Host=localhost;Username=postgres;Password=postgres;Database=postgres";
            await using var dataSource = NpgsqlDataSource.Create(connectionString);

            await using (var cmd = dataSource.CreateCommand(@"
                INSERT INTO inventory.customers(first_name, last_name, email)
                VALUES
                ('John', 'Doe', 'john.doe@test.com'),
                ('Jane', 'Doe', 'jane.doe@test.com')
                "))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[]
            {
                new { first_name = "Anne", last_name = "Kretchmar", email = "annek@noanswer.org"},
                new { first_name = "Edward", last_name = "Walker", email = "ed@walker.com"},
                new { first_name = "George", last_name = "Bailey", email = "gbailey@foobar.com"},
                new { first_name = "Jane", last_name = "Doe", email = "jane.doe@test.com"},
                new { first_name = "John", last_name = "Doe", email = "john.doe@test.com"},
                new { first_name = "Sally", last_name = "Thomas", email = "sally.thomas@acme.com"}
            });

            var actual2 = stream.GetActualRowsAsVectors();

            await using (var cmd = dataSource.CreateCommand(@"
                UPDATE inventory.customers SET first_name = 'Test' WHERE last_name = 'Doe'
                "))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[]
            {
                new { first_name = "Anne", last_name = "Kretchmar", email = "annek@noanswer.org"},
                new { first_name = "Edward", last_name = "Walker", email = "ed@walker.com"},
                new { first_name = "George", last_name = "Bailey", email = "gbailey@foobar.com"},
                new { first_name = "Test", last_name = "Doe", email = "jane.doe@test.com"},
                new { first_name = "Test", last_name = "Doe", email = "john.doe@test.com"},
                new { first_name = "Sally", last_name = "Thomas", email = "sally.thomas@acme.com"}
            });
            var actual3 = stream.GetActualRowsAsVectors();

            await using (var cmd = dataSource.CreateCommand(@"
                DELETE FROM inventory.customers WHERE last_name = 'Doe'
                "))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(new[]
            {
                new { first_name = "Anne", last_name = "Kretchmar", email = "annek@noanswer.org"},
                new { first_name = "Edward", last_name = "Walker", email = "ed@walker.com"},
                new { first_name = "George", last_name = "Bailey", email = "gbailey@foobar.com"},
                new { first_name = "Sally", last_name = "Thomas", email = "sally.thomas@acme.com"}
            });
        }
    }
}

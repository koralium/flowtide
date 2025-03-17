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

using AspireSamples.DataMigration;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Projects;

namespace SqlServerToSqlServerAspire.SqlServerToSqlServer
{
    internal static class SqlServerToSqlServerSample
    {
        public static void RunSample(IDistributedApplicationBuilder builder)
        {
            var blobs = builder.AddAzureStorage("storage")
                .RunAsEmulator()
                .AddBlobs("blobs");

            var sqldb1 = builder.AddSqlServer("sqldb1");

            var sqldb2 = builder.AddSqlServer("sqldb2");

            builder.Eventing.Subscribe<BeforeResourceStartedEvent>(sqldb1.Resource, (r, token) =>
            {
                return Task.CompletedTask;
            });

            DataGenerator dataGenerator = new DataGenerator();

            var dataInsert = DataInsertResource.AddDataInsert(builder, "data-insert",
                async (logger, statusUpdate, token) =>
                {
                    // Create destination tables
                    var connectionStringDb2 = await sqldb2.Resource.GetConnectionStringAsync();
                    SqlConnection sqlConnectionDb2 = new SqlConnection(connectionStringDb2);
                    await sqlConnectionDb2.OpenAsync();

                    var createDestDbCmd = @"
                    IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'test')
                    BEGIN
                      CREATE DATABASE test;
                    END";

                    using SqlCommand createDestDbCommand = new SqlCommand(createDestDbCmd, sqlConnectionDb2);
                    await createDestDbCommand.ExecuteNonQueryAsync();

                    await sqlConnectionDb2.ChangeDatabaseAsync("test");

                    var createDestTableCmd = @"
                    IF OBJECT_ID(N'destinationtable', N'U') IS NULL
                    BEGIN
                        CREATE TABLE destinationtable
                        (
                            OrderKey 	 INT PRIMARY KEY NOT NULL,
                            OrderDate    DATETIME2 NOT NULL,
                            FirstName   VARCHAR(255) NULL,
                            LastName  VARCHAR(255) NULL,
                        );
                    END
                    ";

                    using SqlCommand createDestTableCommand = new SqlCommand(createDestTableCmd, sqlConnectionDb2);
                    await createDestTableCommand.ExecuteNonQueryAsync();

                    await sqlConnectionDb2.CloseAsync();

                    // Initial data insert and table creation
                    var connectionString = await sqldb1.Resource.GetConnectionStringAsync();
                    ServiceCollection services = new ServiceCollection();
                    services.AddDbContext<SampleDbContext>(opt =>
                    {
                        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
                        builder.InitialCatalog = "test";
                        opt.UseSqlServer(builder.ToString());
                    });
                    var provider = services.BuildServiceProvider();

                    var scope = provider.CreateScope();
                    var ctx = scope.ServiceProvider.GetRequiredService<SampleDbContext>();
                    var mig = AccessorExtensions.GetService<IMigrationsSqlGenerator>(ctx);

                    var migrationOperations = ctx.GetMigrationOperations();


                    SqlConnection sqlConnection = new SqlConnection(connectionString);
                    await sqlConnection.OpenAsync();

                    // Create database test
                    SqlCommand sqlCommand = new SqlCommand(@"
                    IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'test')
                    BEGIN
                      CREATE DATABASE test;
                      ALTER DATABASE [test] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
                    END;",
                        sqlConnection);
                    await sqlCommand.ExecuteNonQueryAsync();

                    await sqlConnection.ChangeDatabaseAsync("test");

                    foreach (var migrationOperation in migrationOperations)
                    {
                        var migrationCommands = mig.Generate(new List<MigrationOperation>() { migrationOperation });

                        foreach (var migrationCommand in migrationCommands)
                        {
                            using SqlCommand command = new SqlCommand(migrationCommand.CommandText, sqlConnection);
                            await command.ExecuteNonQueryAsync();
                        }

                        if (migrationOperation is CreateTableOperation createTable)
                        {
                            var enableChangeTrackingCmd = $"ALTER TABLE {createTable.Name} ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);";
                            using SqlCommand command = new SqlCommand(enableChangeTrackingCmd, sqlConnection);
                            await command.ExecuteNonQueryAsync();
                        }
                    }

                    await sqlConnection.CloseAsync();

                    var initialUsers = dataGenerator.GenerateUsers(100_000);
                    var initialOrders = dataGenerator.GenerateOrders(100_000);

                    int insertCount = 0;
                    var totalCount = (double)(initialUsers.Count + initialOrders.Count);
                    for (int i = 0; i < initialUsers.Count; i++)
                    {
                        if (i % 10000 == 0)
                        {
                            statusUpdate($"{(int)((insertCount / totalCount) * 100)} percent");
                            await ctx.SaveChangesAsync();
                        }
                        ctx.Users.Add(initialUsers[i]);
                        insertCount++;
                    }

                    for (int i = 0; i < initialOrders.Count; i++)
                    {
                        if (i % 10000 == 0)
                        {
                            statusUpdate($"{(int)((insertCount / totalCount) * 100)} percent");
                            await ctx.SaveChangesAsync();
                        }
                        ctx.Orders.Add(initialOrders[i]);
                        insertCount++;
                    }

                    await ctx.SaveChangesAsync();
                },
                async (logger, token) =>
                {
                    // Update loop
                    var connectionString = await sqldb1.Resource.GetConnectionStringAsync();
                    ServiceCollection services = new ServiceCollection();
                    services.AddDbContext<SampleDbContext>(opt =>
                    {
                        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(connectionString);
                        builder.InitialCatalog = "test";
                        opt.UseSqlServer(builder.ToString());
                    });
                    var provider = services.BuildServiceProvider();

                    var scope = provider.CreateScope();
                    var ctx = scope.ServiceProvider.GetRequiredService<SampleDbContext>();

                    while (true)
                    {
                        token.ThrowIfCancellationRequested();

                        var newUsers = dataGenerator.GenerateUsers(1000);
                        var newOrders = dataGenerator.GenerateOrders(1000);

                        foreach (var user in newUsers)
                        {
                            ctx.Users.Add(user);
                        }

                        foreach (var order in newOrders)
                        {
                            ctx.Orders.Add(order);
                        }

                        await ctx.SaveChangesAsync();
                    }
                })
                .WaitFor(sqldb1)
                .WaitFor(sqldb2);

            var stream = builder.AddProject<SqlServerToSqlServerStream>("stream")
                .WithHttpHealthCheck("/health")
                .WithReference(sqldb1)
                .WithReference(sqldb2)
                .WithReference(blobs)
                .WaitFor(blobs)
                .WaitFor(dataInsert);

            builder.AddContainer("sqlpad", "sqlpad/sqlpad")
                .WaitFor(sqldb1)
                .WaitFor(sqldb2)
                .WithEndpoint(null, 3000, "http", "http")
                .WithEnvironment(async c =>
                {
                    var sqldb1ConnStr = await sqldb1.Resource.GetConnectionStringAsync();
                    var sqldb1Builder = new SqlConnectionStringBuilder(sqldb1ConnStr);
                    var sqldb2ConnStr = await sqldb2.Resource.GetConnectionStringAsync();
                    var sqldb2Builder = new SqlConnectionStringBuilder(sqldb2ConnStr);

                    c.EnvironmentVariables.Add("SQLPAD_AUTH_DISABLED", "true");

                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb1__name", "sqldb1");
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb1__driver", "sqlserver");
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb1__host", "sqldb1");
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb1__trustServerCertificate", "true");
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb1__database", "test");
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb1__username", sqldb1Builder.UserID);
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb1__password", sqldb1Builder.Password);

                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb2__name", "sqldb2");
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb2__driver", "sqlserver");
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb2__host", "sqldb2");
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb2__trustServerCertificate", "true");
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb2__database", "test");
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb2__username", sqldb2Builder.UserID);
                    c.EnvironmentVariables.Add("SQLPAD_CONNECTIONS__sqldb2__password", sqldb2Builder.Password);
                });

            builder.Build().Run();
        }
    }
}

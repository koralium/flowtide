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

namespace AspireSamples.ElasticsearchExample
{
    internal static class ElasticsearchExampleStartup
    {
        public static void RunSample(IDistributedApplicationBuilder builder)
        {
            var blobs = builder.AddAzureStorage("storage")
                .RunAsEmulator()
                .AddBlobs("blobs");

            var elasticsearch = builder.AddElasticsearch("elasticsearch");

            var sqldb1 = builder.AddSqlServer("SqlServer");

            DataGenerator dataGenerator = new DataGenerator();

            // Data generation
            var dataInsert = DataInsertResource.AddDataInsert(builder, "data-insert",
                async (logger, statusUpdate, token) =>
                {
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
                (logger, token) =>
                {
                    return Task.CompletedTask;
                })
                .WaitFor(sqldb1);

            var project = builder.AddProject<SqlServerToElasticProductionSample>("stream")
                .WithEnvironment("StreamVersion", "1.0.0")
                .WithReference(elasticsearch)
                .WithReference(sqldb1)
                .WithReference(blobs)
                .WaitFor(blobs)
                .WaitFor(elasticsearch)
                .WaitFor(dataInsert);

            builder.Build().Run();
        }
    }
}

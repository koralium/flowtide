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

using FlowtideDotNet.Connector.SqlServer;
using FlowtideDotNet.Substrait.Sql;
using System.Text;

namespace FlowtideDotNet.SqlServer.Tests.e2e
{
    public class SqlServerE2ETests : IClassFixture<SqlServerEndToEndFixture>
    {
        private readonly SqlServerEndToEndFixture _fixture;

        public SqlServerE2ETests(SqlServerEndToEndFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task SelectFromSqlServerIntoSqlServerWithDash()
        {
            var testName = "SelectFromSqlServerIntoSqlServer";


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table] (
                [id] [int] primary key,
                [name] [nvarchar](50) NOT NULL,
                [guid-dash] [uniqueidentifier] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest] (
                [id] [int] primary key,
                [name] [nvarchar](50) NOT NULL,
                [guid-dash] [uniqueidentifier] NOT NULL
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table] ([id], [name], [guid-dash]) VALUES (1, 'test1', '57f20bbe-3a17-45a7-bacc-614d89bde120');
            ");

            var testStream = new SqlServerTestStream(testName, _fixture.ConnectionString);
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest]
                SELECT
                    [id],
                    [name],
                    [guid-dash] 
                FROM [test-db].[dbo].[test-table]
            ");

            var count = 0;
            while (true)
            {
                await testStream.SchedulerTick();
                count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest]", (reader) =>
                {
                    reader.Read();
                    return reader.GetInt32(0);
                });
                if (count > 0)
                {
                    break;
                }
            }
            Assert.Equal(1, count);
        }

        [Fact]
        public async Task CustomPrimaryKeys()
        {
            var testName = "CustomPrimaryKeys";


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table2] (
                [id] [int] primary key,
                [name] [nvarchar](50) NOT NULL,
                [guid-dash] [uniqueidentifier] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table2] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest2] (
                [id] [int] IDENTITY(1,1) PRIMARY KEY,
                [name] [nvarchar](50) NOT NULL,
                [guid-dash] [uniqueidentifier] NOT NULL
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table2] ([id], [name], [guid-dash]) VALUES (1, 'test1', '57f20bbe-3a17-45a7-bacc-614d89bde120');
            ");

            var testStream = new SqlServerTestStream(testName, _fixture.ConnectionString, new List<string>()
            {
                "name"
            });
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest2]
                SELECT
                    [name],
                    [guid-dash] 
                FROM [test-db].[dbo].[test-table2]
            ");

            var count = 0;
            while (true)
            {
                await testStream.SchedulerTick();
                count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest2]", (reader) =>
                {
                    reader.Read();
                    return reader.GetInt32(0);
                });
                if (count > 0)
                {
                    break;
                }
            }
            Assert.Equal(1, count);
        }

        [Fact]
        public async Task CustomPrimaryKeysNotOnPosition0()
        {
            var testName = "CustomPrimaryKeysNotOnPosition0";


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table3] (
                [id] [int] primary key,
                [name] [nvarchar](50) NOT NULL,
                [guid-dash] [uniqueidentifier] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table3] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest3] (
                [id] [int] IDENTITY(1,1) PRIMARY KEY,
                [name] [nvarchar](50) NOT NULL,
                [guid-dash] [uniqueidentifier] NOT NULL
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table3] ([id], [name], [guid-dash]) VALUES (1, 'test1', '57f20bbe-3a17-45a7-bacc-614d89bde120');
            ");

            var testStream = new SqlServerTestStream(testName, _fixture.ConnectionString, new List<string>()
            {
                "name"
            });
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest3]
                SELECT
                    [guid-dash],
                    [name]
                FROM [test-db].[dbo].[test-table3]
            ");

            var count = 0;
            while (true)
            {
                await testStream.SchedulerTick();
                count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest3]", (reader) =>
                {
                    reader.Read();
                    return reader.GetInt32(0);
                });
                if (count > 0)
                {
                    break;
                }
            }
            Assert.Equal(1, count);

            await _fixture.RunCommand(@"
            UPDATE [test-db].[dbo].[test-table3] SET [guid-dash] = '57f20bbe-3a17-45a7-bacc-614d89bde121' WHERE name = 'test1';
            ");

            var expectedGuid = Guid.Parse("57f20bbe-3a17-45a7-bacc-614d89bde121");

            while (true)
            {
                await testStream.SchedulerTick();
                var g = await _fixture.ExecuteReader("SELECT [guid-dash] from [test-db].[dbo].[test-dest3] WHERE name = 'test1'", (reader) =>
                {
                    reader.Read();
                    return reader.GetGuid(0);
                });
                if (g.Equals(expectedGuid))
                {
                    break;
                }
            }
            Assert.Equal(1, count);
        }

        [Fact]
        public async Task DateTimeReadAndWrite()
        {
            var testName = "DateTimeReadAndWrite";


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table4] (
                [id] [int] primary key,
                [created] [datetime] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table4] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest4] (
                [id] [int]  PRIMARY KEY,
                [created] [datetime] NOT NULL
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table4] ([id], [created]) VALUES (1, '2024-01-03');
            ");

            var testStream = new SqlServerTestStream(testName, _fixture.ConnectionString);
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest4]
                SELECT
                    id,
                    created
                FROM [test-db].[dbo].[test-table4]
            ");

            var count = 0;
            while (true)
            {
                await testStream.SchedulerTick();
                count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest4]", (reader) =>
                {
                    reader.Read();
                    return reader.GetInt32(0);
                });
                if (count > 0)
                {
                    break;
                }
            }
            Assert.Equal(1, count);

            var expectedDate = new DateTime(2024, 1, 3);

            var date = await _fixture.ExecuteReader("SELECT [created] from [test-db].[dbo].[test-dest4] WHERE id = 1", (reader) =>
            {
                reader.Read();
                return reader.GetDateTime(0);
            });
            Assert.Equal(expectedDate, date);
        }

        [Fact]
        public async Task DateTimeOffsetReadAndWrite()
        {
            var testName = "DateTimeOffsetReadAndWrite";


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table5] (
                [id] [int] primary key,
                [created] [datetimeoffset] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table5] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest5] (
                [id] [int]  PRIMARY KEY,
                [created] [datetimeoffset] NOT NULL
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table5] ([id], [created]) VALUES (1, '2024-01-03 00:00:00+01:00');
            ");

            var testStream = new SqlServerTestStream(testName, _fixture.ConnectionString);
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest5]
                SELECT
                    id,
                    created
                FROM [test-db].[dbo].[test-table5]
            ");

            var count = 0;
            while (true)
            {
                await testStream.SchedulerTick();
                count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest5]", (reader) =>
                {
                    reader.Read();
                    return reader.GetInt32(0);
                });
                if (count > 0)
                {
                    break;
                }
            }
            Assert.Equal(1, count);

            var expectedDate = new DateTimeOffset(new DateTime(2024, 1, 3), TimeSpan.FromHours(1));

            var date = await _fixture.ExecuteReader("SELECT [created] from [test-db].[dbo].[test-dest5] WHERE id = 1", (reader) =>
            {
                reader.Read();
                return reader.GetDateTimeOffset(0);
            });
            Assert.Equal(expectedDate, date);
        }

        [Fact]
        public async Task SelectFromView()
        {
            var testName = nameof(SelectFromView);
            var sourceTableName = $"{testName}_source";
            var sourceViewName = $"{sourceTableName}_view";
            var destinationTableName = $"{testName}_destination";

            await _fixture.RunCommand($@"
            CREATE TABLE [test-db].[dbo].[{sourceTableName}] (
                [id] [int] primary key,
                [age] [int] NOT NULL
            )");

            await _fixture.RunCommand($@"
            CREATE VIEW [{sourceViewName}] AS 
            SELECT [id], [age] FROM [test-db].[dbo].[{sourceTableName}];
            ");

            await _fixture.RunCommand($"ALTER TABLE [test-db].[dbo].[{sourceTableName}] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand($@"
            CREATE TABLE [test-db].[dbo].[{destinationTableName}] (
                [id] [int]  PRIMARY KEY,
                [age] [int] NOT NULL
            )");

            var expectedValues = new List<(int, int)>
            {
                (1, 1),
                (2, 2),
                (3, 3),
                (4, 4),
                (5, 5)
            };

            // Insert some data
            await _fixture.RunCommand($@"
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES ({expectedValues[0].Item1}, {expectedValues[0].Item2});
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES ({expectedValues[1].Item1}, {expectedValues[1].Item2});
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES ({expectedValues[2].Item1}, {expectedValues[2].Item2});
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES ({expectedValues[3].Item1}, {expectedValues[3].Item2});
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES ({expectedValues[4].Item1}, {expectedValues[4].Item2});
            ");

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                EnableFullReload = true,
                FullReloadInterval = TimeSpan.FromSeconds(60)
            });

            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });

            await testStream.StartStream($@"
                INSERT INTO [test-db].[dbo].[{destinationTableName}]
                SELECT
                    id,
                    age
                FROM [test-db].[dbo].[{sourceViewName}]
            ");

            var count = 0;
            while (true)
            {
                await testStream.SchedulerTick();
                count = await _fixture.ExecuteReader($"SELECT count(*) from [test-db].[dbo].[{destinationTableName}]", (reader) =>
                {
                    reader.Read();
                    return reader.GetInt32(0);
                });

                if (count >= expectedValues.Count)
                {
                    break;
                }
            }

            var result = await _fixture.ExecuteReader($"SELECT [id], [age] from [test-db].[dbo].[{destinationTableName}]", (reader) =>
            {
                var rows = new List<(int, int)>();
                while (reader.Read())
                {
                    rows.Add((reader.GetInt32(0), reader.GetInt32(1)));
                }

                return rows;
            });

            Assert.Equal(expectedValues, result);
        }

        [Fact]
        public async Task FullLoadOnTableWithoutChangeTrackingIfAllowed()
        {
            var testName = nameof(FullLoadOnTableWithoutChangeTrackingIfAllowed);
            var sourceTableName = $"{testName}_source";
            var destinationTableName = $"{testName}_destination";

            await _fixture.RunCommand($@"
            CREATE TABLE [test-db].[dbo].[{sourceTableName}] (
                [id] [int] primary key,
                [age] [int] NOT NULL
            )");

            await _fixture.RunCommand($@"
            CREATE TABLE [test-db].[dbo].[{destinationTableName}] (
                [id] [int]  PRIMARY KEY,
                [age] [int] NOT NULL
            )");

            var expectedValues = new List<(int, int)>
            {
                (1, 1),
                (2, 2),
                (3, 3),
                (4, 4),
                (5, 5)
            };

            // Insert some data
            await _fixture.RunCommand($@"
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES ({expectedValues[0].Item1}, {expectedValues[0].Item2});
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES ({expectedValues[1].Item1}, {expectedValues[1].Item2});
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES ({expectedValues[2].Item1}, {expectedValues[2].Item2});
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES ({expectedValues[3].Item1}, {expectedValues[3].Item2});
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES ({expectedValues[4].Item1}, {expectedValues[4].Item2});
            ");

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                EnableFullReload = true,
                AllowFullReloadOnTablesWithoutChangeTracking = true,
                FullReloadInterval = TimeSpan.FromMinutes(1)
            });

            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });

            await testStream.StartStream($@"
                INSERT INTO [test-db].[dbo].[{destinationTableName}]
                SELECT
                    id,
                    age
                FROM [test-db].[dbo].[{sourceTableName}]
            ");

            var count = 0;
            while (true)
            {
                await testStream.SchedulerTick();
                count = await _fixture.ExecuteReader($"SELECT count(*) from [test-db].[dbo].[{destinationTableName}]", (reader) =>
                {
                    reader.Read();
                    return reader.GetInt32(0);
                });

                if (count >= expectedValues.Count)
                {
                    break;
                }
            }

            var result = await _fixture.ExecuteReader($"SELECT [id], [age] from [test-db].[dbo].[{destinationTableName}]", (reader) =>
            {
                var rows = new List<(int, int)>();
                while (reader.Read())
                {
                    rows.Add((reader.GetInt32(0), reader.GetInt32(1)));
                }

                return rows;
            });

            Assert.Equal(expectedValues, result);
        }

        [Theory]
        [InlineData(true, null)]
        [InlineData(false, 1)]
        public async Task FullLoadOnTableWithoutChangeTrackingThrows(bool allow, int? fromMinutes)
        {
            var testName = $"{nameof(FullLoadOnTableWithoutChangeTrackingThrows)}_{allow}_{fromMinutes}";
            var sourceTableName = $"{testName}_{allow}_{fromMinutes}_source";
            var destinationTableName = $"{testName}_{allow}_{fromMinutes}_destination";

            await _fixture.RunCommand($@"
            CREATE TABLE [test-db].[dbo].[{sourceTableName}] (
                [id] [int] primary key,
                [age] [int] NOT NULL
            )");

            await _fixture.RunCommand($@"
            CREATE TABLE [test-db].[dbo].[{destinationTableName}] (
                [id] [int]  PRIMARY KEY,
                [age] [int] NOT NULL
            )");

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                EnableFullReload = true,
                AllowFullReloadOnTablesWithoutChangeTracking = allow,
                FullReloadInterval = fromMinutes.HasValue ? TimeSpan.FromMinutes(fromMinutes.Value) : null
            });

            var plan = $@"INSERT INTO [test-db].[dbo].[{destinationTableName}]
                SELECT
                    id,
                    age
                FROM [test-db].[dbo].[{sourceTableName}]";

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await testStream.StartStream(plan));
            Assert.NotEmpty(exception.Message);
        }

        [Theory]
        [InlineData(true, null)]
        [InlineData(false, null)]
        public async Task ViewWithoutFullLoadThrows(bool allow, int? fromMinutes)
        {
            var testName = $"{nameof(ViewWithoutFullLoadThrows)}_{allow}_{fromMinutes}";
            var sourceTableName = $"{testName}_{allow}_{fromMinutes}_source";
            var sourceViewName = $"{testName}_{allow}_{fromMinutes}_view_source";
            var destinationTableName = $"{testName}_{allow}_{fromMinutes}_destination";

            await _fixture.RunCommand($@"
            CREATE TABLE [test-db].[dbo].[{sourceTableName}] (
                [id] [int] primary key,
                [age] [int] NOT NULL
            )");
            await _fixture.RunCommand($@"
                CREATE VIEW [{sourceViewName}] AS 
                SELECT [id], [age] FROM [test-db].[dbo].[{sourceTableName}];
            ");
            await _fixture.RunCommand($@"
            CREATE TABLE [test-db].[dbo].[{destinationTableName}] (
                [id] [int]  PRIMARY KEY,
                [age] [int] NOT NULL
            )");

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                EnableFullReload = allow,
                FullReloadInterval = fromMinutes.HasValue ? TimeSpan.FromMinutes(fromMinutes.Value) : null
            });

            var plan = $@"INSERT INTO [test-db].[dbo].[{destinationTableName}]
                SELECT
                    id,
                    age
                FROM [test-db].[dbo].[{sourceViewName}]";

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await testStream.StartStream(plan));
            Assert.NotEmpty(exception.Message);
        }

        [Fact]
        public async Task TableFullLoadWithTooManyRowsThrows()
        {
            var testName = nameof(TableFullLoadWithTooManyRowsThrows);
            var sourceTableName = $"{testName}_source";
            var sourceViewName = $"{testName}_view_source";
            var destinationTableName = $"{testName}_destination";

            await _fixture.RunCommand($@"
            CREATE TABLE [test-db].[dbo].[{sourceTableName}] (
                [id] [int] primary key,
                [age] [int] NOT NULL
            )");

            await _fixture.RunCommand($@"
                CREATE VIEW [{sourceViewName}] AS 
                SELECT [id], [age] FROM [test-db].[dbo].[{sourceTableName}];
            ");

            await _fixture.RunCommand($@"
            CREATE TABLE [test-db].[dbo].[{destinationTableName}] (
                [id] [int]  PRIMARY KEY,
                [age] [int] NOT NULL
            )");

            await _fixture.RunCommand($@"
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES (1, 2);
            INSERT INTO [test-db].[dbo].[{sourceTableName}] ([id], [age]) VALUES (2, 2);
            ");

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                EnableFullReload = true,
                FullReloadInterval = TimeSpan.FromSeconds(30),
                FullLoadMaxRowCount = 1
            }, default);

            var plan = $@"INSERT INTO [test-db].[dbo].[{destinationTableName}]
                SELECT
                    id,
                    age
                FROM [test-db].[dbo].[{sourceViewName}]";

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () => await testStream.StartStream(plan));
            Assert.NotEmpty(exception.Message);
        }

        [Fact]
        public async Task CustomDestinationTable()
        {
            var testName = nameof(CustomDestinationTable);


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table6] (
                [id] [int] primary key,
                [created] [datetimeoffset] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table6] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[testdest6] (
                [id] [int]  PRIMARY KEY,
                [created] [datetimeoffset] NOT NULL,
                [my_column] nvarchar(10)
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table6] ([id], [created]) VALUES (1, '2024-01-03 00:00:00+01:00');
            ");

            SemaphoreSlim waitSemaphore = new SemaphoreSlim(0);

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString
            }, new SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                CustomBulkCopyDestinationTable = (table) => "testdest6",
                OnDataTableCreation = (dataTable, tmpTable, tableName) =>
                {
                    dataTable.Columns.Add("my_column");
                    return ValueTask.CompletedTask;
                },
                ModifyRow = (row, isDeleted, watermark, checkpointId, isInitialData, tmpTable, tableName) =>
                {
                    row["my_column"] = "val";
                },
                OnDataUploaded = (connection, watermark, checkpointId, isInitialData, tmpTable, tableName) =>
                {
                    using var cmd = connection.CreateCommand();
                    cmd.CommandText = "UPDATE testdest6 SET my_column = 'val2' WHERE my_column = 'val'";
                    cmd.ExecuteNonQuery();
                    waitSemaphore.Release();
                    return ValueTask.CompletedTask;
                }
            });
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[testdest6]
                SELECT
                    id,
                    created
                FROM [test-db].[dbo].[test-table6]
            ");

            await waitSemaphore.WaitAsync();

            var expectedDate = new DateTimeOffset(new DateTime(2024, 1, 3), TimeSpan.FromHours(1));

            var result = await _fixture.ExecuteReader("SELECT [created], [my_column] from [test-db].[dbo].[testdest6] WHERE id = 1", (reader) =>
            {
                reader.Read();
                return (reader.GetDateTimeOffset(0), reader.GetString(1));
            });
            Assert.Equal(expectedDate, result.Item1);
            Assert.Equal("val2", result.Item2);
        }

        [Fact]
        public async Task TestWatermarkEachBatch()
        {
            var testName = nameof(TestWatermarkEachBatch);


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table7] (
                [id] [int] primary key,
                [created] [datetimeoffset] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table7] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[testdest7] (
                [id] [int]  PRIMARY KEY,
                [created] [datetimeoffset] NOT NULL
            )");

            // Insert some data

            var bulkInsertCommand = new StringBuilder("INSERT INTO [test-db].[dbo].[test-table7] ([id], [created]) VALUES ");
            for (int i = 0; i < 1_000; i++)
            {
                bulkInsertCommand.Append($"({i}, '2024-01-03 00:00:00+01:00'),");
            }
            // Remove the trailing comma
            bulkInsertCommand.Length--;
            await _fixture.RunCommand(bulkInsertCommand.ToString());


            int batchCount = 0;
            using SemaphoreSlim waitSemaphore = new SemaphoreSlim(0);
            // 100 events per batch
            int expectedBatchCount = 1000 / 100;

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString
            }, new SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ExecutionMode = Core.Operators.Write.ExecutionMode.OnWatermark,
                OnDataUploaded = (connection, watermark, checkpointId, isInitialData, tmpTable, tableName) =>
                {
                    batchCount++;
                    if (batchCount == expectedBatchCount)
                    {
                        waitSemaphore.Release();
                    }
                    
                    return ValueTask.CompletedTask;
                }
            });
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[testdest7]
                SELECT
                    id,
                    created
                FROM [test-db].[dbo].[test-table7] WITH (WATERMARK_OUTPUT_MODE = ON_EACH_BATCH)
            ");

            await waitSemaphore.WaitAsync(TimeSpan.FromSeconds(30));

            var result = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[testdest7]", (reader) =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });
            Assert.Equal(1000, result);
        }

        /// <summary>
        /// Keys declared in the statement replace the table metadata.
        /// The destination identity primary key is never written.
        /// </summary>
        [Fact]
        public async Task PrimaryKeysDeclaredInStatement()
        {
            var testName = "PrimaryKeysDeclaredInStatement";

            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table8] (
                [id] [int] primary key,
                [name] [nvarchar](50) NOT NULL,
                [guid-dash] [uniqueidentifier] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table8] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest8] (
                [id] [int] IDENTITY(1,1) PRIMARY KEY,
                [name] [nvarchar](50) NOT NULL,
                [guid-dash] [uniqueidentifier] NOT NULL
            )");

            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table8] ([id], [name], [guid-dash]) VALUES (1, 'test1', '57f20bbe-3a17-45a7-bacc-614d89bde120');
            ");

            var testStream = new SqlServerTestStream(testName, _fixture.ConnectionString);
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest8] PRIMARY KEY ([name])
                SELECT
                    [guid-dash],
                    [name]
                FROM [test-db].[dbo].[test-table8]
            ");

            var count = 0;
            while (true)
            {
                await testStream.SchedulerTick();
                count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest8]", (reader) =>
                {
                    reader.Read();
                    return reader.GetInt32(0);
                });
                if (count > 0)
                {
                    break;
                }
            }
            Assert.Equal(1, count);

            // An update must land on the existing row.
            await _fixture.RunCommand(@"
            UPDATE [test-db].[dbo].[test-table8] SET [guid-dash] = '57f20bbe-3a17-45a7-bacc-614d89bde121' WHERE name = 'test1';
            ");

            var expectedGuid = Guid.Parse("57f20bbe-3a17-45a7-bacc-614d89bde121");

            while (true)
            {
                await testStream.SchedulerTick();
                var g = await _fixture.ExecuteReader("SELECT [guid-dash] from [test-db].[dbo].[test-dest8] WHERE name = 'test1'", (reader) =>
                {
                    reader.Read();
                    return reader.GetGuid(0);
                });
                if (g.Equals(expectedGuid))
                {
                    break;
                }
            }

            count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest8]", (reader) =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });
            Assert.Equal(1, count);
        }

        /// <summary>
        /// Declaration, source and destination all use different casing.
        /// The merge statement itself is covered by SinkTests.
        /// </summary>
        [Fact]
        public async Task PrimaryKeysDeclaredInStatementWithOtherCasing()
        {
            var testName = "PrimaryKeysDeclaredInStatementWithOtherCasing";

            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table9] (
                [id] [int] primary key,
                [name] [nvarchar](50) NOT NULL,
                [guid-dash] [uniqueidentifier] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table9] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest9] (
                [id] [int] IDENTITY(1,1) PRIMARY KEY,
                [Name] [nvarchar](50) NOT NULL,
                [guid-dash] [uniqueidentifier] NOT NULL
            )");

            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table9] ([id], [name], [guid-dash]) VALUES (1, 'test1', '57f20bbe-3a17-45a7-bacc-614d89bde120');
            ");

            var testStream = new SqlServerTestStream(testName, _fixture.ConnectionString);
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest9] PRIMARY KEY ([NAME])
                SELECT
                    [guid-dash],
                    [name]
                FROM [test-db].[dbo].[test-table9]
            ");

            var count = 0;
            while (true)
            {
                await testStream.SchedulerTick();
                count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest9]", (reader) =>
                {
                    reader.Read();
                    return reader.GetInt32(0);
                });
                if (count > 0)
                {
                    break;
                }
            }
            Assert.Equal(1, count);

            // Forces the merge to take the matched branch.
            await _fixture.RunCommand(@"
            UPDATE [test-db].[dbo].[test-table9] SET [guid-dash] = '57f20bbe-3a17-45a7-bacc-614d89bde121' WHERE name = 'test1';
            ");

            var expectedGuid = Guid.Parse("57f20bbe-3a17-45a7-bacc-614d89bde121");

            while (true)
            {
                await testStream.SchedulerTick();
                var g = await _fixture.ExecuteReader("SELECT [guid-dash] from [test-db].[dbo].[test-dest9] WHERE [Name] = 'test1'", (reader) =>
                {
                    reader.Read();
                    return reader.GetGuid(0);
                });
                if (g.Equals(expectedGuid))
                {
                    break;
                }
            }

            count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest9]", (reader) =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });
            Assert.Equal(1, count);
        }
        [Fact]
        public async Task OnInitializeCalled()
        {
            var testName = nameof(OnInitializeCalled);


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table10] (
                [id] [int] primary key,
                [created] [datetimeoffset] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table10] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest10] (
                [id] [int] PRIMARY KEY,
                [created] [datetimeoffset] NOT NULL
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table10] ([id], [created]) VALUES (1, '2024-01-03 00:00:00+01:00');
            ");

            int initializeCount = 0;
            int connectionProbe = 0;
            string? initializeTmpTable = null;
            IReadOnlyList<string>? initializeTableName = null;
            using SemaphoreSlim waitSemaphore = new SemaphoreSlim(0);

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString
            }, new SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                OnInitialize = async (connection, checkpointId, lastCommittedId, tmpTable, tableName) =>
                {
                    initializeCount++;
                    initializeTmpTable = tmpTable;
                    initializeTableName = tableName;

                    // Hook connection must be open and usable.
                    using var cmd = connection.CreateCommand();
                    cmd.CommandText = "SELECT 1";
                    connectionProbe = (int)(await cmd.ExecuteScalarAsync())!;
                },
                OnDataUploaded = (connection, watermark, checkpointId, isInitialData, tmpTable, tableName) =>
                {
                    waitSemaphore.Release();
                    return ValueTask.CompletedTask;
                }
            });
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest10]
                SELECT
                    id,
                    created
                FROM [test-db].[dbo].[test-table10]
            ");

            Assert.True(await waitSemaphore.WaitAsync(TimeSpan.FromSeconds(30)));

            Assert.Equal(1, initializeCount);
            Assert.Equal(1, connectionProbe);

            // No custom table, hook gets the generated temporary table.
            Assert.NotNull(initializeTmpTable);
            Assert.StartsWith("#tmp_", initializeTmpTable);

            Assert.NotNull(initializeTableName);
            Assert.Equal(3, initializeTableName.Count);
            Assert.Equal("test-db", initializeTableName[0]);
            Assert.Equal("dbo", initializeTableName[1]);
            Assert.Equal("test-dest10", initializeTableName[2]);

            // Hook must not disturb the merge into path.
            var count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest10]", (reader) =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });
            Assert.Equal(1, count);
        }

        [Fact]
        public async Task OnInitializeClearsCustomDestinationTable()
        {
            var testName = nameof(OnInitializeClearsCustomDestinationTable);


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table11] (
                [id] [int] primary key,
                [created] [datetimeoffset] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table11] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest11] (
                [id] [int] PRIMARY KEY,
                [created] [datetimeoffset] NOT NULL
            )");
            // Staging table replaces the temporary table for bulk copy.
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[teststaging11] (
                [id] [int] NOT NULL,
                [created] [datetimeoffset] NOT NULL
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table11] ([id], [created]) VALUES (1, '2024-01-03 00:00:00+01:00');
            ");

            // Stale rows from a previous run, hook clears them.
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[teststaging11] ([id], [created]) VALUES
                (998, '2020-01-01 00:00:00+01:00'),
                (999, '2020-01-01 00:00:00+01:00');
            ");

            string? clearedTable = null;
            using SemaphoreSlim waitSemaphore = new SemaphoreSlim(0);

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString
            }, new SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                CustomBulkCopyDestinationTable = (table) => "teststaging11",
                OnInitialize = async (connection, checkpointId, lastCommittedId, tmpTable, tableName) =>
                {
                    clearedTable = tmpTable;
                    using var cmd = connection.CreateCommand();
                    cmd.CommandText = $"DELETE FROM {tmpTable}";
                    await cmd.ExecuteNonQueryAsync();
                },
                OnDataUploaded = (connection, watermark, checkpointId, isInitialData, tmpTable, tableName) =>
                {
                    waitSemaphore.Release();
                    return ValueTask.CompletedTask;
                }
            });
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest11]
                SELECT
                    id,
                    created
                FROM [test-db].[dbo].[test-table11]
            ");

            Assert.True(await waitSemaphore.WaitAsync(TimeSpan.FromSeconds(30)));

            // Hook gets the custom table, not a temporary one.
            Assert.Equal("teststaging11", clearedTable);

            var stagedIds = await _fixture.ExecuteReader("SELECT [id] from [test-db].[dbo].[teststaging11]", (reader) =>
            {
                var ids = new List<int>();
                while (reader.Read())
                {
                    ids.Add(reader.GetInt32(0));
                }
                return ids;
            });

            // Stale rows gone, only the streamed row remains.
            Assert.Single(stagedIds);
            Assert.Equal(1, stagedIds[0]);

            // Custom table skips merge into, destination stays empty.
            var destinationCount = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest11]", (reader) =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });
            Assert.Equal(0, destinationCount);
        }

        [Fact]
        public async Task CheckpointIdIsTheCheckpointVersion()
        {
            var testName = nameof(CheckpointIdIsTheCheckpointVersion);


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table12] (
                [id] [int] primary key,
                [created] [datetimeoffset] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table12] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest12] (
                [id] [int] PRIMARY KEY,
                [created] [datetimeoffset] NOT NULL
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table12] ([id], [created]) VALUES (1, '2024-01-03 00:00:00+01:00');
            ");

            var initIds = new List<long>();
            var modifyIds = new List<long>();
            var uploadIds = new List<long>();
            using SemaphoreSlim waitSemaphore = new SemaphoreSlim(0);

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString
            }, new SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                // Upload inside the checkpoint, id is the durable checkpoint.
                ExecutionMode = Core.Operators.Write.ExecutionMode.OnCheckpoint,
                OnInitialize = (connection, checkpointId, lastCommittedId, tmpTable, tableName) =>
                {
                    lock (initIds)
                    {
                        initIds.Add(checkpointId);
                    }
                    return ValueTask.CompletedTask;
                },
                ModifyRow = (row, isDeleted, watermark, checkpointId, isInitialData, tmpTable, tableName) =>
                {
                    lock (modifyIds)
                    {
                        if (modifyIds.Count == 0 || modifyIds[^1] != checkpointId)
                        {
                            modifyIds.Add(checkpointId);
                        }
                    }
                },
                OnDataUploaded = (connection, watermark, checkpointId, isInitialData, tmpTable, tableName) =>
                {
                    lock (uploadIds)
                    {
                        uploadIds.Add(checkpointId);
                    }
                    waitSemaphore.Release();
                    return ValueTask.CompletedTask;
                }
            });
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest12]
                SELECT
                    id,
                    created
                FROM [test-db].[dbo].[test-table12]
            ");

            Assert.True(await waitSemaphore.WaitAsync(TimeSpan.FromSeconds(30)));

            long firstUploadId;
            lock (uploadIds)
            {
                firstUploadId = uploadIds[0];
            }

            // A version counter, not a wall clock time.
            Assert.InRange(firstUploadId, 1, 1_000);

            // All three hooks agree on the id.
            Assert.Equal(firstUploadId, initIds[0]);
            Assert.Equal(firstUploadId, modifyIds[0]);

        }

        [Fact]
        public async Task TwoPhaseCommitWithOnCheckpointComplete()
        {
            var testName = nameof(TwoPhaseCommitWithOnCheckpointComplete);


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table13] (
                [id] [int] primary key,
                [created] [datetimeoffset] NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table13] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest13] (
                [id] [int] PRIMARY KEY,
                [created] [datetimeoffset] NOT NULL,
                [md_checkpoint] [bigint] NOT NULL
            )");

            // From a rolled back epoch, the reconcile must remove it.
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-dest13] ([id], [created], [md_checkpoint]) VALUES (777, '2020-01-01 00:00:00+01:00', 4242);
            ");
            // Staging table, rows land here tagged with their checkpoint.
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[teststaging13] (
                [id] [int] NOT NULL,
                [created] [datetimeoffset] NOT NULL,
                [md_checkpoint] [bigint] NOT NULL
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table13] ([id], [created]) VALUES (1, '2024-01-03 00:00:00+01:00');
            ");

            // Staged in a rolled back epoch, never committed.
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[teststaging13] ([id], [created], [md_checkpoint]) VALUES (999, '2020-01-01 00:00:00+01:00', 9999);
            ");

            var uploadedIds = new List<long>();
            var committedIds = new List<long>();
            long reconciledLastCommitted = -1;
            using SemaphoreSlim commitSemaphore = new SemaphoreSlim(0);

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString
            }, new SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                // Staging only well defined for uploads inside the checkpoint.
                ExecutionMode = Core.Operators.Write.ExecutionMode.OnCheckpoint,
                CustomBulkCopyDestinationTable = (table) => "teststaging13",
                OnDataTableCreation = (dataTable, tmpTable, tableName) =>
                {
                    dataTable.Columns.Add("md_checkpoint", typeof(long));
                    return ValueTask.CompletedTask;
                },
                ModifyRow = (row, isDeleted, watermark, checkpointId, isInitialData, tmpTable, tableName) =>
                {
                    // Phase 1, tag the staged row with its checkpoint.
                    row["md_checkpoint"] = checkpointId;
                },
                OnDataUploaded = (connection, watermark, checkpointId, isInitialData, tmpTable, tableName) =>
                {
                    lock (uploadedIds)
                    {
                        uploadedIds.Add(checkpointId);
                    }
                    return ValueTask.CompletedTask;
                },
                OnInitialize = async (connection, checkpointId, lastCommittedId, tmpTable, tableName) =>
                {
                    reconciledLastCommitted = lastCommittedId;

                    // Recovery, redo the lost commit and drop rolled back epochs.
                    using var commit = connection.CreateCommand();
                    commit.CommandText = @"
                        DELETE FROM [test-db].[dbo].[test-dest13] WHERE [md_checkpoint] > @lastCommitted;
                        INSERT INTO [test-db].[dbo].[test-dest13] ([id], [created], [md_checkpoint])
                        SELECT s.[id], s.[created], s.[md_checkpoint] FROM [test-db].[dbo].[teststaging13] s
                        WHERE s.[md_checkpoint] <= @lastCommitted
                          AND NOT EXISTS (SELECT 1 FROM [test-db].[dbo].[test-dest13] d WHERE d.[id] = s.[id]);
                        DELETE FROM [test-db].[dbo].[teststaging13];";
                    commit.Parameters.AddWithValue("@lastCommitted", lastCommittedId);
                    await commit.ExecuteNonQueryAsync();
                },
                OnCheckpointComplete = async (connection, checkpointId, tmpTable, tableName) =>
                {
                    // Phase 2, checkpoint durable, move its staged rows into destination.
                    using var commit = connection.CreateCommand();
                    commit.CommandText = @"
                        INSERT INTO [test-db].[dbo].[test-dest13] ([id], [created], [md_checkpoint])
                        SELECT s.[id], s.[created], s.[md_checkpoint] FROM [test-db].[dbo].[teststaging13] s
                        WHERE s.[md_checkpoint] = @checkpoint
                          AND NOT EXISTS (SELECT 1 FROM [test-db].[dbo].[test-dest13] d WHERE d.[id] = s.[id]);
                        DELETE FROM [test-db].[dbo].[teststaging13] WHERE [md_checkpoint] = @checkpoint;";
                    commit.Parameters.AddWithValue("@checkpoint", checkpointId);
                    var moved = await commit.ExecuteNonQueryAsync();

                    if (moved > 0)
                    {
                        lock (committedIds)
                        {
                            committedIds.Add(checkpointId);
                        }
                        commitSemaphore.Release();
                    }
                }
            });
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest13]
                SELECT
                    id,
                    created
                FROM [test-db].[dbo].[test-table13]
            ");

            Assert.True(await commitSemaphore.WaitAsync(TimeSpan.FromSeconds(60)));

            // Fresh stream, nothing committed, the stale staged row is discarded.
            Assert.Equal(0, reconciledLastCommitted);

            var destination = await _fixture.ExecuteReader("SELECT [id] from [test-db].[dbo].[test-dest13]", (reader) =>
            {
                var ids = new List<int>();
                while (reader.Read())
                {
                    ids.Add(reader.GetInt32(0));
                }
                return ids;
            });

            // Only the streamed row survives, rolled back rows are gone.
            Assert.Single(destination);
            Assert.Equal(1, destination[0]);

            // Commit only for a checkpoint that was staged first.
            long committed;
            lock (committedIds)
            {
                committed = committedIds[0];
            }
            lock (uploadedIds)
            {
                Assert.Contains(committed, uploadedIds);
            }
        }

        [Fact]
        public async Task CustomDestinationTableReturningNullUsesTheDefaultPath()
        {
            var testName = nameof(CustomDestinationTableReturningNullUsesTheDefaultPath);


            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-table14] (
                [id] [int] primary key,
                [name] [nvarchar](50) NOT NULL
            )");
            await _fixture.RunCommand("ALTER TABLE [test-db].[dbo].[test-table14] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF)");
            await _fixture.RunCommand(@"
            CREATE TABLE [test-db].[dbo].[test-dest14] (
                [id] [int] PRIMARY KEY,
                [name] [nvarchar](50) NOT NULL
            )");

            // Insert some data
            await _fixture.RunCommand(@"
            INSERT INTO [test-db].[dbo].[test-table14] ([id], [name]) VALUES (1, 'first');
            ");

            var seenTableNames = new List<string>();

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString
            }, new SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                // Null return, sink falls back to the default path.
                CustomBulkCopyDestinationTable = (table) =>
                {
                    lock (seenTableNames)
                    {
                        seenTableNames.Add(string.Join(".", table));
                    }
                    return null;
                }
            });
            testStream.RegisterTableProviders((builder) =>
            {
                builder.AddSqlServerProvider(() => _fixture.ConnectionString);
            });
            await testStream.StartStream(@"
                INSERT INTO [test-db].[dbo].[test-dest14]
                SELECT
                    id,
                    name
                FROM [test-db].[dbo].[test-table14]
            ");

            // Insert reaches destination via temporary table and merge into.
            var insertDeadline = DateTime.UtcNow.AddSeconds(60);
            while (true)
            {
                await testStream.SchedulerTick();
                var count = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[test-dest14]", (reader) =>
                {
                    reader.Read();
                    return reader.GetInt32(0);
                });
                if (count > 0)
                {
                    break;
                }
                if (DateTime.UtcNow > insertDeadline)
                {
                    Assert.Fail("The row never reached the destination table, the null return did not fall back to the default path.");
                }
            }

            // Updates need the operation metadata column and prepared merge.
            await _fixture.RunCommand(@"
            UPDATE [test-db].[dbo].[test-table14] SET [name] = 'second' WHERE [id] = 1;
            ");

            var updateDeadline = DateTime.UtcNow.AddSeconds(60);
            while (true)
            {
                await testStream.SchedulerTick();
                var name = await _fixture.ExecuteReader("SELECT [name] from [test-db].[dbo].[test-dest14] WHERE [id] = 1", (reader) =>
                {
                    reader.Read();
                    return reader.GetString(0);
                });
                if (name == "second")
                {
                    break;
                }
                if (DateTime.UtcNow > updateDeadline)
                {
                    Assert.Fail($"The update never reached the destination table, name was '{name}'.");
                }
            }

            // Callback was consulted with the destination table name.
            lock (seenTableNames)
            {
                Assert.Contains("test-db.dbo.test-dest14", seenTableNames);
            }
        }

    }
}

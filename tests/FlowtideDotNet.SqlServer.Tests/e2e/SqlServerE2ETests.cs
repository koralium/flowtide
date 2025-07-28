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
                CustomBulkCopyDestinationTable = "testdest6",
                OnDataTableCreation = (dataTable) =>
                {
                    dataTable.Columns.Add("my_column");
                    return ValueTask.CompletedTask;
                },
                ModifyRow = (row, isDeleted, watermark, checkpointId, isInitialData) =>
                {
                    row["my_column"] = "val";
                },
                OnDataUploaded = (connection, watermark, checkpointId, isInitialData) =>
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
            SemaphoreSlim waitSemaphore = new SemaphoreSlim(0);
            // 100 events per batch
            int expectedBatchCount = 1000 / 100;

            var testStream = new SqlServerTestStream(testName, new SqlServerSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString
            }, new SqlServerSinkOptions()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ExecutionMode = Core.Operators.Write.ExecutionMode.OnWatermark,
                OnDataUploaded = (connection, watermark, checkpointId, isInitialData) =>
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

            var expectedDate = new DateTimeOffset(new DateTime(2024, 1, 3), TimeSpan.FromHours(1));

            var result = await _fixture.ExecuteReader("SELECT count(*) from [test-db].[dbo].[testdest7]", (reader) =>
            {
                reader.Read();
                return reader.GetInt32(0);
            });
            Assert.Equal(1000, result);
        }
    }
}

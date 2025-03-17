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

using FlowtideDotNet.Substrait.Sql;

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
    }
}

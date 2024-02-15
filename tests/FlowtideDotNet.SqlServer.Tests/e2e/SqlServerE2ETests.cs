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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
    }
}

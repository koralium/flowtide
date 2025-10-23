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

using FlowtideDotNet.Connector.Starrocks.Internal;

namespace FlowtideDotNet.Connector.Starrocks.Tests
{
    public class IntegrationTests : IClassFixture<StarrocksFixture>
    {
        private readonly StarrocksFixture fixture;

        public IntegrationTests(StarrocksFixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact]
        public void FetchTableMetadata()
        {
            var factory = new StarrocksSinkFactory(new StarrocksSinkOptions()
            {
                HttpUrl = fixture.Uri,
                Username = "root"
            });

            var found = factory.TryGetTableInformation(["test", "testtable"], out var metadata);

            Assert.True(found);
            Assert.Equal(3, metadata!.Schema.Names.Count);

            Assert.Equal("id", metadata.Schema.Names[0]);
            Assert.Equal("create_time", metadata.Schema.Names[1]);
            Assert.Equal("name", metadata.Schema.Names[2]);
        }

        [Fact]
        public void FetchTableMetadataCaseInsensitive()
        {
            var factory = new StarrocksSinkFactory(new StarrocksSinkOptions()
            {
                HttpUrl = fixture.Uri,
                Username = "root"
            });

            var found = factory.TryGetTableInformation(["test", "testtAble"], out var metadata);
            Assert.True(found);
            Assert.Equal(3, metadata!.Schema.Names.Count);

            Assert.Equal("id", metadata.Schema.Names[0]);
            Assert.Equal("create_time", metadata.Schema.Names[1]);
            Assert.Equal("name", metadata.Schema.Names[2]);
        }

        [Fact]
        public async Task TestInsertData()
        {
            StarrocksTestStream stream = new StarrocksTestStream(fixture, nameof(TestInsertData));

            stream.Generate();
            await stream.StartStream(@"
                INSERT INTO test.testtable
                SELECT UserKey AS id, '2024-01-01 12:00:00' AS create_time, 'testname' AS name
                FROM users;
            ");

            while (true)
            {
                await stream.SchedulerTick();
                var count = await fixture.GetTableCount("test", "testtable");

                if (count == 1000)
                {
                    break;
                }

                await Task.Delay(100);
            }

            var firstUser = stream.Users[0];

            stream.DeleteUser(firstUser);

            while (true)
            {
                await stream.SchedulerTick();
                var count = await fixture.GetTableCount("test", "testtable");

                if (count == 999)
                {
                    break;
                }

                await Task.Delay(100);
            }
        }

        [Fact]
        public async Task InsertNoPrimaryKeyInInsertThrowsException()
        {
            StarrocksTestStream stream = new StarrocksTestStream(fixture, nameof(InsertNoPrimaryKeyInInsertThrowsException));

            stream.Generate();
            await stream.StartStream(@"
                INSERT INTO test.testtable
                SELECT '2024-01-01 12:00:00' AS create_time, 'testname' AS name
                FROM users;
            ");

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                while (true)
                {
                    await stream.SchedulerTick();
                    await Task.Delay(10);
                }
            });

            Assert.Equal("Primary key column 'id' is not inserted into the table, all primary keys must be inserted.", ex.Message);
        }

        [Fact]
        public async Task InsertColumnThatDoesNotExistThrowsException()
        {
            StarrocksTestStream stream = new StarrocksTestStream(fixture, nameof(InsertColumnThatDoesNotExistThrowsException));

            stream.Generate();

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await stream.StartStream(@"
                    INSERT INTO test.testtable
                    SELECT UserKey AS id, '2024-01-01 12:00:00' AS create_time2, 'testname' AS name
                    FROM users;
                ");
            });

            Assert.Equal("Column 'create_time2' not found in Starrocks table 'test.testtable'.", ex.Message);
        }

        [Fact]
        public async Task TestInsertDataNullKey()
        {
            StarrocksTestStream stream = new StarrocksTestStream(fixture, nameof(TestInsertDataNullKey));

            stream.Generate(1);
            await stream.StartStream(@"
                INSERT INTO test.testtable
                SELECT null AS id, '2024-01-01 12:00:00' AS create_time, 'testname' AS name
                FROM users;
            ");

            var exception = await Assert.ThrowsAsync<AggregateException>(async () =>
            {
                while (true)
                {
                    await stream.SchedulerTick();
                    await Task.Delay(10);
                }
            });
            Assert.NotNull(exception.InnerException);
            Assert.Equal("Received a row with primary key 'id' set to 'null'", exception.InnerException.Message);
        }
    }
}
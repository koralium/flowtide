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

using Elasticsearch.Net;
using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Connector.CosmosDB.Tests;
using FlowtideDotNet.Connector.ElasticSearch.Exceptions;
using Nest;

namespace FlowtideDotNet.Connector.ElasticSearch.Tests
{
    public class SinkTests : IClassFixture<ElasticSearchFixture>
    {
        private readonly ElasticSearchFixture elasticSearchFixture;

        public SinkTests(ElasticSearchFixture elasticSearchFixture)
        {
            this.elasticSearchFixture = elasticSearchFixture;

            ElasticClient elasticClient = new ElasticClient(elasticSearchFixture.GetConnectionSettings());
            elasticClient.Indices.Delete("testindex");
        }

        [Fact]
        public async Task TestInsert()
        {
            ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsert");
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT 
                UserKey as _id,
                FirstName as firstName,
                LastName as lastName,
                CAST(BirthDate as TIMESTAMP) as birthDate
            FROM users
            ");

            ElasticClient elasticClient = new ElasticClient(elasticSearchFixture.GetConnectionSettings());

            var lastUser = stream.Users.Last();
            bool success = false;
            StringResponse? stringResponse;
            do
            {
                await stream.SchedulerTick();
                stringResponse = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", lastUser.UserKey.ToString());
                success = stringResponse.ApiCall.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            var resp = await elasticClient.SourceAsync<User>(lastUser.UserKey.ToString(), g => g.Index("testindex"));

            var mappingInfo = elasticClient.Indices.GetMapping<User>(b => b.Index("testindex"));
            var birthDateField = mappingInfo.Indices["testindex"].Mappings.Properties["birthDate"];
            Assert.Equal("date", birthDateField.Type);
            Assert.Equal(lastUser.BirthDate!.Value.ToUniversalTime(), resp.Body.BirthDate!.Value.ToUniversalTime(), TimeSpan.FromMilliseconds(1));
        }

        [Fact]
        public async Task TestInsertWithUpdate()
        {
            ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsertWithUpdate");
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT 
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            ElasticClient elasticClient = new ElasticClient(elasticSearchFixture.GetConnectionSettings());

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", lastUser.UserKey.ToString());
                success = resp.ApiCall.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            stream.Generate();

            lastUser = stream.Users.Last();

            success = false;
            do
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", lastUser.UserKey.ToString());
                success = resp.ApiCall.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            stream.DeleteUser(lastUser);

            success = false;
            do
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", lastUser.UserKey.ToString());
                success = resp.ApiCall.HttpStatusCode == 404;
                await Task.Delay(10);
            } while (!success);
        }

        [Fact]
        public async Task TestInitialDataSent()
        {
            bool calledOnInitialDataSent = false;
            ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestInitialDataSent",
                onInitialDataSent: (client, writeRelation, indexName) =>
                {
                    calledOnInitialDataSent = true;
                    return Task.CompletedTask;
                });
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT 
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            ElasticClient elasticClient = new ElasticClient(elasticSearchFixture.GetConnectionSettings());

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", lastUser.UserKey.ToString());
                success = resp.ApiCall.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            int testCount = 0;
            while (calledOnInitialDataSent == false)
            {
                Assert.True(testCount < 100);
                testCount++;
                await stream.SchedulerTick();
                await Task.Delay(10);
            }
        }

        [Fact]
        public async Task TestOnDataSent()
        {
            bool calledOnDataSent = false;
            ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestOnDataSent",
                onDataSent: (client, writeRelation, indexName, watermark) =>
                {
                    calledOnDataSent = true;
                    return Task.CompletedTask;
                });
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT 
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            ElasticClient elasticClient = new ElasticClient(elasticSearchFixture.GetConnectionSettings());

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", lastUser.UserKey.ToString());
                success = resp.ApiCall.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            int testCount = 0;
            while (calledOnDataSent == false)
            {
                Assert.True(testCount < 100);
                testCount++;
                await stream.SchedulerTick();
                await Task.Delay(10);
            }
        }

        [Fact]
        public async Task TestInsertWithCustomMappingIndexDoesNotExist()
        {
            ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsertWithCustomMappingIndexDoesNotExist", (properties) =>
            {
                properties["FirstName"] = new KeywordProperty();
            });
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT 
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            ElasticClient elasticClient = new ElasticClient(elasticSearchFixture.GetConnectionSettings());

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", lastUser.UserKey.ToString());
                success = resp.ApiCall.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

        }

        [Fact]
        public async Task TestInsertWithCustomMappingIndexExistsWithNoMappings()
        {
            ElasticClient elasticClient = new ElasticClient(elasticSearchFixture.GetConnectionSettings());
            elasticClient.Indices.Create("testindex");
            ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsertWithCustomMappingIndexExistsWithNoMappings", (properties) =>
            {
                properties["FirstName"] = new KeywordProperty();
            });
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT 
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", lastUser.UserKey.ToString());
                success = resp.ApiCall.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);
        }

        [Fact]
        public async Task TestInsertWithCustomMappingIndexExistsWithMappings()
        {
            ElasticClient elasticClient = new ElasticClient(elasticSearchFixture.GetConnectionSettings());
            elasticClient.Indices.Create("testindex", c => c.Map(m => m.Properties(p => p.Keyword(k => k.Name("FirstName")))));
            ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsertWithCustomMappingIndexExistsWithMappings", (properties) =>
            {
                properties["FirstName"] = new KeywordProperty();
            });
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT 
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", lastUser.UserKey.ToString());
                success = resp.ApiCall.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

        }

        [Fact]
        public async Task TestInsertWithCustomMappingIndexExistsWithMappingsCollision()
        {
            ElasticClient elasticClient = new ElasticClient(elasticSearchFixture.GetConnectionSettings());
            elasticClient.Indices.Delete("testindex");
            elasticClient.Indices.Create("testindex", c => c.Map(m => m.Properties(p => p.Text(k => k.Name("FirstName")))));
            ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsertWithCustomMappingIndexExistsWithMappingsCollision", (properties) =>
            {
                properties["FirstName"] = new KeywordProperty();
            });
            stream.Generate();

            var ex = await Assert.ThrowsAsync<FlowtideElasticsearchResponseException>(async () =>
            {
                await stream.StartStream(@"
                    INSERT INTO testindex
                    SELECT 
                        UserKey as _id,
                        FirstName,
                        LastName,
                        UserKey as pk
                    FROM users
                    ");
            });
        }
    }
}
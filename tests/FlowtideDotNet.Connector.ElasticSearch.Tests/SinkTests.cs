using Elasticsearch.Net;
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
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            ElasticClient elasticClient = new ElasticClient(elasticSearchFixture.GetConnectionSettings());

            bool success = false;
            do
            {
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", "15");
                success = resp.ApiCall.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);
            
        }

        [Fact]
        public async Task TestInsertWithUpdate()
        {
            ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsert");
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

            bool success = false;
            do
            {
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", "15");
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

            bool success = false;
            do
            {
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", "15");
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

            bool success = false;
            do
            {
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", "15");
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
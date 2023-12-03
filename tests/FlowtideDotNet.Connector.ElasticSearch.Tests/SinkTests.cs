using Elasticsearch.Net;
using FlowtideDotNet.Connector.CosmosDB.Tests;
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
                var resp = await elasticClient.LowLevel.GetAsync<StringResponse>("testindex", "5");
                success = resp.ApiCall.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);
            
        }
    }
}
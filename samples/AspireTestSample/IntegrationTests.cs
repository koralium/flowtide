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

using FlowtideDotNet.TestFramework;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using System.Text.Json.Serialization;

namespace AspireTestSample.Tests
{
    public class IntegrationTests
    {

        private class ElasticModel
        {
            [JsonPropertyName("orderdate")]
            public DateTime OrderDate { get; set; }

            [JsonPropertyName("firstname")]
            public string? FirstName { get; set; }

            [JsonPropertyName("lastname")]
            public string? LastName { get; set; }
        }

        [Fact]
        public async Task StreamInsertsCorrectlyIntoElasticsearch()
        {
            var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.AspireSamples>([
                "sample=SqlServer-To-Elasticsearch",
                "insert_count=1000",
                "test_mode=true"
            ]);

            await using var app = await appHost.BuildAsync();
            var resourceNotificationService = app.Services.GetRequiredService<ResourceNotificationService>();
            await app.StartAsync();

            var httpClient = app.CreateHttpClient("stream");
            await resourceNotificationService.WaitForResourceAsync("stream", KnownResourceStates.Running).WaitAsync(TimeSpan.FromSeconds(30));

            var streamMonitor = new StreamTestHttpMonitor(httpClient, "sqlservertoelastic");

            await streamMonitor.WaitForCheckpoint();

            var elasticConnStr = await app.GetConnectionStringAsync("elasticsearch");
            var elasticClient = new ElasticsearchClient(new Uri(elasticConnStr!));

            var doc = await elasticClient.GetAsync<ElasticModel>(new GetRequest("docs_1.0.0", "1100"));
            Assert.True(doc.IsSuccess());
        }
    }
}

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

using Microsoft.AspNetCore.Mvc.Testing;
using FlowtideDotNet.DependencyInjection;
using Microsoft.AspNetCore.TestHost;
using FlowtideDotNet.Core;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace FlowtideDotNet.TestFramework.Tests
{
    public class IntegrationTests : IDisposable
    {
        private readonly WebApplicationFactory<Program> _factory;
        private readonly TestDataSink _sink;
        private readonly TestDataTable _source;
        private readonly StreamTestMonitor _inProcessMonitor;

        public IntegrationTests()
        {
            _source = TestDataTable.Create(
                new { val = 0 },
                new { val = 1 },
                new { val = 2 },
                new { val = 3 },
                new { val = 4 }
            );

            _sink = new TestDataSink();
            _inProcessMonitor = new StreamTestMonitor();
            _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
            {
                b.ConfigureTestServices(services =>
                {
                    services.AddFlowtideStream("stream")
                    .AddConnectors(c =>
                    {
                        // Override connectors
                        c.AddTestDataTable("testtable", _source);
                        c.AddTestDataSink(".*", _sink);
                    })
                    .AddStorage(storage =>
                    {
                        // Change to temporary storage for unit tests
                        storage.AddTemporaryDevelopmentStorage();
                    })
                    .AddStreamTestMonitor(_inProcessMonitor);
                });
            });
        }

        public void Dispose()
        {
            _factory.Dispose();
        }

        [Fact]
        public async Task TestHttpMonitor()
        {
            var monitor = new StreamTestHttpMonitor(_factory.CreateClient(), "stream");
            await monitor.WaitForCheckpoint();

            Assert.True(_sink.IsCurrentDataEqual(new[]
            {
                new { val = 0 },
                new { val = 1 },
                new { val = 2 },
                new { val = 3 },
                new { val = 4 }
            }));
        }

        [Fact]
        public async Task TestInProcessMonitor()
        {
            _factory.CreateClient(); //Create a client to start the stream

            await _inProcessMonitor.WaitForCheckpoint();

            Assert.True(_sink.IsCurrentDataEqual(new[] 
            { 
                new { val = 0 },
                new { val = 1 },
                new { val = 2 },
                new { val = 3 },
                new { val = 4 }
            }));

            _source.AddRows(new { val = 5 });
            _source.RemoveRows(new { val = 3 });

            await _inProcessMonitor.WaitForCheckpoint();

            Assert.True(_sink.IsCurrentDataEqual(new[]
            {
                new { val = 0 },
                new { val = 1 },
                new { val = 2 },
                new { val = 4 },
                new { val = 5 }
            }));
        }

        [Fact]
        public async Task DataHubEndpointServesStreamLineage()
        {
            // Own directory, the shared default path stays locked.
            using var factory = _factory.WithWebHostBuilder(b =>
            {
                b.ConfigureTestServices(services =>
                {
                    services.AddFlowtideStream("stream")
                    .AddStorage(storage =>
                    {
                        storage.AddTemporaryDevelopmentStorage(o => o.DirectoryPath = $"./data/tempFiles/datahub{Guid.NewGuid():N}");
                    });
                });
            });
            var client = factory.CreateClient();
            await _inProcessMonitor.WaitForCheckpoint();

            using var config = JsonDocument.Parse(await client.GetStringAsync("/datahub/config"));
            Assert.Equal("true", config.RootElement.GetProperty("noCode").GetString());

            var scroll = await client.PostAsync("/datahub/api/graphql", JsonContent.Create(new { query = "query { scrollAcrossEntities }", variables = new { batchSize = 100 } }));
            Assert.Equal(HttpStatusCode.OK, scroll.StatusCode);
            using var scrollJson = JsonDocument.Parse(await scroll.Content.ReadAsByteArrayAsync());
            var urns = scrollJson.RootElement.GetProperty("data").GetProperty("scrollAcrossEntities").GetProperty("searchResults")
                .EnumerateArray().Select(x => x.GetProperty("entity").GetProperty("urn").GetString()).ToList();
            const string job = "urn:li:dataJob:(urn:li:dataFlow:(flowtide,stream,PROD),test.output)";
            const string input = "urn:li:dataset:(urn:li:dataPlatform:test,testtable,PROD)";
            Assert.Equal(["urn:li:dataFlow:(flowtide,stream,PROD)", job, "urn:li:dataPlatform:flowtide", "urn:li:dataset:(urn:li:dataPlatform:test,output,PROD)", input], urns);

            using var entity = JsonDocument.Parse(await client.GetStringAsync("/datahub/entitiesV2/" + Uri.EscapeDataString(job)));
            var inputOutput = entity.RootElement.GetProperty("aspects").GetProperty("dataJobInputOutput").GetProperty("value");
            Assert.Equal([input], inputOutput.GetProperty("inputDatasets").EnumerateArray().Select(x => x.GetString()));
            var lineage = Assert.Single(inputOutput.GetProperty("fineGrainedLineages").EnumerateArray());
            Assert.Equal($"urn:li:schemaField:({input},val)", Assert.Single(lineage.GetProperty("upstreams").EnumerateArray()).GetString());
        }
    }
}
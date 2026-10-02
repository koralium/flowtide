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
        public async Task DbtManifestServesStreamLineage()
        {
            // Own directory, the shared default path stays locked.
            using var factory = _factory.WithWebHostBuilder(b =>
            {
                b.ConfigureTestServices(services =>
                {
                    services.AddFlowtideStream("stream")
                    .AddStorage(storage =>
                    {
                        storage.AddTemporaryDevelopmentStorage(o => o.DirectoryPath = $"./data/tempFiles/dbt{Guid.NewGuid():N}");
                    });
                });
            });
            var client = factory.CreateClient();
            await _inProcessMonitor.WaitForCheckpoint();

            var response = await client.GetAsync("/dbt/manifest.json");
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            using var manifest = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync());
            var root = manifest.RootElement;
            var model = Assert.Single(root.GetProperty("nodes").EnumerateObject(), x => x.Value.GetProperty("alias").GetString() == "output").Value;
            var source = Assert.Single(root.GetProperty("sources").EnumerateObject(), x => x.Value.GetProperty("identifier").GetString() == "testtable").Value;
            Assert.Equal("model", model.GetProperty("resource_type").GetString());
            Assert.Contains($"FROM {source.GetProperty("relation_name").GetString()}", model.GetProperty("compiled_code").GetString());
            Assert.Contains(source.GetProperty("unique_id").GetString(), model.GetProperty("depends_on").GetProperty("nodes").EnumerateArray().Select(x => x.GetString()));

            Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/dbt/stream/manifest.json")).StatusCode);
            Assert.Equal(HttpStatusCode.OK, (await client.GetAsync("/dbt/catalog.json")).StatusCode);
            Assert.Equal(HttpStatusCode.NotFound, (await client.GetAsync("/dbt/missing/manifest.json")).StatusCode);
            Assert.Equal(HttpStatusCode.MethodNotAllowed, (await client.PostAsync("/dbt/manifest.json", null)).StatusCode);
        }
    }
}
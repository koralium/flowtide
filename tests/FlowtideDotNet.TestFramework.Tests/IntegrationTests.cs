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

namespace FlowtideDotNet.TestFramework.Tests
{
    public class IntegrationTests : IDisposable
    {
        private readonly WebApplicationFactory<Program> _factory;
        private TestDataSink _sink;
        private TestDataTable _source;
        private StreamTestMonitor _inProcessMonitor;

        public IntegrationTests()
        {
            _source = TestDataTable.Create(new[]
            {
                new { val = 0 },
                new { val = 1 },
                new { val = 2 },
                new { val = 3 },
                new { val = 4 }
            });

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
    }
}
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

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Nest;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testcontainers.Elasticsearch;

namespace FlowtideDotNet.Connector.CosmosDB.Tests
{
    public class ElasticSearchFixture : IAsyncLifetime
    {
        private IContainer? container;
        public async Task DisposeAsync()
        {
            await container.DisposeAsync();
        }

        private sealed class WaitUntil : IWaitUntil
        {
            private static readonly IEnumerable<string> Pattern = new string[2] { "\"message\":\"started", "\"message\": \"started\"" };

            public async Task<bool> UntilAsync(IContainer container)
            {
                string item = (await container.GetLogsAsync(default(DateTime), default(DateTime), timestampsEnabled: false).ConfigureAwait(continueOnCapturedContext: false)).Item1;
                return Pattern.Any(new Func<string, bool>(item.Contains));
            }
        }

        public async Task InitializeAsync()
        {
            container = new ContainerBuilder()
                .WithImage("elasticsearch:8.6.1")
                .WithPortBinding(9200, true)
                .WithPortBinding(9300, true)
                .WithEnvironment("discovery.type", "single-node")
                .WithEnvironment("xpack.security.enabled", "false")
                .WithWaitStrategy(Wait.ForUnixContainer().AddCustomWaitStrategy(new WaitUntil()))
                .Build();
            await container.StartAsync();
        }

        public ConnectionSettings GetConnectionSettings()
        {
            return new ConnectionSettings(new Uri($"http://localhost:{container.GetMappedPublicPort(9200)}"));
        }
    }
}

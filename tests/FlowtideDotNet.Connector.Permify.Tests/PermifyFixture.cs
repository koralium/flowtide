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
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Grpc.Net.Client;

namespace FlowtideDotNet.Connector.Permify.Tests
{
    public class PermifyFixture : IAsyncLifetime
    {
        private INetwork _network;
        private IContainer _container;
        private IContainer _postgresContainer;

        public PermifyFixture()
        {
            _network = new NetworkBuilder()
                .Build();

            _postgresContainer = new DotNet.Testcontainers.Builders.ContainerBuilder()
                .WithImage("postgres:16")
                .WithNetwork(_network)
                .WithNetworkAliases("postgres")
                .WithPortBinding(5432, 5432)
                .WithEnvironment("POSTGRES_USER", "postgres")
                .WithEnvironment("POSTGRES_PASSWORD", "password")
                .WithEnvironment("POSTGRES_DB", "permify")
                .WithCommand("-c", "fsync=off")
                .WithCommand("-c", "full_page_writes=off")
                .WithCommand("-c", "synchronous_commit=off")
                .WithCommand("-c", "track_commit_timestamp=on")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("database system is ready to accept connections"))
                .Build();

            var builder = new ContainerBuilder()
                .WithImage("ghcr.io/permify/permify")
                .WithNetwork(_network)
                .WithPortBinding(3478, true)
                .WithPortBinding(3476, true)
                .WithCommand("serve")
                .WithCommand("--database-engine", "postgres")
                .WithCommand("--database-uri", "postgres://postgres:password@postgres:5432/permify")
                .WithCommand("--database-max-open-connections", "20")
                .WithCommand("--database-max-idle-connections", "1")
                .WithCommand("--service-watch-enabled")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Fine-grained Authorization Service"));
            _container = builder.Build();

        }
        public async Task DisposeAsync()
        {
            await _container.DisposeAsync();
            await _postgresContainer.DisposeAsync();
        }

        public async Task InitializeAsync()
        {
            await _postgresContainer.StartAsync();
            await _container.StartAsync();
        }

        public GrpcChannel GetChannel()
        {
            return GrpcChannel.ForAddress($"http://localhost:{_container.GetMappedPublicPort(3478)}");
        }
    }
}

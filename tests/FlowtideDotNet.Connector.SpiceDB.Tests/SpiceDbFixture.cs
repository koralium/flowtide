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
using Grpc.Net.Client;

namespace FlowtideDotNet.Connector.SpiceDB.Tests
{
    public class SpiceDbFixture : IAsyncLifetime
    {
        private IContainer _container;
        public SpiceDbFixture()
        {
            var builder = new ContainerBuilder()
                .WithImage("authzed/spicedb")
                .WithPortBinding(50051, true)
                .WithCommand("serve-testing")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("grpc server started serving"));
            _container = builder.Build();
        }

        public async Task DisposeAsync()
        {
            await _container.DisposeAsync();
        }

        public async Task InitializeAsync()
        {
            await _container.StartAsync();
        }

        public GrpcChannel GetChannel()
        {
            return GrpcChannel.ForAddress($"http://localhost:{_container.GetMappedPublicPort(50051)}");
        }
    }
}

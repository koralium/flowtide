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
using Testcontainers.MongoDb;

namespace FlowtideDotNet.Connector.MongoDB.Tests
{
    public class MongoDBFixture : IAsyncLifetime
    {
        private readonly IContainer _mongoDbContainer = new ContainerBuilder()
                .WithImage("mongo:7.0")
                .WithPortBinding(27017, false)
                .WithCommand("--replSet", "rs0", "--bind_ip_all", "--port", "27017")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Waiting for connections"))
            .Build();

        private readonly MongoDbContainer _mongoDbContainer2 =
        new MongoDbBuilder().WithCommand("--replSet", "rs0", "--bind_ip_all", "--port", "27017").Build();

        public string GetConnectionString()
        {
            return $"mongodb://localhost:{_mongoDbContainer.GetMappedPublicPort(27017)}"; // $"mongodb://localhost:{_mongoDbContainer.GetMappedPublicPort(27017)}";
            //return _mongoDbContainer.GetConnectionString();
        }

        public async Task DisposeAsync()
        {
            await _mongoDbContainer.DisposeAsync();
        }

        public async Task InitializeAsync()
        {
            await _mongoDbContainer.StartAsync();
            var execResult = await _mongoDbContainer.ExecAsync(new List<string>() { "/bin/bash", "-c", "echo \"rs.initiate({_id:'rs0',members:[{_id:0,host:'host.docker.internal:27017'}]})\" | mongosh --port 27017 --quiet" });
        }
    }
}

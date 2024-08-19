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
using OpenFga.Sdk.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.OpenFGA.Tests
{
    public class OpenFGAFixture : IAsyncLifetime
    {
        private IContainer _container;
        public OpenFGAFixture()
        {
            var builder = new ContainerBuilder()
                .WithImage("openfga/openfga")
                .WithPortBinding(8080, false)
                .WithPortBinding(8081, false)
                .WithPortBinding(3000, false)
                .WithCommand("run")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("starting HTTP server"));
            _container = builder.Build();
        }
        public async Task DisposeAsync()
        {
            await _container.StopAsync();
            await _container.DisposeAsync();
        }

        public async Task InitializeAsync()
        {
            await _container.StartAsync();
        }

        public ClientConfiguration Configuration
        {
            get
            {
                return new ClientConfiguration()
                {
                    ApiUrl = $"http://localhost:{_container.GetMappedPublicPort(8080)}"
                };
            }
        }

        public string DashboardUrl
        {
            get
            {
                return $"http://localhost:{_container.GetMappedPublicPort(3000)}/playground";
            }
        }
    }
}

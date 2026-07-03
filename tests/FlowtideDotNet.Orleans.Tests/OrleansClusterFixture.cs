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

using FlowtideDotNet.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.TestingHost;

namespace FlowtideDotNet.Orleans.Tests
{
    /// <summary>
    /// Test cluster with a single silo, matching a localhost clustered deployment where the
    /// grains share a process and grain messages pass by reference. Multiple silos would
    /// require serialization support for the exchanged stream events.
    /// </summary>
    public sealed class OrleansClusterFixture : IDisposable
    {
        public TestCluster Cluster { get; }

        public OrleansClusterFixture()
        {
            var builder = new TestClusterBuilder(1);
            builder.AddSiloBuilderConfigurator<TestSiloConfigurator>();
            Cluster = builder.Build();
            Cluster.Deploy();
        }

        public void Dispose()
        {
            Cluster.StopAllSilos();
            Cluster.Dispose();
        }

        private sealed class TestSiloConfigurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                siloBuilder.AddMemoryGrainStorage("stream_metadata");
                siloBuilder.Services.AddFlowtideOrleans(connectors =>
                {
                    connectors.AddSource(new TestDataSourceFactory("*"));
                    connectors.AddSink(new TestDataSinkFactory("*"));
                }, (streamName, substreamName, storage) =>
                {
                    storage.AddTemporaryDevelopmentStorage(options =>
                    {
                        options.DirectoryPath = $"./temp/orleans_tests/{streamName}/{substreamName}";
                    });
                });
            }
        }
    }
}

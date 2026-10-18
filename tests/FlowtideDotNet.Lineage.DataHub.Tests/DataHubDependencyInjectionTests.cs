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

using FlowtideDotNet.Core.Sinks;
using FlowtideDotNet.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

namespace FlowtideDotNet.Lineage.DataHub.Tests
{
    public class DataHubDependencyInjectionTests
    {
        [Fact]
        public void StreamsShareOneStoreAndAreExpected()
        {
            var services = new ServiceCollection();
            services.AddFlowtideStream("a").AddDataHubLineage();
            services.AddFlowtideStream("b").AddDataHubLineage();
            services.AddFlowtideDataHubLineage();

            using var provider = services.BuildServiceProvider();
            var store = provider.GetRequiredService<DataHubLineageStore>();

            Assert.Single(services, x => x.ServiceType == typeof(DataHubLineageStore));
            Assert.Same(store, provider.GetRequiredService<DataHubLineageStore>());
            Assert.True(store.IsWarmingUp);
        }

        [Fact]
        public void ConfiguredOptionsReachTheStore()
        {
            var services = new ServiceCollection();
            services.AddFlowtideStream("a").AddDataHubLineage();
            services.AddFlowtideDataHubLineage(o => o.WarmupTimeout = TimeSpan.Zero);

            using var provider = services.BuildServiceProvider();

            Assert.False(provider.GetRequiredService<DataHubLineageStore>().IsWarmingUp);
        }

        [Fact]
        public void StreamConfigurationsAddUpOnTheSharedStore()
        {
            var services = new ServiceCollection();
            services.AddFlowtideStream("a").AddDataHubLineage(o => o.Env = "DEV");
            services.AddFlowtideStream("b").AddDataHubLineage(o => o.WarmupTimeout = TimeSpan.Zero);

            using var provider = services.BuildServiceProvider();
            var store = provider.GetRequiredService<DataHubLineageStore>();
            store.Register(LineageTestData.Snapshot([], [LineageTestData.Output("postgres", "t", [LineageTestData.Col("x")], new())]), "a");

            Assert.Single(services, x => x.ServiceType == typeof(DataHubLineageStore));
            Assert.False(store.IsWarmingUp);
            Assert.Contains("urn:li:dataFlow:(flowtide,a,DEV)", store.GetSnapshot().Urns);
        }

        [Fact]
        public void LaterStreamConfigurationWinsOnConflict()
        {
            var services = new ServiceCollection();
            services.AddFlowtideStream("a").AddDataHubLineage(o => o.Env = "DEV");
            services.AddFlowtideStream("b").AddDataHubLineage(o => o.Env = "TEST");

            using var provider = services.BuildServiceProvider();
            var store = provider.GetRequiredService<DataHubLineageStore>();
            store.Register(LineageTestData.Snapshot([], [LineageTestData.Output("postgres", "t", [LineageTestData.Col("x")], new())]), "a");

            Assert.Contains("urn:li:dataFlow:(flowtide,a,TEST)", store.GetSnapshot().Urns);
        }

        [Fact]
        public void InvalidEnvironmentFailsWhenTheStoreIsResolved()
        {
            var services = new ServiceCollection();
            services.AddFlowtideDataHubLineage(o => o.Env = "local");

            using var provider = services.BuildServiceProvider();

            Assert.Throws<ArgumentException>(() => provider.GetRequiredService<DataHubLineageStore>());
        }

        [Fact]
        public async Task BuildingTheStreamRegistersItInTheStore()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddFlowtideStream("di")
                .AddSqlTextAsPlan("INSERT INTO output SELECT 1 AS a")
                .AddConnectors(c => c.AddBlackholeSink("*"))
                .AddStorage(s => s.AddTemporaryDevelopmentStorage())
                .AddDataHubLineage();

            await using var provider = services.BuildServiceProvider();
            var store = provider.GetRequiredService<DataHubLineageStore>();
            Assert.True(store.IsWarmingUp);

            provider.GetRequiredKeyedService<FlowtideDotNet.Base.Engine.DataflowStream>("di");

            Assert.False(store.IsWarmingUp);
            Assert.Contains("urn:li:dataJob:(urn:li:dataFlow:(flowtide,di,PROD),blackhole.output)", store.GetSnapshot().Urns);
        }

        [Fact]
        public void UserRegisteredStoreWins()
        {
            var store = new DataHubLineageStore();
            var services = new ServiceCollection();
            services.AddSingleton(store);
            services.AddFlowtideStream("a").AddDataHubLineage();

            using var provider = services.BuildServiceProvider();

            Assert.Same(store, provider.GetRequiredService<DataHubLineageStore>());
        }
    }
}

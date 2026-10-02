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

using FlowtideDotNet.Core.Lineage.Dbt;
using FlowtideDotNet.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace FlowtideDotNet.AspNetCore.Tests
{
    public class DbtManifestDependencyInjectionTests
    {
        [Fact]
        public void StreamsShareOneStoreAndAreExpected()
        {
            var services = new ServiceCollection();
            services.AddFlowtideStream("a").AddDbtManifest();
            services.AddFlowtideStream("b").AddDbtManifest();
            services.AddFlowtideDbtManifest();

            using var provider = services.BuildServiceProvider();
            var store = provider.GetRequiredService<DbtManifestStore>();

            Assert.Single(services, x => x.ServiceType == typeof(DbtManifestStore));
            Assert.Same(store, provider.GetRequiredService<DbtManifestStore>());
            Assert.True(store.IsWarmingUp);
        }

        [Fact]
        public void ConfiguredOptionsReachTheStore()
        {
            var services = new ServiceCollection();
            services.AddFlowtideStream("a").AddDbtManifest();
            services.AddFlowtideDbtManifest(o => o.WarmupTimeout = TimeSpan.Zero);

            using var provider = services.BuildServiceProvider();

            Assert.False(provider.GetRequiredService<DbtManifestStore>().IsWarmingUp);
        }

        [Fact]
        public void StoreWithoutExpectedStreamsIsNotWarmingUp()
        {
            var services = new ServiceCollection();
            services.AddFlowtideDbtManifest();

            using var provider = services.BuildServiceProvider();

            Assert.False(provider.GetRequiredService<DbtManifestStore>().IsWarmingUp);
        }

        [Fact]
        public void UserRegisteredStoreWins()
        {
            var store = new DbtManifestStore();
            var services = new ServiceCollection();
            services.AddSingleton(store);
            services.AddFlowtideStream("a").AddDbtManifest();

            using var provider = services.BuildServiceProvider();

            Assert.Same(store, provider.GetRequiredService<DbtManifestStore>());
        }
    }
}

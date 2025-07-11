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

using FlowtideDotNet.AcceptanceTests.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.OptionsMonitorTests
{
    internal class TestOptions
    {
        public string? Name { get; set; }
    }

    internal class OptionsDataTestStream : FlowtideTestStream
    {
        private readonly IOptionsMonitor<TestOptions> options;

        public OptionsDataTestStream(IOptionsMonitor<TestOptions> options, string testName) : base(testName)
        {
            this.options = options;
        }

        protected override void AddReadResolvers(IConnectorManager manager)
        {
            manager.AddOptionsSource<TestOptions>("testtable", options);
        }
    }

    public class OptionsDataSourceTests
    {
        [Fact]
        public async Task TestOptionSource()
        {
            Dictionary<string, string?> optionsDict = new Dictionary<string, string?>();
            optionsDict["config:name"] = "hello";
            var configBuilder = new ConfigurationBuilder()
                .AddInMemoryCollection(optionsDict);
            var config = configBuilder.Build();
            ServiceCollection services = new ServiceCollection();

            services.AddOptions<TestOptions>()
                .Bind(config.GetSection("config"));

            var provider = services.BuildServiceProvider();

            var options = provider.GetRequiredService<IOptionsMonitor<TestOptions>>();

            var stream = new OptionsDataTestStream(options, nameof(TestOptionSource));

            await stream.StartStream(@"
                INSERT INTO output
                SELECT name FROM testtable
            ");

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] { new { name = "hello" } });

            config["config:name"] = "hello2";
            config.Reload();

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[] { new { name = "hello2" } });
        }
    }
}

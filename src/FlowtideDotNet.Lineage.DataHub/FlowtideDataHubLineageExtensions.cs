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
using FlowtideDotNet.Lineage.DataHub.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace FlowtideDotNet.Lineage.DataHub
{
    public static class FlowtideDataHubLineageExtensions
    {
        /// <summary>
        /// Registers the shared DataHub lineage store and options.
        /// </summary>
        /// <param name="services">The service collection to add to.</param>
        /// <param name="configure">Optional host wide options configuration.</param>
        /// <returns>The same service collection.</returns>
        public static IServiceCollection AddFlowtideDataHubLineage(this IServiceCollection services, Action<DataHubLineageOptions>? configure = null)
        {
            ArgumentNullException.ThrowIfNull(services);
            var optionsBuilder = services.AddOptions<DataHubLineageOptions>();
            if (configure != null)
            {
                optionsBuilder.Configure(configure);
            }
            // A store registered by the user wins.
            services.TryAddSingleton(provider =>
            {
                var store = new DataHubLineageStore(provider.GetRequiredService<IOptions<DataHubLineageOptions>>().Value);
                foreach (var expected in provider.GetServices<DataHubExpectedStream>())
                {
                    store.ExpectStream(expected.Name);
                }
                return store;
            });
            return services;
        }

        /// <summary>
        /// Publishes this stream's lineage to DataHub.
        /// </summary>
        /// <param name="builder">The stream builder to opt in.</param>
        /// <returns>The same builder for chaining.</returns>
        public static IFlowtideDIBuilder AddDataHubLineage(this IFlowtideDIBuilder builder)
        {
            ArgumentNullException.ThrowIfNull(builder);
            builder.Services.AddFlowtideDataHubLineage();
            // Early requests get 503 until this stream registers.
            builder.Services.AddSingleton(new DataHubExpectedStream(builder.StreamName));
            builder.AddCustomOptions((provider, flowtideBuilder) =>
            {
                flowtideBuilder.WithDataHubLineageStore(provider.GetRequiredService<DataHubLineageStore>());
            });
            return builder;
        }
    }
}

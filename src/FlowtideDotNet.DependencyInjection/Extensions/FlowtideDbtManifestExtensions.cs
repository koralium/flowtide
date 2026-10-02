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
using FlowtideDotNet.DependencyInjection.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace FlowtideDotNet.DependencyInjection
{
    public static class FlowtideDbtManifestExtensions
    {
        /// <summary>
        /// Registers the shared dbt manifest store and options.
        /// </summary>
        /// <param name="services">The service collection to add to.</param>
        /// <param name="configure">Optional host wide options configuration.</param>
        /// <returns>The same service collection.</returns>
        public static IServiceCollection AddFlowtideDbtManifest(this IServiceCollection services, Action<DbtManifestOptions>? configure = null)
        {
            ArgumentNullException.ThrowIfNull(services);
            var optionsBuilder = services.AddOptions<DbtManifestOptions>();
            if (configure != null)
            {
                optionsBuilder.Configure(configure);
            }
            // A store registered by the user wins.
            services.TryAddSingleton(provider =>
            {
                var store = new DbtManifestStore(provider.GetRequiredService<IOptions<DbtManifestOptions>>().Value);
                foreach (var expected in provider.GetServices<DbtExpectedStream>())
                {
                    store.ExpectStream(expected.Name);
                }
                return store;
            });
            return services;
        }

        /// <summary>
        /// Publishes this stream's lineage in the dbt manifest.
        /// </summary>
        /// <param name="builder">The stream builder to opt in.</param>
        /// <returns>The same builder for chaining.</returns>
        public static IFlowtideDIBuilder AddDbtManifest(this IFlowtideDIBuilder builder)
        {
            ArgumentNullException.ThrowIfNull(builder);
            builder.Services.AddFlowtideDbtManifest();
            // Early requests get 503 until this stream registers.
            builder.Services.AddSingleton(new DbtExpectedStream(builder.StreamName));
            builder.AddCustomOptions((provider, flowtideBuilder) =>
            {
                flowtideBuilder.WithDbtManifestStore(provider.GetRequiredService<DbtManifestStore>());
            });
            return builder;
        }
    }
}

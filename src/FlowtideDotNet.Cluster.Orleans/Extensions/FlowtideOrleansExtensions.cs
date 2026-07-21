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

using FlowtideDotNet.Core;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Cluster.Orleans;
using FlowtideDotNet.Cluster.Orleans.Internal;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class FlowtideOrleansExtensions
    {
        /// <summary>
        /// Registers Flowtide stream hosting for Orleans with the same connectors for every
        /// stream. Use the overload whose connectors callback receives the stream name to
        /// configure connectors per stream.
        /// </summary>
        public static IServiceCollection AddFlowtideOrleans(this IServiceCollection services,
            Action<IConnectorManager> connectors,
            Action<string, string, IFlowtideStorageBuilder> storageBuilder,
            Action<FlowtideOrleansOptions>? options = null)
        {
            return AddFlowtideOrleans(services, (streamName, connectorManager) => connectors(connectorManager), storageBuilder, options);
        }

        /// <summary>
        /// Registers Flowtide stream hosting for Orleans.
        /// </summary>
        /// <param name="connectors">
        /// Configures the connectors for a stream, called with the stream name whenever a
        /// grain needs the streams connectors. Must be deterministic per stream name, every
        /// substream grain of a stream builds its plan from the connectors it returns.
        /// </param>
        /// <param name="storageBuilder">
        /// Configures the state storage per substream, called with the stream name and the
        /// substream name. Every substream must get its own storage location.
        /// </param>
        /// <param name="options">Optional host wide options.</param>
        public static IServiceCollection AddFlowtideOrleans(this IServiceCollection services,
            Action<string, IConnectorManager> connectors,
            Action<string, string, IFlowtideStorageBuilder> storageBuilder,
            Action<FlowtideOrleansOptions>? options = null)
        {
            services.AddSingleton(new ConnectorManagerFactory(connectors));

            services.AddSingleton<Action<string, string, IFlowtideStorageBuilder>>(storageBuilder);

            var orleansOptions = new FlowtideOrleansOptions();
            options?.Invoke(orleansOptions);
            services.AddSingleton(orleansOptions);

            return services;
        }
    }
}

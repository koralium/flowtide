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

using FlowtideDotNet.AspNetCore.Internal;
using FlowtideDotNet.Core.Lineage.Dbt;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.AspNetCore.Extensions
{
    public static class FlowtideDbtManifestEndpointRouteBuilderExtensions
    {
        /// <summary>
        /// Maps dbt manifest and catalog routes from DI.
        /// </summary>
        /// <param name="endpoints">The route builder to map on.</param>
        /// <param name="route">Route prefix of the four routes.</param>
        /// <returns>The group, for authorization and other conventions.</returns>
        public static RouteGroupBuilder MapFlowtideDbtManifest(this IEndpointRouteBuilder endpoints, string route = "/dbt")
        {
            ArgumentNullException.ThrowIfNull(endpoints);
            var store = endpoints.ServiceProvider.GetService<DbtManifestStore>();
            if (store == null)
            {
                throw new InvalidOperationException("No DbtManifestStore is registered. Call AddDbtManifest() on a stream added with AddFlowtideStream, AddFlowtideDbtManifest() on the service collection, or pass a store to MapFlowtideDbtManifest.");
            }
            return endpoints.MapFlowtideDbtManifest(store, route);
        }

        /// <summary>
        /// Maps dbt manifest and catalog routes for a store.
        /// </summary>
        /// <param name="endpoints">The route builder to map on.</param>
        /// <param name="store">The store serving the artifacts.</param>
        /// <param name="route">Route prefix of the four routes.</param>
        /// <returns>The group, for authorization and other conventions.</returns>
        public static RouteGroupBuilder MapFlowtideDbtManifest(this IEndpointRouteBuilder endpoints, DbtManifestStore store, string route = "/dbt")
        {
            ArgumentNullException.ThrowIfNull(endpoints);
            ArgumentNullException.ThrowIfNull(store);
            ArgumentException.ThrowIfNullOrWhiteSpace(route);

            var logger = endpoints.ServiceProvider.GetService<ILoggerFactory>()?.CreateLogger("FlowtideDotNet.AspNetCore.DbtManifest") ?? NullLogger.Instance;

            var group = endpoints.MapGroup(route);
            group.MapGet("/manifest.json", (RequestDelegate)(context => DbtManifestEndpointHandler.HandleCombinedAsync(context, store, DbtEndpointArtifact.Manifest, logger)));
            group.MapGet("/catalog.json", (RequestDelegate)(context => DbtManifestEndpointHandler.HandleCombinedAsync(context, store, DbtEndpointArtifact.Catalog, logger)));
            group.MapGet("/{streamName}/manifest.json", (RequestDelegate)(context => DbtManifestEndpointHandler.HandleStreamAsync(context, store, DbtEndpointArtifact.Manifest, logger)));
            group.MapGet("/{streamName}/catalog.json", (RequestDelegate)(context => DbtManifestEndpointHandler.HandleStreamAsync(context, store, DbtEndpointArtifact.Catalog, logger)));
            return group;
        }
    }
}

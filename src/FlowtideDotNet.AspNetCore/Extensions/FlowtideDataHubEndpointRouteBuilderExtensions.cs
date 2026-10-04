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
using FlowtideDotNet.Core.Lineage.DataHub;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.AspNetCore.Extensions
{
    public static class FlowtideDataHubEndpointRouteBuilderExtensions
    {
        /// <summary>
        /// Maps the DataHub lineage routes from DI.
        /// </summary>
        /// <param name="endpoints">The route builder to map on.</param>
        /// <param name="route">Route prefix, the datahub_api server of the recipe.</param>
        /// <returns>The group, for authorization and other conventions.</returns>
        public static RouteGroupBuilder MapFlowtideDataHubLineage(this IEndpointRouteBuilder endpoints, string route = "/datahub")
        {
            ArgumentNullException.ThrowIfNull(endpoints);
            var store = endpoints.ServiceProvider.GetService<DataHubLineageStore>();
            if (store == null)
            {
                throw new InvalidOperationException("No DataHubLineageStore is registered. Call AddDataHubLineage() on a stream added with AddFlowtideStream, AddFlowtideDataHubLineage() on the service collection, or pass a store to MapFlowtideDataHubLineage.");
            }
            return endpoints.MapFlowtideDataHubLineage(store, route);
        }

        /// <summary>
        /// Maps the DataHub lineage routes for a store.
        /// </summary>
        /// <param name="endpoints">The route builder to map on.</param>
        /// <param name="store">The store serving the entities.</param>
        /// <param name="route">Route prefix, the datahub_api server of the recipe.</param>
        /// <returns>The group, for authorization and other conventions.</returns>
        public static RouteGroupBuilder MapFlowtideDataHubLineage(this IEndpointRouteBuilder endpoints, DataHubLineageStore store, string route = "/datahub")
        {
            ArgumentNullException.ThrowIfNull(endpoints);
            ArgumentNullException.ThrowIfNull(store);
            ArgumentException.ThrowIfNullOrWhiteSpace(route);

            var logger = endpoints.ServiceProvider.GetService<ILoggerFactory>()?.CreateLogger("FlowtideDotNet.AspNetCore.DataHubLineage") ?? NullLogger.Instance;

            var group = endpoints.MapGroup(route);
            group.MapGet("/config", (RequestDelegate)(context => DataHubEndpointHandler.HandleConfigAsync(context, store)));
            group.MapPost("/api/graphql", (RequestDelegate)(context => DataHubEndpointHandler.HandleGraphQlAsync(context, store, logger)));
            group.MapGet("/entitiesV2/{**urn}", (RequestDelegate)(context => DataHubEndpointHandler.HandleEntityAsync(context, store, logger)));
            group.MapPost("/aspects", (RequestDelegate)(context => DataHubEndpointHandler.HandleAspectsAsync(context)));
            return group;
        }
    }
}

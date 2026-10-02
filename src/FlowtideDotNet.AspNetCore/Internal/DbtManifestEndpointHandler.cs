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
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Net.Http.Headers;

namespace FlowtideDotNet.AspNetCore.Internal
{
    internal enum DbtEndpointArtifact
    {
        Manifest,
        Catalog
    }

    internal static class DbtManifestEndpointHandler
    {
        public static Task HandleCombinedAsync(HttpContext context, DbtManifestStore store, DbtEndpointArtifact artifact, ILogger logger)
        {
            return WriteAsync(context, store, logger, () => artifact == DbtEndpointArtifact.Manifest ? store.GetManifest() : store.GetCatalog());
        }

        public static Task HandleStreamAsync(HttpContext context, DbtManifestStore store, DbtEndpointArtifact artifact, ILogger logger)
        {
            return WriteAsync(context, store, logger, () =>
            {
                if (context.Request.RouteValues["streamName"] is not string streamName)
                {
                    return null;
                }
                DbtArtifact? result;
                var found = artifact == DbtEndpointArtifact.Manifest ? store.TryGetManifest(streamName, out result) : store.TryGetCatalog(streamName, out result);
                return found ? result : null;
            });
        }

        internal static async Task WriteAsync(HttpContext context, DbtManifestStore store, ILogger logger, Func<DbtArtifact?> getArtifact)
        {
            // Partial manifests would soft delete catalog entities.
            if (store.IsWarmingUp)
            {
                context.Response.Headers.RetryAfter = "10";
                await WriteTextAsync(context, StatusCodes.Status503ServiceUnavailable, "The dbt manifest is warming up.");
                return;
            }

            DbtArtifact? artifact;
            try
            {
                artifact = getArtifact();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to generate the dbt artifact for {Path}.", context.Request.Path);
                await WriteTextAsync(context, StatusCodes.Status500InternalServerError, "Failed to generate the dbt artifact.");
                return;
            }

            // Never echo the requested stream name.
            if (artifact == null)
            {
                await WriteTextAsync(context, StatusCodes.Status404NotFound, "Unknown stream.");
                return;
            }

            context.Response.Headers.ETag = artifact.ETag;
            context.Response.Headers.CacheControl = "no-cache";
            if (IsNotModified(context.Request, artifact.ETag))
            {
                context.Response.StatusCode = StatusCodes.Status304NotModified;
                return;
            }

            context.Response.StatusCode = StatusCodes.Status200OK;
            context.Response.ContentType = "application/json";
            context.Response.ContentLength = artifact.Utf8Json.Length;
            await context.Response.Body.WriteAsync(artifact.Utf8Json, context.RequestAborted);
        }

        private static bool IsNotModified(HttpRequest request, string eTag)
        {
            var ifNoneMatch = request.GetTypedHeaders().IfNoneMatch;
            if (ifNoneMatch.Count == 0)
            {
                return false;
            }
            var current = EntityTagHeaderValue.Parse(eTag);
            foreach (var candidate in ifNoneMatch)
            {
                if (candidate.Equals(EntityTagHeaderValue.Any) || candidate.Compare(current, useStrongComparison: false))
                {
                    return true;
                }
            }
            return false;
        }

        private static Task WriteTextAsync(HttpContext context, int statusCode, string text)
        {
            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "text/plain; charset=utf-8";
            context.Response.Headers.CacheControl = "no-store";
            return context.Response.WriteAsync(text, context.RequestAborted);
        }
    }
}

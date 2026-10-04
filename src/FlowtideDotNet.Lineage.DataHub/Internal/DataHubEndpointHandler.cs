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

using FlowtideDotNet.Lineage.DataHub;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    // The GMS calls made by the DataHub source with pull_from_datahub_api.
    internal static class DataHubEndpointHandler
    {
        private const int DefaultPageSize = 10000;
        private const int MaxRequestBytes = 1024 * 1024;
        private const string EntitiesV2Marker = "/entitiesV2/";

        // noCode passes the GMS check, statefulIngestionCapable the checkpoint provider.
        private static readonly byte[] s_config = Encoding.UTF8.GetBytes("{\"noCode\":\"true\",\"statefulIngestionCapable\":true,\"datahub\":{\"serverType\":\"flowtide\"}}");

        private static readonly byte[] s_emptyTimeseries = Encoding.UTF8.GetBytes("{\"value\":{\"values\":[]}}");

        public static Task HandleConfigAsync(HttpContext context, DataHubLineageStore store)
        {
            if (store.IsWarmingUp)
            {
                return WriteWarmingUpAsync(context);
            }
            return WriteJsonAsync(context, StatusCodes.Status200OK, s_config);
        }

        public static async Task HandleGraphQlAsync(HttpContext context, DataHubLineageStore store, ILogger logger)
        {
            if (store.IsWarmingUp)
            {
                await WriteWarmingUpAsync(context);
                return;
            }

            ScrollRequest? request;
            try
            {
                request = await ReadScrollRequestAsync(context);
            }
            // Invalid UTF-16 in a string surfaces as InvalidOperationException.
            catch (Exception ex) when (ex is JsonException || ex is InvalidDataException || ex is InvalidOperationException)
            {
                await WriteGraphQlErrorAsync(context, StatusCodes.Status400BadRequest, "The request is not a GraphQL JSON request.");
                return;
            }
            if (request == null)
            {
                await WriteGraphQlErrorAsync(context, StatusCodes.Status200OK, "Only scrollAcrossEntities is supported.");
                return;
            }
            if (!TryParseScrollId(request.ScrollId, out var offset) || request.Count <= 0)
            {
                await WriteGraphQlErrorAsync(context, StatusCodes.Status200OK, "Invalid scrollId or count.");
                return;
            }

            if (!TryGetSnapshot(context, store, logger, out var snapshot))
            {
                await WriteMessageAsync(context, StatusCodes.Status500InternalServerError, "Failed to generate the DataHub lineage.");
                return;
            }

            IReadOnlyList<string> urns = snapshot.Urns;
            if (request.Types != null)
            {
                urns = urns.Where(x => request.Types.Contains(GetGraphQlEntityType(x))).ToList();
            }

            // Offsets index the current snapshot, a change between pages is converged by the next run.
            var count = request.Count ?? DefaultPageSize;
            var start = Math.Min(offset, urns.Count);
            var end = (int)Math.Min((long)start + count, urns.Count);

            var buffer = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                writer.WriteStartObject("data");
                writer.WriteStartObject("scrollAcrossEntities");
                if (end < urns.Count)
                {
                    writer.WriteString("nextScrollId", end.ToString(CultureInfo.InvariantCulture));
                }
                else
                {
                    writer.WriteNull("nextScrollId");
                }
                writer.WriteStartArray("searchResults");
                for (int i = start; i < end; i++)
                {
                    writer.WriteStartObject();
                    writer.WriteStartObject("entity");
                    writer.WriteString("urn", urns[i]);
                    writer.WriteEndObject();
                    writer.WriteStartArray("extraProperties");
                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
                writer.WriteEndObject();
                writer.WriteEndObject();
            }
            await WriteJsonAsync(context, StatusCodes.Status200OK, buffer.WrittenMemory);
        }

        public static async Task HandleEntityAsync(HttpContext context, DataHubLineageStore store, ILogger logger)
        {
            if (store.IsWarmingUp)
            {
                await WriteWarmingUpAsync(context);
                return;
            }

            var urn = GetRequestedUrn(context);
            if (string.IsNullOrEmpty(urn))
            {
                await WriteMessageAsync(context, StatusCodes.Status400BadRequest, "Missing urn.");
                return;
            }
            if (!TryGetSnapshot(context, store, logger, out var snapshot))
            {
                await WriteMessageAsync(context, StatusCodes.Status500InternalServerError, "Failed to generate the DataHub lineage.");
                return;
            }

            if (snapshot.TryGetEntity(urn, out var entity))
            {
                await WriteJsonAsync(context, StatusCodes.Status200OK, entity);
                return;
            }

            // Like GMS an unknown urn has no aspects, so a run never fails on a removed entity.
            var buffer = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                writer.WriteString("entityName", GetEntityName(urn));
                writer.WriteString("urn", urn);
                writer.WriteStartObject("aspects");
                writer.WriteEndObject();
                writer.WriteEndObject();
            }
            await WriteJsonAsync(context, StatusCodes.Status200OK, buffer.WrittenMemory);
        }

        // Checkpoint reads find nothing, writes are refused.
        public static Task HandleAspectsAsync(HttpContext context)
        {
            var action = context.Request.Query["action"].ToString();
            if (string.Equals(action, "getTimeseriesAspectValues", StringComparison.Ordinal))
            {
                return WriteJsonAsync(context, StatusCodes.Status200OK, s_emptyTimeseries);
            }
            return WriteMessageAsync(context, StatusCodes.Status400BadRequest, "The Flowtide DataHub endpoint is read-only.");
        }

        private sealed class ScrollRequest
        {
            public string? ScrollId { get; init; }

            public int? Count { get; init; }

            public HashSet<string>? Types { get; init; }
        }

        // Null when the query is not a scroll.
        private static async Task<ScrollRequest?> ReadScrollRequestAsync(HttpContext context)
        {
            if (context.Request.ContentLength > MaxRequestBytes)
            {
                throw new InvalidDataException("The request body is too large.");
            }
            using var body = new MemoryStream();
            var chunk = new byte[16 * 1024];
            int read;
            while ((read = await context.Request.Body.ReadAsync(chunk, context.RequestAborted)) > 0)
            {
                if (body.Length + read > MaxRequestBytes)
                {
                    throw new InvalidDataException("The request body is too large.");
                }
                body.Write(chunk, 0, read);
            }
            using var document = JsonDocument.Parse(body.GetBuffer().AsMemory(0, (int)body.Length));
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object ||
                !root.TryGetProperty("query", out var query) ||
                query.ValueKind != JsonValueKind.String ||
                !query.GetString()!.Contains("scrollAcrossEntities", StringComparison.Ordinal))
            {
                return null;
            }

            string? scrollId = null;
            int? count = null;
            HashSet<string>? types = null;
            if (root.TryGetProperty("variables", out var variables) && variables.ValueKind == JsonValueKind.Object)
            {
                if (variables.TryGetProperty("scrollId", out var scrollIdElement) && scrollIdElement.ValueKind == JsonValueKind.String)
                {
                    scrollId = scrollIdElement.GetString();
                }
                if ((variables.TryGetProperty("batchSize", out var countElement) || variables.TryGetProperty("count", out countElement)) &&
                    countElement.ValueKind == JsonValueKind.Number)
                {
                    // Out of range or fractional counts are rejected as invalid.
                    count = countElement.TryGetInt32(out var value) ? value : 0;
                }
                if (variables.TryGetProperty("types", out var typesElement) && typesElement.ValueKind == JsonValueKind.Array)
                {
                    types = typesElement.EnumerateArray().Where(x => x.ValueKind == JsonValueKind.String).Select(x => x.GetString()!).ToHashSet(StringComparer.Ordinal);
                }
            }
            return new ScrollRequest()
            {
                ScrollId = scrollId,
                Count = count,
                Types = types
            };
        }

        private static bool TryParseScrollId(string? scrollId, out int offset)
        {
            if (scrollId == null)
            {
                offset = 0;
                return true;
            }
            return int.TryParse(scrollId, NumberStyles.None, CultureInfo.InvariantCulture, out offset);
        }

        private static bool TryGetSnapshot(HttpContext context, DataHubLineageStore store, ILogger logger, out DataHubSnapshot snapshot)
        {
            try
            {
                snapshot = store.GetSnapshot();
                return true;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to generate the DataHub lineage for {Path}.", context.Request.Path);
                snapshot = null!;
                return false;
            }
        }

        // The client percent-encodes the whole urn, slashes included, so decode it from the raw target once.
        // The last marker is the route's own, a prefix may contain one too.
        private static string? GetRequestedUrn(HttpContext context)
        {
            var rawTarget = context.Features.Get<IHttpRequestFeature>()?.RawTarget;
            if (!string.IsNullOrEmpty(rawTarget))
            {
                var queryStart = rawTarget.IndexOf('?');
                var rawPath = queryStart < 0 ? rawTarget : rawTarget.Substring(0, queryStart);
                var markerIndex = rawPath.LastIndexOf(EntitiesV2Marker, StringComparison.OrdinalIgnoreCase);
                if (markerIndex >= 0)
                {
                    return Uri.UnescapeDataString(rawPath.Substring(markerIndex + EntitiesV2Marker.Length));
                }
            }
            return context.Request.RouteValues["urn"] as string;
        }

        private static string GetGraphQlEntityType(string urn)
        {
            return GetEntityName(urn) switch
            {
                "dataset" => "DATASET",
                "dataFlow" => "DATA_FLOW",
                "dataJob" => "DATA_JOB",
                "dataPlatform" => "DATA_PLATFORM",
                _ => string.Empty
            };
        }

        private static string GetEntityName(string urn)
        {
            const string prefix = "urn:li:";
            if (!urn.StartsWith(prefix, StringComparison.Ordinal))
            {
                return string.Empty;
            }
            var end = urn.IndexOf(':', prefix.Length);
            return end < 0 ? string.Empty : urn.Substring(prefix.Length, end - prefix.Length);
        }

        private static Task WriteWarmingUpAsync(HttpContext context)
        {
            context.Response.Headers.RetryAfter = "10";
            return WriteMessageAsync(context, StatusCodes.Status503ServiceUnavailable, "The DataHub lineage is warming up.");
        }

        private static Task WriteGraphQlErrorAsync(HttpContext context, int statusCode, string message)
        {
            var buffer = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                writer.WriteStartArray("errors");
                writer.WriteStartObject();
                writer.WriteString("message", message);
                writer.WriteEndObject();
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
            return WriteJsonAsync(context, statusCode, buffer.WrittenMemory);
        }

        private static Task WriteMessageAsync(HttpContext context, int statusCode, string message)
        {
            var buffer = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                writer.WriteString("message", message);
                writer.WriteEndObject();
            }
            return WriteJsonAsync(context, statusCode, buffer.WrittenMemory);
        }

        private static async Task WriteJsonAsync(HttpContext context, int statusCode, ReadOnlyMemory<byte> utf8Json)
        {
            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "application/json";
            context.Response.Headers.CacheControl = "no-store";
            context.Response.ContentLength = utf8Json.Length;
            await context.Response.Body.WriteAsync(utf8Json, context.RequestAborted);
        }
    }
}

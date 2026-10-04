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

using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Core.Lineage.DataHub;
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Substrait.Type;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using System.Text;
using System.Text.Json;
using Xunit;

namespace FlowtideDotNet.AspNetCore.Tests
{
    public class DataHubEndpointTests
    {
        private const string ScrollQuery = "query scrollUrnsWithFilters($batchSize: Int!, $scrollId: String) { scrollAcrossEntities(input: {query: \"*\", count: $batchSize, scrollId: $scrollId}) { nextScrollId searchResults { entity { urn } } } }";
        private const string Flow = "urn:li:dataFlow:(flowtide,orders,PROD)";
        private const string Job = "urn:li:dataJob:(urn:li:dataFlow:(flowtide,orders,PROD),kafka.out/v1)";
        private const string Input = "urn:li:dataset:(urn:li:dataPlatform:kafka,in%2C1,PROD)";
        private const string Output = "urn:li:dataset:(urn:li:dataPlatform:kafka,out/v1,PROD)";

        private static readonly (string Pattern, string Method)[] Routes =
        [
            ("/datahub/config", "GET"),
            ("/datahub/api/graphql", "POST"),
            ("/datahub/entitiesV2/{**urn}", "GET"),
            ("/datahub/aspects", "POST")
        ];

        [Fact]
        public async Task MapsFourRoutesWithAuthorization()
        {
            await using var app = WebApplication.CreateBuilder().Build();
            app.MapFlowtideDataHubLineage(new DataHubLineageStore()).RequireAuthorization();

            var endpoints = GetEndpoints(app);

            Assert.Equal(Routes.Select(x => x.Pattern).Order(), endpoints.Select(x => x.RoutePattern.RawText).Order());
            foreach (var endpoint in endpoints)
            {
                var method = Routes.Single(x => x.Pattern == endpoint.RoutePattern.RawText).Method;
                Assert.Equal([method], endpoint.Metadata.GetRequiredMetadata<IHttpMethodMetadata>().HttpMethods);
                Assert.NotEmpty(endpoint.Metadata.GetOrderedMetadata<IAuthorizeData>());
            }
        }

        [Fact]
        public async Task CustomRoutePrefixIsUsed()
        {
            await using var app = WebApplication.CreateBuilder().Build();
            app.MapFlowtideDataHubLineage(new DataHubLineageStore(), "/lineage/datahub");

            var patterns = GetEndpoints(app).Select(x => x.RoutePattern.RawText).Order();

            Assert.Equal(Routes.Select(x => "/lineage" + x.Pattern).Order(), patterns);
        }

        [Fact]
        public async Task DependencyInjectionOverloadWithoutStoreThrows()
        {
            await using var app = WebApplication.CreateBuilder().Build();

            var ex = Assert.Throws<InvalidOperationException>(() => app.MapFlowtideDataHubLineage());

            Assert.Contains("AddDataHubLineage", ex.Message);
        }

        [Fact]
        public async Task DependencyInjectionOverloadUsesRegisteredStore()
        {
            var builder = WebApplication.CreateBuilder();
            builder.Services.AddFlowtideDataHubLineage();
            await using var app = builder.Build();
            Register(app.Services.GetRequiredService<DataHubLineageStore>());
            app.MapFlowtideDataHubLineage();

            var context = await InvokeAsync(app, "/datahub/api/graphql", HttpMethods.Post, ScrollBody(null, null));

            Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
            Assert.Equal([Flow, Job, Input, Output], ScrollUrns(context));
        }

        [Fact]
        public async Task ConfigPassesTheGmsChecks()
        {
            await using var app = MapStore(new DataHubLineageStore());

            var context = await InvokeAsync(app, "/datahub/config", HttpMethods.Get);

            Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
            Assert.Equal("application/json", context.Response.ContentType);
            var root = ReadJson(context);
            // The CLI compares noCode to the string "true".
            Assert.Equal("true", root.GetProperty("noCode").GetString());
            Assert.True(root.GetProperty("statefulIngestionCapable").GetBoolean());
        }

        [Fact]
        public async Task ScrollPagesThroughEveryUrn()
        {
            var store = new DataHubLineageStore();
            Register(store);
            await using var app = MapStore(store);

            var first = await InvokeAsync(app, "/datahub/api/graphql", HttpMethods.Post, ScrollBody(null, 3));
            var nextScrollId = ReadJson(first).GetProperty("data").GetProperty("scrollAcrossEntities").GetProperty("nextScrollId").GetString();
            var second = await InvokeAsync(app, "/datahub/api/graphql", HttpMethods.Post, ScrollBody(nextScrollId, 3));

            Assert.Equal([Flow, Job, Input], ScrollUrns(first));
            Assert.NotNull(nextScrollId);
            Assert.Equal([Output], ScrollUrns(second));
            Assert.Equal(JsonValueKind.Null, ReadJson(second).GetProperty("data").GetProperty("scrollAcrossEntities").GetProperty("nextScrollId").ValueKind);
        }

        [Fact]
        public async Task ScrollFiltersByEntityType()
        {
            var store = new DataHubLineageStore();
            Register(store);
            await using var app = MapStore(store);
            var body = JsonSerializer.Serialize(new { query = ScrollQuery, variables = new { batchSize = 100, types = new[] { "DATA_JOB", "DATA_FLOW" } } });

            var context = await InvokeAsync(app, "/datahub/api/graphql", HttpMethods.Post, body);

            Assert.Equal([Flow, Job], ScrollUrns(context));
        }

        [Fact]
        public async Task ScrollFiltersDatasets()
        {
            var store = new DataHubLineageStore();
            Register(store);
            await using var app = MapStore(store);
            var body = JsonSerializer.Serialize(new { query = ScrollQuery, variables = new { batchSize = 100, types = new[] { "DATASET" } } });

            var context = await InvokeAsync(app, "/datahub/api/graphql", HttpMethods.Post, body);

            Assert.Equal([Input, Output], ScrollUrns(context));
        }

        [Theory]
        [InlineData("{\"query\":\"query { me { corpUser { urn } } }\"}", StatusCodes.Status200OK)]
        [InlineData("not json", StatusCodes.Status400BadRequest)]
        [InlineData("{\"query\":\"query { scrollAcrossEntities }\",\"variables\":{\"batchSize\":1.5}}", StatusCodes.Status200OK)]
        [InlineData("{\"query\":\"query { scrollAcrossEntities }\",\"variables\":{\"batchSize\":2147483648}}", StatusCodes.Status200OK)]
        [InlineData("{\"query\":\"query { scrollAcrossEntities }\",\"variables\":{\"scrollId\":\"\\ud800\"}}", StatusCodes.Status400BadRequest)]
        [InlineData("{\"query\":\"query { scrollAcrossEntities }\",\"variables\":{\"scrollId\":\"abc\"}}", StatusCodes.Status200OK)]
        [InlineData("{\"query\":\"query { scrollAcrossEntities }\",\"variables\":{\"batchSize\":0}}", StatusCodes.Status200OK)]
        public async Task UnsupportedGraphQlReturnsErrors(string body, int statusCode)
        {
            var store = new DataHubLineageStore();
            Register(store);
            await using var app = MapStore(store);

            var context = await InvokeAsync(app, "/datahub/api/graphql", HttpMethods.Post, body);

            Assert.Equal(statusCode, context.Response.StatusCode);
            Assert.NotEmpty(ReadJson(context).GetProperty("errors").EnumerateArray());
        }

        [Theory]
        [InlineData(Flow)]
        [InlineData(Job)]
        [InlineData(Input)]
        [InlineData(Output)]
        public async Task EntityIsDecodedFromTheRawTargetOnce(string urn)
        {
            var store = new DataHubLineageStore();
            Register(store);
            await using var app = MapStore(store);

            // The CLI percent-encodes the whole urn, so %2C in a name arrives as %252C.
            var context = await InvokeAsync(app, "/datahub/entitiesV2/{**urn}", HttpMethods.Get, rawTarget: "/datahub/entitiesV2/" + Uri.EscapeDataString(urn));

            Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
            Assert.True(store.GetSnapshot().TryGetEntity(urn, out var expected));
            Assert.Equal(expected.ToArray(), ReadBody(context));
        }

        [Fact]
        public async Task EntityIgnoresTheQueryString()
        {
            var store = new DataHubLineageStore();
            Register(store);
            await using var app = MapStore(store);

            var context = await InvokeAsync(app, "/datahub/entitiesV2/{**urn}", HttpMethods.Get, rawTarget: "/datahub/entitiesV2/" + Uri.EscapeDataString(Output) + "?aspects=List(status)");

            Assert.Equal(Output, ReadJson(context).GetProperty("urn").GetString());
        }

        [Theory]
        [InlineData("/lineage/entitiesV2", "/lineage/entitiesV2/entitiesV2/")]
        [InlineData("/datahub", "/datahub/EntitiesV2/")]
        public async Task EntityUsesTheLastMarkerIgnoringCase(string prefix, string rawPrefix)
        {
            var store = new DataHubLineageStore();
            Register(store);
            await using var app = WebApplication.CreateBuilder().Build();
            app.MapFlowtideDataHubLineage(store, prefix);

            var context = await InvokeAsync(app, prefix + "/entitiesV2/{**urn}", HttpMethods.Get, rawTarget: rawPrefix + Uri.EscapeDataString(Output));

            Assert.Equal(Output, ReadJson(context).GetProperty("urn").GetString());
            Assert.NotEmpty(ReadJson(context).GetProperty("aspects").EnumerateObject());
        }

        [Fact]
        public async Task EntityGenerationFailureReturnsServerError()
        {
            var store = new DataHubLineageStore(new DataHubLineageOptions() { DatasetResolver = _ => throw new InvalidOperationException("boom") });
            Register(store);
            await using var app = MapStore(store);

            var context = await InvokeAsync(app, "/datahub/entitiesV2/{**urn}", HttpMethods.Get, rawTarget: "/datahub/entitiesV2/" + Uri.EscapeDataString(Flow));

            Assert.Equal(StatusCodes.Status500InternalServerError, context.Response.StatusCode);
        }

        [Fact]
        public async Task OversizedGraphQlBodyIsRejected()
        {
            await using var app = MapStore(new DataHubLineageStore());
            var body = "{\"query\":\"" + new string('a', 1024 * 1024) + "\"}";

            var context = await InvokeAsync(app, "/datahub/api/graphql", HttpMethods.Post, body);

            Assert.Equal(StatusCodes.Status400BadRequest, context.Response.StatusCode);
        }

        [Fact]
        public async Task EntityFallsBackToTheRouteValue()
        {
            var store = new DataHubLineageStore();
            Register(store);
            await using var app = MapStore(store);

            var context = await InvokeAsync(app, "/datahub/entitiesV2/{**urn}", HttpMethods.Get, urnRouteValue: Flow);

            Assert.Equal(Flow, ReadJson(context).GetProperty("urn").GetString());
        }

        [Fact]
        public async Task UnknownEntityHasNoAspects()
        {
            await using var app = MapStore(new DataHubLineageStore());
            const string urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,gone,PROD)";

            var context = await InvokeAsync(app, "/datahub/entitiesV2/{**urn}", HttpMethods.Get, rawTarget: "/datahub/entitiesV2/" + Uri.EscapeDataString(urn));

            Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
            var root = ReadJson(context);
            Assert.Equal("dataset", root.GetProperty("entityName").GetString());
            Assert.Equal(urn, root.GetProperty("urn").GetString());
            Assert.Empty(root.GetProperty("aspects").EnumerateObject());
        }

        [Fact]
        public async Task CheckpointReadsAreEmptyAndWritesAreRefused()
        {
            await using var app = MapStore(new DataHubLineageStore());

            var read = await InvokeAsync(app, "/datahub/aspects", HttpMethods.Post, "{}", query: "?action=getTimeseriesAspectValues");
            var write = await InvokeAsync(app, "/datahub/aspects", HttpMethods.Post, "{}", query: "?action=ingestProposal");

            Assert.Equal(StatusCodes.Status200OK, read.Response.StatusCode);
            Assert.Empty(ReadJson(read).GetProperty("value").GetProperty("values").EnumerateArray());
            Assert.Equal(StatusCodes.Status400BadRequest, write.Response.StatusCode);
            Assert.Contains("read-only", ReadJson(write).GetProperty("message").GetString());
        }

        [Theory]
        [InlineData("/datahub/config")]
        [InlineData("/datahub/api/graphql")]
        [InlineData("/datahub/entitiesV2/{**urn}")]
        public async Task WarmingUpReturnsServiceUnavailable(string pattern)
        {
            var store = new DataHubLineageStore();
            store.ExpectStream("orders");
            await using var app = MapStore(store);
            var method = pattern.EndsWith("graphql", StringComparison.Ordinal) ? HttpMethods.Post : HttpMethods.Get;

            var context = await InvokeAsync(app, pattern, method, ScrollBody(null, null), urnRouteValue: Flow);

            Assert.Equal(StatusCodes.Status503ServiceUnavailable, context.Response.StatusCode);
            Assert.Equal("10", context.Response.Headers.RetryAfter.ToString());
        }

        [Fact]
        public async Task GenerationFailureReturnsServerError()
        {
            var store = new DataHubLineageStore(new DataHubLineageOptions() { DatasetResolver = _ => throw new InvalidOperationException("boom") });
            Register(store);
            await using var app = MapStore(store);

            var context = await InvokeAsync(app, "/datahub/api/graphql", HttpMethods.Post, ScrollBody(null, null));

            Assert.Equal(StatusCodes.Status500InternalServerError, context.Response.StatusCode);
        }

        [Fact]
        public async Task KestrelPassesEncodedUrnsThrough()
        {
            var store = new DataHubLineageStore();
            Register(store);
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            await using var app = builder.Build();
            app.MapFlowtideDataHubLineage(store);
            await app.StartAsync();
            using var client = new HttpClient() { BaseAddress = new Uri(app.Urls.First()) };

            foreach (var urn in new[] { Job, Input, Output })
            {
                var body = await client.GetStringAsync("/datahub/entitiesV2/" + Uri.EscapeDataString(urn));

                Assert.Equal(urn, JsonDocument.Parse(body).RootElement.GetProperty("urn").GetString());
                Assert.NotEmpty(JsonDocument.Parse(body).RootElement.GetProperty("aspects").EnumerateObject());
            }
            await app.StopAsync();
        }

        // One stream writing kafka "out/v1" from "in,1".
        private static void Register(DataHubLineageStore store)
        {
            var input = new StreamLineageInput()
            {
                Key = "in,1",
                NameParts = ["in,1"],
                Namespace = "kafka",
                TableName = "in,1",
                PlanColumns = [new LineageColumn("x", new Int64Type())]
            };
            var output = new StreamLineageOutput()
            {
                Key = "out/v1",
                NameParts = ["out/v1"],
                Namespace = "kafka",
                TableName = "out/v1",
                PlanColumns = [new LineageColumn("y", new Int64Type())],
                ColumnLineage = new ColumnLineage(
                    new Dictionary<string, ColumnLineageField>()
                    {
                        ["y"] = new ColumnLineageField([new LineageInputField("kafka", "in,1", "x", [new LineageTransformation(LineageTransformationType.Direct, LineageTransformationSubtype.Identity)])])
                    },
                    []),
                UpstreamInputKeys = ["in,1"]
            };
            store.Register(new StreamLineage("orders", null, DateTimeOffset.UnixEpoch, [input], [output]), "orders");
        }

        private static string ScrollBody(string? scrollId, int? batchSize)
        {
            return JsonSerializer.Serialize(new { query = ScrollQuery, variables = new { scrollId, batchSize, types = (string[]?)null } });
        }

        private static List<string> ScrollUrns(HttpContext context)
        {
            return ReadJson(context)
                .GetProperty("data")
                .GetProperty("scrollAcrossEntities")
                .GetProperty("searchResults")
                .EnumerateArray()
                .Select(x => x.GetProperty("entity").GetProperty("urn").GetString()!)
                .ToList();
        }

        private static WebApplication MapStore(DataHubLineageStore store)
        {
            var app = WebApplication.CreateBuilder().Build();
            app.MapFlowtideDataHubLineage(store);
            return app;
        }

        private static List<RouteEndpoint> GetEndpoints(IEndpointRouteBuilder app)
        {
            return app.DataSources.SelectMany(x => x.Endpoints).OfType<RouteEndpoint>().ToList();
        }

        private static async Task<HttpContext> InvokeAsync(
            WebApplication app,
            string pattern,
            string method,
            string? body = null,
            string? rawTarget = null,
            string? urnRouteValue = null,
            string? query = null)
        {
            var endpoint = Assert.Single(GetEndpoints(app), x => x.RoutePattern.RawText == pattern);
            var context = new DefaultHttpContext();
            context.Request.Method = method;
            context.Response.Body = new MemoryStream();
            if (body != null)
            {
                var bytes = Encoding.UTF8.GetBytes(body);
                context.Request.Body = new MemoryStream(bytes);
                context.Request.ContentLength = bytes.Length;
                context.Request.ContentType = "application/json";
            }
            if (rawTarget != null)
            {
                context.Features.Get<IHttpRequestFeature>()!.RawTarget = rawTarget;
            }
            if (urnRouteValue != null)
            {
                context.Request.RouteValues["urn"] = urnRouteValue;
            }
            if (query != null)
            {
                context.Request.QueryString = new QueryString(query);
            }
            await endpoint.RequestDelegate!(context);
            return context;
        }

        private static byte[] ReadBody(HttpContext context)
        {
            return ((MemoryStream)context.Response.Body).ToArray();
        }

        private static JsonElement ReadJson(HttpContext context)
        {
            return JsonDocument.Parse(ReadBody(context)).RootElement;
        }
    }
}

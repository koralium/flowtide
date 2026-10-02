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
using FlowtideDotNet.AspNetCore.Internal;
using FlowtideDotNet.Core.Lineage.Dbt;
using FlowtideDotNet.DependencyInjection;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.Json;
using Xunit;

namespace FlowtideDotNet.AspNetCore.Tests
{
    public class DbtManifestEndpointTests
    {
        private static readonly string[] RoutePatterns =
        [
            "/dbt/manifest.json",
            "/dbt/catalog.json",
            "/dbt/{streamName}/manifest.json",
            "/dbt/{streamName}/catalog.json"
        ];

        [Fact]
        public async Task MapsFourGetRoutesWithAuthorization()
        {
            await using var app = WebApplication.CreateBuilder().Build();
            app.MapFlowtideDbtManifest(new DbtManifestStore()).RequireAuthorization();

            var endpoints = GetEndpoints(app);

            Assert.Equal(RoutePatterns.Order(), endpoints.Select(x => x.RoutePattern.RawText).Order());
            foreach (var endpoint in endpoints)
            {
                Assert.Equal(["GET"], endpoint.Metadata.GetRequiredMetadata<IHttpMethodMetadata>().HttpMethods);
                Assert.NotEmpty(endpoint.Metadata.GetOrderedMetadata<IAuthorizeData>());
            }
        }

        [Fact]
        public async Task CustomRoutePrefixIsUsed()
        {
            await using var app = WebApplication.CreateBuilder().Build();
            app.MapFlowtideDbtManifest(new DbtManifestStore(), "/lineage/dbt");

            var patterns = GetEndpoints(app).Select(x => x.RoutePattern.RawText).Order();

            Assert.Equal(RoutePatterns.Select(x => "/lineage" + x).Order(), patterns);
        }

        [Fact]
        public async Task DependencyInjectionOverloadWithoutStoreThrows()
        {
            await using var app = WebApplication.CreateBuilder().Build();

            var ex = Assert.Throws<InvalidOperationException>(() => app.MapFlowtideDbtManifest());

            Assert.Contains("AddDbtManifest", ex.Message);
        }

        [Fact]
        public async Task DependencyInjectionOverloadUsesRegisteredStore()
        {
            var builder = WebApplication.CreateBuilder();
            builder.Services.AddFlowtideDbtManifest();
            await using var app = builder.Build();
            app.MapFlowtideDbtManifest();

            var context = await InvokeAsync(app, "/dbt/manifest.json");

            Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
            Assert.Equal(app.Services.GetRequiredService<DbtManifestStore>().GetManifest().ETag, context.Response.Headers.ETag.ToString());
        }

        [Fact]
        public async Task EmptyStoreReturnsValidManifest()
        {
            var store = new DbtManifestStore();
            await using var app = MapStore(store);

            var context = await InvokeAsync(app, "/dbt/manifest.json");

            Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
            Assert.Equal("application/json", context.Response.ContentType);
            Assert.Equal(store.GetManifest().ETag, context.Response.Headers.ETag.ToString());
            Assert.Equal("no-cache", context.Response.Headers.CacheControl.ToString());
            var body = ReadBody(context);
            Assert.Equal(body.Length, context.Response.ContentLength);
            Assert.Equal(store.GetManifest().Utf8Json.ToArray(), body);
            using var document = JsonDocument.Parse(body);
            var root = document.RootElement;
            Assert.Equal("https://schemas.getdbt.com/dbt/manifest/v12.json", root.GetProperty("metadata").GetProperty("dbt_schema_version").GetString());
            Assert.Empty(root.GetProperty("nodes").EnumerateObject());
            Assert.Empty(root.GetProperty("sources").EnumerateObject());
        }

        [Fact]
        public async Task EmptyStoreReturnsCatalog()
        {
            var store = new DbtManifestStore();
            await using var app = MapStore(store);

            var context = await InvokeAsync(app, "/dbt/catalog.json");

            Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
            Assert.Equal(store.GetCatalog().Utf8Json.ToArray(), ReadBody(context));
            Assert.Equal(store.GetCatalog().ETag, context.Response.Headers.ETag.ToString());
        }

        [Theory]
        [InlineData("/dbt/{streamName}/manifest.json")]
        [InlineData("/dbt/{streamName}/catalog.json")]
        public async Task UnknownStreamReturnsNotFoundWithoutEcho(string pattern)
        {
            await using var app = MapStore(new DbtManifestStore());

            var context = await InvokeAsync(app, pattern, streamName: "<b>missing</b>");

            Assert.Equal(StatusCodes.Status404NotFound, context.Response.StatusCode);
            Assert.StartsWith("text/plain", context.Response.ContentType);
            Assert.DoesNotContain("missing", Encoding.UTF8.GetString(ReadBody(context)));
        }

        [Theory]
        [InlineData("{0}")]
        [InlineData("W/{0}")]
        [InlineData("*")]
        [InlineData("\"other\", {0}")]
        public async Task MatchingIfNoneMatchReturnsNotModified(string headerFormat)
        {
            var store = new DbtManifestStore();
            await using var app = MapStore(store);
            var eTag = store.GetManifest().ETag;

            var context = await InvokeAsync(app, "/dbt/manifest.json", ifNoneMatch: string.Format(headerFormat, eTag));

            Assert.Equal(StatusCodes.Status304NotModified, context.Response.StatusCode);
            Assert.Equal(eTag, context.Response.Headers.ETag.ToString());
            Assert.Empty(ReadBody(context));
        }

        [Fact]
        public async Task StaleIfNoneMatchReturnsBody()
        {
            var store = new DbtManifestStore();
            await using var app = MapStore(store);

            var context = await InvokeAsync(app, "/dbt/manifest.json", ifNoneMatch: "\"stale\"");

            Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
            Assert.Equal(store.GetManifest().Utf8Json.ToArray(), ReadBody(context));
        }

        [Theory]
        [InlineData("/dbt/manifest.json")]
        [InlineData("/dbt/catalog.json")]
        [InlineData("/dbt/{streamName}/manifest.json")]
        [InlineData("/dbt/{streamName}/catalog.json")]
        public async Task WarmingStoreReturnsServiceUnavailable(string pattern)
        {
            var store = new DbtManifestStore(new DbtManifestOptions() { WarmupTimeout = TimeSpan.FromHours(1) });
            store.ExpectStream("orders");
            await using var app = MapStore(store);

            var context = await InvokeAsync(app, pattern, streamName: "orders");

            Assert.Equal(StatusCodes.Status503ServiceUnavailable, context.Response.StatusCode);
            Assert.Equal("10", context.Response.Headers.RetryAfter.ToString());
            Assert.False(context.Response.Headers.ContainsKey("ETag"));
        }

        [Fact]
        public async Task GenerationFailureReturnsServerErrorAndLogs()
        {
            var logger = new ListLogger();
            var context = CreateContext();

            await DbtManifestEndpointHandler.WriteAsync(context, new DbtManifestStore(), logger, () => throw new InvalidOperationException("secret detail"));

            Assert.Equal(StatusCodes.Status500InternalServerError, context.Response.StatusCode);
            Assert.DoesNotContain("secret", Encoding.UTF8.GetString(ReadBody(context)));
            var entry = Assert.Single(logger.Entries);
            Assert.Equal(LogLevel.Error, entry.Level);
            Assert.IsType<InvalidOperationException>(entry.Exception);
        }

        private static WebApplication MapStore(DbtManifestStore store)
        {
            var app = WebApplication.CreateBuilder().Build();
            app.MapFlowtideDbtManifest(store);
            return app;
        }

        private static List<RouteEndpoint> GetEndpoints(IEndpointRouteBuilder app)
        {
            return app.DataSources.SelectMany(x => x.Endpoints).OfType<RouteEndpoint>().ToList();
        }

        private static DefaultHttpContext CreateContext()
        {
            var context = new DefaultHttpContext();
            context.Request.Method = HttpMethods.Get;
            context.Response.Body = new MemoryStream();
            return context;
        }

        private static async Task<HttpContext> InvokeAsync(WebApplication app, string pattern, string? streamName = null, string? ifNoneMatch = null)
        {
            var endpoint = Assert.Single(GetEndpoints(app), x => x.RoutePattern.RawText == pattern);
            var context = CreateContext();
            if (streamName != null)
            {
                context.Request.RouteValues["streamName"] = streamName;
            }
            if (ifNoneMatch != null)
            {
                context.Request.Headers.IfNoneMatch = ifNoneMatch;
            }
            await endpoint.RequestDelegate!(context);
            return context;
        }

        private static byte[] ReadBody(HttpContext context)
        {
            return ((MemoryStream)context.Response.Body).ToArray();
        }

        private sealed class ListLogger : ILogger
        {
            public List<(LogLevel Level, Exception? Exception)> Entries { get; } = new List<(LogLevel Level, Exception? Exception)>();

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                Entries.Add((logLevel, exception));
            }
        }
    }
}

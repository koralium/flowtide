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
using FlowtideDotNet.AspNetCore.Internal.TimeSeries.Middleware;
using FlowtideDotNet.AspNetCore.TimeSeries;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Primitives;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.AspNetCore.Extensions
{
    public static class UIApplicationBuilderExtensions
    {
        private static void StartMetrics(this IApplicationBuilder app, string path)
        {
            var options = new MetricOptions()
            {
                CaptureRate = TimeSpan.FromSeconds(5),
                Prefixes = new List<string>()
                {
                    "flowtide"
                }
            };
            var metricSeries = new MetricSeries(options);
            metricSeries.Initialize().Wait();

            var metricGatherer = new MetricGatherer(options, metricSeries);
            app.UseMiddleware<MetricMiddleware>(metricGatherer, metricSeries, path);
        }

        public static IApplicationBuilder UseFlowtideUI(this IApplicationBuilder app, string path = "/ui/stream")
        {
            app.StartMetrics(path);
            var stream = app.ApplicationServices.GetRequiredService<FlowtideDotNet.Base.Engine.DataflowStream>();
            return app.UseFlowtideUI(stream, path);
        }

        public static IEndpointConventionBuilder MapFlowtideTestInformation(this IEndpointRouteBuilder endpoints, string route = "/testInformation")
        {
            var checkpointVersionListener = new TestInformationListener();
            return endpoints.MapGet(route, async (HttpContext context) =>
            {
                if (!context.Request.Query.TryGetValue("stream", out var streamId))
                {
                    context.Response.StatusCode = 400;
                    context.Response.ContentType = "text/plain";
                    await context.Response.WriteAsync("Missing stream query parameter");
                    return;
                }
                var captured = checkpointVersionListener;

                var streamIdString = streamId.First();

                if (streamIdString != null && captured.TryGetCheckpointVersion(streamIdString, out var testInformation))
                {
                    context.Response.StatusCode = 200;
                    context.Response.ContentType = "application/json";
                    await context.Response.BodyWriter.WriteAsync(JsonSerializer.SerializeToUtf8Bytes(testInformation));
                    return;
                }

                context.Response.StatusCode = 404;
                context.Response.ContentType = "text/plain";
                await context.Response.WriteAsync($"Stream '{streamIdString}' not found");
                return;
            });
        }

        public static IApplicationBuilder UseFlowtideUI(this IApplicationBuilder app, FlowtideDotNet.Base.Engine.DataflowStream dataflowStream, string path = "/ui/stream")
        {
            return app.UseMiddleware<UiMiddleware>(new UiMiddlewareState(new DiagnosticsEndpoint(dataflowStream), new ReactEndpoint(path), path));
        }

        public static IApplicationBuilder UseFlowtidePauseResumeEndpoints(this IApplicationBuilder app, string basePath = "/ui/stream")
        {
            if (basePath == "/")
            {
                basePath = string.Empty;
            }
            app.Map($"{basePath}/pause", appBuilder =>
            {
                appBuilder.Use((HttpContext context, Func<Task> next) =>
                {
                    context.RequestServices.GetRequiredService<FlowtideDotNet.Base.Engine.DataflowStream>().Pause();
                    context.Response.StatusCode = 200;
                    context.Response.ContentType = "application/json";
                    return context.Response.BodyWriter.WriteAsync(JsonSerializer.SerializeToUtf8Bytes(new { message = "Stream paused" })).AsTask();
                });
            });

            app.Map($"{basePath}/resume", appBuilder =>
            {
                appBuilder.Use((HttpContext context, Func<Task> next) =>
                {
                    context.RequestServices.GetRequiredService<FlowtideDotNet.Base.Engine.DataflowStream>().Resume();
                    context.Response.StatusCode = 200;
                    context.Response.ContentType = "application/json";
                    return context.Response.BodyWriter.WriteAsync(JsonSerializer.SerializeToUtf8Bytes(new { message = "Stream resumed" })).AsTask();
                });
            });
            return app;
        }
    }
}

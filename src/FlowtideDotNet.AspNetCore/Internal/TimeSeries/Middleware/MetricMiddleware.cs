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

using FlowtideDotNet.AspNetCore.TimeSeries;
using Microsoft.AspNetCore.Http;
using Superpower;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.AspNetCore.Internal.TimeSeries.Middleware
{
    internal class MetricMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly MetricGatherer _gatherer;
        private readonly MetricSeries _series;

        public MetricMiddleware(RequestDelegate next, MetricGatherer gatherer, MetricSeries series)
        {
            _next = next;
            this._gatherer = gatherer;
            this._series = series;
        }

        public Task Invoke(HttpContext httpContext)
        {
            if (httpContext == null)
                throw new ArgumentNullException(nameof(httpContext));

            var requestPath = httpContext.Request.Path.ToString();

            if (requestPath == "/v1/api/query")
            {
                return HandleQuery(httpContext);
            }
            else if (requestPath == "/v1/api/query_range")
            {
                return HandleQueryRange(httpContext);
            }
            else if(requestPath == "/v1/api/series")
            {
                return HandleSeries(httpContext);
            }
            return _next(httpContext);
        }

        private Task WriteError(HttpContext httpContext, int statusCode, string message)
        {
            httpContext.Response.StatusCode = statusCode;
            httpContext.Response.ContentType = "text/html";
            byte[] outputBytes = Encoding.UTF8.GetBytes(message);
            return httpContext.Response.Body.WriteAsync(outputBytes, 0, outputBytes.Length);
        }

        private async Task HandleQueryRange(HttpContext httpContext)
        {
            if (!httpContext.Request.Query.TryGetValue("query", out var queryValue))
            {
                await WriteError(httpContext, 400, "Query parameter is missing");
                return;
            }
            var queryString = queryValue.First();

            if (!httpContext.Request.Query.TryGetValue("start", out var startValue))
            {
                await WriteError(httpContext, 400, "Start parameter is missing");
                return;
            }
            var start = startValue.First();

            if (!httpContext.Request.Query.TryGetValue("end", out var endValue))
            {
                await WriteError(httpContext, 400, "End parameter is missing");
                return;
            }
            var end = endValue.First();

            if (start!.Contains("."))
            {
                start = start.Remove(start.IndexOf('.'));
                start += "000";
            }
            if (end!.Contains("."))
            {
                end = end.Remove(end.IndexOf('.'));
                end += "000";
            }

            long startTimestamp;
            if (DateTimeOffset.TryParse(start, out var startDate))
            {
                startTimestamp = startDate.ToUnixTimeMilliseconds();
            }
            else if (!long.TryParse(start, out startTimestamp))
            {
                await WriteError(httpContext, 400, "Invalid start date");
                return;
            }
            long endTimestamp;
            if (DateTimeOffset.TryParse(end, out var endDate))
            {
                endTimestamp = endDate.ToUnixTimeMilliseconds();
            }
            else if (!long.TryParse(end, out endTimestamp))
            {
                await WriteError(httpContext, 400, "Invalid end date");
                return;
            }

            int stepDurationMs;
            if (httpContext.Request.Query.TryGetValue("step", out var stepValue))
            {
                var step = stepValue.First();
                var tokenizer = new PromQL.Parser.Tokenizer();
                var durationTokens = tokenizer.Tokenize(step!);
                var stepDurationResult = PromQL.Parser.Parser.Duration.TryParse(durationTokens);
                stepDurationMs = (int)stepDurationResult.Value.Value.TotalMilliseconds;
            }
            else
            {
                stepDurationMs = (int)_series.Rate.TotalMilliseconds;
            }

            var promExecutor = new PromExecutor(_series);

            await promExecutor.RangeQuery(httpContext.Response.Body, queryString!, startTimestamp, endTimestamp, stepDurationMs);
        }

        private async Task HandleSeries(HttpContext httpContext)
        {
            var promExecutor = new PromExecutor(_series);

            await promExecutor.Series(httpContext.Response.Body);
        }

        private async Task HandleQuery(HttpContext httpContext)
        {
            if (!httpContext.Request.Query.TryGetValue("query", out var queryValue))
            {
                await WriteError(httpContext, 400, "Query parameter is missing");
                return;
            }
            var queryString = queryValue.First();

            long? timestamp = default;
            if (httpContext.Request.Query.TryGetValue("time", out var timeValue))
            {
                var timeString = timeValue.FirstOrDefault();
                if (DateTimeOffset.TryParse(timeString, out var startDate))
                {
                    timestamp = startDate.ToUnixTimeMilliseconds();
                }
            }

            var promExecutor = new PromExecutor(_series);

            await promExecutor.InstantQuery(httpContext.Response.Body, queryString, timestamp);
        }
    }
}

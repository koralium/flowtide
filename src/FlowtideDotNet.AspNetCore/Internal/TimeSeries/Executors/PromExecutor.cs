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

using System.Globalization;

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class PromExecutor
    {
        private readonly MetricSeries metricSeries;

        public PromExecutor(MetricSeries metricSeries)
        {
            this.metricSeries = metricSeries;
        }

        public async Task<string> ExecuteToString(string query, long startTimestamp, long endTimestamp, int stepWidth)
        {
            MemoryStream stream = new MemoryStream();
            await RangeQuery(stream, query, startTimestamp, endTimestamp, stepWidth);
            stream.Position = 0;
            return new StreamReader(stream).ReadToEnd();
        }

        public async Task Series(Stream stream)
        {
            await metricSeries.Lock();
            try
            {
                var allSeries = metricSeries.GetAllSeries();

                var writer = new System.Text.Json.Utf8JsonWriter(stream, new System.Text.Json.JsonWriterOptions()
                {
                    Indented = true
                });

                writer.WriteStartObject();
                writer.WriteString("status", "success");
                writer.WriteStartArray("data");

                foreach (var series in allSeries)
                {
                    writer.WriteStartObject();
                    writer.WriteString("__name__", series.Name);
                    foreach (var tag in series.Tags)
                    {
                        writer.WriteString(tag.Key, tag.Value);
                    }
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
                await writer.FlushAsync();
            }
            finally
            {
                metricSeries.Unlock();
            }

        }

        public async Task InstantQuery(Stream stream, string query, long? time)
        {
            await metricSeries.Lock();
            try
            {
                long endTime;
                if (!time.HasValue)
                {
                    time = metricSeries.LastIngestedTime;
                    endTime = metricSeries.LastIngestedTime;
                }
                else
                {
                    if (time.Value > metricSeries.LastIngestedTime)
                    {
                        time = metricSeries.LastIngestedTime;
                    }
                    // Set end time to half the capture rate to only get one value
                }

                time = time.Value - (long)(metricSeries.Rate.TotalMilliseconds / 2);
                endTime = time.Value + (long)(metricSeries.Rate.TotalMilliseconds / 2);


                var ast = PromQL.Parser.Parser.ParseExpression(query);
                var visitor = new PromQLVisitor(metricSeries);

                ast.Accept(visitor);
                var result = visitor.GetResult();

                var writer = new System.Text.Json.Utf8JsonWriter(stream, new System.Text.Json.JsonWriterOptions()
                {
                    Indented = true
                });

                writer.WriteStartObject();
                writer.WriteString("status", "success");
                writer.WriteStartObject("data");
                writer.WriteString("resultType", "vector");

                writer.WriteStartArray("result");

                var newLine = writer.Options.Indented ? Environment.NewLine : "";
                var valueIndention = "          ";
                foreach (var series in result)
                {
                    var iterator = series.GetValues(time.Value, endTime, (int)metricSeries.Rate.TotalMilliseconds).GetAsyncEnumerator();

                    if (await iterator.MoveNextAsync())
                    {
                        writer.WriteStartObject();
                        writer.WriteStartObject("metric");
                        writer.WriteString("__name__", series.Name);
                        foreach (var tag in series.Tags)
                        {
                            writer.WriteString(tag.Key, tag.Value);
                        }

                        writer.WriteEndObject();

                        writer.WriteStartArray("value");

                        var value = iterator.Current;
                        writer.WriteRawValue($"{newLine}{valueIndention}{(value.timestamp / 1000m).ToString("F3", CultureInfo.InvariantCulture)}");
                        writer.WriteRawValue($"{newLine}{valueIndention}\"{value.value.ToString("0.###", CultureInfo.InvariantCulture)}\"");
                        await iterator.DisposeAsync();
                        writer.WriteEndArray();

                        writer.WriteEndObject();
                    }
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
                writer.WriteEndObject();
                await writer.FlushAsync();
            }
            finally
            {
                metricSeries.Unlock();
            }

        }

        public async Task RangeQuery(Stream stream, string query, long startTimestamp, long endTimestamp, int stepWidth)
        {
            await metricSeries.Lock();

            try
            {
                var ast = PromQL.Parser.Parser.ParseExpression(query);
                var visitor = new PromQLVisitor(metricSeries);

                ast.Accept(visitor);
                var result = visitor.GetResult();

                var writer = new System.Text.Json.Utf8JsonWriter(stream, new System.Text.Json.JsonWriterOptions()
                {
                    Indented = true
                });

                writer.WriteStartObject();
                writer.WriteString("status", "success");
                writer.WriteStartObject("data");
                writer.WriteString("resultType", "matrix");

                writer.WriteStartArray("result");

                var newLine = writer.Options.Indented ? Environment.NewLine : "";
                var valueIndention = "          ";

                foreach (var series in result)
                {
                    writer.WriteStartObject();
                    writer.WriteStartObject("metric");
                    writer.WriteString("__name__", series.Name);
                    foreach (var tag in series.Tags)
                    {
                        writer.WriteString(tag.Key, tag.Value);
                    }

                    writer.WriteEndObject();

                    writer.WriteStartArray("values");
                    await foreach (var value in series.GetValues(startTimestamp, endTimestamp, stepWidth))
                    {
                        writer.WriteRawValue($"{newLine}{valueIndention}[ {(value.timestamp / 1000m).ToString("F3", CultureInfo.InvariantCulture)}, \"{value.value.ToString("0.###", CultureInfo.InvariantCulture)}\" ]", true);
                    }
                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
                writer.WriteEndObject();
                await writer.FlushAsync();
            }
            finally
            {
                metricSeries.Unlock();
            }
        }
    }
}

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
using System.Diagnostics.Metrics;
using Xunit;

namespace FlowtideDotNet.AspNetCore.Tests
{
    public class MetricGathererTests
    {
        private static MetricOptions CreateOptions()
        {
            return new MetricOptions()
            {
                // Keep the background gathering loop out of the way, measurements are
                // stored through direct StoreMeasurements calls for determinism.
                CaptureRate = TimeSpan.FromHours(1),
                Prefixes = new List<string>()
                {
                    "flowtide"
                }
            };
        }

        /// <summary>
        /// Regression test for multiple streams in one process (distributed substreams in a
        /// single silo). Instruments that record identical series (same name, same tags, here
        /// untagged) collide on the same timestamp in one store pass. That collision previously
        /// threw inside the store pass, which silently dropped every instrument enumerated after
        /// it, so most streams' series never appeared in the metrics API.
        /// </summary>
        [Fact]
        public async Task AllStreamsAreStoredWhenUntaggedInstrumentsCollide()
        {
            const int streamCount = 8;
            var options = CreateOptions();
            var series = new MetricSeries(options);
            await series.Initialize();
            var gatherer = new MetricGatherer(options, series);

            var meters = new List<Meter>();
            try
            {
                for (int i = 0; i < streamCount; i++)
                {
                    // Mirrors one stream: its own meter with a stream tagged counter and an
                    // untagged counter whose series collides with every other stream.
                    var meter = new Meter($"flowtide.stream{i}.storage");
                    meters.Add(meter);
                    var tagged = meter.CreateCounter<long>("flowtide_tagged");
                    tagged.Add(1, new KeyValuePair<string, object?>("stream", $"stream{i}"));
                    var untagged = meter.CreateCounter<long>("flowtide_untagged");
                    untagged.Add(1);
                }

                // Far in the future so the gathering loops own initial pass cannot interfere.
                var firstTimestamp = DateTimeOffset.UtcNow.AddMinutes(10).ToUnixTimeMilliseconds();
                var secondTimestamp = firstTimestamp + 5000;

                await series.Lock();
                try
                {
                    await gatherer.StoreMeasurements(firstTimestamp, series);
                    await gatherer.StoreMeasurements(secondTimestamp, series);
                }
                finally
                {
                    series.Unlock();
                }

                var taggedSeries = series.GetSeries("flowtide_tagged_total").ToList();
                Assert.Equal(streamCount, taggedSeries.Count);
                var streamsSeen = taggedSeries.Select(x => x.Tags["stream"]).ToHashSet();
                for (int i = 0; i < streamCount; i++)
                {
                    Assert.Contains($"stream{i}", streamsSeen);
                }

                // Every stream has values at both store passes.
                foreach (var serie in taggedSeries)
                {
                    var timestamps = new List<long>();
                    await foreach (var value in serie.GetValues(firstTimestamp - 1000, secondTimestamp + 1000, 1000))
                    {
                        timestamps.Add(value.timestamp);
                    }
                    Assert.Contains(firstTimestamp / 1000 * 1000, timestamps);
                    Assert.Contains(secondTimestamp / 1000 * 1000, timestamps);
                }

                // The colliding untagged instruments end up as a single series.
                var untaggedSeries = series.GetSeries("flowtide_untagged_total").ToList();
                Assert.Single(untaggedSeries);
            }
            finally
            {
                foreach (var meter in meters)
                {
                    meter.Dispose();
                }
            }
        }

        /// <summary>
        /// Storing the same timestamp twice to a serie must not throw, the first value wins and
        /// later values with the same or older timestamps are skipped.
        /// </summary>
        [Fact]
        public async Task DuplicateTimestampsAreSkipped()
        {
            var series = new MetricSeries(CreateOptions());
            await series.Initialize();

            var tags = new Dictionary<string, string>() { { "stream", "a" } };
            await series.SetValueToSerie("flowtide_metric", tags, 1000, 1);
            await series.SetValueToSerie("flowtide_metric", tags, 1000, 2);
            await series.SetValueToSerie("flowtide_metric", tags, 500, 3);
            await series.SetValueToSerie("flowtide_metric", tags, 2000, 4);

            var serie = Assert.Single(series.GetSeries("flowtide_metric"));

            var values = new List<(double value, long timestamp)>();
            await foreach (var value in serie.GetValues(0, 3000, 1))
            {
                values.Add((value.value, value.timestamp));
            }

            Assert.Equal(new[] { (1d, 1000L), (4d, 2000L) }, values);
        }
    }
}

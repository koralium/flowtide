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


using System.Collections.Concurrent;

namespace FlowtideDotNet.AspNetCore.TimeSeries.Instruments
{
    internal class HistogramInstrument : IMetricInstrument
    {
        private readonly string name;
        private ConcurrentDictionary<string, HistogramValue> _values = new ConcurrentDictionary<string, HistogramValue>();

        public HistogramInstrument(string name)
        {
            this.name = name;
        }

        public void Record(ReadOnlySpan<KeyValuePair<string, object?>> tags, double value)
        {
            Dictionary<string, string> labels = new Dictionary<string, string>();

            foreach (var tag in tags)
            {
                labels.Add(tag.Key, tag.Value.ToString()!);
            }

            var key = string.Join(',', labels.Select(t => $"{t.Key}={t.Value}"));

            if (!_values.TryGetValue(key, out var metricValue))
            {
                metricValue = new HistogramValue(labels.ToDictionary(x => x.Key.Replace(".", "_"), x => x.Value));
                metricValue.Value = value;
                metricValue.Count = 1;
                _values.AddOrUpdate(key, metricValue, (k, v) =>
                {
                    v.Value += value;
                    v.Count += 1;
                    return v;
                });
            }
            else
            {
                metricValue.Value += value;
                metricValue.Count += 1;
            }
        }

        async ValueTask IMetricInstrument.StoreMeasurements(long timestamp, MetricSeries series)
        {
            foreach (var value in _values)
            {
                await series.SetValueToSerie($"{name}_sum", value.Value.Tags, timestamp, value.Value.Value);
                await series.SetValueToSerie($"{name}_count", value.Value.Tags, timestamp, value.Value.Count);
            }
        }
    }
}

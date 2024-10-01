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
using System.Text;

namespace FlowtideDotNet.AspNetCore.TimeSeries.Instruments
{
    internal class CounterInstrument : IMetricInstrument
    {
        private readonly string name;
        private readonly bool isObservable;
        private ConcurrentDictionary<string, MetricValue> _values = new ConcurrentDictionary<string, MetricValue>();

        public CounterInstrument(string name, bool isObservable)
        {
            this.name = $"{name}_total";
            this.isObservable = isObservable;
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
                metricValue = new MetricValue(labels.ToDictionary(x => x.Key.Replace(".", "_"), x => x.Value));
                metricValue.Value = value;
                _values.AddOrUpdate(key, metricValue, (k ,v) =>
                {
                    if (isObservable)
                    {
                        v.Value = value;
                    }
                    else
                    {
                        v.Value += value;
                    }
                    
                    return v;
                });
            }
            else
            {
                if (isObservable)
                {
                    metricValue.Value = value;
                }
                else
                {
                    metricValue.Value += value;
                }
            }
        }

        public async ValueTask StoreMeasurements(long timestamp, MetricSeries series)
        {
            foreach(var value in _values)
            {
                await series.SetValueToSerie(name, value.Value.Tags, timestamp, value.Value.Value);
            }
        }
    }
}

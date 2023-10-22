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

using FlowtideDotNet.Base.Metrics.Extensions;
using System.Collections.Concurrent;

namespace FlowtideDotNet.Base.Metrics.Gauge
{
    internal class GaugeMetric : IMetricInstrument
    {
        private readonly Func<IGaugeMetricImpl> _createMetricFunc;
        private readonly ConcurrentDictionary<string, IGaugeMetricImpl> _gauges;

        public GaugeMetric(Type type, string name, string? unit, string? description)
        {
            Name = name;
            Unit = unit;
            Description = description;
            _gauges = new ConcurrentDictionary<string, IGaugeMetricImpl>();

            if (type.Equals(typeof(byte)) ||
                type.Equals(typeof(short)) ||
                type.Equals(typeof(int)) ||
                type.Equals(typeof(long))
                )
            {
                _createMetricFunc = () => new GaugeMetricInteger();
            }
            else if (type.Equals(typeof(float)) ||
                type.Equals(typeof(double)) ||
                type.Equals(typeof(decimal))
                )
            {
                _createMetricFunc = () => new GaugeMetricFloating();
            }
            else
            {
                throw new NotSupportedException("The type provided is not supported");
            }
        }

        public string Name { get; }

        public string? Description { get; }

        public string? Unit { get; }

        public void RecordMeasurement<T>(T value, ReadOnlySpan<KeyValuePair<string, object?>> tags) where T : struct
        {
            var tagsString = tags.ConvertToString();

            if (!_gauges.TryGetValue(tagsString, out var metric))
            {
                metric = _createMetricFunc();
                _gauges.AddOrUpdate(tagsString, metric, (key, old) => metric);
            }
            metric.SetValue(value);
        }

        public GaugeSnapshot GetSnapshot()
        {
            Dictionary<string, GaugeTagSnapshot> dimensions = new Dictionary<string, GaugeTagSnapshot>();

            foreach(var gauge in _gauges)
            {
                dimensions.Add(gauge.Key, gauge.Value.GetSnapshot());
            }

            return new GaugeSnapshot(Name, Unit, Description, dimensions);
        }
    }
}

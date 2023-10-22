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
using System.Collections.Immutable;

namespace FlowtideDotNet.Base.Metrics.Counter
{
    public class CounterMetric : IMetricInstrument
    {
        private readonly ConcurrentDictionary<string, ICounterMetric> _counters;
        private readonly Func<ICounterMetric> _createMetricFunc;
        public CounterMetric(Type type, string name, string? unit, string? description, Func<long>? timeSecondsFunc = null)
        {
            Name = name;
            Unit = unit;
            Description = description;
            _counters = new ConcurrentDictionary<string, ICounterMetric>();

            // Integer counter
            if (type.Equals(typeof(byte)) ||
                type.Equals(typeof(short)) ||
                type.Equals(typeof(int)) ||
                type.Equals(typeof(long))
                )
            {
                _createMetricFunc = () => new CounterMetricInteger(timeSecondsFunc);
            }
            else if (type.Equals(typeof(float)) ||
                type.Equals(typeof(double)) ||
                type.Equals(typeof(decimal))
                )
            {
                _createMetricFunc = () => new CounterMetricFloating(timeSecondsFunc);
            }
            else
            {
                throw new NotSupportedException("The type provided is not supported");
            }
        }

        public string Name { get; }

        public string? Description { get; }

        public string? Unit { get; }

        public void Add<T>(T value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var tagsString = tags.ConvertToString();
            
            if (!_counters.TryGetValue(tagsString, out var metric))
            {
                metric = _createMetricFunc();
                _counters.AddOrUpdate(tagsString, metric, (key, old) => metric);
            }
            metric.Add(value);
        }

        public CounterSnapshot GetSnapshot()
        {
            decimal totalSum = 0;
            decimal totalRate = 0;

            // If there are no recorded values yet
            if (_counters.IsEmpty)
            {
                return new CounterSnapshot(Name, Unit, Description, CounterTagSnapshot.Empty, false, ImmutableDictionary<string, CounterTagSnapshot>.Empty);
            }

            Dictionary<string, CounterTagSnapshot> tagValues = new Dictionary<string, CounterTagSnapshot>();
            foreach(var kv in _counters)
            {
                var snapshot = kv.Value.GetSnapshot();
                totalSum += snapshot.Sum;
                totalRate += snapshot.RateLastMinute;
                tagValues.Add(kv.Key, snapshot);
            }

            return new CounterSnapshot(Name, Unit, Description, new CounterTagSnapshot(totalSum, totalRate), true, tagValues);
        }

        public void RecordMeasurement<T1>(T1 value, ReadOnlySpan<KeyValuePair<string, object?>> tags) where T1: struct
        {
            Add(value, tags);
        }
    }
}

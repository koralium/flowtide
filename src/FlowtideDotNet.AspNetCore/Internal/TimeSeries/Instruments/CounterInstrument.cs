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

using FlowtideDotNet.AspNetCore.Internal.TimeSeries.Utils;
using System.Collections.Concurrent;
using System.Text;

namespace FlowtideDotNet.AspNetCore.TimeSeries.Instruments
{
    internal class CounterInstrument : IMetricInstrument
    {
        private readonly string name;
        private List<KeyValuePair<string, string>[]> _tagsList = new List<KeyValuePair<string, string>[]>();
        private List<MetricValue> _metrics = new List<MetricValue>();
        private ReaderWriterLockSlim _rwLock = new ReaderWriterLockSlim();

        public CounterInstrument(string name)
        {
            this.name = $"{name}_total";
        }

        public void Record(ReadOnlySpan<KeyValuePair<string, object?>> tags, double value)
        {
            _rwLock.EnterReadLock();
            int index = TagListUtils.BinarySearchList(tags, _tagsList);

            if (index >= 0)
            {
                _metrics[index].Value += value;
                _rwLock.ExitReadLock();
            }
            else
            {
                _rwLock.ExitReadLock();
                _rwLock.EnterWriteLock();
                index = TagListUtils.BinarySearchList(tags, _tagsList);

                if (index >= 0)
                {
                    _metrics[index].Value += value;
                    _rwLock.ExitWriteLock();
                    return;
                }

                // Not existing
                index = ~index;

                var tagList = new KeyValuePair<string, string>[tags.Length];

                for (int i = 0; i < tags.Length; i++)
                {
                    tagList[i] = new KeyValuePair<string, string>(tags[i].Key, tags[i].Value!.ToString()!);
                }
                    
                var metricValue = new MetricValue(tagList.ToDictionary(x => x.Key.Replace(".", "_"), x => x.Value));
                metricValue.Value = value;

                _tagsList.Insert(index, tagList);
                _metrics.Insert(index, metricValue);

                _rwLock.ExitWriteLock();
            }
        }

        public async ValueTask StoreMeasurements(long timestamp, MetricSeries series)
        {
            _rwLock.EnterReadLock();

            for (int i = 0; i < _metrics.Count; i++)
            {
                var metric = _metrics[i];
                await series.SetValueToSerie(name, metric.Tags, timestamp, metric.Value);
            }
            _rwLock.ExitReadLock();
        }
    }
}

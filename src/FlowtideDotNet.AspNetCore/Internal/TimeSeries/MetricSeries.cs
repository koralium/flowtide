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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Immutable;

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class MetricSeries
    {
        private readonly Dictionary<string, Dictionary<string, MetricSerie>> _series;
        private readonly MetricOptions metricOptions;
        private IStateClient<IBPlusTreeNode, BPlusTreeMetadata>? _client;
        private TimestampKeySerializer? _keySerializer;
        private DoubleValueSerializer? _valueSerializer;
        private StateManagerSync<object>? _stateManager;
        private SemaphoreSlim _lock = new SemaphoreSlim(1);

        public MetricSeries(MetricOptions metricOptions)
        {
            _series = new Dictionary<string, Dictionary<string, MetricSerie>>();
            this.metricOptions = metricOptions;
        }

        public TimeSpan Rate => metricOptions.CaptureRate;

        public long LastIngestedTime { get; private set; }

        public Task Lock()
        {
            return _lock.WaitAsync();
        }

        public void Unlock()
        {
            _lock.Release();
        }

        public async Task Initialize()
        {
            _stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                MinCachePageCount = 0,
                CachePageCount = 1000,
            }, NullLogger.Instance, new System.Diagnostics.Metrics.Meter(string.Empty), string.Empty);

            await _stateManager.InitializeAsync();


            _keySerializer = new TimestampKeySerializer(GlobalMemoryManager.Instance);
            _valueSerializer = new DoubleValueSerializer(GlobalMemoryManager.Instance);
            _client = await _stateManager.CreateClientAsync<IBPlusTreeNode, BPlusTreeMetadata>(string.Empty, new FlowtideDotNet.Storage.StateManager.Internal.StateClientOptions<IBPlusTreeNode>()
            {
                ValueSerializer = new BPlusTreeSerializer<long, double, TimestampKeyContainer, DoubleValueContainer>(_keySerializer, _valueSerializer),
            });
        }

        public async ValueTask SetValueToSerie(string name, IReadOnlyDictionary<string, string> tags, long timestamp, double value)
        {
            LastIngestedTime = timestamp;
            var key = string.Join(",", tags.Select(x => $"{x.Key}={x.Value}"));

            if (!_series.TryGetValue(name, out var series))
            {
                series = new Dictionary<string, MetricSerie>();
                _series.Add(name, series);
            }
            if (!series.TryGetValue(key, out var serie))
            {
                var tmp = new TemporarySyncStateClient<IBPlusTreeNode, BPlusTreeMetadata>(new StateClientMetadata<BPlusTreeMetadata>(), _client!);
                var tree = new BPlusTree<long, double, TimestampKeyContainer, DoubleValueContainer>(tmp, new BPlusTreeOptions<long, double, TimestampKeyContainer, DoubleValueContainer>()
                {
                    Comparer = new TimestampComparer(),
                    KeySerializer = _keySerializer!,
                    ValueSerializer = _valueSerializer!,
                    BucketSize = 1024,
                    UseByteBasedPageSizes = false,
                    PageSizeBytes = 32 * 1024,
                });
                await tree.InitializeAsync();
                serie = new MetricSerie(name, tags, tree);
                series.Add(key, serie);
            }
            await serie.SetValue(timestamp, value);
        }

        public IEnumerable<MetricSerie> GetAllSeries()
        {
            return _series.Values.SelectMany(x => x.Values);
        }

        public IEnumerable<MetricSerie> GetSeries(string name)
        {
            if (_series.TryGetValue(name, out var series))
            {
                return series.Values;
            }
            return ImmutableArray<MetricSerie>.Empty;
        }
    }
}

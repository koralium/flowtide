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

using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Validators;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Nexmark.Internal.Diagnosers
{
    /// <summary>
    /// Reports the page cache hit percentages for the run, overall plus split by path.
    /// Read hits are the table read hits plus the state client lookup table hits, since the
    /// lock-free fast path bypasses the shared table counter. The read percentage measures
    /// cache quality for query processing, the commit percentage measures how many dirty
    /// pages survived in cache until the checkpoint.
    /// </summary>
    internal class CacheHitRateDiagnoser : IInProcessDiagnoser
    {
        private readonly Dictionary<BenchmarkCase, (long ReadHits, long ReadMisses, long CommitHits, long CommitMisses, double AvgSize, long MaxSize, long Promotions, double AvgMain, long Refills)> results = [];

        public IEnumerable<string> Ids => [nameof(CacheHitRateDiagnoser)];

        public IEnumerable<IExporter> Exporters => [];

        public IEnumerable<IAnalyser> Analysers => [];

        public void DeserializeResults(BenchmarkCase benchmarkCase, string serializedResults)
        {
            var parts = serializedResults.Split('|');
            results.Add(benchmarkCase, (
                long.Parse(parts[0], CultureInfo.InvariantCulture),
                long.Parse(parts[1], CultureInfo.InvariantCulture),
                long.Parse(parts[2], CultureInfo.InvariantCulture),
                long.Parse(parts[3], CultureInfo.InvariantCulture),
                double.Parse(parts[4], CultureInfo.InvariantCulture),
                long.Parse(parts[5], CultureInfo.InvariantCulture),
                long.Parse(parts[6], CultureInfo.InvariantCulture),
                double.Parse(parts[7], CultureInfo.InvariantCulture),
                long.Parse(parts[8], CultureInfo.InvariantCulture)));
        }

        public void DisplayResults(ILogger logger)
        {
        }

        public InProcessDiagnoserHandlerData GetHandlerData(BenchmarkCase benchmarkCase)
        {
            return new(typeof(CacheHitRateHandler), null);
        }

        public RunMode GetRunMode(BenchmarkCase benchmarkCase)
        {
            return RunMode.ExtraIteration;
        }

        public ValueTask HandleAsync(HostSignal signal, DiagnoserActionParameters parameters, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public IEnumerable<Metric> ProcessResults(DiagnoserResults diagnoserResults)
        {
            if (results.TryGetValue(diagnoserResults.BenchmarkCase, out var counts))
            {
                var hits = counts.ReadHits + counts.CommitHits;
                var total = hits + counts.ReadMisses + counts.CommitMisses;
                if (total > 0)
                {
                    yield return new Metric(new CacheHitRateMetricDescriptor(), 100.0 * hits / total);
                }
                var readTotal = counts.ReadHits + counts.ReadMisses;
                if (readTotal > 0)
                {
                    yield return new Metric(new ReadHitRateMetricDescriptor(), 100.0 * counts.ReadHits / readTotal);
                }
                var commitTotal = counts.CommitHits + counts.CommitMisses;
                if (commitTotal > 0)
                {
                    yield return new Metric(new CommitHitRateMetricDescriptor(), 100.0 * counts.CommitHits / commitTotal);
                }
                if (counts.MaxSize > 0)
                {
                    // Sampled resident page count, shows whether an eviction backlog let the
                    // cache grow past its configured limit and inflated the hit percentages.
                    yield return new Metric(new AvgCachePagesMetricDescriptor(), counts.AvgSize);
                    yield return new Metric(new MaxCachePagesMetricDescriptor(), counts.MaxSize);
                    // Shows whether the promotion machinery participates at all.
                    yield return new Metric(new PromotionsMetricDescriptor(), counts.Promotions);
                    yield return new Metric(new AvgMainPagesMetricDescriptor(), counts.AvgMain);
                    // Only nonzero when the long usage credit design is active.
                    yield return new Metric(new RefillsMetricDescriptor(), counts.Refills);
                }
            }
        }

        public IAsyncEnumerable<ValidationError> ValidateAsync(ValidationParameters validationParameters)
        {
            return AsyncEnumerable.Empty<ValidationError>();
        }

        internal class CacheHitRateMetricDescriptor() : IMetricDescriptor
        {
            public string Id => "CacheHitRate";
            public string DisplayName => "Cache hit %";
            public string Legend => "";
            public string NumberFormat => "#0.00";
            public UnitType UnitType => UnitType.Dimensionless;
            public string Unit => "%";
            public bool TheGreaterTheBetter => true;
            public int PriorityInCategory => 1;
            public bool GetIsAvailable(Metric metric)
                => true;
        }

        internal class ReadHitRateMetricDescriptor() : IMetricDescriptor
        {
            public string Id => "ReadHitRate";
            public string DisplayName => "Read hit %";
            public string Legend => "";
            public string NumberFormat => "#0.00";
            public UnitType UnitType => UnitType.Dimensionless;
            public string Unit => "%";
            public bool TheGreaterTheBetter => true;
            public int PriorityInCategory => 2;
            public bool GetIsAvailable(Metric metric)
                => true;
        }

        internal class CommitHitRateMetricDescriptor() : IMetricDescriptor
        {
            public string Id => "CommitHitRate";
            public string DisplayName => "Commit hit %";
            public string Legend => "";
            public string NumberFormat => "#0.00";
            public UnitType UnitType => UnitType.Dimensionless;
            public string Unit => "%";
            public bool TheGreaterTheBetter => true;
            public int PriorityInCategory => 3;
            public bool GetIsAvailable(Metric metric)
                => true;
        }

        internal class AvgCachePagesMetricDescriptor() : IMetricDescriptor
        {
            public string Id => "CacheAvgPages";
            public string DisplayName => "Avg pages";
            public string Legend => "Sampled resident cache pages, above the configured limit means an eviction backlog inflated the hit rate";
            public string NumberFormat => "#0";
            public UnitType UnitType => UnitType.Dimensionless;
            public string Unit => "Count";
            public bool TheGreaterTheBetter => false;
            public int PriorityInCategory => 4;
            public bool GetIsAvailable(Metric metric)
                => true;
        }

        internal class MaxCachePagesMetricDescriptor() : IMetricDescriptor
        {
            public string Id => "CacheMaxPages";
            public string DisplayName => "Max pages";
            public string Legend => "";
            public string NumberFormat => "#0";
            public UnitType UnitType => UnitType.Dimensionless;
            public string Unit => "Count";
            public bool TheGreaterTheBetter => false;
            public int PriorityInCategory => 5;
            public bool GetIsAvailable(Metric metric)
                => true;
        }

        internal class PromotionsMetricDescriptor() : IMetricDescriptor
        {
            public string Id => "CachePromotions";
            public string DisplayName => "Promotions";
            public string Legend => "Small to main queue promotions, zero means the frequency machinery never participates";
            public string NumberFormat => "#0";
            public UnitType UnitType => UnitType.Dimensionless;
            public string Unit => "Count";
            public bool TheGreaterTheBetter => false;
            public int PriorityInCategory => 6;
            public bool GetIsAvailable(Metric metric)
                => true;
        }

        internal class AvgMainPagesMetricDescriptor() : IMetricDescriptor
        {
            public string Id => "CacheAvgMainPages";
            public string DisplayName => "Avg main";
            public string Legend => "";
            public string NumberFormat => "#0";
            public UnitType UnitType => UnitType.Dimensionless;
            public string Unit => "Count";
            public bool TheGreaterTheBetter => false;
            public int PriorityInCategory => 7;
            public bool GetIsAvailable(Metric metric)
                => true;
        }

        internal class RefillsMetricDescriptor() : IMetricDescriptor
        {
            public string Id => "CacheLongRefills";
            public string DisplayName => "Refills";
            public string Legend => "Long usage credit refills in the main queue scan";
            public string NumberFormat => "#0";
            public UnitType UnitType => UnitType.Dimensionless;
            public string Unit => "Count";
            public bool TheGreaterTheBetter => false;
            public int PriorityInCategory => 8;
            public bool GetIsAvailable(Metric metric)
                => true;
        }
    }

    public class CacheHitRateHandler : IInProcessDiagnoserHandler
    {
        // Observable counters are cumulative, polling must store the last value per
        // instrument instead of adding, and lookup hits has one instrument per state
        // client so the final value is the sum of the last values.
        private readonly System.Collections.Concurrent.ConcurrentDictionary<Instrument, long> _counterValues = new();
        private long _sizeSum;
        private long _sizeSamples;
        private long _sizeMax;
        private long _mainSizeSum;
        private long _mainSizeSamples;
        private MeterListener? _listener;
        private System.Threading.Timer? _timer;

        public ValueTask HandleAsync(BenchmarkSignal signal, InProcessDiagnoserActionArgs args, CancellationToken cancellationToken)
        {
            switch (signal)
            {
                case BenchmarkSignal.BeforeExtraIteration:
                    SetupMetricGatherer();
                    break;
                case BenchmarkSignal.AfterExtraIteration:
                    _timer?.Dispose();
                    if (_listener != null)
                    {
                        // Final pull while the streams meters are still alive, iteration
                        // cleanup disposes them later.
                        _listener.RecordObservableInstruments();
                        _listener.Dispose();
                    }
                    break;
            }
            return ValueTask.CompletedTask;
        }

        private void SetupMetricGatherer()
        {
            _listener = new MeterListener();
            _listener.InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Name == "flowtide_cache_read_hits" ||
                    instrument.Name == "flowtide_cache_read_misses" ||
                    instrument.Name == "flowtide_cache_commit_hits" ||
                    instrument.Name == "flowtide_cache_commit_misses" ||
                    instrument.Name == "flowtide_state_client_lookup_hits" ||
                    instrument.Name == "flowtide_s3fifo_small_queue_promotions" ||
                    instrument.Name == "flowtide_s3fifo_long_usage_refills" ||
                    instrument.Name == "flowtide_s3fifo_main_queue_size" ||
                    instrument.Name == "flowtide_lru_table_size")
                {
                    meterListener.EnableMeasurementEvents(instrument, null);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
            {
                _counterValues[instrument] = measurement;
            });
            // The size gauges sample the resident page count during the run, so the result
            // shows whether an eviction backlog let the cache grow past its configured
            // limit, which would inflate the hit percentages, and how populated the main
            // queue actually is.
            _listener.SetMeasurementEventCallback<int>((instrument, measurement, tags, state) =>
            {
                if (instrument.Name == "flowtide_s3fifo_main_queue_size")
                {
                    Interlocked.Add(ref _mainSizeSum, measurement);
                    Interlocked.Increment(ref _mainSizeSamples);
                    return;
                }
                Interlocked.Add(ref _sizeSum, measurement);
                Interlocked.Increment(ref _sizeSamples);
                long current;
                while (measurement > (current = Volatile.Read(ref _sizeMax)))
                {
                    if (Interlocked.CompareExchange(ref _sizeMax, measurement, current) == current)
                    {
                        break;
                    }
                }
            });
            _listener.Start();
            _timer = new System.Threading.Timer(_ =>
            {
                try
                {
                    _listener?.RecordObservableInstruments();
                }
                catch
                {
                    // The listener can be disposed while a tick is in flight.
                }
            }, null, 20, 20);
        }

        public void Initialize(string? serializedConfig)
        {
        }

        public string SerializeResults()
        {
            long readHits = 0;
            long readMisses = 0;
            long commitHits = 0;
            long commitMisses = 0;
            long promotions = 0;
            long refills = 0;
            foreach (var kv in _counterValues)
            {
                switch (kv.Key.Name)
                {
                    case "flowtide_cache_read_hits":
                    case "flowtide_state_client_lookup_hits":
                        readHits += kv.Value;
                        break;
                    case "flowtide_cache_read_misses":
                        readMisses += kv.Value;
                        break;
                    case "flowtide_cache_commit_hits":
                        commitHits += kv.Value;
                        break;
                    case "flowtide_cache_commit_misses":
                        commitMisses += kv.Value;
                        break;
                    case "flowtide_s3fifo_small_queue_promotions":
                        promotions += kv.Value;
                        break;
                    case "flowtide_s3fifo_long_usage_refills":
                        refills += kv.Value;
                        break;
                }
            }
            var samples = Volatile.Read(ref _sizeSamples);
            var avgSize = samples > 0 ? (double)Volatile.Read(ref _sizeSum) / samples : 0;
            var mainSamples = Volatile.Read(ref _mainSizeSamples);
            var avgMain = mainSamples > 0 ? (double)Volatile.Read(ref _mainSizeSum) / mainSamples : 0;
            return string.Create(CultureInfo.InvariantCulture, $"{readHits}|{readMisses}|{commitHits}|{commitMisses}|{avgSize:0.##}|{Volatile.Read(ref _sizeMax)}|{promotions}|{avgMain:0.##}|{refills}");
        }
    }
}

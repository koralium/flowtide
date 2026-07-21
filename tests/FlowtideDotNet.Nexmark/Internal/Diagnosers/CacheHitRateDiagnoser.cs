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
    /// Reports the page cache hit percentage for the run.
    /// Hits are the shared table hits plus the state client lookup table hits, since the
    /// lock-free fast path bypasses the shared table counter. Misses are the table misses.
    /// </summary>
    internal class CacheHitRateDiagnoser : IInProcessDiagnoser
    {
        private readonly Dictionary<BenchmarkCase, (long Hits, long Misses)> results = [];

        public IEnumerable<string> Ids => [nameof(CacheHitRateDiagnoser)];

        public IEnumerable<IExporter> Exporters => [];

        public IEnumerable<IAnalyser> Analysers => [];

        public void DeserializeResults(BenchmarkCase benchmarkCase, string serializedResults)
        {
            var parts = serializedResults.Split('|');
            results.Add(benchmarkCase, (long.Parse(parts[0], CultureInfo.InvariantCulture), long.Parse(parts[1], CultureInfo.InvariantCulture)));
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
                var total = counts.Hits + counts.Misses;
                if (total > 0)
                {
                    yield return new Metric(new CacheHitRateMetricDescriptor(), 100.0 * counts.Hits / total);
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
    }

    public class CacheHitRateHandler : IInProcessDiagnoserHandler
    {
        private long _hits;
        private long _misses;
        private MeterListener? _listener;

        public ValueTask HandleAsync(BenchmarkSignal signal, InProcessDiagnoserActionArgs args, CancellationToken cancellationToken)
        {
            switch (signal)
            {
                case BenchmarkSignal.BeforeExtraIteration:
                    SetupMetricGatherer();
                    break;
                case BenchmarkSignal.AfterExtraIteration:
                    if (_listener != null)
                    {
                        // The counters are observable, they only report when polled.
                        // One pull here reads the final cumulative values while the streams
                        // meters are still alive, iteration cleanup disposes them later.
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
                // Exact names, cache_hits is a prefix of cache_hits_percentage.
                if (instrument.Name == "flowtide_lru_table_cache_hits" ||
                    instrument.Name == "flowtide_lru_table_cache_misses" ||
                    instrument.Name == "flowtide_state_client_lookup_hits")
                {
                    meterListener.EnableMeasurementEvents(instrument, null);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
            {
                if (instrument.Name == "flowtide_lru_table_cache_misses")
                {
                    Interlocked.Add(ref _misses, measurement);
                }
                else
                {
                    Interlocked.Add(ref _hits, measurement);
                }
            });
            _listener.Start();
        }

        public void Initialize(string? serializedConfig)
        {
        }

        public string SerializeResults()
        {
            return string.Create(CultureInfo.InvariantCulture, $"{Volatile.Read(ref _hits)}|{Volatile.Read(ref _misses)}");
        }
    }
}

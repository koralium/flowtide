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
        private readonly Dictionary<BenchmarkCase, (long ReadHits, long ReadMisses, long CommitHits, long CommitMisses)> results = [];

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
                long.Parse(parts[3], CultureInfo.InvariantCulture)));
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
    }

    public class CacheHitRateHandler : IInProcessDiagnoserHandler
    {
        private long _readHits;
        private long _readMisses;
        private long _commitHits;
        private long _commitMisses;
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
                if (instrument.Name == "flowtide_cache_read_hits" ||
                    instrument.Name == "flowtide_cache_read_misses" ||
                    instrument.Name == "flowtide_cache_commit_hits" ||
                    instrument.Name == "flowtide_cache_commit_misses" ||
                    instrument.Name == "flowtide_state_client_lookup_hits")
                {
                    meterListener.EnableMeasurementEvents(instrument, null);
                }
            };
            _listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
            {
                switch (instrument.Name)
                {
                    case "flowtide_cache_read_hits":
                    case "flowtide_state_client_lookup_hits":
                        Interlocked.Add(ref _readHits, measurement);
                        break;
                    case "flowtide_cache_read_misses":
                        Interlocked.Add(ref _readMisses, measurement);
                        break;
                    case "flowtide_cache_commit_hits":
                        Interlocked.Add(ref _commitHits, measurement);
                        break;
                    case "flowtide_cache_commit_misses":
                        Interlocked.Add(ref _commitMisses, measurement);
                        break;
                }
            });
            _listener.Start();
        }

        public void Initialize(string? serializedConfig)
        {
        }

        public string SerializeResults()
        {
            return string.Create(CultureInfo.InvariantCulture, $"{Volatile.Read(ref _readHits)}|{Volatile.Read(ref _readMisses)}|{Volatile.Read(ref _commitHits)}|{Volatile.Read(ref _commitMisses)}");
        }
    }
}

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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Nexmark.Diagnosers
{
    internal class EventCountDiagnoser : IInProcessDiagnoser
    {
        private readonly Dictionary<BenchmarkCase, long> results = [];

        public IEnumerable<string> Ids => [nameof(EventCountDiagnoser)];

        public IEnumerable<IExporter> Exporters => [];

        public IEnumerable<IAnalyser> Analysers => [];

        public void DeserializeResults(BenchmarkCase benchmarkCase, string serializedResults)
        {
            results.Add(benchmarkCase, long.Parse(serializedResults));
        }

        public void DisplayResults(ILogger logger)
        {
        }

        public InProcessDiagnoserHandlerData GetHandlerData(BenchmarkCase benchmarkCase)
        {
            return new(typeof(EventCountHandler), null);
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
            if (results.TryGetValue(diagnoserResults.BenchmarkCase, out var rowCount))
            {
                double execNano = diagnoserResults.Measurements.First(m => m.IterationStage == IterationStage.Result).Nanoseconds / 1_000_000_000.0;
                yield return new Metric(new EventCountMetricDescriptor(), rowCount / execNano);
            }
        }

        public IAsyncEnumerable<ValidationError> ValidateAsync(ValidationParameters validationParameters)
        {
            return AsyncEnumerable.Empty<ValidationError>();
        }

        internal class EventCountMetricDescriptor() : IMetricDescriptor
        {
            public string Id => "IngressFrequency";
            public string DisplayName => "Ingress rows / s";
            public string Legend => "";
            public string NumberFormat => "#0.0000";
            public UnitType UnitType => UnitType.Dimensionless;
            public string Unit => "Count";
            public bool TheGreaterTheBetter => false;
            public int PriorityInCategory => 0;
            public bool GetIsAvailable(Metric metric)
                => true;
        }
    }

    public class EventCountHandler : IInProcessDiagnoserHandler
    {
        private int _eventCount = 0;
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
                if (instrument.Name.Contains("IngressRowCount"))
                {
                    meterListener.EnableMeasurementEvents(instrument, null);
                }
            };
            _listener.SetMeasurementEventCallback<int>((instrument, measurement, tags, state) =>
            {
                Interlocked.Add(ref _eventCount, measurement);
            });
            _listener.Start();
        }

        public void Initialize(string? serializedConfig)
        {
        }

        public string SerializeResults()
        {
            return _eventCount.ToString();
        }
    }
}

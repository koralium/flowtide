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

using FlowtideDotNet.AspNetCore.TimeSeries.Instruments;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class MetricGatherer
    {
        private readonly MeterListener _meterListener;

        private readonly ConcurrentDictionary<string, IMetricInstrument> _instruments = new ConcurrentDictionary<string, IMetricInstrument>();
        private readonly MetricOptions options;
        private readonly MetricSeries series;
        private readonly ILogger? _logger;
        private Task _gatheringTask;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public MetricGatherer(MetricOptions options, MetricSeries series, ILogger? logger = null)
        {
            this.options = options;
            this.series = series;
            _logger = logger;
            _cancellationTokenSource = new CancellationTokenSource();
            _meterListener = new MeterListener();

            _meterListener.InstrumentPublished = OnInstrumentPublished;

            _meterListener.SetMeasurementEventCallback((Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var metricInstrument))
                {
                    metricInstrument.Record(tags, measurement);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, long measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var metricInstrument))
                {
                    metricInstrument.Record(tags, measurement);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, byte measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var metricInstrument))
                {
                    metricInstrument.Record(tags, measurement);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, short measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var metricInstrument))
                {
                    metricInstrument.Record(tags, measurement);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, float measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var metricInstrument))
                {
                    metricInstrument.Record(tags, measurement);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, double measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var metricInstrument))
                {
                    metricInstrument.Record(tags, measurement);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, decimal measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var metricInstrument))
                {
                    metricInstrument.Record(tags, (double)measurement);
                }
            });

            _meterListener.Start();


            _gatheringTask = Task.Factory.StartNew(GatheringLoop, TaskCreationOptions.LongRunning)
                .Unwrap();
        }

        private async Task GatheringLoop()
        {
            int iterationsSinceLastPrune = 0;
            while (true)
            {
                _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                try
                {
                    _meterListener.RecordObservableInstruments();
                }
                catch (Exception e)
                {
                    // A throwing observable callback must not stop the values that were
                    // recorded from being stored.
                    _logger?.LogWarning(e, "Exception while recording observable instruments.");
                }

                // Unlock must only run when the lock was taken, an unbalanced release
                // silently breaks the mutual exclusion with the query endpoints.
                await series.Lock();
                try
                {
                    await StoreMeasurements(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), series);

                    if (options.MaxLifetime != null)
                    {
                        if (iterationsSinceLastPrune > 60)
                        {
                            await series.Prune(DateTimeOffset.UtcNow.Subtract(options.MaxLifetime.Value).ToUnixTimeMilliseconds());
                            iterationsSinceLastPrune = 0;
                        }
                        else
                        {
                            iterationsSinceLastPrune++;
                        }
                    }
                }
                catch (Exception e)
                {
                    _logger?.LogWarning(e, "Exception while storing metric measurements.");
                }
                finally
                {
                    series.Unlock();
                }

                await Task.Delay(series.Rate, _cancellationTokenSource.Token);
            }
        }

        public async ValueTask StoreMeasurements(long timestamp, MetricSeries series)
        {
            foreach (var instrument in _instruments)
            {
                try
                {
                    await instrument.Value.StoreMeasurements(timestamp, series);
                }
                catch (Exception e)
                {
                    // One faulty instrument must not stop the remaining instruments from
                    // being stored, otherwise every instrument that enumerates after it
                    // silently disappears from the metrics API.
                    _logger?.LogWarning(e, "Exception while storing measurements for instrument {instrument}.", instrument.Key);
                }
            }
        }

        private void OnInstrumentPublished(Instrument instrument, MeterListener meterListener)
        {
            if (options.Prefixes != null)
            {
                var meterName = instrument.Name;
                // Check if the meter name starts with any of the prefixes
                if (!options.Prefixes.Any(prefix => meterName.StartsWith(prefix)))
                {
                    return;
                }
            }
            var typeDefinition = instrument.GetType().GetGenericTypeDefinition();
            var innerType = instrument.GetType().GetGenericArguments()[0];
            if (typeDefinition.Equals(typeof(Counter<>)))
            {
                AddCounter(innerType, instrument, meterListener, false);
            }
            else if (typeDefinition.Equals(typeof(Histogram<>)))
            {
                AddHistogram(innerType, instrument, meterListener);
            }
            else if (typeDefinition.Equals(typeof(UpDownCounter<>)))
            {
                AddCounter(innerType, instrument, meterListener, false);
            }
            else if (typeDefinition.Equals(typeof(ObservableCounter<>)))
            {
                AddCounter(innerType, instrument, meterListener, true);
            }
            else if (typeDefinition.Equals(typeof(ObservableUpDownCounter<>)))
            {
                AddCounter(innerType, instrument, meterListener, true);
            }
            else if (typeDefinition.Equals(typeof(ObservableGauge<>)))
            {
                AddGauge(innerType, instrument, meterListener);
            }
        }

        private void AddGauge(Type innerType, Instrument instrument, MeterListener meterListener)
        {
            var gauge = new GaugeInstrument(instrument.Name.Replace(".", "_"));
            _instruments.AddOrUpdate($"{instrument.Meter.Name}.{instrument.Name}", gauge, (key, old) => gauge);
            meterListener.EnableMeasurementEvents(instrument);
        }

        private void AddHistogram(Type innerType, Instrument instrument, MeterListener meterListener)
        {
            var histogram = new HistogramInstrument(instrument.Name.Replace(".", "_"));
            _instruments.AddOrUpdate($"{instrument.Meter.Name}.{instrument.Name}", histogram, (key, old) => histogram);
            meterListener.EnableMeasurementEvents(instrument);
        }

        private void AddCounter(Type innerType, Instrument instrument, MeterListener meterListener, bool isObservable)
        {
            var counter = new CounterInstrument(instrument.Name.Replace(".", "_"), isObservable);
            _instruments.AddOrUpdate($"{instrument.Meter.Name}.{instrument.Name}", counter, (key, old) => counter);
            meterListener.EnableMeasurementEvents(instrument);
        }
    }
}

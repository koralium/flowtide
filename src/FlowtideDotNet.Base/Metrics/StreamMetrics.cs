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

using FlowtideDotNet.Base.Metrics.Counter;
using FlowtideDotNet.Base.Metrics.Gauge;
using FlowtideDotNet.Base.Metrics.Internal;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Base.Metrics
{
    public class StreamMetrics : IDisposable
    {
        private readonly MeterListener _meterListener;
        private readonly ConcurrentDictionary<string, IMetricInstrument> _instruments = new ConcurrentDictionary<string, IMetricInstrument>();
        private readonly string _streamName;
        private readonly Dictionary<string, IMeter> _meters = new Dictionary<string, IMeter>();
        private bool disposedValue;
        private readonly Dictionary<string, VertexInstruments> _vertices = new Dictionary<string, VertexInstruments>();
        private readonly object _lock = new object();

        public StreamMetrics(string streamName)
        {
            _meterListener = new MeterListener();

            _meterListener.InstrumentPublished = OnInstrumentPublished;

            _meterListener.SetMeasurementEventCallback((Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var instrumentBase))
                {
                    instrumentBase.RecordMeasurement(measurement, tags);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, long measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var instrumentBase))
                {
                    instrumentBase.RecordMeasurement(measurement, tags);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, byte measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var instrumentBase))
                {
                    instrumentBase.RecordMeasurement(measurement, tags);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, short measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var instrumentBase))
                {
                    instrumentBase.RecordMeasurement(measurement, tags);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, float measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var instrumentBase))
                {
                    instrumentBase.RecordMeasurement(measurement, tags);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, double measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var instrumentBase))
                {
                    instrumentBase.RecordMeasurement(measurement, tags);
                }
            });
            _meterListener.SetMeasurementEventCallback((Instrument instrument, decimal measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            {
                var instrumentKey = $"{instrument.Meter.Name}.{instrument.Name}";
                if (_instruments.TryGetValue(instrumentKey, out var instrumentBase))
                {
                    instrumentBase.RecordMeasurement(measurement, tags);
                }
            });
            _meterListener.Start();
            this._streamName = streamName;
        }

        private string GetStreamPrefix()
        {
            return $"flowtide.{_streamName}.operator.";
        }

        private string GetVertexMeterName(string operatorName)
        {
            return $"{GetStreamPrefix()}{operatorName}";
        }

        private void AddCounter(Type innerType, string operatorName, Instrument instrument, MeterListener meterListener)
        {
            var counter = new CounterMetric(innerType, instrument.Name, instrument.Unit, instrument.Description);
            lock (_lock)
            {
                if (!_vertices.TryGetValue(operatorName, out var instruments))
                {
                    instruments = new VertexInstruments(operatorName);
                    _vertices.Add(operatorName, instruments);
                }
                instruments.Counters.Add(counter);
            }
            _instruments.AddOrUpdate($"{instrument.Meter.Name}.{instrument.Name}", counter, (key, old) => counter);
            meterListener.EnableMeasurementEvents(instrument);
        }

        private void AddGauge(Type innerType, string operatorName, Instrument instrument, MeterListener meterListener)
        {
            var gauge = new GaugeMetric(innerType, instrument.Name, instrument.Unit, instrument.Description);
            lock (_lock)
            {
                if (!_vertices.TryGetValue(operatorName, out var instruments))
                {
                    instruments = new VertexInstruments(operatorName);
                    _vertices.Add(operatorName, instruments);
                }
                instruments.Gauges.Add(gauge);
            }
            _instruments.AddOrUpdate($"{instrument.Meter.Name}.{instrument.Name}", gauge, (key, old) => gauge);
            meterListener.EnableMeasurementEvents(instrument);
        }

        private void OnInstrumentPublished(Instrument instrument, MeterListener meterListener)
        {
            var streamPrefix = GetStreamPrefix();
            // Check that the instrument is regarding this stream
            if (instrument.Meter.Name.StartsWith(streamPrefix))
            {
                var typeDefinition = instrument.GetType().GetGenericTypeDefinition();
                var operatorName = instrument.Meter.Name.Substring(streamPrefix.Length);
                var innerType = instrument.GetType().GetGenericArguments()[0];

                var key = $"{instrument.Meter.Name}.{instrument.Name}";

                // Do not create more instruments that already exist
                if (_instruments.ContainsKey(key))
                {
                    return;
                }

                if (typeDefinition.Equals(typeof(Counter<>)))
                {
                    AddCounter(innerType, operatorName, instrument, meterListener);
                }
                else if (typeDefinition.Equals(typeof(Histogram<>)))
                {
                    // TODO: Not yet supported
                }
                else if (typeDefinition.Equals(typeof(UpDownCounter<>)))
                {
                    AddCounter(innerType, operatorName, instrument, meterListener);
                }
                else if (typeDefinition.Equals(typeof(ObservableCounter<>)))
                {
                    AddCounter(innerType, operatorName, instrument, meterListener);
                }
                else if (typeDefinition.Equals(typeof(ObservableUpDownCounter<>)))
                {
                    AddCounter(innerType, operatorName, instrument, meterListener);
                }
                else if (typeDefinition.Equals(typeof(ObservableGauge<>)))
                {
                    AddGauge(innerType, operatorName, instrument, meterListener);
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        public IMeter GetOrCreateVertexMeter(string operatorName, Func<string> displayName)
        {
            if (!_meters.TryGetValue(operatorName, out var meter))
            {
                var m = new Meter(GetVertexMeterName(operatorName));
                var tagList = new TagList()
                {
                    { "operator", operatorName },
                    { "stream", _streamName }
                };
                meter = new FlowtideMeter(m, tagList, displayName);
                _meters.Add(operatorName, meter);
            }
            return meter;
        }

        public Dictionary<string, GraphNodeMetrics> GetSnapshot()
        {
            _meterListener.RecordObservableInstruments();

            Dictionary<string, GraphNodeMetrics> output = new Dictionary<string, GraphNodeMetrics>();
            foreach(var vertex in _vertices)
            {
                output.Add(vertex.Key, new GraphNodeMetrics(
                    vertex.Value.OperatorName, 
                    vertex.Value.Counters.Select(x => x.GetSnapshot()).ToList(),
                    vertex.Value.Gauges.Select(x => x.GetSnapshot()).ToList()
                ));
            }

            return output;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                }

                _meterListener.Dispose();
                foreach(var kv in _meters)
                {
                    kv.Value.Dispose();
                }
                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        ~StreamMetrics()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

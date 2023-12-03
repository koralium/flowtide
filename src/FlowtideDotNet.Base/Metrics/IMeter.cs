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

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Base.Metrics
{
    /// <summary>
    /// Copy of System.Diagnostics.Metrics.Meter as an interface
    /// </summary>
    public interface IMeter : IDisposable
    {
        /// <summary>
        /// Returns the Meter name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Returns the Meter Version.
        /// </summary>
        public string? Version { get; }

        /// <summary>
        /// Create a metrics Counter object.
        /// </summary>
        /// <param name="name">The instrument name. cannot be null.</param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        /// <remarks>
        /// Counter is an Instrument which supports non-negative increments.
        /// Example uses for Counter: count the number of bytes received, count the number of requests completed, count the number of accounts created, count the number of checkpoints run, and count the number of HTTP 5xx errors.
        /// </remarks>
        public ICounter<T> CreateCounter<T>(string name, string? unit = null, string? description = null) where T : struct;

        /// <summary>
        /// Histogram is an Instrument which can be used to report arbitrary values that are likely to be statistically meaningful. It is intended for statistics such as histograms, summaries, and percentile.
        /// </summary>
        /// <param name="name">The instrument name. cannot be null.</param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        /// <remarks>
        /// Example uses for Histogram: the request duration and the size of the response payload.
        /// </remarks>
        public IHistogram<T> CreateHistogram<T>(string name, string? unit = null, string? description = null) where T : struct;

        /// <summary>
        /// Create a metrics UpDownCounter object.
        /// </summary>
        /// <param name="name">The instrument name. Cannot be null.</param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        /// <remarks>
        /// UpDownCounter is an Instrument which supports reporting positive or negative metric values.
        /// Example uses for UpDownCounter: reporting the change in active requests or queue size.
        /// </remarks>
        public IUpDownCounter<T> CreateUpDownCounter<T>(string name, string? unit = null, string? description = null) where T : struct;

        /// <summary>
        /// Create an ObservableUpDownCounter object. ObservableUpDownCounter is an Instrument which reports increasing or decreasing value(s) when the instrument is being observed.
        /// </summary>
        /// <param name="name">The instrument name. Cannot be null.</param>
        /// <param name="observeValue">The callback to call to get the measurements when the <see cref="ObservableInstrument{t}.Observe()" /> is called by <see cref="MeterListener.RecordObservableInstruments" />.</param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        /// <remarks>
        /// Example uses for ObservableUpDownCounter: the process heap size or the approximate number of items in a lock-free circular buffer.
        /// </remarks>
        public IObservableUpDownCounter<T> CreateObservableUpDownCounter<T>(string name, Func<T> observeValue, string? unit = null, string? description = null) where T : struct;


        /// <summary>
        /// Create an ObservableUpDownCounter object. ObservableUpDownCounter is an Instrument which reports increasing or decreasing value(s) when the instrument is being observed.
        /// </summary>
        /// <param name="name">The instrument name. Cannot be null.</param>
        /// <param name="observeValue">The callback to call to get the measurements when the <see cref="ObservableInstrument{t}.Observe()" /> is called by <see cref="MeterListener.RecordObservableInstruments" /></param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        /// <remarks>
        /// Example uses for ObservableUpDownCounter: the process heap size or the approximate number of items in a lock-free circular buffer.
        /// </remarks>
        public IObservableUpDownCounter<T> CreateObservableUpDownCounter<T>(string name, Func<Measurement<T>> observeValue, string? unit = null, string? description = null) where T : struct;

        /// <summary>
        /// Create an ObservableUpDownCounter object. ObservableUpDownCounter is an Instrument which reports increasing or decreasing value(s) when the instrument is being observed.
        /// </summary>
        /// <param name="name">The instrument name. Cannot be null.</param>
        /// <param name="observeValues">The callback to call to get the measurements when the <see cref="ObservableInstrument{t}.Observe()" /> is called by <see cref="MeterListener.RecordObservableInstruments" />.</param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        /// <remarks>
        /// Example uses for ObservableUpDownCounter: the process heap size or the approximate number of items in a lock-free circular buffer.
        /// </remarks>
        public IObservableUpDownCounter<T> CreateObservableUpDownCounter<T>(string name, Func<IEnumerable<Measurement<T>>> observeValues, string? unit = null, string? description = null) where T : struct;


        /// <summary>
        /// ObservableCounter is an Instrument which reports monotonically increasing value(s) when the instrument is being observed.
        /// </summary>
        /// <param name="name">The instrument name. cannot be null.</param>
        /// <param name="observeValue">The callback to call to get the measurements when the <see cref="ObservableInstrument{t}.Observe()" /> is called by <see cref="MeterListener.RecordObservableInstruments" />.</param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        /// <remarks>
        /// Example uses for ObservableCounter: The number of page faults for each process.
        /// </remarks>
        public IObservableCounter<T> CreateObservableCounter<T>(string name, Func<T> observeValue, string? unit = null, string? description = null) where T : struct;


        /// <summary>
        /// ObservableCounter is an Instrument which reports monotonically increasing value(s) when the instrument is being observed.
        /// </summary>
        /// <param name="name">The instrument name. cannot be null.</param>
        /// <param name="observeValue">The callback to call to get the measurements when the <see cref="ObservableInstrument{t}.Observe()" /> is called by <see cref="MeterListener.RecordObservableInstruments" /></param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        /// <remarks>
        /// Example uses for ObservableCounter: The number of page faults for each process.
        /// </remarks>
        public IObservableCounter<T> CreateObservableCounter<T>(string name, Func<Measurement<T>> observeValue, string? unit = null, string? description = null) where T : struct;

        /// <summary>
        /// ObservableCounter is an Instrument which reports monotonically increasing value(s) when the instrument is being observed.
        /// </summary>
        /// <param name="name">The instrument name. cannot be null.</param>
        /// <param name="observeValues">The callback to call to get the measurements when the <see cref="ObservableInstrument{t}.Observe()" /> is called by <see cref="MeterListener.RecordObservableInstruments" />.</param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        /// <remarks>
        /// Example uses for ObservableCounter: The number of page faults for each process.
        /// </remarks>
        public IObservableCounter<T> CreateObservableCounter<T>(string name, Func<IEnumerable<Measurement<T>>> observeValues, string? unit = null, string? description = null) where T : struct;


        /// <summary>
        /// ObservableGauge is an asynchronous Instrument which reports non-additive value(s) (e.g. the room temperature - it makes no sense to report the temperature value from multiple rooms and sum them up) when the instrument is being observed.
        /// </summary>
        /// <param name="name">The instrument name. cannot be null.</param>
        /// <param name="observeValue">The callback to call to get the measurements when the <see cref="ObservableInstrument{t}.Observe()" /> is called by <see cref="MeterListener.RecordObservableInstruments" />.</param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        public IObservableGauge<T> CreateObservableGauge<T>(string name, Func<T> observeValue, string? unit = null, string? description = null) where T : struct;

        /// <summary>
        /// ObservableGauge is an asynchronous Instrument which reports non-additive value(s) (e.g. the room temperature - it makes no sense to report the temperature value from multiple rooms and sum them up) when the instrument is being observed.
        /// </summary>
        /// <param name="name">The instrument name. cannot be null.</param>
        /// <param name="observeValue">The callback to call to get the measurements when the <see cref="ObservableInstrument{t}.Observe()" /> is called by <see cref="MeterListener.RecordObservableInstruments" />.</param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        public IObservableGauge<T> CreateObservableGauge<T>(string name, Func<Measurement<T>> observeValue, string? unit = null, string? description = null) where T : struct;

        /// <summary>
        /// ObservableGauge is an asynchronous Instrument which reports non-additive value(s) (e.g. the room temperature - it makes no sense to report the temperature value from multiple rooms and sum them up) when the instrument is being observed.
        /// </summary>
        /// <param name="name">The instrument name. cannot be null.</param>
        /// <param name="observeValues">The callback to call to get the measurements when the <see cref="ObservableInstrument{t}.Observe()" /> is called by <see cref="MeterListener.RecordObservableInstruments" />.</param>
        /// <param name="unit">Optional instrument unit of measurements.</param>
        /// <param name="description">Optional instrument description.</param>
        public IObservableGauge<T> CreateObservableGauge<T>(string name, Func<IEnumerable<Measurement<T>>> observeValues, string? unit = null, string? description = null) where T : struct;
    }
}

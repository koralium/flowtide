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

using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Base.Metrics.Internal
{
    internal class FlowtideMeter : IMeter
    {
        private readonly Meter meter;
        private readonly TagList predefinedTagList;
        private readonly Func<string> displayName;

        public FlowtideMeter(Meter meter, TagList tagList, Func<string> displayName)
        {
            this.meter = meter;
            this.predefinedTagList = tagList;
            this.displayName = displayName;
        }

        public string Name => meter.Name;

        public string? Version => meter.Version;

        private static string GetMeasurementName(string name)
        {
            return $"flowtide_{name}";
        }

        private TagList GetTagList()
        {
            var l = predefinedTagList;
            l.Add("title", displayName());
            return l;
        }

        public ICounter<T> CreateCounter<T>(string name, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideCounter<T>(meter, GetMeasurementName(name), GetTagList());
        }

        public IHistogram<T> CreateHistogram<T>(string name, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideHistogram<T>(meter.CreateHistogram<T>(GetMeasurementName(name), unit, description), GetTagList());
        }

        public IObservableCounter<T> CreateObservableCounter<T>(string name, Func<T> observeValue, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideObservableCounter<T>(meter.CreateObservableCounter<T>(GetMeasurementName(name), () =>
            {
                return new Measurement<T>(observeValue(), GetTagList());
            }, unit, description));
        }

        public IObservableCounter<T> CreateObservableCounter<T>(string name, Func<Measurement<T>> observeValue, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideObservableCounter<T>(meter.CreateObservableCounter<T>(GetMeasurementName(name), () =>
            {
                var m = observeValue();
                TagList outputTags = new TagList();
                foreach(var t in m.Tags)
                {
                    outputTags.Add(t);
                }
                foreach (var t in GetTagList())
                {
                    outputTags.Add(t);
                }
                return new Measurement<T>(m.Value, outputTags);
            }, unit, description));
        }

        public IObservableCounter<T> CreateObservableCounter<T>(string name, Func<IEnumerable<Measurement<T>>> observeValues, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideObservableCounter<T>(meter.CreateObservableCounter<T>(GetMeasurementName(name), () =>
            {
                List<Measurement<T>> output = new List<Measurement<T>>();
                var values = observeValues();
                foreach(var val in values)
                {
                    TagList outputTags = new TagList();
                    foreach (var t in val.Tags)
                    {
                        outputTags.Add(t);
                    }
                    foreach (var t in GetTagList())
                    {
                        outputTags.Add(t);
                    }
                    output.Add(new Measurement<T>(val.Value, outputTags));
                }
                return output;
            }, unit, description));
        }

        public IObservableGauge<T> CreateObservableGauge<T>(string name, Func<T> observeValue, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideObservableGauge<T>(meter.CreateObservableGauge<T>(GetMeasurementName(name), () =>
            {
                return new Measurement<T>(observeValue(), GetTagList());
            }, unit, description));
        }

        public IObservableGauge<T> CreateObservableGauge<T>(string name, Func<Measurement<T>> observeValue, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideObservableGauge<T>(meter.CreateObservableGauge<T>(GetMeasurementName(name), () =>
            {
                var m = observeValue();
                TagList outputTags = new TagList();
                foreach (var t in m.Tags)
                {
                    outputTags.Add(t);
                }
                foreach (var t in GetTagList())
                {
                    outputTags.Add(t);
                }
                return new Measurement<T>(m.Value, outputTags);
            }, unit, description));
        }

        public IObservableGauge<T> CreateObservableGauge<T>(string name, Func<IEnumerable<Measurement<T>>> observeValues, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideObservableGauge<T>(meter.CreateObservableGauge<T>(GetMeasurementName(name), () =>
            {
                List<Measurement<T>> output = new List<Measurement<T>>();
                var values = observeValues();
                foreach (var val in values)
                {
                    TagList outputTags = new TagList();
                    foreach (var t in val.Tags)
                    {
                        outputTags.Add(t);
                    }
                    foreach (var t in GetTagList())
                    {
                        outputTags.Add(t);
                    }
                    output.Add(new Measurement<T>(val.Value, outputTags));
                }
                return output;
            }, unit, description));
        }

        public IObservableUpDownCounter<T> CreateObservableUpDownCounter<T>(string name, Func<T> observeValue, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideObservableUpDownCounter<T>(meter.CreateObservableUpDownCounter<T>(GetMeasurementName(name), () =>
            {
                return new Measurement<T>(observeValue(), GetTagList());
            }, unit, description));
        }

        public IObservableUpDownCounter<T> CreateObservableUpDownCounter<T>(string name, Func<Measurement<T>> observeValue, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideObservableUpDownCounter<T>(meter.CreateObservableUpDownCounter<T>(GetMeasurementName(name), () =>
            {
                var m = observeValue();
                TagList outputTags = new TagList();
                foreach (var t in m.Tags)
                {
                    outputTags.Add(t);
                }
                foreach (var t in GetTagList())
                {
                    outputTags.Add(t);
                }
                return new Measurement<T>(m.Value, outputTags);
            }, unit, description));
        }

        public IObservableUpDownCounter<T> CreateObservableUpDownCounter<T>(string name, Func<IEnumerable<Measurement<T>>> observeValues, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideObservableUpDownCounter<T>(meter.CreateObservableUpDownCounter<T>(GetMeasurementName(name), () =>
            {
                List<Measurement<T>> output = new List<Measurement<T>>();
                var values = observeValues();
                foreach (var val in values)
                {
                    TagList outputTags = new TagList();
                    foreach (var t in val.Tags)
                    {
                        outputTags.Add(t);
                    }
                    foreach (var t in GetTagList())
                    {
                        outputTags.Add(t);
                    }
                    output.Add(new Measurement<T>(val.Value, outputTags));
                }
                return output;
            }, unit, description));
        }

        public IUpDownCounter<T> CreateUpDownCounter<T>(string name, string? unit = null, string? description = null) where T : struct
        {
            return new FlowtideUpDownCounter<T>(meter.CreateUpDownCounter<T>(GetMeasurementName(name), unit, description), GetTagList());
        }

        public void Dispose()
        {
            meter.Dispose();
        }
    }
}

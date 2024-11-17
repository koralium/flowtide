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
    internal class FlowtideCounter<T> : ICounter<T>
        where T : struct
    {
        private readonly ObservableCounter<long> m_counter;
        private readonly KeyValuePair<string, object?>[] m_globalTags;
        private long _value;

        public FlowtideCounter(Meter meter, string name, TagList globalTags)
        {
            this.m_globalTags = globalTags.ToArray();
            this.m_counter = meter.CreateObservableCounter(name, () =>
            {
                return new Measurement<long>(_value, m_globalTags);
            });
        }

        public void Add(T delta)
        {
            if (delta is long l)
            {
                Interlocked.Add(ref _value, l);
            }
            else if (delta is int i)
            {
                Interlocked.Add(ref _value, i);
            }
            else
            {
                Interlocked.Add(ref _value, Convert.ToInt64(delta));
            }
        }
    }
}

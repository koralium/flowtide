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

namespace FlowtideDotNet.Base.Metrics
{
    public class GraphNode
    {
        public GraphNode(
            string operatorName,
            string displayName,
            IReadOnlyList<CounterSnapshot> counters,
            IReadOnlyList<GaugeSnapshot> gauges)
        {
            OperatorName = operatorName;
            DisplayName = displayName;
            Counters = counters;
            Gauges = gauges;
        }

        public string OperatorName { get; }

        public string DisplayName { get; }

        public IReadOnlyList<CounterSnapshot> Counters { get; }

        public IReadOnlyList<GaugeSnapshot> Gauges { get; }
    }
}

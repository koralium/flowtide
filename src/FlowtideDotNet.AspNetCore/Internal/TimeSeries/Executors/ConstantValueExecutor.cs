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


using System.Collections.Immutable;
using System.Globalization;

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class ConstantValueExecutor : IMetricExecutor
    {
        private readonly double value;

        public ConstantValueExecutor(double value)
        {
            this.value = value;
        }

        public IReadOnlyDictionary<string, string> Tags => ImmutableDictionary<string, string>.Empty;

        public SerieType SerieType => SerieType.Scalar;

        public string Name => value.ToString(CultureInfo.InvariantCulture);

        public async IAsyncEnumerable<MetricResult> GetValues(long startTimestamp, long endTimestamp, int stepWidth)
        {
            yield return new MetricResult(value, startTimestamp);
        }
    }
}

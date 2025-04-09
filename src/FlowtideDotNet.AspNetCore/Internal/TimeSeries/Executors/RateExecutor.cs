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


namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class RateExecutor : IMetricExecutor
    {
        private readonly IMetricExecutor serie;
        private readonly long length;

        public RateExecutor(IMetricExecutor serie, long length)
        {
            this.serie = serie;
            this.length = length;
        }

        public IReadOnlyDictionary<string, string> Tags => serie.Tags;

        public SerieType SerieType => serie.SerieType;

        public string Name => $"rate({serie.Name})";

        public async IAsyncEnumerable<MetricResult> GetValues(long startTimestamp, long endTimestamp, int stepWidth)
        {
            long rate_length = length;

            if (rate_length < stepWidth)
            {
                rate_length = stepWidth;
            }

            var indexDistance = (int)(rate_length / stepWidth);

            // Multiply by 1.5 to make sure a value is available for the value.
            var newStartTime = startTimestamp - (int)((double)rate_length * 1.5);
            var valueIterator = serie.GetValues(newStartTime, endTimestamp, stepWidth);

            double[] oldValues = new double[indexDistance + 1];
            int index = 0;
            var divisorInSeconds = ((double)rate_length) / 1000;
            bool initialYield = false;
            await foreach (var val in valueIterator)
            {
                oldValues[index % (indexDistance + 1)] = val.value;

                if (!initialYield)
                {
                    yield return new MetricResult((val.value - oldValues[(Math.Abs(index - indexDistance)) % (indexDistance + 1)]) / divisorInSeconds, val.timestamp);
                    initialYield = true;
                }
                else if (index >= indexDistance)
                {
                    yield return new MetricResult((val.value - oldValues[(index - indexDistance) % (indexDistance + 1)]) / divisorInSeconds, val.timestamp);
                }
                index++;
            }
        }
    }
}

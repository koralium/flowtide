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

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class SumExecutor : IMetricExecutor
    {
        private readonly List<IMetricExecutor> series;

        public IReadOnlyDictionary<string, string> Tags { get; }

        public SerieType SerieType => SerieType.Matrix;

        public string Name => "Sum";

        public SumExecutor(List<IMetricExecutor> series, IReadOnlyDictionary<string, string>? tags = default)
        {
            this.series = series;
            Tags = tags ?? ImmutableDictionary<string, string>.Empty;
        }

        public async IAsyncEnumerable<MetricResult> GetValues(long startTimestamp, long endTimestamp, int stepWidth)
        {
            var iterators = series.Select(v => v.GetValues(startTimestamp, endTimestamp, stepWidth).GetAsyncEnumerator()).ToList();
            
            bool[] done = new bool[iterators.Count];
            long[] lastTimestamps = new long[iterators.Count];
            List<(IAsyncEnumerator<MetricResult>, int)> outputIterators = new List<(IAsyncEnumerator<MetricResult>, int)>();

            for(int i = 0; i < iterators.Count; i++)
            {
                outputIterators.Add((iterators[i], i));
            }
            
            while (!done.All(x => x))
            {
                // Find iterators that have the same lowest timestamp, output those values
                // Only move forward iterators that are behind
                foreach(var iterator in outputIterators)
                {
                    if (!await iterator.Item1.MoveNextAsync())
                    {
                        done[iterator.Item2] = true;
                    }
                }
                outputIterators.Clear();

                long lowestTimestamp = long.MaxValue;
                for (int i = 0; i < iterators.Count; i++)
                {
                    if (done[i])
                    {
                        continue;
                    }
                    if (iterators[i].Current.timestamp < lowestTimestamp)
                    {
                        lowestTimestamp = iterators[i].Current.timestamp;
                        if (outputIterators.Count > 0)
                        {
                            outputIterators.Clear();
                        }
                        outputIterators.Add((iterators[i], i));
                    }
                    else if (iterators[i].Current.timestamp == lowestTimestamp)
                    {
                        outputIterators.Add((iterators[i], i));
                    }
                }
                if (outputIterators.Count > 0)
                {
                    yield return new MetricResult(outputIterators.Sum(i => i.Item1.Current.value), lowestTimestamp);
                }
            }
        }
    }
}

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

using FlowtideDotNet.Storage.AppendTree.Internal;

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class MetricSerie : IMetricExecutor
    {
        private readonly AppendTree<long, double, TimestampKeyContainer, DoubleValueContainer> tree;
        public MetricSerie(
            string name,
            IReadOnlyDictionary<string, string> tags,
            AppendTree<long, double, TimestampKeyContainer, DoubleValueContainer> tree)
        {
            Name = name;
            this.tree = tree;
            Tags = tags;
        }

        public IReadOnlyDictionary<string, string> Tags { get; }

        public string Name { get; }

        public SerieType SerieType => SerieType.Matrix;

        public ValueTask SetValue(long timestamp, double value)
        {
            return tree.Append(timestamp, value);
        }

        public ValueTask Prune(long timestamp)
        {
            return tree.Prune(timestamp);
        }

        public async IAsyncEnumerable<MetricResult> GetValues(long startTimestamp, long endTimestamp, int stepWidth)
        {
            using var iterator = tree.CreateIterator();
            await iterator.Seek(startTimestamp);

            //bool ended = false;
            long lastTimestamp = 0;
            await foreach (var kv in iterator)
            {
                //if (ended)
                //{
                //    yield break;
                //}
                //foreach (var kv in page)
                //{
                    // Skip values that do not follow the step width
                    if (kv.Key < (lastTimestamp + stepWidth))
                    {
                        continue;
                    }
                    if (kv.Key > endTimestamp)
                    {
                        //ended = true;
                        yield break;
                    }
                    
                    // Remove the step width from the info to help match in aggregate operators.
                    var timestamp = (kv.Key / stepWidth) * stepWidth;
                    lastTimestamp = timestamp;
                    yield return new MetricResult(kv.Value, timestamp);
                //}
            }
        }
    }
}

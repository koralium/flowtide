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

using BenchmarkDotNet.Attributes;
using FlowtideDotNet.Core.ColumnStore.Utils;

namespace FlowtideDotNet.Benchmarks.ColumnStore.Utils
{
    public class IntArrayAdditionBenchmark
    {
        int[]? source;
        [IterationSetup]
        public void IterationSetup()
        {
            int elementCount = 100_000_000;
            source = new int[elementCount];

            for (int i = 0; i < elementCount; i++)
            {
                source[i] = i;
            }
        }

        [Benchmark]
        public void AddWithAvx()
        {
            AvxUtils.AddValueToElements(source, 5);
        }

        [Benchmark]
        public void AddInLoop()
        {
            for (int i = 0; i < source!.Length; i++)
            {
                source[i] += 5;
            }
        }
    }
}

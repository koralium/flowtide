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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Diagnostics;

namespace FlowtideDotNet.Benchmarks.DataStructures
{
    public class PrimitiveListDeleteBatchBenchmark
    {
        [Params(10_000, 100_000)]
        public int BaseCount { get; set; }

        [Params(1000, 10_000)]
        public int DeleteCount { get; set; }

        // Raw data generated once in GlobalSetup
        private int[] _baseData = Array.Empty<int>();
        private int[] _deleteTargets = Array.Empty<int>();

        // Pre-built PrimitiveList recreated before every iteration
        private PrimitiveList<int>? _targetList = null;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var rng = new Random(42);

            _baseData = new int[BaseCount];
            for (int i = 0; i < BaseCount; i++)
            {
                _baseData[i] = rng.Next();
            }

            var positions = new HashSet<int>();
            while (positions.Count < DeleteCount)
            {
                positions.Add(rng.Next(0, BaseCount));
            }
            _deleteTargets = positions.OrderBy(x => x).ToArray();
        }

        [IterationSetup]
        public void IterationSetup()
        {
            _targetList = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < _baseData.Length; i++)
            {
                _targetList.Add(_baseData[i]);
            }
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            Debug.Assert(_targetList != null);
            _targetList.Dispose();
        }

        [Benchmark(Baseline = true)]
        public void RemoveOneByOne()
        {
            Debug.Assert(_targetList != null);
            // Iterate in reverse so indices stay valid after each removal
            for (int i = _deleteTargets.Length - 1; i >= 0; i--)
            {
                _targetList.RemoveAt(_deleteTargets[i]);
            }
        }

        [Benchmark]
        public void DeleteBatch()
        {
            Debug.Assert(_targetList != null);
            _targetList.DeleteBatch(_deleteTargets.AsSpan());
        }

        [Benchmark]
        public void CreateNewFilteredList()
        {
            Debug.Assert(_targetList != null);
            var filtered = new PrimitiveList<int>(GlobalMemoryManager.Instance);

            int deleteIdx = 0;
            for (int i = 0; i < BaseCount; i++)
            {
                // Skip elements that match the next deletion target
                if (deleteIdx < _deleteTargets.Length && _deleteTargets[deleteIdx] == i)
                {
                    deleteIdx++;
                    continue;
                }

                filtered.Add(_targetList.Get(i));
            }

            filtered.Dispose();
        }
    }
}

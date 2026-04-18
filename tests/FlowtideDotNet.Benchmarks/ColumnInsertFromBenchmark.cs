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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Benchmarks
{
    [MemoryDiagnoser]
    public class ColumnInsertFromBenchmark
    {
        private Column? _target;
        private Column? _source;
        private int[]? _sortedLookup;
        private int[]? _insertPositions;

        [Params(1000, 10_000)]
        public int InsertCount { get; set; }

        [Params(10_000, 100_000)]
        public int TargetStartSize { get; set; }

        [IterationSetup]
        public void Setup()
        {
            var rng = new Random(42);

            _target = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < TargetStartSize; i++)
            {
                _target.Add(new StringValue($"target_{i:D5}"));
            }

            _source = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < InsertCount; i++)
            {
                _source.Add(new StringValue($"source_{i:D5}"));
            }

            _insertPositions = new int[InsertCount];
            for (int i = 0; i < InsertCount; i++)
            {
                _insertPositions[i] = rng.Next(0, TargetStartSize + 1);
            }
            Array.Sort(_insertPositions);

            _sortedLookup = new int[InsertCount];
            for (int i = 0; i < InsertCount; i++)
            {
                _sortedLookup[i] = i;
            }
        }

        [IterationCleanup]
        public void Cleanup()
        {
            _target?.Dispose();
            _source?.Dispose();
        }

        [Benchmark(Baseline = true)]
        public void InsertFromBatch()
        {
            _target!.InsertFrom(_source!, _sortedLookup.AsSpan(), _insertPositions.AsSpan());
        }

        [Benchmark]
        public void InsertFromIterative()
        {
            for (int i = _sortedLookup!.Length - 1; i >= 0; i--)
            {
                var value = _source!.GetValueAt(_sortedLookup[i], default);
                _target!.InsertAt(_insertPositions![i], value);
            }
        }
    }
}

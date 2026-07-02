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
    public class BitmapListInsertFromBenchmark
    {
        [Params(10_000, 100_000)]
        public int BaseCount { get; set; }

        [Params(1000, 10_000)]
        public int InsertCount { get; set; }

        // Raw data generated once in GlobalSetup
        private bool[] _baseData = Array.Empty<bool>();
        private bool[] _otherData = Array.Empty<bool>();
        private int[] _sortedLookup = Array.Empty<int>();
        private int[] _insertPositions = Array.Empty<int>();

        // Pre-built BitmapLists recreated before every iteration
        private BitmapList? _targetList = null;
        private BitmapList? _sourceList = null;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var rng = new Random(42);

            _baseData = new bool[BaseCount];
            for (int i = 0; i < BaseCount; i++)
            {
                _baseData[i] = rng.Next(0, 2) == 1;
            }

            _otherData = new bool[InsertCount];
            for (int i = 0; i < InsertCount; i++)
            {
                _otherData[i] = rng.Next(0, 2) == 1;
            }

            var positions = new HashSet<int>();
            while (positions.Count < InsertCount)
            {
                positions.Add(rng.Next(0, BaseCount + 1));
            }
            _insertPositions = positions.OrderBy(x => x).ToArray();

            _sortedLookup = new int[InsertCount];
            for (int i = 0; i < InsertCount; i++)
            {
                _sortedLookup[i] = i;
            }
        }

        [IterationSetup]
        public void IterationSetup()
        {
            _targetList = new BitmapList(GlobalMemoryManager.Instance);
            for (int i = 0; i < _baseData.Length; i++)
            {
                _targetList.Add(_baseData[i]);
            }

            _sourceList = new BitmapList(GlobalMemoryManager.Instance);
            for (int i = 0; i < _otherData.Length; i++)
            {
                _sourceList.Add(_otherData[i]);
            }
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            Debug.Assert(_targetList != null && _sourceList != null);
            _targetList.Dispose();
            _sourceList.Dispose();
        }

        [Benchmark(Baseline = true)]
        public void InsertOneByOne()
        {
            Debug.Assert(_targetList != null && _sourceList != null);
            for (int i = 0; i < _sortedLookup.Length; i++)
            {
                int oIdx = _sortedLookup[i];
                bool value = _sourceList.Get(oIdx);
                _targetList.InsertAt(_insertPositions[i] + i, value);
            }
        }

        [Benchmark]
        public void InsertFromBatch()
        {
            Debug.Assert(_targetList != null && _sourceList != null);
            ReadOnlySpan<int> sl = _sortedLookup; ReadOnlySpan<int> ip = _insertPositions; _targetList.InsertFrom(in _sourceList!, in sl, in ip, -1);
        }

        [Benchmark]
        public void CreateNewMergedList()
        {
            Debug.Assert(_targetList != null && _sourceList != null);
            var merged = new BitmapList(GlobalMemoryManager.Instance);

            int baseIdx = 0;
            int sourceIdx = 0;

            while (baseIdx < BaseCount || sourceIdx < InsertCount)
            {
                while (sourceIdx < InsertCount && _insertPositions[sourceIdx] == baseIdx)
                {
                    int oIdx = _sortedLookup[sourceIdx];
                    merged.Add(_sourceList.Get(oIdx));
                    sourceIdx++;
                }

                if (baseIdx < BaseCount)
                {
                    merged.Add(_targetList.Get(baseIdx));
                    baseIdx++;
                }
            }

            merged.Dispose();
        }
    }
}


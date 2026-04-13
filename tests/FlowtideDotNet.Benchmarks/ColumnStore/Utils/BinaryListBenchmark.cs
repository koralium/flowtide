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
using FlowtideDotNet.Storage.Memory;
using System.Diagnostics;

namespace FlowtideDotNet.Benchmarks.ColumnStore.Utils
{
    public class BinaryListBenchmark
    {
        public static List<byte[]> GenerateBinaryList(int count, int size)
        {
            var list = new List<byte[]>(count);
            for (int i = 0; i < count; i++)
            {
                list.Add(GenerateRandomBytes(size));
            }
            return list;
        }

        private static byte[] GenerateRandomBytes(int size)
        {
            var random = new Random();
            var bytes = new byte[size];
            random.NextBytes(bytes);
            return bytes;
        }

        private List<byte[]> toInsert = new List<byte[]>();

        [GlobalSetup]
        public void GlobalSetup()
        {
            toInsert = GenerateBinaryList(1_000_000, 100);
        }

        [Benchmark]
        public int TestInsertPointers()
        {
            var bl = new List<byte[]>();

            foreach (var item in toInsert)
            {
                bl.Add(item);
            }
            return bl.Count;
        }

        [Benchmark]
        public void TestInsert()
        {
            var bl = new BinaryList(GlobalMemoryManager.Instance);

            foreach (var item in toInsert)
            {
                bl.Add(item);
            }

        }


    }

    public class BinaryListInsertFromBenchmark
    {
        [Params(10_000, 100_000)]
        public int BaseCount { get; set; }

        [Params(1000, 10_000)]
        public int InsertCount { get; set; }

        // Raw data generated once in GlobalSetup
        private byte[][] _baseData = Array.Empty<byte[]>();
        private byte[][] _otherData = Array.Empty<byte[]>();
        private int[] _sortedLookup = Array.Empty<int>();
        private int[] _insertPositions = Array.Empty<int>();

        // Pre-built BinaryLists recreated before every iteration
        private BinaryList? _targetList = null;
        private BinaryList? _sourceList = null;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var rng = new Random(42);

            _baseData = new byte[BaseCount][];
            for (int i = 0; i < BaseCount; i++)
            {
                _baseData[i] = new byte[rng.Next(10, 100)];
                rng.NextBytes(_baseData[i]);
            }

            _otherData = new byte[InsertCount][];
            for (int i = 0; i < InsertCount; i++)
            {
                _otherData[i] = new byte[rng.Next(10, 100)];
                rng.NextBytes(_otherData[i]);
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
            _targetList = new BinaryList(GlobalMemoryManager.Instance);
            for (int i = 0; i < _baseData.Length; i++)
            {
                _targetList.Add(_baseData[i]);
            }

            _sourceList = new BinaryList(GlobalMemoryManager.Instance);
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
                var data = _sourceList.Get(oIdx);
                _targetList.Insert(_insertPositions[i] + i, data);
            }
        }

        [Benchmark]
        public void InsertFromBatch()
        {
            Debug.Assert(_targetList != null && _sourceList != null);
            _targetList.InsertFrom(_sourceList, _sortedLookup.AsSpan(), _insertPositions.AsSpan());
        }
    }
}

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
            ReadOnlySpan<int> sl = _sortedLookup.AsSpan();
            ReadOnlySpan<int> ip = _insertPositions.AsSpan();
            _targetList.InsertFrom(_sourceList, in sl, in ip, -1);
        }

        [Benchmark]
        public void CreateNewMergedList()
        {
            Debug.Assert(_targetList != null && _sourceList != null);
            var merged = new BinaryList(GlobalMemoryManager.Instance);

            int baseIdx = 0;
            int sourceIdx = 0;

            while (baseIdx < BaseCount || sourceIdx < InsertCount)
            {
                // Add all source elements whose insert position equals the current base index
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

    public class BinaryListDeleteBatchBenchmark
    {
        [Params(10_000, 100_000)]
        public int BaseCount { get; set; }

        [Params(1000, 10_000)]
        public int DeleteCount { get; set; }

        // Raw data generated once in GlobalSetup
        private byte[][] _baseData = Array.Empty<byte[]>();
        private int[] _deleteTargets = Array.Empty<int>();

        // Pre-built BinaryList recreated before every iteration
        private BinaryList? _targetList = null;

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
            _targetList = new BinaryList(GlobalMemoryManager.Instance);
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
            var filtered = new BinaryList(GlobalMemoryManager.Instance);

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

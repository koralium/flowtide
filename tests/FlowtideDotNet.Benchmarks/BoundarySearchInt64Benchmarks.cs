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
using BenchmarkDotNet.Diagnosers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using System.Diagnostics;

namespace FlowtideDotNet.Benchmarks
{
    [HardwareCounters(HardwareCounter.BranchMispredictions, HardwareCounter.BranchInstructions)]
    public class BoundarySearchInt64Benchmarks
    {
        private const int SearchSpace = 1024 * 1;

        private long[]? _arr;
        private Column? _column;
        private NativeLongList? _nativeLongList;
        private Int64Column? _int64Column;

        [GlobalSetup]
        public void GlobalSetup()
        {
            _arr = Enumerable.Range(0, SearchSpace).Select(x => (long)x).ToArray();
            _column = new Column(GlobalMemoryManager.Instance);
            _nativeLongList = new NativeLongList(GlobalMemoryManager.Instance);
            _int64Column = new Int64Column(GlobalMemoryManager.Instance);
            foreach (var item in _arr)
            {
                _column.Add(new Int64Value(item));
                _nativeLongList.Add(item);
                _int64Column.Add(new Int64Value(item));
            }
        }

        [Benchmark]
        public void ArrayBinarySearchBenchmark()
        {
            Debug.Assert(_arr != null);

            for (int i = 0; i < SearchSpace; i++)
            {
                Array.BinarySearch(_arr, i);
            }
        }


        [Benchmark]
        public void ColumnBoundarySearchBenchmark()
        {
            Debug.Assert(_column != null);

            for (int i = 0; i < SearchSpace; i++)
            {
                _column.SearchBoundries(new Int64Value(i), 0, SearchSpace - 1, default);
            }
        }

        [Benchmark]
        public void NativeLongListBoundarySearchBenchmark()
        {
            Debug.Assert(_nativeLongList != null);

            for (int i = 0; i < SearchSpace; i++)
            {
                BoundarySearch.SearchBoundriesInt64Asc(_nativeLongList, 0, SearchSpace - 1, i);
            }
        }

        [Benchmark]
        public void Int64ColumnBoundarySearchBenchmark()
        {
            Debug.Assert(_int64Column != null);

            for (int i = 0; i < SearchSpace; i++)
            {
                _int64Column.SearchBoundries(new Int64Value(i), 0, SearchSpace - 1, default, false);
            }
        }
    }
}

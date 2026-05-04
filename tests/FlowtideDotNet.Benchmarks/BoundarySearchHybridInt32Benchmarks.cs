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
using FlowtideDotNet.Core.ColumnStore.BoundarySearching;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Benchmarks
{
    /// <summary>
    /// Compares SearchBoundries_Hybrid_Int32 vs FallbackMethod vs raw BoundarySearch.SearchBoundriesAsc(int*).
    /// The raw pointer benchmark shows the cost of the FallbackMethod's interface dispatch overhead.
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class BoundarySearchHybridInt32Benchmarks
    {
        private Column _treeColumn = null!;
        private Column _inputColumn = null!;
        private int[] _inputSortedLookup = null!;
        private int[] _lowerBounds = null!;
        private int[] _upperBounds = null!;
        private DataValueContainer _xContainer = null!;
        private DataValueContainer _yContainer = null!;

        // Raw pointer for the direct SearchBoundriesAsc benchmark
        private int* _treeDataPtr;
        private int* _inputDataPtr;

        [Params(128, 1024, 8192)]
        public int TreeSize { get; set; }

        [Params(64, 512, 4096)]
        public int InputSize { get; set; }

        /// <summary>
        /// Number of times each value is repeated in the tree.
        /// 1 = all unique, 4 = each value repeated 4 times.
        /// </summary>
        [Params(1, 4)]
        public int DuplicateFactor { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            // Build tree column with Int32-range values (> short.MaxValue)
            // Each unique value is repeated DuplicateFactor times
            int uniqueValues = TreeSize / DuplicateFactor;
            _treeColumn = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < uniqueValues; i++)
            {
                for (int d = 0; d < DuplicateFactor; d++)
                {
                    _treeColumn.Add(new Int64Value(100000 + i));
                }
            }

            // Build input column: pick InputSize evenly spaced values from the tree's value range
            // Some will match, some will fall in gaps
            _inputColumn = Column.Create(GlobalMemoryManager.Instance);
            int valueRange = uniqueValues;
            for (int i = 0; i < InputSize; i++)
            {
                long value = 100000 + (long)i * valueRange / InputSize;
                _inputColumn.Add(new Int64Value(value));
            }

            // Identity lookup (input is already sorted ascending)
            _inputSortedLookup = Enumerable.Range(0, InputSize).ToArray();

            // Pre-allocate output arrays
            _lowerBounds = new int[InputSize];
            _upperBounds = new int[InputSize];

            // Containers for FallbackMethod
            _xContainer = new DataValueContainer();
            _yContainer = new DataValueContainer();

            // Get raw pointers for the direct SearchBoundriesAsc benchmark
            SelfComparePointers treePointers = default;
            _treeColumn.SetSelfComparePointers(ref treePointers);
            _treeDataPtr = (int*)treePointers.dataPointer;

            SelfComparePointers inputPointers = default;
            _inputColumn.SetSelfComparePointers(ref inputPointers);
            _inputDataPtr = (int*)inputPointers.dataPointer;
        }

        private void ResetBounds()
        {
            Array.Fill(_lowerBounds, 0);
            Array.Fill(_upperBounds, _treeColumn.Count - 1);
        }

        //[Benchmark(Baseline = true)]
        //public void FallbackMethod()
        //{
        //    ResetBounds();
        //    ColumnBoundarySearchDelegates.FallbackMethod(
        //        _treeColumn,
        //        _inputColumn,
        //        _inputSortedLookup,
        //        _lowerBounds,
        //        _upperBounds,
        //        _xContainer,
        //        _yContainer);
        //}

        [Benchmark]
        public void HybridInt32()
        {
            ResetBounds();
            BoundarySearchHybridPrimitiveNoNull.SearchBoundries_Hybrid<int>(
                _treeColumn,
                _inputColumn,
                _inputSortedLookup,
                _lowerBounds,
                _upperBounds,
                _xContainer,
                _yContainer);
        }

        /// <summary>
        /// Calls BoundarySearch.SearchBoundriesAsc(int*) directly for each input value.
        /// Shows the raw cost of branchless binary search without any interface dispatch,
        /// GetValueAt, or DataValueContainer overhead.
        /// </summary>
        [Benchmark]
        public void RawPointerPerElement()
        {
            int treeEnd = _treeColumn.Count - 1;
            int currentFastForward = 0;
            for (int i = 0; i < _inputSortedLookup.Length; i++)
            {
                int inputIndex = _inputSortedLookup[i];
                int value = _inputDataPtr[inputIndex];
                int start = currentFastForward;
                var (lower, _) = BoundarySearch.SearchBoundriesAsc(_treeDataPtr, value, start, treeEnd);
                currentFastForward = lower < 0 ? ~lower : lower;
            }
        }
    }
}

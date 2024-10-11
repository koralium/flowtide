using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using InlineIL;
using static InlineIL.IL.Emit;
using static Substrait.Protobuf.Expression.Types.Literal.Types;

namespace FlowtideDotNet.Benchmarks
{
    [HardwareCounters(HardwareCounter.BranchMispredictions, HardwareCounter.BranchInstructions)]
    public class BinarySearchBenchmarks
    {
        private const int SearchSpace = 1024 * 1;

        private long[] _arr;
        private Column _column;
        private NativeLongList _nativeLongList;
        private Int64Column _int64Column;

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
            for (int i = 0; i < SearchSpace; i++)
            {
                Array.BinarySearch(_arr, i);
            }
        }


        [Benchmark]
        public void ColumnBoundarySearchBenchmark()
        {
            for (int i = 0; i < SearchSpace; i++)
            {
                _column.SearchBoundries(new Int64Value(i), 0, SearchSpace - 1, default);
            }
        }

        [Benchmark]
        public void NativeLongListBoundarySearchBenchmark()
        {
            for (int i = 0; i < SearchSpace; i++)
            {
                BoundarySearch.SearchBoundriesInt64Asc(_nativeLongList, 0, SearchSpace - 1, i);
            }
        }

        [Benchmark]
        public void Int64ColumnBoundarySearchBenchmark()
        {
            for (int i = 0; i < SearchSpace; i++)
            {
                _int64Column.SearchBoundries(new Int64Value(i), 0, SearchSpace - 1, default, false);
            }
        }
    }
}

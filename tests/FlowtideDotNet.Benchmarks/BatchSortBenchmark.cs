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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Benchmarks
{
    //[MemoryDiagnoser]
    //[DisassemblyDiagnoser(printSource: true, maxDepth: 3)]
    public class BatchSortBenchmark
    {
        //public const int Count = 1_000;
        [Params(100, 1000, 100_000)]
        public int Count { get; set; }

        int[] indices = null!;
        int[] raw_data = null!;
        private IColumn[] columns = null!;
        private EventBatchData data = null!;
        private SortCompiler.SortDelegate sortMethod = null!;
        private readonly SelfComparePointers[] pointers = new SelfComparePointers[1];
        private BatchSortCompiler.CompareDelegate _compareDelegate = null!;
        private BatchSorter _batchSorter = null!;

        [GlobalSetup]
        public void GlobalSetup()
        {
            indices = new int[Count];
            raw_data = new int[Count];
            Column column = new Column(GlobalMemoryManager.Instance);
            Random r = new Random(123);
            for (int i = 0; i < Count; i++)
            {
                var next = r.Next();
                raw_data[i] = next;
                column.Add(new Int64Value(next));
            }
            columns = new IColumn[1] {column };
            data = new EventBatchData(columns);
            sortMethod = SortCompiler.GetOrCompile(columns);
            _compareDelegate = BatchSortCompiler.Compile(columns).Compile();
            _batchSorter = new BatchSorter(1);
        }

        private struct ComparerWithDelegate : IComparer<int>
        {
            private SortCompareContext context;
            private readonly BatchSortCompiler.CompareDelegate d;

            public ComparerWithDelegate(SortCompareContext context, BatchSortCompiler.CompareDelegate d)
            {
                this.context = context;
                this.d = d;
            }

            public int Compare(int x, int y)
            {
                return d(ref context, x, y);
            }
        }

        private class OldSortMethod : IComparer<int>
        {
            private readonly IColumn[] columns;
            private DataValueContainer _c1 = new DataValueContainer();
            private DataValueContainer _c2 = new DataValueContainer();

            public OldSortMethod(IColumn[] columns)
            {
                this.columns = columns;
            }
            public int Compare(int x, int y)
            {
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].GetValueAt(x, _c1, default);
                    columns[i].GetValueAt(y, _c2, default);
                    var result = DataValueComparer.CompareTo(_c1, _c2);
                    //var result = columns[i].CompareTo(columns[i], x, y);
                    if (result != 0)
                    {
                        return result;
                    }
                }
                return 0;
            }
        }

        [Benchmark]
        public void PureCsharp()
        {
            for (int i = 0; i < Count; i++)
            {
                indices[i] = i;
            }
            Array.Sort(indices, (x, y) => raw_data[x].CompareTo(raw_data[y]));
        }

        [Benchmark]
        public void OldMethod()
        {
            for (int i = 0; i < Count; i++)
            {
                indices[i] = i;
            }
            var comparer = new OldSortMethod(columns);
            indices.AsSpan().Sort(comparer);
        }

        [Benchmark]
        public void SortWithDelegate()
        {
            for (int i = 0; i < Count; i++)
            {
                indices[i] = i;
            }
            for (int i = 0; i < pointers.Length; i++)
            {
                columns[i].SetSelfComparePointers(ref pointers[i]);
            }
            var c = new ComparerWithDelegate(new SortCompareContext(columns, pointers), _compareDelegate);
            indices.AsSpan().Sort(c);
        }

        [Benchmark]
        public void SortMethod()
        {
            for (int i = 0; i < Count; i++)
            {
                indices[i] = i;
            }
            for (int i = 0; i < pointers.Length; i++)
            {
                columns[i].SetSelfComparePointers(ref pointers[i]);
            }
            var span = indices.AsSpan();
            sortMethod(new SortCompareContext(columns, pointers), ref span);
        }

        [Benchmark]
        public void BatchSorter()
        {
            for (int i = 0; i < Count; i++)
            {
                indices[i] = i;
            }
            var span = indices.AsSpan();
            _batchSorter.SortData(columns, ref span);
        }
    }
}

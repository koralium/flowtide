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
    [MemoryDiagnoser]
    public class BatchSortBenchmark
    {
        public const int Count = 1_000_000;

        int[] indices = new int[Count];
        int[] raw_data = new int[Count];
        private IColumn[] columns = null!;
        private EventBatchData data = null!;
        private SortCompiler.SortDelegate sortMethod = null!;
        private SelfComparePointers[] pointers = new SelfComparePointers[1];

        [GlobalSetup]
        public void GlobalSetup()
        {
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
            sortMethod = SortCompiler.Compile(columns);
        }


        [IterationSetup]
        public void BeforeIteration()
        {
            for (int i = 0; i < Count; i++)
            {
                indices[i] = i;
            }
        }

        private struct OldSortMethod : IComparer<int>
        {
            private readonly IColumn[] columns;

            public OldSortMethod(IColumn[] columns)
            {
                this.columns = columns;
            }
            public int Compare(int x, int y)
            {
                for (int i = 0; i < columns.Length; i++)
                {
                    var result = columns[i].CompareTo(columns[i], x, y);
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
            Array.Sort(indices, (x, y) => raw_data[x].CompareTo(raw_data[y]));
        }

        [Benchmark]
        public void OldMethod()
        {
            var comparer = new OldSortMethod(columns);
            indices.AsSpan().Sort(comparer);
        }

        [Benchmark]
        public void SortMethod()
        {
            for (int i = 0; i < pointers.Length; i++)
            {
                columns[i].SetSelfComparePointers(ref pointers[i]);
            }
            var span = indices.AsSpan();
            sortMethod(new SortCompareContext(columns, pointers), ref span);
        }
    }
}

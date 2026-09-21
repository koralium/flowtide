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
using FlowtideDotNet.Core.ColumnStore.Hash;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Columnar.Functions.HashFunctions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static FlowtideDotNet.Core.ColumnStore.Sort.BatchSortCompiler;

namespace FlowtideDotNet.Benchmarks
{
    public class XxHashBenchmark
    {
        [Params(1000)]
        public int Count { get; set; }

        [Params(1, 2)]
        public int ColumnCount { get; set; }

        private IColumn[] columns = null!;
        private EventBatchData data = null!;

        private XxHash32 _xxhash = new XxHash32();

        private BatchHasher? _batchHasher;
        private Func<EventBatchData, int, uint>? _hashFunction;

        [GlobalSetup]
        public void GlobalSetup()
        {
            Column[] columnArray = new Column[ColumnCount];
            Random r = new Random(123);
            for (int c = 0; c < ColumnCount; c++)
            {
                columnArray[c] = new Column(GlobalMemoryManager.Instance);
            }
            for (int i = 0; i < Count; i++)
            {
                var next = r.Next();
                for (int c = 0; c < ColumnCount; c++)
                {
                    columnArray[c].Add(new Int64Value(next));
                }
            }
            columns = new IColumn[ColumnCount];
            for (int c = 0; c < ColumnCount; c++)
            {
                columns[c] = columnArray[c];
            }
            data = new EventBatchData(columns);
            var fields = new List<Substrait.Expressions.Expression>();
            for (int i = 0; i < ColumnCount; i++)
            {
                fields.Add(new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = i } });
            } 
            _hashFunction = ColumnHashCompiler.CompileGetHashCode(fields, new FunctionsRegister());
            _batchHasher = new BatchHasher(Enumerable.Range(0, ColumnCount).ToArray());
        }

        [Benchmark]
        public void PerRow()
        {
            for (int i = 0; i < Count; i++)
            {
                _xxhash.Reset();
                for (int c = 0; c < ColumnCount; c++)
                {
                    columns[c].AddToHash(i, default, _xxhash);
                }
                _xxhash.GetCurrentHashAsUInt32();
            }
        }

        [Benchmark]
        public void HashFunctionDelegatePerRow()
        {
            for (int i = 0; i < Count; i++)
            {
                _hashFunction!(data, i);
            }
        }

        [Benchmark]
        public void PerColumn()
        {
            _batchHasher!.HashBatch(data);
        }
    }
}

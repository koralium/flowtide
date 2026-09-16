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
using static SqlParser.Ast.FetchDirection;

namespace FlowtideDotNet.Benchmarks
{
    public class XxHashBenchmark
    {
        [Params(1000)]
        public int Count { get; set; }

        int[] indices = null!;
        int[] raw_data = null!;
        private IColumn[] columns = null!;
        private EventBatchData data = null!;

        private XxHash32 _xxhash = new XxHash32();
        private Xxh32RowState[]? _states;
        private int[]? _hashIndices;
        private int[]? _scratch;
        private uint[]? _destination;

        private Func<EventBatchData, int, uint>? _hashFunction;

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
            columns = new IColumn[1] { column };
            data = new EventBatchData(columns);
            _states = new Xxh32RowState[Count];
            _hashIndices = new int[Count];
            for (int i = 0; i < _hashIndices.Length; i++)
            {
                _hashIndices[i] = i;
            }
            _scratch = new int[Count];
            _destination = new uint[Count];
            _hashFunction = ColumnHashCompiler.CompileGetHashCode(new List<Substrait.Expressions.Expression>() { new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 0 } } }, new FunctionsRegister());
        }

        [Benchmark]
        public void PerRow()
        {
            for (int i = 0; i < Count; i++)
            {
                _xxhash.Reset();
                columns[0].AddToHash(i, default, _xxhash);
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
            for (int i = 0; i < _states!.Length; i++)
            {
                _states[i].Init();
            }
            columns[0].AppendToXxHash32(_hashIndices!, default, _states!, _scratch!);

            for (int i = 0; i < _states.Length; i++)
            {
                XxHash32Implementation.GetCurrentHashAsUInt32(ref _states![i]);
            }
        }

        [Benchmark]
        public void ToXxHash32()
        {
            columns[0].ToXxHash32(_hashIndices!, default, _destination!, _scratch!);
        }
    }
}

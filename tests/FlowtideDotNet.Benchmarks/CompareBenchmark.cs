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
using BenchmarkDotNet.Columns;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Join;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Benchmarks
{
    public class CompareBenchmark
    {

        private class Comparer : IComparer<JoinStreamEvent>
        {
            public int Compare(JoinStreamEvent x, JoinStreamEvent y)
            {
                var c1 = x.GetColumn(0);
                var c2 = y.GetColumn(0);

                return FlxValueComparer.CompareTo(c1, c2);
            }
        }

        private class RefStructComparer : IComparer<JoinStreamEvent>
        {
            public int Compare(JoinStreamEvent x, JoinStreamEvent y)
            {
                var c1 = x.GetColumnRef(0);
                var c2 = y.GetColumnRef(0);

                return FlxValueRefComparer.CompareTo(c1, c2);
            }
        }

        private List<JoinStreamEvent> compactData = new List<JoinStreamEvent>();
        private List<JoinStreamEvent> arrayData = new List<JoinStreamEvent>();
        private FlowtideDotNet.Core.ColumnStore.Column column = new FlowtideDotNet.Core.ColumnStore.Column();
        private List<string> longList = new List<string>();
        private StringColumn stringColumn = new StringColumn();

        [GlobalSetup]
        public void GlobalSetup()
        {
            Random r = new Random(123);
            for (int i = 0; i < 1_000_000; i++)
            {
                var val = r.Next();
                stringColumn.Add(new FlowtideDotNet.Core.ColumnStore.StringValue(val.ToString()));
            }
        }


        [IterationSetup()]
        public void IterationSetup()
        {
            Random r = new Random(123);
            compactData.Clear();
            arrayData.Clear();
            column = new Core.ColumnStore.Column();
            longList.Clear();
            for (int i = 0; i < 1_000_000; i++)
            {
                var val = r.Next();
                var e = RowEvent.Create(1, 0, b =>
                {
                    b.Add(val);
                });
                longList.Add(val.ToString());
                compactData.Add(new JoinStreamEvent(0, 0, e.RowData));
                arrayData.Add(new JoinStreamEvent(0, 0, ArrayRowData.Create(e.RowData, default)));

                column.InsertAt(i, new FlowtideDotNet.Core.ColumnStore.Int64Value(val));
            }
        }

        [Benchmark]
        public void CompareAgainstColumnStoreString()
        {
            var strVal = new FlowtideDotNet.Core.ColumnStore.StringValue("0");
            for (int i = 0; i < arrayData.Count; i++)
            {
                stringColumn.CompareTo(i, strVal);
            }
        }

        [Benchmark]
        public void CompareLongList()
        {
            for (int i = 0; i < compactData.Count; i++)
            {
                longList[0].CompareTo(longList[i]);
            }
        }

        [Benchmark]
        public void CompareAgainstColumnStore()
        {
            var val = new FlowtideDotNet.Core.ColumnStore.Int64Value(0);
            for (int i = 0; i < arrayData.Count; i++)
            {
                column.CompareTo(i, val);
            }
        }

        [Benchmark]
        public void CompareLongCompactRowData()
        {
            var comparer = new Comparer();
            for (int i = 0; i < compactData.Count; i++)
            {
                comparer.Compare(compactData[0], compactData[i]);
            }
        }

        [Benchmark]
        public void CompareLongCompactRowDataRefStruct()
        {
            var comparer = new RefStructComparer();
            for (int i = 0; i < compactData.Count; i++)
            {
                comparer.Compare(compactData[0], compactData[i]);
            }
        }

        [Benchmark]
        public void CompareLongArrayRowData()
        {
            var comparer = new Comparer();
            for (int i = 0; i < arrayData.Count; i++)
            {
                comparer.Compare(arrayData[0], arrayData[i]);
            }
        }

        [Benchmark]
        public void CompareLongArrayRowDataRefStruct()
        {
            var comparer = new RefStructComparer();
            for (int i = 0; i < arrayData.Count; i++)
            {
                comparer.Compare(arrayData[0], arrayData[i]);
            }
        }

        
    }
}

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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage.Memory;
using System.Reflection;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinPadSumGroupingTests
    {
        // The sums the drain would look up, per distinct key written as text
        private static List<string> SumPerKey(IColumn[] keys, int[] weights)
        {
            int count = weights.Length;
            var join = new MultiJoinNode
            {
                RightKey = new MultiJoinKeySide { Input = 0, Columns = Enumerable.Range(0, keys.Length).ToArray() }
            };
            var table = new MultiJoinPadTable(join, true, 0);
            var flags = BindingFlags.Instance | BindingFlags.NonPublic;
            typeof(MultiJoinPadTable).GetField("_allocator", flags)!.SetValue(table, GlobalMemoryManager.Instance);
            try
            {
                Assert.True(table.Add(keys, count, Enumerable.Repeat(true, count).ToArray(), weights).IsCompletedSuccessfully);
                int distinct = (int)typeof(MultiJoinPadTable).GetMethod("SumMemoryKeys", flags)!.Invoke(table, null)!;
                var memoryKeys = (Column[])typeof(MultiJoinPadTable).GetField("_memoryKeys", flags)!.GetValue(table)!;
                var first = (int[])typeof(MultiJoinPadTable).GetField("_memoryFirst", flags)!.GetValue(table)!;
                var sums = (int[])typeof(MultiJoinPadTable).GetField("_sums", flags)!.GetValue(table)!;
                var result = new List<string>();
                for (int i = 0; i < distinct; i++)
                {
                    var key = string.Join(",", memoryKeys.Select(c => Text(c.GetValueAt(first[i], default))));
                    result.Add($"{key}={sums[i]}");
                }
                result.Sort(StringComparer.Ordinal);
                return result;
            }
            finally
            {
                table.Abandon();
            }
        }

        private static string Text(IDataValue value)
        {
            switch (value.Type)
            {
                case ArrowTypeId.Int64:
                    return "i" + value.AsLong;
                case ArrowTypeId.Double:
                    return "d" + (long)value.AsDouble;
                default:
                    return value.Type.ToString();
            }
        }

        [Fact]
        public void KeysOfSeveralTypesAreSummedPerKey()
        {
            using var key = Column.Create(GlobalMemoryManager.Instance);
            key.Add(new Int64Value(1));
            key.Add(new DoubleValue(2));
            key.Add(new Int64Value(1));
            key.Add(new DoubleValue(2));
            key.Add(new Int64Value(3));
            key.Add(new DoubleValue(1));
            key.Add(new Int64Value(1));

            var sums = SumPerKey(new IColumn[] { key }, new[] { 1, 1, 2, -1, 1, 4, -1 });

            // The double 2 sums to nothing, the double 1 is another key than the integer 1
            Assert.Equal(new[] { "d1=4", "i1=2", "i3=1" }, sums);
        }

        [Fact]
        public void KeysOfMoreThanSevenColumnsAreSummedPerKey()
        {
            // The first two keys differ in the eighth column only
            var rows = new long[][]
            {
                new long[] { 1, 1, 1, 1, 1, 1, 1, 1 },
                new long[] { 1, 1, 1, 1, 1, 1, 1, 2 },
                new long[] { 1, 1, 1, 1, 1, 1, 1, 1 },
                new long[] { 1, 1, 1, 1, 1, 1, 1, 2 },
                new long[] { 2, 1, 1, 1, 1, 1, 1, 1 },
                new long[] { 1, 1, 1, 1, 1, 1, 1, 1 },
            };
            var columns = new Column[8];
            try
            {
                for (int c = 0; c < columns.Length; c++)
                {
                    columns[c] = Column.Create(GlobalMemoryManager.Instance);
                    foreach (var row in rows)
                    {
                        columns[c].Add(new Int64Value(row[c]));
                    }
                }

                var sums = SumPerKey(columns, new[] { 1, 1, 2, 1, 1, 3 });

                Assert.Equal(new[] { "i1,i1,i1,i1,i1,i1,i1,i1=6", "i1,i1,i1,i1,i1,i1,i1,i2=2", "i2,i1,i1,i1,i1,i1,i1,i1=1" }, sums);
            }
            finally
            {
                foreach (var column in columns)
                {
                    column?.Dispose();
                }
            }
        }
    }
}

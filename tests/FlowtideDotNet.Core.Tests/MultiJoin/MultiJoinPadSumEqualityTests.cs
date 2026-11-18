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
    public class MultiJoinPadSumEqualityTests
    {
        private static T Field<T>(MultiJoinPadTable table, string name) =>
            (T)typeof(MultiJoinPadTable).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(table)!;

        private static (MultiJoinPadTable table, Func<int> sum) Create(int width)
        {
            var table = new MultiJoinPadTable(new MultiJoinNode
            {
                RightKey = new MultiJoinKeySide { Input = 0, Columns = Enumerable.Range(0, width).ToArray() }
            }, true, 0);
            typeof(MultiJoinPadTable).GetField("_allocator", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(table, GlobalMemoryManager.Instance);
            return (table, typeof(MultiJoinPadTable).GetMethod("SumMemoryKeys", BindingFlags.NonPublic | BindingFlags.Instance)!
                .CreateDelegate<Func<int>>(table));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(8)]
        public void PadSumsPreserveTypedEqualitySignedCancellationAndReuse(int width)
        {
            var (table, sum) = Create(width);
            try
            {
                for (int generation = 0; generation < 2; generation++)
                {
                    var columns = Enumerable.Range(0, width).Select(_ => Column.Create(GlobalMemoryManager.Instance)).ToArray();
                    try
                    {
                        IDataValue[] values = generation == 0
                            ? new IDataValue[] { new Int64Value(1), new DoubleValue(1), new Int64Value(1), new DoubleValue(1), new DoubleValue(double.NaN), new DoubleValue(double.NaN), new DoubleValue(-0.0), new DoubleValue(0.0), new Int64Value(2) }
                            : new IDataValue[] { new Int64Value(3), new DoubleValue(3), new Int64Value(3), new DoubleValue(3), new DoubleValue(double.NaN), new DoubleValue(double.NaN), new DoubleValue(-0.0), new DoubleValue(0.0), new Int64Value(4) };
                        int[] weights = { 3, 7, -3, -2, 2, -1, 4, -4, -2 };
                        for (int row = 0; row < values.Length; row++)
                        {
                            for (int c = 0; c < width - 1; c++) columns[c].Add(new Int64Value(10));
                            columns[^1].Add(values[row]);
                        }
                        Assert.True(table.Add(columns, values.Length, Enumerable.Repeat(true, values.Length).ToArray(), weights).IsCompletedSuccessfully);
                        Assert.Equal(3, sum());
                        var first = Field<int[]>(table, "_memoryFirst");
                        var sums = Field<int[]>(table, "_sums");
                        var result = new Dictionary<string, int>();
                        for (int i = 0; i < 3; i++)
                        {
                            var value = values[first[i]];
                            result.Add(value.Type == ArrowTypeId.Int64 ? "integer" : double.IsNaN(value.AsDouble) ? "nan" : "double", sums[i]);
                        }
                        Assert.Equal(-2, result["integer"]);
                        Assert.Equal(5, result["double"]);
                        Assert.Equal(1, result["nan"]);
                        table.Abandon();
                        Assert.False(table.HasRows);
                    }
                    finally
                    {
                        foreach (var column in columns) column.Dispose();
                    }
                }
            }
            finally
            {
                table.Abandon();
            }
        }

        [Fact]
        public void PadSumsDoNotBoxTheEighthScalarKey()
        {
            long Measure(int count)
            {
                var (table, sum) = Create(8);
                var columns = Enumerable.Range(0, 8).Select(_ => Column.Create(GlobalMemoryManager.Instance)).ToArray();
                try
                {
                    for (int row = 0; row < count; row++)
                    {
                        for (int c = 0; c < 8; c++) columns[c].Add(new Int64Value(c == 7 ? row : 1));
                    }
                    Assert.True(table.Add(columns, count, Enumerable.Repeat(true, count).ToArray(), Enumerable.Repeat(1, count).ToArray()).IsCompletedSuccessfully);
                    for (int warm = 0; warm < 8; warm++) Assert.Equal(count, sum());
                    long before = GC.GetAllocatedBytesForCurrentThread();
                    int actual = sum();
                    long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                    Assert.Equal(count, actual);
                    return allocated;
                }
                finally
                {
                    table.Abandon();
                    foreach (var column in columns) column.Dispose();
                }
            }

            long small = Measure(64);
            long large = Measure(512);
            Assert.True(large - small <= 4096, $"Wide pad keys allocate per row: 64={small}, 512={large} bytes.");
        }
    }
}

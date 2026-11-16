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
    public class MultiJoinNestedPadKeyTests
    {
        private static long CopyAfterStructKey(IColumn source, int count, StructHeader header)
        {
            var join = new MultiJoinNode
            {
                RightKey = new MultiJoinKeySide { Input = 0, Columns = new[] { 0 } }
            };
            var table = new MultiJoinPadTable(join, true, 0);
            typeof(MultiJoinPadTable).GetField("_allocator", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(table, GlobalMemoryManager.Instance);
            using var firstKey = Column.Create(GlobalMemoryManager.Instance);
            firstKey.Add(new StructValue(header, new Int64Value(0)));
            var firstColumns = new IColumn[] { firstKey };
            var sourceColumns = new[] { source };
            var found = Enumerable.Repeat(true, count).ToArray();
            var weights = Enumerable.Repeat(1, count).ToArray();
            int previousLimit = MultiJoinPadTable.MemoryKeys;
            MultiJoinPadTable.MemoryKeys = int.MaxValue;
            try
            {
                for (int i = 0; i < 4; i++)
                {
                    Assert.True(table.Add(firstColumns, 1, found, weights).IsCompletedSuccessfully);
                    Assert.True(table.Add(sourceColumns, count, found, weights).IsCompletedSuccessfully);
                    table.Abandon();
                }
                Assert.True(table.Add(firstColumns, 1, found, weights).IsCompletedSuccessfully);
                long before = GC.GetAllocatedBytesForCurrentThread();
                var copy = table.Add(sourceColumns, count, found, weights);
                long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                Assert.True(copy.IsCompletedSuccessfully);
                return allocated;
            }
            finally
            {
                table.Abandon();
                MultiJoinPadTable.MemoryKeys = previousLimit;
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void StructPadKeysDoNotBoxEveryChildValue(bool mixedSource)
        {
            var header = StructHeader.Create("x");
            using var integers = Column.Create(GlobalMemoryManager.Instance);
            using var changed = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < 512; i++)
            {
                integers.Add(new StructValue(header, new Int64Value(i + 1)));
                IDataValue value = mixedSource && i % 2 == 0
                    ? new Int64Value(i + 1)
                    : new DoubleValue(i + 0.5);
                changed.Add(new StructValue(header, value));
            }
            long integerSmall = CopyAfterStructKey(integers, 64, header);
            long changedSmall = CopyAfterStructKey(changed, 64, header);
            long integerLarge = CopyAfterStructKey(integers, 512, header);
            long changedLarge = CopyAfterStructKey(changed, 512, header);
            long excessGrowth = (changedLarge - integerLarge) - (changedSmall - integerSmall);
            Assert.True(excessGrowth <= 4096,
                $"Nested pad-key copying allocates per incoming row: integer 64/512={integerSmall}/{integerLarge}, changed={changedSmall}/{changedLarge}, excess={excessGrowth}.");
        }
    }
}

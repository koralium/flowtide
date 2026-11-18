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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Reflection;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinPadAllocationTests
    {
        private static long CopyKeys(IColumn key, int count)
        {
            var join = new MultiJoinNode
            {
                RightKey = new MultiJoinKeySide { Input = 0, Columns = new[] { 0 } }
            };
            var table = new MultiJoinPadTable(join, true, 0);
            typeof(MultiJoinPadTable).GetField("_allocator", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(table, GlobalMemoryManager.Instance);
            var found = Enumerable.Repeat(true, count).ToArray();
            var weights = Enumerable.Repeat(1, count).ToArray();
            var columns = new[] { key };
            int previousLimit = MultiJoinPadTable.MemoryKeys;
            MultiJoinPadTable.MemoryKeys = int.MaxValue;
            try
            {
                // Warm the reusable arrays and both copy paths before measuring.
                for (int i = 0; i < 4; i++)
                {
                    var warm = table.Add(columns, count, found, weights);
                    Assert.True(warm.IsCompletedSuccessfully);
                    table.Abandon();
                }
                long before = GC.GetAllocatedBytesForCurrentThread();
                var write = table.Add(columns, count, found, weights);
                long bytes = GC.GetAllocatedBytesForCurrentThread() - before;
                Assert.True(write.IsCompletedSuccessfully);
                Assert.Equal(count * 5L, table.KeysAdded);
                return bytes;
            }
            finally
            {
                table.Abandon();
                MultiJoinPadTable.MemoryKeys = previousLimit;
            }
        }

        [Fact]
        public void PadKeysFromOffsetViewsDoNotAllocateAnObjectPerRow()
        {
            const int count = 512;
            using var values = Column.Create(GlobalMemoryManager.Instance);
            using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < count; i++)
            {
                values.Add(new Int64Value(i));
                offsets.Add(count - i - 1);
            }
            var indirect = new ColumnWithOffset(values, offsets);
            long denseSmall = CopyKeys(values, 64);
            long indirectSmall = CopyKeys(indirect, 64);
            long denseLarge = CopyKeys(values, count);
            long indirectLarge = CopyKeys(indirect, count);
            long excessGrowth = (indirectLarge - denseLarge) - (indirectSmall - denseSmall);
            Assert.True(excessGrowth <= 4096,
                $"Pad-key managed allocation grows with offset rows: dense 64/512 = {denseSmall}/{denseLarge}, indirect = {indirectSmall}/{indirectLarge}; excess growth = {excessGrowth} bytes.");
        }
    }
}

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
    public class MultiJoinPadSumTests
    {
        private static long SumKeys(IColumn key, int count)
        {
            var join = new MultiJoinNode
            {
                RightKey = new MultiJoinKeySide { Input = 0, Columns = new[] { 0 } }
            };
            var table = new MultiJoinPadTable(join, true, 0);
            typeof(MultiJoinPadTable).GetField("_allocator", BindingFlags.Instance | BindingFlags.NonPublic)!
                .SetValue(table, GlobalMemoryManager.Instance);
            var sum = typeof(MultiJoinPadTable).GetMethod("SumMemoryKeys", BindingFlags.Instance | BindingFlags.NonPublic)!
                .CreateDelegate<Func<int>>(table);
            var found = Enumerable.Repeat(true, count).ToArray();
            var weights = Enumerable.Repeat(1, count).ToArray();
            try
            {
                Assert.True(table.Add(new[] { key }, count, found, weights).IsCompletedSuccessfully);
                for (int i = 0; i < 8; i++)
                {
                    Assert.Equal(count, sum());
                }
                long before = GC.GetAllocatedBytesForCurrentThread();
                int distinct = sum();
                long bytes = GC.GetAllocatedBytesForCurrentThread() - before;
                Assert.Equal(count, distinct);
                return bytes;
            }
            finally
            {
                table.Abandon();
            }
        }

        // Not with lists: reading a list value boxes it, in the tree lookup on such keys too
        [Fact]
        public void DrainingInMemoryPadKeysDoesNotAllocatePerKey()
        {
            using var plain = Column.Create(GlobalMemoryManager.Instance);
            using var complex = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < 512; i++)
            {
                plain.Add(new Int64Value(i));
                if (i % 2 == 0) complex.Add(new Int64Value(i));
                else complex.Add(new DoubleValue(i));
            }
            long plainSmall = SumKeys(plain, 64);
            long plainLarge = SumKeys(plain, 512);
            long complexSmall = SumKeys(complex, 64);
            long complexLarge = SumKeys(complex, 512);
            long excess = (complexLarge - complexSmall) - (plainLarge - plainSmall);
            Assert.True(excess <= 4096,
                $"Pad-key summation allocates per key: plain 64/512={plainSmall}/{plainLarge}, complex={complexSmall}/{complexLarge}, excess={excess} bytes.");
        }
    }
}

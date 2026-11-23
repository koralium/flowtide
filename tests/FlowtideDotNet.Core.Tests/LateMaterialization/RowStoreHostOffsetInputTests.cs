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
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowStoreHostOffsetInputTests
    {
        private static EventBatchData EveryOtherRow(int rows, string text, List<PrimitiveList<int>> offsetLists)
        {
            var inner = new Column[4];
            for (int c = 0; c < inner.Length; c++)
            {
                inner[c] = Column.Create(GlobalMemoryManager.Instance);
            }
            for (int i = 0; i < rows; i++)
            {
                inner[0].Add(new Int64Value(i));
                inner[1].Add(new Int64Value(i * 10));
                inner[2].Add(new StringValue(text + i));
                inner[3].Add(new Int64Value(i * 100));
            }
            var columns = new IColumn[4];
            for (int c = 0; c < columns.Length; c++)
            {
                var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
                for (int i = 0; i < rows; i += 2)
                {
                    offsets.Add(i);
                }
                offsetLists.Add(offsets);
                columns[c] = new ColumnWithOffset(inner[c], offsets);
            }
            return new EventBatchData(columns);
        }

        [Fact]
        public async Task RowsInColumnsWithOffsetsAreStoredLikePlainColumns()
        {
            using var stream = await Open();
            var options = new RowStoreOptions()
            {
                Mode = RowStoreMode.Keyed,
                Heap = new RowHeapOptions() { PageTargetBytes = 2000 }
            };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", new RowStoreHostLayout(4, [0], [1], [2, 3]), options, GlobalMemoryManager.Instance);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < 10; i++)
            {
                weights.Add(1);
            }

            var offsetLists = new List<PrimitiveList<int>>();
            try
            {
                using (var batch = EveryOtherRow(20, "text", offsetLists))
                {
                    using var first = await host.ApplyAsync(batch, weights, null);
                    Assert.Equal(10, first.InsertRows.Count);
                }
                using (var batch = EveryOtherRow(20, "other", offsetLists))
                {
                    using var second = await host.ApplyAsync(batch, weights, null);
                    Assert.Equal(10, second.InsertRows.Count);
                    Assert.Equal(10, second.DeleteRows.Count);
                }
            }
            finally
            {
                foreach (var offsets in offsetLists)
                {
                    offsets.Dispose();
                }
            }
        }
    }
}

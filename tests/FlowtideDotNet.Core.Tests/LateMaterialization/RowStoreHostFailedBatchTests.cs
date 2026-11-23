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
    public class RowStoreHostFailedBatchTests
    {
        [Fact]
        public async Task AfterAFailedBatchTheHostRefusesACheckpoint()
        {
            using var stream = await Open();
            var options = new RowStoreOptions()
            {
                Mode = RowStoreMode.Keyed,
                Heap = new RowHeapOptions() { PageTargetBytes = 2000 }
            };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", new RowStoreHostLayout(4, [0], [1], [2, 3]), options, GlobalMemoryManager.Instance);
            var columns = new IColumn[4];
            for (int c = 0; c < columns.Length; c++)
            {
                columns[c] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            foreach (var key in new long[] { 1, 2, 1, 3 })
            {
                columns[0].Add(new Int64Value(key));
                columns[1].Add(new Int64Value(key * 10));
                columns[2].Add(new StringValue("text" + key));
                columns[3].Add(new Int64Value(key * 100));
                weights.Add(1);
            }
            Func<EventBatchData, int, bool> filter = (rows, row) => row == 3 ? throw new InvalidOperationException("filter failed") : true;

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await host.ApplyAsync(batch, weights, filter));

            await Assert.ThrowsAsync<InvalidOperationException>(() => host.BeginCheckpointAsync());
        }

        [Fact]
        public async Task AfterAFailedKeyRemovalTheHostRefusesACheckpoint()
        {
            using var stream = await Open();
            var options = new RowStoreOptions()
            {
                Mode = RowStoreMode.Keyed,
                Heap = new RowHeapOptions() { PageTargetBytes = 2000 }
            };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", new RowStoreHostLayout(4, [0], [1], [2, 3]), options, GlobalMemoryManager.Instance);
            var columns = new IColumn[4];
            for (int c = 0; c < columns.Length; c++)
            {
                columns[c] = Column.Create(GlobalMemoryManager.Instance);
            }
            using var batch = new EventBatchData(columns);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            foreach (var key in new long[] { 1, 2, 3 })
            {
                columns[0].Add(new Int64Value(key));
                columns[1].Add(new Int64Value(key * 10));
                columns[2].Add(new StringValue("text" + key));
                columns[3].Add(new Int64Value(key * 100));
                weights.Add(1);
            }
            using (await host.ApplyAsync(batch, weights, null))
            {
            }
            using var keysWithoutColumns = new EventBatchData(Array.Empty<IColumn>());
            using var result = host.CreateResult();

            await Assert.ThrowsAnyAsync<Exception>(async () => await host.RemoveAsync(keysWithoutColumns, 1, result));

            await Assert.ThrowsAsync<InvalidOperationException>(() => host.BeginCheckpointAsync());
        }

        [Fact]
        public async Task AFailedHostGivesNoKeyIterator()
        {
            using var stream = await Open();
            var options = new RowStoreOptions()
            {
                Mode = RowStoreMode.Keyed,
                Heap = new RowHeapOptions() { PageTargetBytes = 2000 }
            };
            using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", new RowStoreHostLayout(4, [0], [1], [2, 3]), options, GlobalMemoryManager.Instance);
            host.MarkFailed();

            Assert.Throws<InvalidOperationException>(() => host.CreateKeyIterator());
        }
    }
}

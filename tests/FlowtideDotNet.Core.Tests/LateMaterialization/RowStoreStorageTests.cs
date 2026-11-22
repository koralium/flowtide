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
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowStoreStorageTests
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task IdleCheckpointsDoNotGrowThePersistentPageDirectory(bool keyed)
        {
            using var stream = await RowHeapTests.Open();
            using var store = await RowStore.CreateAsync(
                stream.Client,
                "store",
                1,
                new RowStoreOptions() { Mode = keyed ? RowStoreMode.Keyed : RowStoreMode.Append },
                GlobalMemoryManager.Instance);

            for (int i = 0; i < 4; i++)
            {
                await store.BeginCheckpointAsync();
                await store.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
            }
            int settledPages = stream.Storage.PageFileLocationsForTests.Count;

            for (int i = 0; i < 20; i++)
            {
                await store.BeginCheckpointAsync();
                await store.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
                await stream.Manager.Compact();
            }

            Assert.Equal(0, store.RowCount);
            Assert.Equal(0, store.WaitingRows);
            Assert.True(stream.Storage.PageFileLocationsForTests.Count <= settledPages,
                $"Idle store retained {stream.Storage.PageFileLocationsForTests.Count} persistent pages after starting with {settledPages}.");
        }

        [Fact]
        public async Task RetiredCohortsDoNotGrowThePersistentPageDirectory()
        {
            using var stream = await RowHeapTests.Open();
            using var store = await RowStore.CreateAsync(
                stream.Client,
                "store",
                1,
                new RowStoreOptions() { Mode = RowStoreMode.Keyed },
                GlobalMemoryManager.Instance);
            using var payload = RowHeapTests.Batch(new[] { "int" }, 0, 256);
            using var survivor = Column.Create(GlobalMemoryManager.Instance);
            await store.AppendAsync(payload, 0, 1, survivor);
            int settledPages = 0;

            for (int cohort = 0; cohort < 24; cohort++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await store.AppendAsync(payload, 0, payload.Count, references);
                for (int row = 0; row < references.Count; row++)
                {
                    await store.RetireAsync((ulong)references.GetValueAt(row, default).AsLong);
                }
                for (int barrier = 0; barrier < 3; barrier++)
                {
                    await store.BeginCheckpointAsync();
                    await store.CheckpointAsync();
                    await stream.Manager.CheckpointAsync();
                }
                await stream.Manager.Compact();

                Assert.Equal(1, store.RowCount);
                Assert.Equal(0, store.WaitingRows);
                Assert.Equal(1, store.Heap.PageCount);
                if (cohort == 3)
                {
                    settledPages = stream.Storage.PageFileLocationsForTests.Count;
                }
            }

            Assert.True(store.Heap.IsLive((ulong)survivor.GetValueAt(0, default).AsLong));
            Assert.True(stream.Storage.PageFileLocationsForTests.Count <= settledPages,
                $"Store with one live row retained {stream.Storage.PageFileLocationsForTests.Count} persistent pages after starting with {settledPages}.");
        }
    }
}

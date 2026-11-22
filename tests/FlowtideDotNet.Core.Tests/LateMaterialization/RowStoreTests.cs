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
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapCollectionTests;
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowStoreTests
    {
        private static Task<RowStore> Store(RowHeapTests.Stream stream, RowStoreMode mode, int buffer = 65536)
        {
            var options = new RowStoreOptions()
            {
                Mode = mode,
                RetireBufferSize = buffer,
                Heap = new RowHeapOptions() { PageTargetBytes = 2000 }
            };
            return RowStore.CreateAsync(stream.Client, "store", 2, options, GlobalMemoryManager.Instance);
        }

        private static async Task<ulong[]> Append(RowStore store, long firstRow, int rows)
        {
            using var batch = Batch(Kinds, firstRow, rows);
            using var references = Column.Create(GlobalMemoryManager.Instance);
            await store.AppendAsync(batch, 0, rows, references);
            var result = new ulong[references.Count];
            for (int i = 0; i < result.Length; i++)
            {
                result[i] = (ulong)references.GetValueAt(i, default).AsLong;
            }
            return result;
        }

        private static async Task Checkpoint(RowHeapTests.Stream stream, RowStore store)
        {
            await store.BeginCheckpointAsync();
            await store.CheckpointAsync();
            await stream.Manager.CheckpointAsync();
        }

        private static async Task AssertReads(RowStore store, ulong[] references, IEnumerable<int> rows)
        {
            using var reader = store.CreateReader();
            var asked = rows.ToArray();
            var fetched = await Gather(reader, asked.Select(x => (ulong?)references[x]).ToArray(), Both);
            AssertRows(Kinds, Both, asked.Select(x => (long?)x).ToArray(), fetched);
        }

        private static int Live(RowStore store, ulong[] references, IEnumerable<int> rows)
        {
            return rows.Count(x => store.Heap.IsLive(references[x]));
        }

        [Fact]
        public async Task AStoreWithBothRetirementCohortsRecoversOnTheSameManager()
        {
            using var stream = await Open();
            var store = await Store(stream, RowStoreMode.Keyed, buffer: 2);
            var references = await Append(store, 0, 60);
            await Checkpoint(stream, store);
            for (int row = 0; row < 10; row++)
            {
                await store.RetireAsync(references[row]);
            }
            await store.BeginCheckpointAsync();
            for (int row = 10; row < 20; row++)
            {
                await store.RetireAsync(references[row]);
            }
            var late = await Append(store, 60, 10);
            await store.CheckpointAsync();
            Assert.Equal(60, store.RowCount);
            Assert.Equal(10, store.WaitingRows);
            store.Dispose();

            await stream.Manager.InitializeAsync();
            using var restored = await Store(stream, RowStoreMode.Keyed, buffer: 2);
            Assert.Equal(60, restored.RowCount);
            Assert.Equal(0, restored.WaitingRows);
            Assert.Equal(60, Live(restored, references, Enumerable.Range(0, 60)));
            Assert.Equal(0, Live(restored, late, Enumerable.Range(0, late.Length)));
            await AssertReads(restored, references, Enumerable.Range(0, 60));
            for (int row = 0; row < 20; row++)
            {
                await restored.RetireAsync(references[row]);
            }
            await Checkpoint(stream, restored);

            Assert.Equal(40, restored.RowCount);
            Assert.Equal(0, restored.WaitingRows);
            Assert.Equal(0, Live(restored, references, Enumerable.Range(0, 20)));
            Assert.Equal(40, Live(restored, references, Enumerable.Range(20, 40)));
            await AssertReads(restored, references, Enumerable.Range(20, 40));
        }

        [Fact]
        public async Task AnAppendStoreKeepsEveryRowBehindItsReference()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Append);

            var references = await Append(store, 0, 500);
            await Checkpoint(stream, store);

            Assert.Equal(RowStoreMode.Append, store.Mode);
            Assert.Equal(500, store.RowCount);
            Assert.Equal(500, references.Distinct().Count());
            Assert.Equal(0, store.WaitingRows);
            await AssertReads(store, references, Enumerable.Range(0, 500));
        }

        [Fact]
        public async Task AStoreTakesNoRowThatIsTakenBack()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Append);
            using var batch = Batch(Kinds, 0, 3);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            using var references = Column.Create(GlobalMemoryManager.Instance);
            weights.Add(1);
            weights.Add(-1);
            weights.Add(2);

            var refused = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.AppendAsync(batch, weights, references));
            weights[1] = 3;
            await store.AppendAsync(batch, weights, references);

            Assert.Equal("Row 1 of the batch has weight -1, row store 'store' only takes rows that are added.", refused.Message);
            Assert.Equal(3, store.RowCount);
            Assert.Equal(3, references.Count);
        }

        [Fact]
        public async Task AnAppendStoreRetiresNoRow()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Append);
            var references = await Append(store, 0, 10);

            var refused = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.RetireAsync(references[3]));

            Assert.Equal("Row store 'store' is in append mode, a row cannot be retired.", refused.Message);
            Assert.Equal(10, store.RowCount);
        }

        [Fact]
        public async Task AnAppendStoreMergesItsSmallPages()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Append);
            var references = new List<ulong>();
            for (int i = 0; i < 300; i++)
            {
                references.AddRange(await Append(store, i, 1));
            }
            long pagesBefore = store.Heap.PageCount;

            bool more = true;
            while (more)
            {
                more = await store.CompactAsync();
            }
            await Checkpoint(stream, store);
            await Checkpoint(stream, store);

            Assert.Equal(300, pagesBefore);
            Assert.True(store.Heap.PagesMerged > 100, $"{store.Heap.PagesMerged} of 300 small pages were merged.");
            Assert.True(store.Heap.PageCount < 150, $"{store.Heap.PageCount} pages are left.");
            await AssertReads(store, references.ToArray(), Enumerable.Range(0, 300));
        }

        [Fact]
        public async Task ARetiredRowIsReadUntilItsCheckpointIsCommitted()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Keyed);
            var references = await Append(store, 0, 40);
            var retired = Enumerable.Range(0, 40).Where(x => x % 3 == 0).ToArray();

            foreach (int row in retired)
            {
                await store.RetireAsync(references[row]);
            }
            long waiting = store.WaitingRows;
            await store.BeginCheckpointAsync();
            int liveAtTheBarrier = Live(store, references, retired);
            await AssertReads(store, references, Enumerable.Range(0, 40));
            await store.CheckpointAsync();
            await stream.Manager.CheckpointAsync();

            Assert.Equal(14, waiting);
            Assert.Equal(14, liveAtTheBarrier);
            Assert.Equal(0, store.WaitingRows);
            Assert.Equal(26, store.RowCount);
            Assert.Equal(0, Live(store, references, retired));
            await AssertReads(store, references, Enumerable.Range(0, 40).Except(retired));
        }

        [Fact]
        public async Task ARowRetiredAfterTheBarrierWaitsForTheNextCheckpoint()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Keyed);
            var references = await Append(store, 0, 40);
            var first = Enumerable.Range(0, 10).ToArray();
            var second = Enumerable.Range(10, 10).ToArray();

            foreach (int row in first)
            {
                await store.RetireAsync(references[row]);
            }
            await store.BeginCheckpointAsync();
            foreach (int row in second)
            {
                await store.RetireAsync(references[row]);
            }
            await store.CheckpointAsync();
            await stream.Manager.CheckpointAsync();
            int firstLive = Live(store, references, first);
            int secondLive = Live(store, references, second);
            long waiting = store.WaitingRows;
            await Checkpoint(stream, store);

            Assert.Equal(0, firstLive);
            Assert.Equal(10, secondLive);
            Assert.Equal(10, waiting);
            Assert.Equal(0, Live(store, references, second));
            Assert.Equal(20, store.RowCount);
            await AssertReads(store, references, Enumerable.Range(20, 20));
        }

        [Fact]
        public async Task RetiredRowsAreGoneAfterARestartFromTheirCheckpoint()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            var retired = Enumerable.Range(0, 200).Where(x => x % 4 == 1).ToArray();
            using (var stream = await Open(provider))
            {
                using var store = await Store(stream, RowStoreMode.Keyed, buffer: 8);
                references = await Append(store, 0, 200);
                foreach (int row in retired)
                {
                    await store.RetireAsync(references[row]);
                }
                await Checkpoint(stream, store);
            }

            using var restarted = await Open(provider);
            using var again = await Store(restarted, RowStoreMode.Keyed, buffer: 8);
            long rowsAfterTheRestart = again.RowCount;
            int liveAfterTheRestart = Live(again, references, retired);
            await Checkpoint(restarted, again);

            Assert.Equal(150, rowsAfterTheRestart);
            Assert.Equal(0, liveAfterTheRestart);
            Assert.Equal(0, again.WaitingRows);
            Assert.Equal(150, again.RowCount);
            await AssertReads(again, references, Enumerable.Range(0, 200).Except(retired));
        }


        [Fact]
        public async Task ARetirementAfterTheCheckpointIsForgottenByARestore()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            using (var stream = await Open(provider))
            {
                using var store = await Store(stream, RowStoreMode.Keyed, buffer: 4);
                references = await Append(store, 0, 60);
                for (int row = 0; row < 10; row++)
                {
                    await store.RetireAsync(references[row]);
                }
                await Checkpoint(stream, store);
                for (int row = 10; row < 30; row++)
                {
                    await store.RetireAsync(references[row]);
                }
            }

            using var restarted = await Open(provider);
            using var again = await Store(restarted, RowStoreMode.Keyed, buffer: 4);
            long rowsAfterTheRestart = again.RowCount;
            int forgottenLive = Live(again, references, Enumerable.Range(10, 20));
            for (int row = 10; row < 30; row++)
            {
                await again.RetireAsync(references[row]);
            }
            await Checkpoint(restarted, again);

            Assert.Equal(50, rowsAfterTheRestart);
            Assert.Equal(20, forgottenLive);
            Assert.Equal(30, again.RowCount);
            Assert.Equal(0, again.WaitingRows);
            await AssertReads(again, references, Enumerable.Range(30, 30));
        }

        [Fact]
        public async Task ACheckpointThatWasNotStoredIsForgotten()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            long rowsBeforeTheCrash;
            using (var stream = await Open(provider))
            {
                using var store = await Store(stream, RowStoreMode.Keyed);
                references = await Append(store, 0, 50);
                await Checkpoint(stream, store);
                for (int row = 0; row < 20; row++)
                {
                    await store.RetireAsync(references[row]);
                }
                await store.BeginCheckpointAsync();
                await store.CheckpointAsync();
                rowsBeforeTheCrash = store.RowCount;
            }

            using var restarted = await Open(provider);
            using var again = await Store(restarted, RowStoreMode.Keyed);

            Assert.Equal(30, rowsBeforeTheCrash);
            Assert.Equal(50, again.RowCount);
            Assert.Equal(0, again.WaitingRows);
            Assert.Equal(20, Live(again, references, Enumerable.Range(0, 20)));
            await AssertReads(again, references, Enumerable.Range(0, 50));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(4096)]
        public async Task ARowIsNotRetiredTwice(int buffer)
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Keyed, buffer);
            var references = await Append(store, 0, 30);

            await store.RetireAsync(references[7]);
            var inOneInterval = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await store.RetireAsync(references[7]);
                await store.BeginCheckpointAsync();
            });

            Assert.Equal($"The row at slot {RowReference.Slot(references[7])} of page {RowReference.Sequence(references[7])} in row store 'store' was already retired.", inOneInterval.Message);
        }

        [Fact]
        public async Task ARowRetiredAgainAfterTheBarrierFailsTheNextCheckpointBeforeItIsStored()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            using (var stream = await Open(provider))
            {
                using var store = await Store(stream, RowStoreMode.Keyed);
                references = await Append(store, 0, 30);
                await store.RetireAsync(references[7]);
                await store.BeginCheckpointAsync();
                await store.RetireAsync(references[7]);
                await store.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
                await store.BeginCheckpointAsync();

                var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => store.CheckpointAsync());
                var after = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.RetireAsync(references[8]));

                Assert.Equal($"The row at slot {RowReference.Slot(references[7])} of page {RowReference.Sequence(references[7])} in heap 'store' was already released.", refused.Message);
                Assert.Equal("Row store 'store' failed in an earlier call, it is opened anew from the last checkpoint.", after.Message);
            }

            using var restarted = await Open(provider);
            using var again = await Store(restarted, RowStoreMode.Keyed);
            await Checkpoint(restarted, again);

            Assert.Equal(29, again.RowCount);
            Assert.Equal(0, again.WaitingRows);
            await AssertReads(again, references, Enumerable.Range(0, 30).Where(x => x != 7));
        }

        [Fact]
        public async Task RowsAppendedAfterTheBarrierAreLeftToTheNextCheckpoint()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            ulong[] late;
            long storedAtTheBarrier;
            long sequencesAtTheBarrier;
            long pageAtTheBarrier;
            long pageAfterTheBarrier;
            using (var stream = await Open(provider))
            {
                using var store = await Store(stream, RowStoreMode.Keyed);
                references = await Append(store, 0, 100);
                for (int row = 0; row < 10; row++)
                {
                    await store.RetireAsync(references[row]);
                }
                await store.BeginCheckpointAsync();
                storedAtTheBarrier = store.Heap.StoredBytes;
                sequencesAtTheBarrier = store.Heap.SequenceCount;
                pageAtTheBarrier = store.Heap.Client.NextPageId;
                late = await Append(store, 100, 50);
                pageAfterTheBarrier = store.Heap.Client.NextPageId;
                for (int row = 10; row < 15; row++)
                {
                    await store.RetireAsync(references[row]);
                }
                await store.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
                Assert.Equal(140, store.RowCount);
                Assert.Equal(5, store.WaitingRows);
            }

            using var restarted = await Open(provider);
            using var again = await Store(restarted, RowStoreMode.Keyed);
            long rowsAfterTheRestart = again.RowCount;
            long storedAfterTheRestart = again.Heap.StoredBytes;
            long sequencesAfterTheRestart = again.Heap.SequenceCount;
            int lateLive = Live(again, late, Enumerable.Range(0, 50));
            int retiredLateLive = Live(again, references, Enumerable.Range(10, 5));
            var appendedAgain = await Append(again, 100, 50);
            await Checkpoint(restarted, again);
            int latePagesStored = 0;
            for (long id = pageAtTheBarrier; id < pageAfterTheBarrier; id++)
            {
                if (restarted.Storage.IsStored(id))
                {
                    latePagesStored++;
                }
            }

            Assert.True(pageAfterTheBarrier > pageAtTheBarrier);
            Assert.Equal(90, rowsAfterTheRestart);
            Assert.Equal(storedAtTheBarrier, storedAfterTheRestart);
            Assert.Equal(sequencesAtTheBarrier, sequencesAfterTheRestart);
            Assert.Equal(0, lateLive);
            Assert.Equal(5, retiredLateLive);
            Assert.Equal(0, latePagesStored);
            Assert.Equal(140, again.RowCount);
            await AssertReads(again, references, Enumerable.Range(10, 90));
            using var reader = again.CreateReader();
            var fetched = await Gather(reader, appendedAgain.Select(x => (ulong?)x).ToArray(), Both);
            AssertRows(Kinds, Both, Enumerable.Range(100, 50).Select(x => (long?)x).ToArray(), fetched);
        }

        [Fact]
        public async Task RowsAppendedAfterTheBarrierAreInTheNextCheckpoint()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            ulong[] late;
            using (var stream = await Open(provider))
            {
                using var store = await Store(stream, RowStoreMode.Keyed);
                references = await Append(store, 0, 100);
                await store.BeginCheckpointAsync();
                late = await Append(store, 100, 50);
                await store.RetireAsync(references[3]);
                await store.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
                await Checkpoint(stream, store);
            }

            using var restarted = await Open(provider);
            using var again = await Store(restarted, RowStoreMode.Keyed);

            Assert.Equal(149, again.RowCount);
            Assert.Equal(50, Live(again, late, Enumerable.Range(0, 50)));
            Assert.False(again.Heap.IsLive(references[3]));
            await AssertReads(again, references, Enumerable.Range(0, 100).Where(x => x != 3));
            using var reader = again.CreateReader();
            var fetched = await Gather(reader, late.Select(x => (ulong?)x).ToArray(), Both);
            AssertRows(Kinds, Both, Enumerable.Range(100, 50).Select(x => (long?)x).ToArray(), fetched);
        }

        [Fact]
        public async Task AFailedOpenLeavesNoHeapBehind()
        {
            using var stream = await Open(cachePages: 400);
            var table = stream.Manager.CacheTable;
            int countersBefore = table.ExternalHitCounterCountForTests;
            await stream.Client.GetOrCreateObjectStateAsync<int>("bad_retired_0");

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => RowStore.CreateAsync(stream.Client, "bad", 2, new RowStoreOptions() { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance));

            Assert.Equal(0, table.ReservedHeldPages);
            Assert.Equal(countersBefore, table.ExternalHitCounterCountForTests);
            using var good = await Store(stream, RowStoreMode.Keyed);
            Assert.Equal(0, good.RowCount);
        }

        [Fact]
        public async Task MergesWaitBetweenTheBarrierAndTheCommit()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Append);
            for (int i = 0; i < 300; i++)
            {
                await Append(store, i, 1);
            }

            await store.BeginCheckpointAsync();
            bool mergedAfterTheBarrier = await store.CompactAsync();
            long mergedBeforeTheCommit = store.Heap.PagesMerged;
            await store.CheckpointAsync();
            await stream.Manager.CheckpointAsync();
            bool more = true;
            while (more)
            {
                more = await store.CompactAsync();
            }

            Assert.False(mergedAfterTheBarrier);
            Assert.Equal(0, mergedBeforeTheCommit);
            Assert.True(store.Heap.PagesMerged > 100, $"{store.Heap.PagesMerged} of 300 small pages were merged.");
        }

        [Fact]
        public async Task ACheckpointIsBegunOnceBeforeItIsCommitted()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Keyed);
            await Append(store, 0, 5);

            var notBegun = await Assert.ThrowsAsync<InvalidOperationException>(() => store.CheckpointAsync());
            await store.BeginCheckpointAsync();
            var twice = await Assert.ThrowsAsync<InvalidOperationException>(() => store.BeginCheckpointAsync());

            Assert.Equal("Row store 'store' commits a checkpoint that was not begun at a barrier.", notBegun.Message);
            Assert.Equal("Row store 'store' has a checkpoint that is not committed yet.", twice.Message);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AKeyedStoreLeavesNoPagesOfItsRetiredRowsInStorage(bool retire)
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Keyed);
            var references = await Append(store, 0, 4000);
            await Checkpoint(stream, store);
            await Checkpoint(stream, store);
            long first = stream.Manager.GetNewPageId();
            for (int round = 0; round < 20; round++)
            {
                if (retire)
                {
                    for (int row = round * 200; row < round * 200 + 200; row++)
                    {
                        await store.RetireAsync(references[row]);
                    }
                }
                await Checkpoint(stream, store);
            }
            await Checkpoint(stream, store);
            await Checkpoint(stream, store);
            long last = stream.Manager.GetNewPageId();
            int stored = 0;
            for (long id = first; id <= last; id++)
            {
                if (stream.Storage.IsStored(id))
                {
                    stored++;
                }
            }

            Assert.True(stored <= 8, $"{stored} of the {last - first + 1} pages made over 22 checkpoints are still stored.");
        }

        [Fact]
        public async Task ManyRetiredWordsFillATreeOfSeveralLevels()
        {
            var provider = new MemoryFileProvider();
            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = provider });
            using var manager = new StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = storage,
                    DefaultBPlusTreePageSize = 64,
                    DefaultBPlusTreePageSizeBytes = 512,
                    TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"./data/rowStore/{Guid.NewGuid():N}" }
                },
                NullLoggerFactory.Instance,
                new Meter("rowStore"),
                "rowStore",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            var client = manager.GetOrCreateClient("store");
            var options = new RowStoreOptions()
            {
                Mode = RowStoreMode.Keyed,
                RetireBufferSize = 32,
                Heap = new RowHeapOptions() { PageTargetBytes = 1 }
            };
            using var store = await RowStore.CreateAsync(client, "store", 2, options, GlobalMemoryManager.Instance);
            var references = await Append(store, 0, 2_500);
            var random = new Random(5);
            var order = Enumerable.Range(0, references.Length).OrderBy(x => random.Next()).ToArray();

            for (int i = 0; i < order.Length; i++)
            {
                await store.RetireAsync(references[order[i]]);
            }
            long waiting = store.WaitingRows;
            await store.BeginCheckpointAsync();
            await store.CheckpointAsync();
            await manager.CheckpointAsync();

            Assert.Equal(2_500, waiting);
            Assert.Equal(0, store.RowCount);
            Assert.Equal(0, store.WaitingRows);
        }

        [Fact]
        public async Task AStoreTakesAsManyWeightsAsRows()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Append);
            using var batch = Batch(Kinds, 0, 3);
            using var references = Column.Create(GlobalMemoryManager.Instance);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            weights.Add(1);
            weights.Add(1);

            var refused = await Assert.ThrowsAsync<ArgumentException>(async () => await store.AppendAsync(batch, weights, references));

            Assert.StartsWith("The batch has 3 rows and 2 weights.", refused.Message);
            Assert.Equal(0, store.RowCount);
        }

        [Fact]
        public async Task ARowBeyondTheFirstWordOfItsPageIsNotRetiredTwice()
        {
            using var stream = await Open();
            var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = 1 << 20 } };
            using var store = await RowStore.CreateAsync(stream.Client, "store", 2, options, GlobalMemoryManager.Instance);
            var references = await Append(store, 0, 200);

            await store.RetireAsync(references[130]);
            await store.RetireAsync(references[130]);
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => store.BeginCheckpointAsync());

            Assert.Equal(130, RowReference.Slot(references[130]));
            Assert.Equal($"The row at slot 130 of page {RowReference.Sequence(references[130])} in row store 'store' was already retired.", refused.Message);
        }

        [Fact]
        public async Task RetiredRowsOfMergedPagesAreReleased()
        {
            var provider = new MemoryFileProvider();
            var references = new List<ulong>();
            var early = Enumerable.Range(0, 300).Where(x => x % 3 == 0).ToArray();
            var late = Enumerable.Range(0, 300).Where(x => x % 3 == 1).ToArray();
            using (var stream = await Open(provider))
            {
                using var store = await Store(stream, RowStoreMode.Keyed);
                for (int i = 0; i < 300; i++)
                {
                    references.AddRange(await Append(store, i, 1));
                }
                foreach (int row in early)
                {
                    await store.RetireAsync(references[row]);
                }
                bool more = true;
                while (more)
                {
                    more = await store.CompactAsync();
                }
                long merged = store.Heap.PagesMerged;
                int earlyLiveAfterTheMerge = Live(store, references.ToArray(), early);
                foreach (int row in late)
                {
                    await store.RetireAsync(references[row]);
                }
                await Checkpoint(stream, store);

                Assert.True(merged > 100, $"{merged} of 300 small pages were merged.");
                Assert.Equal(100, earlyLiveAfterTheMerge);
                Assert.Equal(100, store.RowCount);
                Assert.Equal(0, Live(store, references.ToArray(), early.Concat(late)));
            }

            using var restarted = await Open(provider);
            using var again = await Store(restarted, RowStoreMode.Keyed);

            Assert.Equal(100, again.RowCount);
            Assert.Equal(0, Live(again, references.ToArray(), early.Concat(late)));
            Assert.Equal(100, Live(again, references.ToArray(), Enumerable.Range(0, 300).Where(x => x % 3 == 2)));
            await AssertReads(again, references.ToArray(), Enumerable.Range(0, 300).Where(x => x % 3 == 2));
        }

        [Fact]
        public async Task AStoreThatFailedACheckpointIsOpenedAnewFromTheLastOne()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            bool fail = false;
            using (var stream = await Open(provider))
            {
                var options = new RowStoreOptions()
                {
                    Mode = RowStoreMode.Keyed,
                    Heap = new RowHeapOptions()
                    {
                        PageTargetBytes = 2000,
                        PageWriteHookForTests = () =>
                        {
                            if (fail)
                            {
                                throw new IOException("The page was not written.");
                            }
                        }
                    }
                };
                using var store = await RowStore.CreateAsync(stream.Client, "store", 2, options, GlobalMemoryManager.Instance);
                references = await Append(store, 0, 40);
                await Checkpoint(stream, store);
                for (int row = 0; row < 10; row++)
                {
                    await store.RetireAsync(references[row]);
                }
                fail = true;
                await Assert.ThrowsAnyAsync<Exception>(async () =>
                {
                    await Append(store, 40, 40);
                    await store.BeginCheckpointAsync();
                    await store.CheckpointAsync();
                });
                await Assert.ThrowsAnyAsync<Exception>(() => store.BeginCheckpointAsync());
                fail = false;
            }

            using var restarted = await Open(provider);
            using var again = await Store(restarted, RowStoreMode.Keyed);
            long rowsAfterTheRestart = again.RowCount;
            for (int row = 0; row < 10; row++)
            {
                await again.RetireAsync(references[row]);
            }
            await Checkpoint(restarted, again);

            Assert.Equal(40, rowsAfterTheRestart);
            Assert.Equal(30, again.RowCount);
            await AssertReads(again, references, Enumerable.Range(10, 30));
        }

        [Fact]
        public async Task AStoreThatFailedACheckpointRecoversOnTheSameManager()
        {
            using var stream = await Open();
            bool fail = false;
            var options = new RowStoreOptions()
            {
                Mode = RowStoreMode.Keyed,
                Heap = new RowHeapOptions()
                {
                    PageTargetBytes = 2000,
                    PageWriteHookForTests = () =>
                    {
                        if (Volatile.Read(ref fail))
                        {
                            throw new IOException("The page was not written.");
                        }
                    }
                }
            };
            var store = await RowStore.CreateAsync(stream.Client, "store", 2, options, GlobalMemoryManager.Instance);
            var references = await Append(store, 0, 40);
            await Checkpoint(stream, store);
            for (int row = 0; row < 10; row++)
            {
                await store.RetireAsync(references[row]);
            }
            await store.BeginCheckpointAsync();
            for (int row = 10; row < 15; row++)
            {
                await store.RetireAsync(references[row]);
            }
            Volatile.Write(ref fail, true);
            await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await Append(store, 40, 40);
                await store.CheckpointAsync();
            });
            Volatile.Write(ref fail, false);
            store.Dispose();
            await stream.Manager.InitializeAsync();
            using var again = await RowStore.CreateAsync(stream.Client, "store", 2, options, GlobalMemoryManager.Instance);
            long rowsAfterTheRecovery = again.RowCount;
            long waitingAfterTheRecovery = again.WaitingRows;
            int liveAfterTheRecovery = Live(again, references, Enumerable.Range(0, 40));
            for (int row = 0; row < 10; row++)
            {
                await again.RetireAsync(references[row]);
            }
            await Checkpoint(stream, again);

            Assert.Equal(40, rowsAfterTheRecovery);
            Assert.Equal(0, waitingAfterTheRecovery);
            Assert.Equal(40, liveAfterTheRecovery);
            Assert.Equal(30, again.RowCount);
            await AssertReads(again, references, Enumerable.Range(10, 30));
        }

        [Fact]
        public async Task AStoreOfAnotherFormatIsRefused()
        {
            var provider = new MemoryFileProvider();
            using (var stream = await Open(provider))
            {
                using var store = await Store(stream, RowStoreMode.Keyed);
                await Checkpoint(stream, store);
                var state = await stream.Client.GetOrCreateObjectStateAsync<RowStoreState>("store_store");
                state.Value!.FormatVersion = 7;
                await state.Commit();
                await stream.Manager.CheckpointAsync();
            }

            using var restarted = await Open(provider);
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => Store(restarted, RowStoreMode.Keyed));

            Assert.Equal("Row store 'store' is stored with format 7 in mode Keyed, the stream expects format 1 in mode Keyed.", refused.Message);
        }

        [Fact]
        public async Task AHeapTakesOneBarrierBeforeItsCommit()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);

            heap.BeginCheckpoint();
            var twice = Assert.Throws<InvalidOperationException>(() => heap.BeginCheckpoint());
            await heap.CheckpointAsync();
            heap.BeginCheckpoint();

            Assert.Equal("Row heap 'heap' has a checkpoint that is not committed yet.", twice.Message);
        }

        [Fact]
        public async Task AReleasedRowIsNotRetired()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Keyed);
            var references = await Append(store, 0, 30);

            await store.RetireAsync(references[7]);
            await Checkpoint(stream, store);
            await Checkpoint(stream, store);
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.RetireAsync(references[7]));

            Assert.EndsWith("points at no row, it cannot be retired.", refused.Message);
            Assert.Equal(0, store.WaitingRows);
        }

        [Fact]
        public async Task AReferenceToNoRowIsNotRetired()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Keyed);
            var references = await Append(store, 0, 5);
            ulong beyondThePage = RowReference.Pack(RowReference.Sequence(references[0]), 5);
            ulong noSuchPage = RowReference.Pack(RowReference.Sequence(references[0]) + 1000, 0);

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.RetireAsync(beyondThePage));
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.RetireAsync(noSuchPage));
            await Checkpoint(stream, store);
            await Checkpoint(stream, store);

            Assert.Equal(0, store.WaitingRows);
            Assert.Equal(5, store.RowCount);
        }

        [Fact]
        public async Task ManyRetiredRowsOverManyFlushesAreEachReleasedOnce()
        {
            using var stream = await Open();
            using var store = await Store(stream, RowStoreMode.Keyed, buffer: 64);
            var references = await Append(store, 0, 20_000);
            var random = new Random(11);
            var retired = Enumerable.Range(0, 20_000).Where(x => x % 3 != 0).OrderBy(x => random.Next()).ToArray();
            var kept = Enumerable.Range(0, 20_000).Where(x => x % 3 == 0).ToArray();
            long storedBefore = store.Heap.StoredBytes;
            long rowsAfterOne = 0;

            for (int i = 0; i < retired.Length; i++)
            {
                await store.RetireAsync(references[retired[i]]);
                if (i == retired.Length / 2)
                {
                    await Checkpoint(stream, store);
                    rowsAfterOne = store.RowCount;
                }
            }
            await Checkpoint(stream, store);
            long rowsAfterTwo = store.RowCount;
            for (int round = 0; round < 3; round++)
            {
                bool more = true;
                while (more)
                {
                    more = await store.CompactAsync();
                }
                await Checkpoint(stream, store);
            }

            Assert.Equal(20_000 - retired.Length / 2 - 1, rowsAfterOne);
            Assert.Equal(kept.Length, rowsAfterTwo);
            Assert.Equal(kept.Length, store.RowCount);
            Assert.Equal(0, store.WaitingRows);
            Assert.Equal(0, Live(store, references, retired));
            Assert.True(store.Heap.PagesMerged > 0);
            Assert.True(store.Heap.StoredBytes < storedBefore * 2 / 3, $"{store.Heap.StoredBytes} bytes are stored of {storedBefore}, a third of the rows are left.");
            await AssertReads(store, references, kept);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AStoreIsOpenedInTheModeItWasStoredIn(bool keyed)
        {
            var stored = keyed ? RowStoreMode.Keyed : RowStoreMode.Append;
            var asked = keyed ? RowStoreMode.Append : RowStoreMode.Keyed;
            var provider = new MemoryFileProvider();
            using (var stream = await Open(provider))
            {
                using var store = await Store(stream, stored);
                await Append(store, 0, 10);
                await Checkpoint(stream, store);
            }

            using var restarted = await Open(provider);
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => Store(restarted, asked));
            using var again = await Store(restarted, stored);

            Assert.Equal($"Row store 'store' is stored with format 1 in mode {stored}, the stream expects format 1 in mode {asked}.", refused.Message);
            Assert.Equal(10, again.RowCount);
        }

        [Fact]
        public async Task AStoreChecksItsSettings()
        {
            using var stream = await Open();

            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => RowStore.CreateAsync(stream.Client, "store", 2, new RowStoreOptions() { Mode = 0 }, GlobalMemoryManager.Instance));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => RowStore.CreateAsync(stream.Client, "store", 2, new RowStoreOptions() { Mode = RowStoreMode.Keyed, RetireBufferSize = 0 }, GlobalMemoryManager.Instance));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => RowStore.CreateAsync(stream.Client, "store", 2, new RowStoreOptions() { Mode = RowStoreMode.Keyed, RetireBufferSize = RowStore.MaxRetireBufferSize + 1 }, GlobalMemoryManager.Instance));
            using var store = await Store(stream, RowStoreMode.Keyed);

            Assert.Equal(0, store.RowCount);
        }

        [Fact]
        public async Task ADisposedStoreRefusesItsCalls()
        {
            using var stream = await Open();
            var store = await Store(stream, RowStoreMode.Keyed);
            var references = await Append(store, 0, 5);
            using var batch = Batch(Kinds, 0, 1);
            using var added = Column.Create(GlobalMemoryManager.Instance);
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            weights.Add(1);

            store.Dispose();
            store.Dispose();

            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await store.AppendAsync(batch, 0, 1, added));
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await store.AppendAsync(batch, weights, added));
            await Assert.ThrowsAsync<ObjectDisposedException>(() => store.BeginCheckpointAsync());
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await store.RetireAsync(references[0]));
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await store.CompactAsync());
            await Assert.ThrowsAsync<ObjectDisposedException>(() => store.CheckpointAsync());
            Assert.Throws<ObjectDisposedException>(() => store.CreateReader());
            Assert.Throws<ObjectDisposedException>(() => store.Heap.Release(references[0]));
        }

        [Fact]
        public async Task AHeapKnowsWhichOfItsRowsAreLive()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await RowHeapTests.Append(heap, Kinds, 0, 200);
            long lastSequence = RowReference.Sequence(references[199]);
            int rowsOfTheLast = references.Count(x => RowReference.Sequence(x) == lastSequence);

            heap.Release(references[0]);
            heap.Release(references[198]);

            Assert.False(heap.IsLive(references[0]));
            Assert.True(heap.IsLive(references[1]));
            Assert.False(heap.IsLive(references[198]));
            Assert.True(heap.IsLive(references[199]));
            Assert.True(heap.IsLive(references[100]));
            Assert.False(heap.IsLive(RowReference.Pack(lastSequence, rowsOfTheLast)));
            Assert.False(heap.IsLive(RowReference.Pack(RowReference.Sequence(references[100]), 60_000)));
            Assert.False(heap.IsLive(RowReference.Pack(lastSequence + 1, 0)));
            Assert.False(heap.IsLive(RowReference.Pack(0, 0)));
        }

        [Fact]
        public async Task AHeapRefusesToReleaseARowAppendedAfterTheBarrierBeforeItsCommit()
        {
            var provider = new MemoryFileProvider();
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                await RowHeapTests.Append(heap, Kinds, 0, 10);
                heap.BeginCheckpoint();
                var late = await RowHeapTests.Append(heap, Kinds, 10, 5);

                Assert.Throws<InvalidOperationException>(() => heap.Release(late[0]));
                await heap.CheckpointAsync();
                await stream.Manager.CheckpointAsync();
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(2);

            Assert.Equal(10, again.RowCount);
        }
    }
}

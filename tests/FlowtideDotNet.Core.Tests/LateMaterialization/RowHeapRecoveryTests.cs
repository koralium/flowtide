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
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics.Metrics;
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapCollectionTests;
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowHeapRecoveryTests
    {
        // Fails the reads of stored pages while it is told to
        private sealed class FailingProvider : MemoryFileProvider
        {
            public bool FailReads { get; set; }

            public override ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
            {
                if (FailReads)
                {
                    throw new IOException("The test disk fails.");
                }
                return base.GetMemoryAsync(fileId, offset, length, crc32, cancellationToken);
            }

            public override ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default)
            {
                if (FailReads)
                {
                    throw new IOException("The test disk fails.");
                }
                return base.ReadAsync(fileId, offset, length, crc32, stateSerializer, cancellationToken);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AMergeThatCannotReadAPageLosesNothingAndIsDoneLater(bool restore)
        {
            var disk = new FailingProvider();
            var allocator = new CountingAllocator();
            var options = new RowHeapOptions() { PageTargetBytes = 2000 };
            var stream = await Open(disk);
            var heap = await RowHeap.CreateAsync(stream.Client, "heap", 2, options, allocator);
            var references = await Append(heap, Kinds, 0, 2000);
            long sequences = heap.SequenceCount;
            await Checkpoint(stream, heap);
            var kept = Enumerable.Range(0, 2000).Where(x => x % 2 == 1).ToArray();
            for (int i = 0; i < references.Length; i += 2)
            {
                heap.Release(references[i]);
            }
            for (long sequence = 1; sequence <= sequences; sequence++)
            {
                stream.Manager.DeleteFromCache(PageId(heap, sequence));
            }

            disk.FailReads = true;
            var failed = await Assert.ThrowsAnyAsync<Exception>(async () => await heap.CompactAsync());
            disk.FailReads = false;
            long mergedByTheFailedCall = heap.PagesMerged;
            long rowsAfterTheFailure = heap.RowCount;
            if (restore)
            {
                heap.Dispose();
                await stream.Manager.InitializeAsync();
                heap = await RowHeap.CreateAsync(stream.Client, "heap", 2, options, allocator);
                Assert.Equal(2000, heap.RowCount);
                for (int i = 0; i < references.Length; i += 2)
                {
                    heap.Release(references[i]);
                }
            }
            while (await heap.CompactAsync())
            {
            }
            long merged = heap.PagesMerged;
            await AssertReads(heap, references, kept);
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            long storedBytes = heap.StoredBytes;
            long liveBytes = heap.LiveBytes;
            var client = heap.Client;
            heap.Dispose();
            stream.Dispose();
            await client.DisposalTask.WaitAsync(Limit);

            Assert.Contains("The test disk fails.", failed.ToString());
            Assert.Equal(0, mergedByTheFailedCall);
            Assert.Equal(1000, rowsAfterTheFailure);
            Assert.True(merged >= sequences / 3, $"{merged} of {sequences} pages were merged.");
            Assert.True(storedBytes <= liveBytes * 3 / 2 + 2 * 2000, $"{storedBytes} bytes are stored for {liveBytes} bytes of live rows.");
            Assert.Equal(0, allocator.BytesFreedByFinalizers);
            Assert.Equal(0, allocator.OutstandingBytes);
        }

        [Fact]
        public async Task ACheckpointThatFailsAfterItsDeletesIsRolledBackByARestore()
        {
            using var stream = await Open();
            var gate = new Gate();
            bool fail = false;
            var options = new RowHeapOptions() { PageTargetBytes = 2000 };
            options.PageWriteHookForTests = () =>
            {
                if (Volatile.Read(ref fail))
                {
                    gate.Pass();
                    throw new IOException("The test disk fails.");
                }
            };
            var heap = await stream.Heap(2, options);
            var references = await Append(heap, Kinds, 0, 300);
            var first = RowsOfPage(references, 1);
            long firstPage = PageId(heap, 1);
            foreach (int row in first)
            {
                heap.Release(references[row]);
            }
            await Checkpoint(stream, heap);
            Volatile.Write(ref fail, true);
            await Append(heap, Kinds, 300, 20);
            Assert.True(gate.Entered.Wait(Limit));

            var checkpoint = heap.CheckpointAsync();
            gate.Release.Set();
            var failed = await Assert.ThrowsAnyAsync<Exception>(() => checkpoint);
            Volatile.Write(ref fail, false);
            long deletedByTheFailedCheckpoint = heap.PagesDeleted;
            heap.Dispose();
            await stream.Manager.InitializeAsync();
            using var restored = await stream.Heap(2, options);
            bool storedAfterTheRestore = restored.Client.IsStored(firstPage);
            long pagesAfterTheRestore = restored.PageCount;
            await AssertReads(restored, references, Enumerable.Range(0, 300).Except(first));
            await Checkpoint(stream, restored);
            bool storedAfterACheckpoint = restored.Client.IsStored(firstPage);

            Assert.Contains("The test disk fails.", failed.ToString());
            Assert.Equal(1, deletedByTheFailedCheckpoint);
            Assert.True(storedAfterTheRestore);
            Assert.Equal(300 - first.Length, restored.RowCount);
            Assert.Equal(1, restored.PagesDeleted);
            Assert.False(storedAfterACheckpoint);
            Assert.Equal(pagesAfterTheRestore - 1, restored.PageCount);
        }

        [Fact]
        public async Task AHeapStoredWithAnotherRowCountThanItsPagesFailsToOpen()
        {
            var provider = new MemoryFileProvider();
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                await Append(heap, Kinds, 0, 10);
                await Checkpoint(stream, heap);
                var state = await stream.Client.GetOrCreateObjectStateAsync<RowHeapState>("heap_state");
                state.Value!.RowCount = 11;
                await state.Commit();
                await stream.Manager.CheckpointAsync();
            }

            using var restarted = await Open(provider);
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => restarted.Heap(2));
            int reserved = restarted.Manager.CacheTable.ReservedHeldPages;

            Assert.Equal("Row heap 'heap' is stored with 11 rows, its pages hold 10.", refused.Message);
            Assert.Equal(0, reserved);
        }

        [Fact]
        public async Task AnAppendThatCannotTakeAPageIdFreesItsPage()
        {
            var allocator = new CountingAllocator();
            var stream = await Open();
            var heap = await RowHeap.CreateAsync(stream.Client, "heap", 2, new RowHeapOptions() { PageTargetBytes = 2000 }, allocator);
            heap.Client.NextPageId = heap.Client.LastReservedPageId + 1;

            var refused = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Append(heap, Kinds, 0, 10));
            var client = heap.Client;
            heap.Dispose();
            stream.Dispose();
            await client.DisposalTask.WaitAsync(Limit);

            Assert.StartsWith("The page client has used all", refused.Message);
            Assert.Equal(0, heap.RowCount);
            Assert.Equal(0, allocator.BytesFreedByFinalizers);
            Assert.Equal(0, allocator.OutstandingBytes);
        }

        [Fact]
        public async Task AMeasureThatCannotReadAPageLosesNothingAndIsDoneLater()
        {
            const int target = 32 * 1024;
            var disk = new FailingProvider();
            using var stream = await Open(disk);
            using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target });
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            batch.Columns[0].Add(new StringValue(new string('z', 30_000)));
            batch.Columns[0].Add(new StringValue("x"));
            batch.Columns[0].Add(new StringValue("x"));
            batch.Columns[0].Add(new StringValue("x"));
            var survivors = new List<ulong?>();
            for (int page = 0; page < 8; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 4, references);
                heap.Release((ulong)references.GetValueAt(0, default).AsLong);
                for (int slot = 1; slot < 4; slot++)
                {
                    survivors.Add((ulong)references.GetValueAt(slot, default).AsLong);
                }
            }
            await Checkpoint(stream, heap);
            for (long sequence = 1; sequence <= heap.SequenceCount; sequence++)
            {
                stream.Manager.DeleteFromCache(PageId(heap, sequence));
            }

            disk.FailReads = true;
            for (int i = 0; i < 8; i++)
            {
                await Assert.ThrowsAnyAsync<Exception>(async () => await heap.CompactAsync());
            }
            disk.FailReads = false;
            while (await heap.CompactAsync())
            {
            }
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            using var reader = heap.CreateReader();
            var fetched = await Gather(reader, survivors.ToArray(), new int[] { 0 });
            using var result = fetched[0];

            Assert.Equal(24, heap.RowCount);
            Assert.Equal(24, result.Count);
            Assert.True(heap.PagesMeasured >= 6, $"{heap.PagesMeasured} skewed pages were measured after the read failures cleared.");
            Assert.True(heap.PagesMerged >= 6, $"{heap.PagesMerged} skewed pages were merged after the read failures cleared.");
            Assert.True(heap.StoredBytes <= 2L * target + 1000, $"{heap.StoredBytes} bytes remain stored for 24 small rows.");
        }

        [Fact]
        public async Task HeapsThatFailToOpenLeaveNothingOfTheirPageClientsBehind()
        {
            string meterName = $"rowHeap-{Guid.NewGuid():N}";
            int published = 0;
            int ended = 0;
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, meters) =>
            {
                if (instrument.Meter.Name == meterName && instrument.Name.StartsWith("flowtide_page_client"))
                {
                    Interlocked.Increment(ref published);
                    meters.EnableMeasurementEvents(instrument);
                }
            };
            listener.MeasurementsCompleted = (instrument, state) => Interlocked.Increment(ref ended);
            listener.Start();
            using var stream = await Open(cachePages: 330, meterName: meterName);
            var table = stream.Manager.CacheTable;
            int before = table.ExternalHitCounterCountForTests;

            for (int attempt = 0; attempt < 16; attempt++)
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() => stream.Heap(1));
                Assert.Equal(0, table.ReservedHeldPages);
            }

            Assert.Equal(before, table.ExternalHitCounterCountForTests);
            Assert.Equal(16 * 3, published);
            Assert.Equal(published, ended);
        }

        [Theory]
        [InlineData((7L << 60) | 5, 1)]
        [InlineData((3L << 60) | (1L << 10), 0)]
        public async Task AHeapStoredWithAnEntryItDoesNotKnowFailsToOpen(long key, long value)
        {
            var provider = new MemoryFileProvider();
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                await Append(heap, Kinds, 0, 10);
                await Checkpoint(stream, heap);
                var index = await stream.Client.GetOrCreateTree("heap_index", new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                {
                    Comparer = new PrimitiveListComparer<long>(),
                    KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });
                await index.Upsert(key, value);
                await index.Commit();
                await stream.Manager.CheckpointAsync();
            }

            using var restarted = await Open(provider);
            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => restarted.Heap(2));

            Assert.Equal($"Row heap 'heap' is stored with an entry that does not fit it: key {key}, value {value}.", refused.Message);
        }
    }
}

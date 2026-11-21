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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using System.Reflection;
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapCollectionTests;
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowHeapMemoryTests
    {
        private static long Queued(RowHeap heap)
        {
            return heap.MergeQueueBytes / sizeof(long);
        }

        [Fact]
        public async Task AOneRowLiveSetUpdatedInOneRowBatchesKeepsTheMergeQueuesBounded()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var previous = await Append(heap, Kinds, 0, 1);
            long highest = 0;
            for (int round = 1; round <= 2000; round++)
            {
                var next = await Append(heap, Kinds, round, 1);
                heap.Release(previous[0]);
                previous = next;
                await heap.CompactAsync();
                if (round % 100 == 0)
                {
                    await Checkpoint(stream, heap);
                    highest = Math.Max(highest, Queued(heap));
                }
            }

            Assert.Equal(1, heap.RowCount);
            Assert.True(heap.PageCount < 300, $"{heap.PageCount} pages for one live row.");
            Assert.True(highest <= 20, $"{highest} pages waited in the merge queues after a checkpoint, for one live row.");
        }

        [Fact]
        public async Task ASmallLiveSetOfFullPagesKeepsTheMergeQueuesBounded()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2, pageTargetBytes: 8000);
            var kinds = new string[] { "int", "wide" };
            var previous = await Append(heap, kinds, 0, 7);
            long highest = 0;
            long highestBetween = 0;
            for (int round = 1; round <= 1000; round++)
            {
                var next = await Append(heap, kinds, round * 7, 7);
                for (int i = 0; i < previous.Length; i++)
                {
                    heap.Release(previous[i]);
                }
                previous = next;
                await heap.CompactAsync();
                highestBetween = Math.Max(highestBetween, Queued(heap));
                if (round % 100 == 0)
                {
                    await Checkpoint(stream, heap);
                    highest = Math.Max(highest, Queued(heap));
                }
            }

            Assert.Equal(7, heap.RowCount);
            Assert.True(heap.PageCount < 300, $"{heap.PageCount} pages for seven live rows.");
            Assert.True(highest <= 20, $"{highest} pages waited in the merge queues after a checkpoint, for seven live rows.");
            Assert.True(highestBetween <= 100 * 8 + 20, $"{highestBetween} pages waited in the merge queues between two checkpoints.");
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DeletedPagesDoNotStayInTheMergeQueues(bool small)
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2, pageTargetBytes: small ? 32768 : 2000);
            var live = await Append(heap, Kinds, 0, small ? 2 : 300);
            for (int round = 0; round < 32; round++)
            {
                var next = await Append(heap, Kinds, (round + 1) * 300, live.Length);
                foreach (ulong reference in live)
                {
                    heap.Release(reference);
                }
                live = next;
                await heap.CompactAsync();
                await Checkpoint(stream, heap);
            }
            await Checkpoint(stream, heap);
            await heap.CompactAsync();

            Assert.Equal(live.Length, heap.RowCount);
            Assert.True(heap.PageCount < 10);
            Assert.True(Queued(heap) <= 4 * heap.PageCount + 16, $"{Queued(heap)} pages wait in the merge queues for {heap.PageCount} stored pages.");
        }

        [Fact]
        public void ThePageTableIsAsLargeAsItsPagesNotAsThePagesEverAdded()
        {
            var pages = new RowHeapPageTable();
            pages.GetOrAdd(0).Rows = 1;
            long withOne = pages.ByteSize;

            for (long page = 1; page <= 1_048_576; page++)
            {
                pages.GetOrAdd(page).Rows = 1;
                pages.Remove(page - 1);
            }

            Assert.Equal(1, pages.Count);
            Assert.Equal(withOne, pages.ByteSize);
            Assert.Equal(1, pages.Rows(1_048_576));
        }

        // Appends pages of one large and three small rows, releases the large
        private static async Task<List<ulong?>> AppendAndReleaseLargeRows(RowHeap heap, int pages, bool compact)
        {
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            batch.Columns[0].Add(new StringValue(new string('z', 30_000)));
            batch.Columns[0].Add(new StringValue("x"));
            batch.Columns[0].Add(new StringValue("x"));
            batch.Columns[0].Add(new StringValue("x"));
            var survivors = new List<ulong?>();
            for (int page = 0; page < pages; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 4, references);
                heap.Release((ulong)references.GetValueAt(0, default).AsLong);
                for (int slot = 1; slot < 4; slot++)
                {
                    survivors.Add((ulong)references.GetValueAt(slot, default).AsLong);
                }
                if (compact)
                {
                    await heap.CompactAsync();
                }
            }
            return survivors;
        }

        private static async Task AssertSmallRows(RowHeap heap, List<ulong?> survivors)
        {
            using var reader = heap.CreateReader();
            var fetched = await Gather(reader, survivors.ToArray(), new int[] { 0 });
            using var result = fetched[0];
            Assert.Equal(survivors.Count, result.Count);
            for (int row = 0; row < result.Count; row++)
            {
                Assert.Equal("x", result.GetValueAt(row, default).AsString.ToString());
            }
        }

        [Fact]
        public async Task ReleasedLargeRowsDoNotStayBehindSmallSurvivors()
        {
            const int target = 32 * 1024;
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: target);

            var survivors = await AppendAndReleaseLargeRows(heap, 8, true);
            long takenAsLive = heap.LiveBytes;
            await Checkpoint(stream, heap);
            await heap.CompactAsync();
            await Checkpoint(stream, heap);

            Assert.Equal(24, heap.RowCount);
            await AssertSmallRows(heap, survivors);
            Assert.True(heap.PagesMeasured >= 6, $"{heap.PagesMeasured} pages were measured.");
            Assert.True(heap.PagesMerged >= 6, $"{heap.PagesMerged} pages were merged.");
            Assert.True(takenAsLive <= 2L * target, $"The heap took {takenAsLive} bytes as live.");
            Assert.True(heap.StoredBytes <= 2L * target + 1000, $"{heap.StoredBytes} bytes are stored for 24 small rows, the heap takes {heap.LiveBytes} bytes as live.");
        }

        [Fact]
        public async Task LargeRowsReleasedFromAMergedPageAreFoundToo()
        {
            const int target = 32 * 1024;
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: target);
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            batch.Columns[0].Add(new StringValue(new string('a', 10_000)));
            batch.Columns[0].Add(new StringValue(new string('b', 10_000)));
            for (int i = 0; i < 4; i++)
            {
                batch.Columns[0].Add(new StringValue("x"));
            }
            var second = new List<ulong>();
            var survivors = new List<ulong?>();
            for (int page = 0; page < 8; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 6, references);
                heap.Release((ulong)references.GetValueAt(0, default).AsLong);
                second.Add((ulong)references.GetValueAt(1, default).AsLong);
                for (int slot = 2; slot < 6; slot++)
                {
                    survivors.Add((ulong)references.GetValueAt(slot, default).AsLong);
                }
            }
            await CompactAll(heap);
            long mergedOnce = heap.PagesMerged;
            long measuredOnce = heap.PagesMeasured;
            int largeRowsInMergedPages = second.Count(x => PageId(heap, RowReference.Sequence(x)) >= heap.Client.FirstPageId + 8);
            await Checkpoint(stream, heap);

            foreach (ulong reference in second)
            {
                heap.Release(reference);
            }
            await CompactAll(heap);
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);

            Assert.True(mergedOnce >= 3, $"{mergedOnce} pages were merged at first.");
            Assert.True(largeRowsInMergedPages >= 3, $"{largeRowsInMergedPages} large rows were moved to merged pages.");
            Assert.True(heap.PagesMeasured > measuredOnce, $"{heap.PagesMeasured} pages were measured, {measuredOnce} before the merged pages lost their large rows.");
            Assert.True(heap.LiveBytes <= 4000, $"The heap takes {heap.LiveBytes} bytes as live, for 32 small rows.");
            Assert.True(heap.StoredBytes <= 2L * target + 1000, $"{heap.StoredBytes} bytes are stored for 32 small rows.");
            await AssertSmallRows(heap, survivors);
        }

        [Fact]
        public async Task PagesWithALargeReleasedRowAreMeasuredAgainAfterARestart()
        {
            const int target = 32 * 1024;
            var provider = new MemoryFileProvider();
            List<ulong?> survivors;
            long measuredBefore;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target, MaxStoredRatio = 4 });
                survivors = await AppendAndReleaseLargeRows(heap, 8, false);
                measuredBefore = heap.PagesMeasured;
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, pageTargetBytes: target);
            long takenAsLive = again.LiveBytes;
            await CompactAll(again);
            await Checkpoint(restarted, again);
            await Checkpoint(restarted, again);

            Assert.Equal(0, measuredBefore);
            Assert.True(takenAsLive > 8 * 20_000, $"The heap took {takenAsLive} bytes as live after the restart.");
            Assert.True(again.PagesMeasured >= 6, $"{again.PagesMeasured} pages were measured.");
            Assert.True(again.StoredBytes <= 2L * target + 1000, $"{again.StoredBytes} bytes are stored for 24 small rows.");
            await AssertSmallRows(again, survivors);
        }

        [Fact]
        public async Task SmallRowsReleasedFromSkewedPagesDoNotMergePagesThatAreNearlyFull()
        {
            const int target = 32 * 1024;
            using var stream = await Open();
            using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target, CompactPagesPerCall = 2 });
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            batch.Columns[0].Add(new StringValue(new string('z', 30_000)));
            for (int i = 0; i < 99; i++)
            {
                batch.Columns[0].Add(new StringValue("x"));
            }
            for (int page = 0; page < 10; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 100, references);
                for (int slot = 1; slot < 100; slot++)
                {
                    heap.Release((ulong)references.GetValueAt(slot, default).AsLong);
                }
            }

            await CompactAll(heap);

            Assert.Equal(10, heap.RowCount);
            Assert.Equal(0, heap.PagesMerged);
            Assert.Equal(10, heap.PageCount);
        }

        [Fact]
        public async Task UnmarkedPagesWithLargeSurvivorsStopMergingAtThePageTarget()
        {
            const int target = 32 * 1024;
            using var stream = await Open();
            using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target, CompactPagesPerCall = 2 });
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            for (int i = 0; i < 8; i++)
            {
                batch.Columns[0].Add(new StringValue(new string('z', 3800)));
            }
            for (int i = 0; i < 400; i++)
            {
                batch.Columns[0].Add(new StringValue("x"));
            }
            for (int page = 0; page < 12; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 408, references);
                for (int slot = 8; slot < 408; slot++)
                {
                    heap.Release((ulong)references.GetValueAt(slot, default).AsLong);
                }
            }

            await heap.CompactAsync();
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);

            Assert.Equal(96, heap.RowCount);
            Assert.True(heap.PagesMerged <= 2, $"One call with a budget of one page merged {heap.PagesMerged} pages that were nearly full.");
        }

        [Fact]
        public async Task LargeRowsMergedFromUnmarkedPagesAreMeasuredWhenReleased()
        {
            const int target = 32 * 1024;
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: target);
            using var largeBatch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            largeBatch.Columns[0].Add(new StringValue(new string('z', 15_000)));
            using var smallBatch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            for (int i = 0; i < 30; i++)
            {
                smallBatch.Columns[0].Add(new StringValue("x"));
            }
            var largeRows = new List<ulong>();
            var survivors = new List<ulong?>();
            for (int round = 0; round < 8; round++)
            {
                using (var references = Column.Create(GlobalMemoryManager.Instance))
                {
                    await heap.AppendAsync(largeBatch, 0, 1, references);
                    largeRows.Add((ulong)references.GetValueAt(0, default).AsLong);
                }
                for (int b = 0; b < 12; b++)
                {
                    using var references = Column.Create(GlobalMemoryManager.Instance);
                    await heap.AppendAsync(smallBatch, 0, 30, references);
                    for (int slot = 0; slot < 30; slot++)
                    {
                        survivors.Add((ulong)references.GetValueAt(slot, default).AsLong);
                    }
                }
                await CompactAll(heap);
            }
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);

            foreach (ulong reference in largeRows)
            {
                heap.Release(reference);
            }
            await CompactAll(heap);
            await Checkpoint(stream, heap);
            await CompactAll(heap);
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);

            Assert.Equal(survivors.Count, heap.RowCount);
            await AssertSmallRows(heap, survivors);
            Assert.True(heap.PagesMeasured >= 4, $"{heap.PagesMeasured} merged pages were measured after losing their large rows.");
            Assert.True(heap.StoredBytes <= 2L * target + 1000, $"{heap.StoredBytes} bytes stay stored for {survivors.Count} small rows.");
        }

        [Fact]
        public async Task DeadSequencesInAMergedPagesSlotMapAreNotCountedAsLive()
        {
            const int target = 16 * 1024;
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: target);
            using var skewedBatch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            skewedBatch.Columns[0].Add(new StringValue(new string('z', 300)));
            skewedBatch.Columns[0].Add(new StringValue("x"));
            skewedBatch.Columns[0].Add(new StringValue("x"));
            using var oneRow = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            oneRow.Columns[0].Add(new StringValue("x"));
            var released = new List<ulong>();
            var kept = new List<ulong?>();
            using (var references = Column.Create(GlobalMemoryManager.Instance))
            {
                await heap.AppendAsync(skewedBatch, 0, 3, references);
                released.Add((ulong)references.GetValueAt(0, default).AsLong);
                kept.Add((ulong)references.GetValueAt(1, default).AsLong);
                kept.Add((ulong)references.GetValueAt(2, default).AsLong);
            }
            for (int i = 0; i < 2679; i++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(oneRow, 0, 1, references);
                released.Add((ulong)references.GetValueAt(0, default).AsLong);
            }
            await CompactAll(heap);
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            Assert.True(heap.PagesMerged > 200, $"{heap.PagesMerged} pages were merged.");

            foreach (ulong reference in released)
            {
                heap.Release(reference);
            }
            await CompactAll(heap);
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);

            Assert.Equal(2, heap.RowCount);
            await AssertSmallRows(heap, kept);
            Assert.True(heap.LiveBytes <= 1000, $"The heap counts {heap.LiveBytes} bytes as live for two small rows.");
            Assert.True(heap.StoredBytes <= 2L * target + 1000, $"{heap.StoredBytes} bytes stay stored for two small rows.");
        }

        [Fact]
        public async Task LargeRowsReleasedFromMergedPagesAreFoundAfterARestart()
        {
            const int target = 2800;
            var provider = new MemoryFileProvider();
            var survivors = new List<ulong?>();
            long mergedBefore;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, pageTargetBytes: target);
                using var large = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
                using var small = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
                large.Columns[0].Add(new StringValue(new string('z', 1000)));
                small.Columns[0].Add(new StringValue("x"));
                var toRelease = new List<ulong>();
                for (int group = 0; group < 100; group++)
                {
                    using var largeReferences = Column.Create(GlobalMemoryManager.Instance);
                    await heap.AppendAsync(large, 0, 1, largeReferences);
                    toRelease.Add((ulong)largeReferences.GetValueAt(0, default).AsLong);
                    for (int row = 0; row < 60; row++)
                    {
                        using var smallReferences = Column.Create(GlobalMemoryManager.Instance);
                        await heap.AppendAsync(small, 0, 1, smallReferences);
                        survivors.Add((ulong)smallReferences.GetValueAt(0, default).AsLong);
                    }
                }
                await Checkpoint(stream, heap);
                await CompactAll(heap);
                await Checkpoint(stream, heap);
                await Checkpoint(stream, heap);
                mergedBefore = heap.PagesMerged;
                foreach (ulong reference in toRelease)
                {
                    heap.Release(reference);
                }
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, pageTargetBytes: target);
            for (int round = 0; round < 4; round++)
            {
                await CompactAll(again);
                await Checkpoint(restarted, again);
            }
            using var reader = again.CreateReader();
            var columns = await Gather(reader, survivors.ToArray(), new int[] { 0 });
            using var result = columns[0];
            long live = result.GetByteSize() + 24L * survivors.Count;

            Assert.True(mergedBefore > 6000, $"{mergedBefore} pages were merged before the restart.");
            Assert.Equal(6000, result.Count);
            Assert.True(again.PagesMeasured > 0, "No page was measured after the restart.");
            Assert.True(again.StoredBytes <= live * 3 / 2 + 2L * target,
                $"Stored {again.StoredBytes} bytes for {live} live bytes after the restart; estimated {again.LiveBytes}, measured {again.PagesMeasured}, merged {again.PagesMerged}.");
        }

        [Fact]
        public async Task LargeRowsMergedAmongSmallOnesAreFoundWhenTheyAreReleased()
        {
            const int target = 32768;
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: target);
            using var large = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            using var small = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            large.Columns[0].Add(new StringValue(new string('z', 9000)));
            for (int row = 0; row < 100; row++)
            {
                small.Columns[0].Add(new StringValue("x"));
            }
            var toRelease = new List<ulong>();
            var survivors = new List<ulong?>();
            for (int pair = 0; pair < 16; pair++)
            {
                using var largeReferences = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(large, 0, large.Count, largeReferences);
                toRelease.Add((ulong)largeReferences.GetValueAt(0, default).AsLong);
                using var smallReferences = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(small, 0, small.Count, smallReferences);
                for (int row = 0; row < small.Count; row++)
                {
                    survivors.Add((ulong)smallReferences.GetValueAt(row, default).AsLong);
                }
                await CompactAll(heap);
            }
            Assert.True(heap.PagesMerged > 0);
            foreach (ulong reference in toRelease)
            {
                heap.Release(reference);
            }
            await CompactAll(heap);
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            using var reader = heap.CreateReader();
            var columns = await Gather(reader, survivors.ToArray(), new int[] { 0 });
            using var result = columns[0];
            long liveBytes = result.GetByteSize();
            Assert.Equal(1600, result.Count);
            for (int row = 0; row < result.Count; row++)
            {
                Assert.Equal("x", result.GetValueAt(row, default).AsString.ToString());
            }
            Assert.True(heap.StoredBytes <= 2L * target + liveBytes * 3 / 2,
                $"Stored {heap.StoredBytes} for {liveBytes} actual live bytes; estimated {heap.LiveBytes}, measured {heap.PagesMeasured}, merged {heap.PagesMerged} pages.");
        }

        [Fact]
        public async Task SeveralLargerRowsReleasedFromAPageDoNotHideMostOfIt()
        {
            const int target = 32768;
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: target);
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            for (int row = 0; row < 9; row++)
            {
                batch.Columns[0].Add(new StringValue(new string('z', 3000)));
            }
            for (int row = 0; row < 27; row++)
            {
                batch.Columns[0].Add(new StringValue("x"));
            }
            var survivors = new List<ulong?>();
            for (int page = 0; page < 8; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, batch.Count, references);
                for (int row = 0; row < 9; row++)
                {
                    heap.Release((ulong)references.GetValueAt(row, default).AsLong);
                }
                for (int row = 9; row < batch.Count; row++)
                {
                    survivors.Add((ulong)references.GetValueAt(row, default).AsLong);
                }
                await CompactAll(heap);
            }
            await Checkpoint(stream, heap);
            await CompactAll(heap);
            await Checkpoint(stream, heap);
            using var reader = heap.CreateReader();
            var columns = await Gather(reader, survivors.ToArray(), new int[] { 0 });
            using var result = columns[0];
            long liveBytes = result.GetByteSize();
            Assert.Equal(216, result.Count);
            for (int row = 0; row < result.Count; row++)
            {
                Assert.Equal("x", result.GetValueAt(row, default).AsString.ToString());
            }
            Assert.True(heap.StoredBytes <= 2L * target + liveBytes * 3 / 2,
                $"Stored {heap.StoredBytes} for {liveBytes} actual live bytes; estimated {heap.LiveBytes}, measured {heap.PagesMeasured}, merged {heap.PagesMerged} pages.");
        }

        [Fact]
        public async Task TheReleasedRowsOfNarrowPagesTakeLessThanTheRowsThatAreLeft()
        {
            var kinds = new string[] { "int" };
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 32 * 1024);
            var references = await Append(heap, kinds, 0, 200_000);
            long sequences = heap.SequenceCount;
            for (int i = 0; i < references.Length; i++)
            {
                if (RowReference.Slot(references[i]) != 0)
                {
                    heap.Release(references[i]);
                }
            }
            for (int i = 0; i < 4; i++)
            {
                await CompactAll(heap);
                await Checkpoint(stream, heap);
            }

            Assert.Equal(sequences, heap.RowCount);
            Assert.True(sequences >= 20, $"{sequences} pages were appended.");
            Assert.True(heap.ReleasedSlotBytes <= sequences * 84, $"{heap.ReleasedSlotBytes} bytes are kept for the released rows of {sequences} page sequences with one row left.");
        }

        [Fact]
        public async Task LargeRowsReleasedFromAlreadyUnmeasuredPagesAreCountedInTheBudget()
        {
            const int target = 32 * 1024;
            using var stream = await Open();
            using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target, CompactPagesPerCall = 4 });
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            for (int row = 0; row < 3; row++)
            {
                batch.Columns[0].Add(new StringValue(new string('z', 9000)));
            }
            for (int row = 0; row < 45; row++)
            {
                batch.Columns[0].Add(new StringValue("x"));
            }
            var firstSmall = new List<ulong>();
            var largeRows = new List<ulong>();
            var survivors = new List<ulong?>();
            for (int page = 0; page < 100; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, batch.Count, references);
                for (int row = 0; row < 3; row++)
                {
                    largeRows.Add((ulong)references.GetValueAt(row, default).AsLong);
                }
                firstSmall.Add((ulong)references.GetValueAt(3, default).AsLong);
                for (int row = 4; row < batch.Count; row++)
                {
                    survivors.Add((ulong)references.GetValueAt(row, default).AsLong);
                }
            }
            foreach (ulong reference in firstSmall)
            {
                heap.Release(reference);
            }
            for (int checkpoint = 0; checkpoint < 2; checkpoint++)
            {
                await CompactAll(heap);
                await Checkpoint(stream, heap);
            }
            foreach (ulong reference in largeRows)
            {
                heap.Release(reference);
            }
            for (int checkpoint = 0; checkpoint < 2; checkpoint++)
            {
                await CompactAll(heap);
                await Checkpoint(stream, heap);
            }
            await CompactAll(heap);
            long mergedAfterTwoCheckpoints = heap.PagesMerged;
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);

            Assert.Equal(survivors.Count, heap.RowCount);
            await AssertSmallRows(heap, survivors);
            Assert.True(mergedAfterTwoCheckpoints >= 90, $"{mergedAfterTwoCheckpoints} of 100 pages were merged two checkpoints after their large rows were released, {heap.PagesMeasured} were measured.");
            Assert.True(heap.StoredBytes <= 2L * target + survivors.Count * 10L,
                $"Stored {heap.StoredBytes} bytes for {survivors.Count} small rows once the merged pages were deleted; live {heap.LiveBytes}, measured {heap.PagesMeasured}, merged {heap.PagesMerged}, budget {heap.CompactBudget}.");
        }

        // Pages of one large row and 99 small ones, the large released
        private static async Task<List<ulong?>> AppendSkewedPagesAndReleaseTheirLargeRows(RowHeap heap, int pages)
        {
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            batch.Columns[0].Add(new StringValue(new string('z', 30_000)));
            for (int i = 0; i < 99; i++)
            {
                batch.Columns[0].Add(new StringValue("x"));
            }
            var survivors = new List<ulong?>();
            for (int page = 0; page < pages; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 100, references);
                heap.Release((ulong)references.GetValueAt(0, default).AsLong);
                for (int slot = 1; slot < 100; slot++)
                {
                    survivors.Add((ulong)references.GetValueAt(slot, default).AsLong);
                }
            }
            return survivors;
        }

        [Fact]
        public async Task PagesMeasuredBeforeTheirFirstCheckpointAreFoundSparseAfterARestart()
        {
            const int target = 32 * 1024;
            var provider = new MemoryFileProvider();
            List<ulong?> survivors;
            long measuredBefore;
            long mergedBefore;
            long storedBefore;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target, CompactPagesPerCall = 4 });
                survivors = await AppendSkewedPagesAndReleaseTheirLargeRows(heap, 3);
                await heap.CompactAsync();
                measuredBefore = heap.PagesMeasured;
                mergedBefore = heap.PagesMerged;
                storedBefore = heap.StoredBytes;
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, pageTargetBytes: target);
            for (int round = 0; round < 4; round++)
            {
                await CompactAll(again);
                await Checkpoint(restarted, again);
            }

            Assert.True(measuredBefore >= 1, $"{measuredBefore} pages were measured before the checkpoint.");
            Assert.Equal(0, mergedBefore);
            Assert.True(storedBefore > 3 * 30_000, $"{storedBefore} bytes were stored before the checkpoint.");
            Assert.True(again.StoredBytes <= 2L * target + 1000, $"{again.StoredBytes} bytes are stored for 297 small rows after the restart, the heap takes {again.LiveBytes} bytes as live and measured {again.PagesMeasured} pages.");
            await AssertSmallRows(again, survivors);
        }

        [Fact]
        public async Task ThePagesMeasuredBeforeACheckpointAreSparseWithoutARestart()
        {
            const int target = 32 * 1024;
            using var stream = await Open();
            using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target, CompactPagesPerCall = 4 });
            var survivors = await AppendSkewedPagesAndReleaseTheirLargeRows(heap, 3);
            await heap.CompactAsync();
            await Checkpoint(stream, heap);
            for (int round = 0; round < 4; round++)
            {
                await CompactAll(heap);
                await Checkpoint(stream, heap);
            }

            Assert.True(heap.StoredBytes <= 2L * target + 1000, $"{heap.StoredBytes} bytes are stored for 297 small rows, the heap takes {heap.LiveBytes} bytes as live.");
            await AssertSmallRows(heap, survivors);
        }

        // Groups of 25 one-row pages and a 100-row page, merged
        private static async Task<(List<ulong> Short, List<ulong?> Long)> AppendShortAndLongSequences(RowHeapTests.Stream stream, RowHeap heap)
        {
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            for (int row = 0; row < 100; row++)
            {
                batch.Columns[0].Add(new StringValue("x"));
            }
            var release = new List<ulong>();
            var keep = new List<ulong?>();
            for (int group = 0; group < 100; group++)
            {
                for (int singleton = 0; singleton < 25; singleton++)
                {
                    using var references = Column.Create(GlobalMemoryManager.Instance);
                    await heap.AppendAsync(batch, 0, 1, references);
                    release.Add((ulong)references.GetValueAt(0, default).AsLong);
                }
                using var keptReferences = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 100, keptReferences);
                for (int row = 0; row < 100; row++)
                {
                    keep.Add((ulong)keptReferences.GetValueAt(row, default).AsLong);
                }
            }
            await Checkpoint(stream, heap);
            await CompactAll(heap);
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            return (release, keep);
        }

        private static async Task AssertNoDeadMapBytesAreHidden(RowHeapTests.Stream stream, RowHeap heap, List<ulong?> keep, int target)
        {
            for (int round = 0; round < 4; round++)
            {
                await CompactAll(heap);
                await Checkpoint(stream, heap);
            }
            using var reader = heap.CreateReader();
            var columns = await Gather(reader, keep.ToArray(), new int[] { 0 });
            using var result = columns[0];
            long live = result.GetByteSize() + 100L * 24;

            Assert.Equal(10000, result.Count);
            Assert.True(heap.StoredBytes <= live * 1.72 + 2.0 * target,
                $"Stored {heap.StoredBytes} bytes for {live} live payload and slot-map bytes; estimated {heap.LiveBytes}; measured {heap.PagesMeasured}; merged {heap.PagesMerged}.");
        }

        [Fact]
        public async Task ReleasingShortSequencesDoesNotHideTheirDeadSlotMapBytes()
        {
            const int target = 2000;
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: target);
            var (release, keep) = await AppendShortAndLongSequences(stream, heap);
            long mergedBefore = heap.PagesMerged;
            foreach (ulong reference in release)
            {
                heap.Release(reference);
            }

            Assert.True(mergedBefore > 2000);
            await AssertNoDeadMapBytesAreHidden(stream, heap, keep, target);
        }

        [Fact]
        public async Task ShortSequencesReleasedBeforeARestartDoNotHideTheirDeadSlotMapBytes()
        {
            const int target = 2000;
            var provider = new MemoryFileProvider();
            List<ulong?> keep;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, pageTargetBytes: target);
                var appended = await AppendShortAndLongSequences(stream, heap);
                keep = appended.Long;
                foreach (ulong reference in appended.Short)
                {
                    heap.Release(reference);
                }
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, pageTargetBytes: target);

            await AssertNoDeadMapBytesAreHidden(restarted, again, keep, target);
        }

        [Fact]
        public async Task AMeasuredPageCountsTheBytesOfItsLiveRows()
        {
            const int target = 32 * 1024;
            using var stream = await Open();
            using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target, CompactPagesPerCall = 4 });
            using var large = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            large.Columns[0].Add(new StringValue(new string('z', 30_000)));
            using var references = Column.Create(GlobalMemoryManager.Instance);
            await heap.AppendAsync(large, 0, 1, references);
            long largeRow = heap.StoredBytes;
            await AppendSkewedPagesAndReleaseTheirLargeRows(heap, 3);
            long page = (heap.StoredBytes - largeRow) / 3;

            await heap.CompactAsync();

            Assert.Equal(2, heap.PagesMeasured);
            Assert.Equal(0, heap.PagesMerged);
            long rows = largeRow + 2 * (page - largeRow) + page - page / 100;
            Assert.InRange(heap.LiveBytes, rows - 16, rows + 16);
        }

        // Small pages of small rows, merged, then some rows released
        private static async Task<string> MergeSmallRowsAndRelease(RowHeapTests.Stream stream, RowHeap heap, int rowsPerPage, int every, int rows = 300_000)
        {
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            for (int row = 0; row < rowsPerPage; row++)
            {
                batch.Columns[0].Add(new StringValue("x"));
            }
            var all = new List<ulong>();
            int pages = rows / rowsPerPage;
            for (int page = 0; page < pages; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, rowsPerPage, references);
                for (int row = 0; row < rowsPerPage; row++)
                {
                    all.Add((ulong)references.GetValueAt(row, default).AsLong);
                }
            }
            for (int round = 0; round < 4; round++)
            {
                await CompactAll(heap);
                await Checkpoint(stream, heap);
            }
            for (int i = 0; i < all.Count; i++)
            {
                if (i % every == 3)
                {
                    heap.Release(all[i]);
                }
            }
            return $"{pages} pages appended, {heap.PagesMerged} merged into {heap.PageCount} pages; stored {heap.StoredBytes}, live {heap.LiveBytes}";
        }

        [Theory]
        [InlineData(2000, 10, 10)]
        [InlineData(32 * 1024, 1000, 10)]
        [InlineData(2000, 10, 4)]
        public async Task MergedPagesOfSmallRowsAreNotReadWithoutARestart(int target, int rowsPerPage, int every)
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: target);
            string built = await MergeSmallRowsAndRelease(stream, heap, rowsPerPage, every);
            long readBefore = heap.PagesRead;
            for (int round = 0; round < 2; round++)
            {
                await CompactAll(heap);
                await Checkpoint(stream, heap);
            }

            Assert.True(heap.PagesMerged > 0, built);
            Assert.True(heap.PagesRead - readBefore <= 2, $"{heap.PagesRead - readBefore} pages read with one row in {every} released; {built}.");
        }

        [Theory]
        [InlineData(2000, 10, 10)]
        [InlineData(32 * 1024, 1000, 10)]
        [InlineData(2000, 10, 4)]
        public async Task MergedPagesOfSmallRowsAreNotReadAfterARestart(int target, int rowsPerPage, int every)
        {
            var provider = new MemoryFileProvider();
            string built;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, pageTargetBytes: target);
                built = await MergeSmallRowsAndRelease(stream, heap, rowsPerPage, every);
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, pageTargetBytes: target);
            int budget = again.CompactBudget;
            for (int round = 0; round < 2; round++)
            {
                await CompactAll(again);
                await Checkpoint(restarted, again);
            }

            Assert.True(again.PagesRead <= 2, $"{again.PagesRead} pages read after the restart, {again.PagesMeasured} measured, {again.PagesMerged} merged, budget at restore {budget}; stored {again.StoredBytes} for {again.LiveBytes} live; before: {built}.");
        }

        [Fact]
        public async Task MergedPagesOfOneRowSequencesAreNotReadAfterARestartWhenAFifthWasReleased()
        {
            const int target = 2000;
            var provider = new MemoryFileProvider();
            string built;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, pageTargetBytes: target);
                built = await MergeSmallRowsAndRelease(stream, heap, 1, 5, 20_000);
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, pageTargetBytes: target);
            for (int round = 0; round < 2; round++)
            {
                await CompactAll(again);
                await Checkpoint(restarted, again);
            }

            Assert.Equal(16_000, again.RowCount);
            Assert.True(again.PagesRead <= 2, $"{again.PagesRead} pages read after the restart, {again.PagesMeasured} measured; {built}.");
        }

        [Fact]
        public async Task SmallPagesAreMergedAfterARestartWithoutBeingMeasured()
        {
            var kinds = new string[] { "int" };
            var provider = new MemoryFileProvider();
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, pageTargetBytes: 500);
                for (int i = 0; i < 1000; i++)
                {
                    await Append(heap, kinds, i, 1);
                }
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, pageTargetBytes: 500);
            await CompactAll(again);

            Assert.True(again.PagesMerged > 500, $"{again.PagesMerged} of 1000 small pages were merged after the restart.");
            Assert.Equal(0, again.PagesMeasured);
        }

        [Fact]
        public void TheStoredCountOfSequencesBoundsTheRealOneFromBothSides()
        {
            int exactUpTo = 0;
            double mostOver = 0;
            for (int sequences = 1; sequences <= RowReference.MaxRowsPerPage; sequences++)
            {
                int code = RowHeap.SequencesCodeOf(sequences);
                int most = RowHeap.MostSequences(code);
                Assert.InRange(code, 0, byte.MaxValue);
                Assert.InRange(sequences, RowHeap.FewestSequences(code), most);
                Assert.True(code == 0 || RowHeap.MostSequences(code - 1) < sequences);
                if (most == sequences && exactUpTo == sequences - 1)
                {
                    exactUpTo = sequences;
                }
                mostOver = Math.Max(mostOver, (double)(most - sequences) / sequences);
            }

            Assert.True(exactUpTo >= 20, $"The count is exact up to {exactUpTo}.");
            Assert.True(mostOver < 0.05, $"The count is up to {mostOver:P1} over.");
        }

        // Says its rows are as large as it is told
        private sealed class SizedColumn : Column, IColumn
        {
            private readonly int _rowBytes;

            public SizedColumn(int rowBytes, int count) : base(GlobalMemoryManager.Instance)
            {
                _rowBytes = rowBytes;
                for (int row = 0; row < count; row++)
                {
                    Add(new Int64Value(row));
                }
            }

            void IColumn.GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
            {
                for (int row = 0; row < indices.Length; row++)
                {
                    sizes[row] += (row + 1) * _rowBytes;
                }
            }
        }

        [Fact]
        public async Task MergesDoNotCombineSurvivorsBeyondTheSignedPageSize()
        {
            using var stream = await RowHeapTests.Open();
            using var heap = await stream.Heap(2, pageTargetBytes: 1_800_000_000);
            using var batch = new EventBatchData(new IColumn[] { new SizedColumn(90_000_000, 10), new SizedColumn(90_000_000, 10) });
            var references = new List<ulong>();
            for (int page = 0; page < 10; page++)
            {
                using var appended = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 10, appended);
                for (int row = 0; row < 4; row++)
                {
                    references.Add((ulong)appended.GetValueAt(row, default).AsLong);
                }
            }
            await heap.Client.DrainAsync();
            using (var window = heap.Client.CreateWindow())
            {
                for (long sequence = 1; sequence <= 10; sequence++)
                {
                    long pageId = heap.Client.FirstPageId + RowHeap.PageOf(heap.Directory.Get(sequence));
                    var page = await heap.Client.GetAsync(pageId, window);
                    var columns = page.Data.GetColumns_Unsafe();
                    for (int column = 0; column < columns.Length; column++)
                    {
                        var original = columns[column];
                        columns[column] = new SizedColumn(90_000_000, 10);
                        original.Dispose();
                    }
                    window.Release(page);
                }
            }
            foreach (ulong reference in references)
            {
                heap.Release(reference);
            }
            long nextPage = heap.Client.NextPageId;

            await heap.CompactAsync();

            Assert.True(heap.PagesMerged > 0);
            Assert.Equal(heap.PagesMerged, heap.Client.NextPageId - nextPage);
        }

        [Fact]
        public async Task NullableStructPagesIncludeTheirOwnValidityBytes()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 128);
            using var batch = NullableStructBatch(64);
            using var references = Column.Create(GlobalMemoryManager.Instance);

            Assert.True(batch.GetByteSize() > 0);
            await heap.AppendAsync(batch, 0, batch.Count, references);

            Assert.True(heap.StoredBytes >= batch.GetByteSize(),
                $"The nullable struct stores {batch.GetByteSize()} bitmap bytes, but the heap accounts for {heap.StoredBytes} bytes.");
        }

        [Fact]
        public async Task SmallNullableStructPagesAreCoalesced()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 128);
            using var batch = NullableStructBatch(64);

            Assert.True(batch.GetByteSize() > 0);
            for (int page = 0; page < 40; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, batch.Count, references);
            }
            await CompactAll(heap);

            Assert.True(heap.PagesMerged > 0,
                $"Forty {batch.GetByteSize()}-byte nullable struct pages reached a 128-byte target, but {heap.PagesMerged} pages were merged; stored bytes {heap.StoredBytes}.");
        }

        [Fact]
        public async Task DeadNullableStructValidityBytesAreCollected()
        {
            const int target = 8192;
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: target);
            using var batch = NullableStructBatch(32768);
            var survivors = new List<ulong>();

            Assert.True(batch.GetByteSize() >= 4096);
            for (int page = 0; page < 16; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, batch.Count, references);
                survivors.Add((ulong)references.GetValueAt(0, default).AsLong);
                for (int row = 1; row < references.Count; row++)
                {
                    heap.Release((ulong)references.GetValueAt(row, default).AsLong);
                }
            }
            for (int checkpoint = 0; checkpoint < 4; checkpoint++)
            {
                await CompactAll(heap);
                await Checkpoint(stream, heap);
            }
            long actualBytes = 0;
            var pages = new HashSet<long>();
            using var window = heap.Client.CreateWindow();
            foreach (ulong reference in survivors)
            {
                long page = RowHeap.PageOf(heap.Directory.Get(RowReference.Sequence(reference)));
                if (pages.Add(page))
                {
                    var stored = await heap.Client.GetAsync(heap.Client.FirstPageId + page, window);
                    actualBytes += stored.Data.GetByteSize() + (stored.Map?.ByteSize ?? 0);
                    window.Release(stored);
                }
            }

            Assert.Equal(survivors.Count, heap.RowCount);
            Assert.True(actualBytes <= 2L * target + 26L * survivors.Count,
                $"The {survivors.Count} surviving structs retain {actualBytes} actual payload and map bytes; the heap accounts for {heap.StoredBytes}, measured {heap.PagesMeasured} pages and merged {heap.PagesMerged} pages.");
        }

        private static EventBatchData NullableStructBatch(int rows)
        {
            var column = Column.Create(GlobalMemoryManager.Instance);
            var header = StructHeader.Create("value");
            for (int row = 0; row < rows; row++)
            {
                if (row % 2 == 0)
                {
                    column.Add(new StructValue(header, NullValue.Instance));
                }
                else
                {
                    column.Add(NullValue.Instance);
                }
            }
            return new EventBatchData(new IColumn[] { column });
        }

        [Fact]
        public async Task AFullRowCapPageKeepsItsCountThroughTwoRestarts()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, pageTargetBytes: 600_000);
                references = await Append(heap, new[] { "int" }, 0, RowReference.MaxRowsPerPage);
                Assert.Equal(1, heap.PageCount);
                await Checkpoint(stream, heap);
            }
            using (var restarted = await Open(provider))
            {
                using var heap = await restarted.Heap(1, pageTargetBytes: 600_000);
                Assert.Equal(RowReference.MaxRowsPerPage, heap.RowCount);
                Assert.Equal(RowReference.MaxRowsPerPage, RowHeap.LiveOf(heap.Directory.Get(1)));
                heap.Release(references[0]);
                heap.Release(references[^1]);
                await Checkpoint(restarted, heap);
            }
            using var last = await Open(provider);
            using var again = await last.Heap(1, pageTargetBytes: 600_000);
            using var reader = again.CreateReader();
            var columns = await Gather(reader, new ulong?[] { references[1], references[^2] }, new[] { 0 });
            using var result = columns[0];

            Assert.Equal(RowReference.MaxRowsPerPage - 2, again.RowCount);
            Assert.Equal(4, result.GetValueAt(0, default).AsLong);
            Assert.Equal((RowReference.MaxRowsPerPage - 2L) * 7 - 3, result.GetValueAt(1, default).AsLong);
        }

        [Fact]
        public async Task AMergedRecordWithItsSignBitSetRestoresItsRowsAndBytes()
        {
            var provider = new MemoryFileProvider();
            var references = new List<ulong?>();
            var options = new RowHeapOptions { PageTargetBytes = 40_000, CompactPagesPerCall = 1024 };
            string text = new string('x', 40);
            long storedBytes;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, options);
                using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
                batch.Columns[0].Add(new StringValue(text));
                for (int sequence = 0; sequence < 1000; sequence++)
                {
                    using var appended = Column.Create(GlobalMemoryManager.Instance);
                    await heap.AppendAsync(batch, 0, 1, appended);
                    references.Add((ulong)appended.GetValueAt(0, default).AsLong);
                }
                await Checkpoint(stream, heap);
                await CompactAll(heap);
                using (var window = heap.Client.CreateWindow())
                {
                    var page = await heap.Client.GetAsync(PageId(heap, 1), window);
                    Assert.NotNull(page.Map);
                    Assert.True(RowHeap.SequencesCodeOf(page.Map!.Count) >= 128);
                    window.Release(page);
                }
                await Checkpoint(stream, heap);
                await Checkpoint(stream, heap);
                storedBytes = heap.StoredBytes;
            }
            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, options);
            using var reader = again.CreateReader();
            var columns = await Gather(reader, references.ToArray(), new[] { 0 });
            using var result = columns[0];

            Assert.Equal(1000, again.RowCount);
            Assert.Equal(storedBytes, again.StoredBytes);
            Assert.Equal(storedBytes, again.LiveBytes);
            Assert.Equal(1000, result.Count);
            for (int row = 0; row < result.Count; row++)
            {
                Assert.Equal(text, result.GetValueAt(row, default).AsString.ToString());
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(3)]
        [InlineData(17)]
        public async Task NestedNullableStructsCountEveryValidityLevelForSlicedAppends(int start)
        {
            using var stream = await RowHeapTests.Open();
            using var heap = await stream.Heap(2, pageTargetBytes: 8192);
            using var batch = new EventBatchData(new IColumn[]
            {
                Column.Create(GlobalMemoryManager.Instance),
                Column.Create(GlobalMemoryManager.Instance)
            });
            var outer = StructHeader.Create("child", "nothing");
            var inner = StructHeader.Create("leaf");
            for (int row = 0; row < 160; row++)
            {
                if (row % 3 == 0)
                {
                    batch.Columns[0].Add(NullValue.Instance);
                }
                else if (row % 3 == 1)
                {
                    batch.Columns[0].Add(new StructValue(outer, NullValue.Instance, NullValue.Instance));
                }
                else
                {
                    batch.Columns[0].Add(new StructValue(outer,
                        new StructValue(inner, NullValue.Instance), NullValue.Instance));
                }
                batch.Columns[1].Add(new StructValue(inner, NullValue.Instance));
            }
            const int count = 96;
            var indices = Enumerable.Range(start, count).ToArray();
            var expected = new int[count];
            foreach (var column in batch.Columns)
            {
                column.GetPrefixSumByteSizes(indices, expected);
            }
            using var references = Column.Create(GlobalMemoryManager.Instance);

            await heap.AppendAsync(batch, start, count, references);

            Assert.Equal(36, expected[^1]);
            Assert.Equal(expected[^1], heap.StoredBytes);
            Assert.Equal(count, heap.RowCount);
        }

        private const int Target = 2000;
        private const int Sequences = 3000;

        private static RowHeapPageTable Pages(RowHeap heap)
        {
            return (RowHeapPageTable)typeof(RowHeap).GetField("_pages", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(heap)!;
        }

        private static (int Rows, int Live, int Bytes, int LiveBytes, int Uncertain, bool Merged, bool Stored) Snapshot(RowHeapPageTable table, long page)
        {
            if (table.State(page) != RowHeapPageState.Stored)
            {
                return (0, 0, 0, 0, 0, false, false);
            }
            ref var entry = ref table.Get(page);
            return (entry.Rows, entry.Live, entry.Bytes, entry.LiveBytes, entry.Uncertain, entry.Merged, true);
        }

        // Measures every stored page, the worst LiveBytes - Uncertain - measured
        private static async Task<(long Worst, string Where, int Pages, int Merged)> WorstHidden(RowHeap heap)
        {
            var table = Pages(heap);
            var measure = typeof(RowHeap).GetMethod("MeasureAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
            long pages = heap.Client.NextPageId - heap.Client.FirstPageId;
            long worst = long.MinValue;
            string where = "";
            int count = 0;
            int merged = 0;
            for (long page = 0; page < pages; page++)
            {
                var before = Snapshot(table, page);
                if (!before.Stored || before.Live == 0)
                {
                    continue;
                }
                await (Task)measure.Invoke(heap, new object[] { page })!;
                int measured = table.LiveBytes(page);
                long over = (long)before.LiveBytes - before.Uncertain - measured;
                count++;
                if (before.Merged)
                {
                    merged++;
                }
                if (over > worst)
                {
                    worst = over;
                    where = $"page {page}: rows {before.Rows}, live {before.Live}, bytes {before.Bytes}, estimate {before.LiveBytes}, uncertain {before.Uncertain}, measured {measured}, merged {before.Merged}";
                }
            }
            return (worst, where, count, merged);
        }

        private static async Task<List<ulong>[]> AppendSequences(RowHeap heap, EventBatchData batch, int rowsPerSequence, int sequences)
        {
            var all = new List<ulong>[sequences];
            for (int sequence = 0; sequence < sequences; sequence++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                int start = batch.Count == rowsPerSequence ? 0 : sequence % (batch.Count - rowsPerSequence + 1);
                await heap.AppendAsync(batch, start, rowsPerSequence, references);
                all[sequence] = new List<ulong>();
                for (int row = 0; row < rowsPerSequence; row++)
                {
                    all[sequence].Add((ulong)references.GetValueAt(row, default).AsLong);
                }
            }
            return all;
        }

        private static async Task Merge(RowHeapTests.Stream stream, RowHeap heap)
        {
            for (int round = 0; round < 4; round++)
            {
                await CompactAll(heap);
                await Checkpoint(stream, heap);
            }
        }

        private static EventBatchData Strings(int rows, Func<int, string> text)
        {
            var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            for (int row = 0; row < rows; row++)
            {
                batch.Columns[0].Add(new StringValue(text(row)));
            }
            return batch;
        }

        // Builds, merges and releases one shape, returns what was released
        private static async Task Build(RowHeapTests.Stream stream, RowHeap heap, string shape)
        {
            switch (shape)
            {
                case "quarter":
                {
                    using var batch = Strings(10, _ => "x");
                    var all = await AppendSequences(heap, batch, 10, Sequences);
                    await Merge(stream, heap);
                    foreach (var sequence in all)
                    {
                        heap.Release(sequence[3]);
                        heap.Release(sequence[7]);
                    }
                    break;
                }
                case "dying":
                {
                    using var batch = Strings(10, _ => "x");
                    var all = await AppendSequences(heap, batch, 10, Sequences);
                    await Merge(stream, heap);
                    for (int s = 0; s < all.Length; s++)
                    {
                        if (s % 3 == 0)
                        {
                            foreach (var reference in all[s])
                            {
                                heap.Release(reference);
                            }
                        }
                        else
                        {
                            heap.Release(all[s][3]);
                        }
                    }
                    break;
                }
                case "listed":
                {
                    // Even sequences lose row 0 first, so their slots are listed
                    using var batch = Strings(10, _ => "x");
                    var all = await AppendSequences(heap, batch, 10, Sequences);
                    for (int s = 0; s < all.Length; s += 2)
                    {
                        heap.Release(all[s][0]);
                    }
                    await Merge(stream, heap);
                    for (int s = 0; s < all.Length; s += 2)
                    {
                        for (int row = 1; row < 6; row++)
                        {
                            heap.Release(all[s][row]);
                        }
                    }
                    break;
                }
                case "listedOne":
                {
                    // Same, one row more released per listed sequence than the rest
                    using var batch = Strings(10, _ => "x");
                    var all = await AppendSequences(heap, batch, 10, Sequences);
                    for (int s = 0; s < all.Length; s += 4)
                    {
                        heap.Release(all[s][0]);
                    }
                    await Merge(stream, heap);
                    for (int s = 0; s < all.Length; s += 4)
                    {
                        for (int row = 1; row < 9; row++)
                        {
                            heap.Release(all[s][row]);
                        }
                    }
                    break;
                }
                case "listedLong":
                {
                    // Long listed sequences lose rows, the map share is small
                    using var batch = Strings(100, _ => "x");
                    var all = await AppendSequences(heap, batch, 100, 600);
                    for (int s = 0; s < all.Length; s += 2)
                    {
                        heap.Release(all[s][0]);
                    }
                    await Merge(stream, heap);
                    for (int s = 0; s < all.Length; s += 2)
                    {
                        for (int row = 1; row < 51; row++)
                        {
                            heap.Release(all[s][row]);
                        }
                    }
                    break;
                }
                case "large":
                {
                    using var batch = Strings(10, row => row == 0 ? new string('z', 300) : "x");
                    var all = await AppendSequences(heap, batch, 10, Sequences / 4);
                    await Merge(stream, heap);
                    for (int s = 0; s < all.Length; s += 2)
                    {
                        heap.Release(all[s][0]);
                    }
                    break;
                }
                case "nullable":
                {
                    // One-row sequences, half of them null
                    using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
                    batch.Columns[0].Add(NullValue.Instance);
                    batch.Columns[0].Add(new Int64Value(5));
                    var all = await AppendSequences(heap, batch, 1, Sequences);
                    await Merge(stream, heap);
                    for (int s = 0; s < all.Length; s += 5)
                    {
                        heap.Release(all[s][0]);
                    }
                    break;
                }
                default:
                    throw new ArgumentException(shape);
            }
        }

        [Theory]
        [InlineData("quarter")]
        [InlineData("dying")]
        [InlineData("listed")]
        [InlineData("listedOne")]
        [InlineData("listedLong")]
        [InlineData("large")]
        [InlineData("nullable")]
        public async Task ARunningHeapsUncertaintyBoundsWhatAMeasureFinds(string shape)
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: Target);
            await Build(stream, heap, shape);
            string built = $"merged {heap.PagesMerged} into {heap.PageCount} pages, stored {heap.StoredBytes}, live {heap.LiveBytes}";

            var (worst, where, pages, merged) = await WorstHidden(heap);

            Assert.True(merged > 0, built);
            Assert.True(worst <= 0, $"running: {worst} B over the bound at {where}; {pages} pages, {merged} merged; {built}");
        }

        [Theory]
        [InlineData("quarter")]
        [InlineData("dying")]
        [InlineData("listed")]
        [InlineData("listedOne")]
        [InlineData("listedLong")]
        [InlineData("large")]
        [InlineData("nullable")]
        public async Task ARestoredHeapsUncertaintyBoundsWhatAMeasureFinds(string shape)
        {
            var provider = new MemoryFileProvider();
            string built;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, pageTargetBytes: Target);
                await Build(stream, heap, shape);
                built = $"merged {heap.PagesMerged} into {heap.PageCount} pages, stored {heap.StoredBytes}, live {heap.LiveBytes}";
                await Checkpoint(stream, heap);
            }
            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, pageTargetBytes: Target);

            var (worst, where, pages, merged) = await WorstHidden(again);

            Assert.True(merged > 0, built);
            Assert.True(worst <= 0, $"restored: {worst} B over the bound at {where}; {pages} pages, {merged} merged; {built}");
        }

        [Fact]
        public async Task RestoredDeadListedSlotsTriggerCollectionBelowTheRowCountThreshold()
        {
            const int groups = 64;
            var provider = new MemoryFileProvider();
            var options = new RowHeapOptions { PageTargetBytes = 1000, MaxStoredRatio = 1.9 };
            var kinds = new[] { "double" };
            var released = new List<ulong>();
            var survivors = new List<ulong?>();
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, options);
                for (int group = 0; group < groups; group++)
                {
                    var first = await Append(heap, kinds, group * 110, 50);
                    heap.Release(first[0]);
                    released.AddRange(first.Skip(1));
                    var second = await Append(heap, kinds, group * 110 + 50, 60);
                    survivors.AddRange(second.Select(reference => (ulong?)reference));
                }
                await Checkpoint(stream, heap);
                await CompactAll(heap);
                await Checkpoint(stream, heap);
                await Checkpoint(stream, heap);
                using (var window = heap.Client.CreateWindow())
                {
                    var page = await heap.Client.GetAsync(PageId(heap, 1), window);
                    Assert.NotNull(page.Map);
                    Assert.Equal(2, page.Map!.Count);
                    Assert.Equal(49, page.Map.Slots(0)!.Length);
                    Assert.Null(page.Map.Slots(1));
                    window.Release(page);
                }
                Assert.Equal(groups + 1, heap.PageCount);
                foreach (var reference in released)
                {
                    heap.Release(reference);
                }
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, options);
            Assert.True(again.StoredBytes < again.LiveBytes * options.MaxStoredRatio);
            for (int round = 0; round < 4; round++)
            {
                await CompactAll(again);
                await Checkpoint(restarted, again);
            }
            using var reader = again.CreateReader();
            var columns = await Gather(reader, survivors.ToArray(), new[] { 0 });
            using var result = columns[0];
            long liveBytes = result.GetByteSize() + 24L * groups;

            Assert.Equal(groups * 60, again.RowCount);
            Assert.Equal(groups * 60, result.Count);
            Assert.Equal(12.5, result.GetValueAt(0, default).AsDouble);
            Assert.Equal(((groups - 1) * 110 + 109) * 0.25, result.GetValueAt(result.Count - 1, default).AsDouble);
            Assert.True(again.PagesMeasured > 0, "Dead listed slots were not measured after the restart.");
            Assert.True(again.StoredBytes <= liveBytes * options.MaxStoredRatio + 2.0 * options.PageTargetBytes,
                $"Stored {again.StoredBytes} bytes for {liveBytes} live bytes; measured {again.PagesMeasured}, merged {again.PagesMerged}.");
        }
    }

    [CollectionDefinition(nameof(RowHeapRetainedMemoryCollection), DisableParallelization = true)]
    public class RowHeapRetainedMemoryCollection
    {
    }

    [Collection(nameof(RowHeapRetainedMemoryCollection))]
    public class RowHeapRetainedMemoryTests
    {
        // Churns whole pages beside a fixed set of rows, with checkpoints
        private static async Task<long> Churn(RowHeapTests.Stream stream, RowHeap heap, long nextRow, int rounds)
        {
            var kinds = new string[] { "int" };
            for (int round = 0; round < rounds; round++)
            {
                var batch = await Append(heap, kinds, nextRow, 620);
                nextRow += 620;
                for (int i = 0; i < batch.Length; i++)
                {
                    heap.Release(batch[i]);
                }
                await heap.CompactAsync();
                if (round % 50 == 49)
                {
                    await Checkpoint(stream, heap);
                    await stream.Manager.Compact();
                }
            }
            return nextRow;
        }

        [Fact]
        public async Task PagesRetiredByReleasingEveryRowLeaveNoMemoryBehind()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 500);
            var live = await Append(heap, new string[] { "int" }, 0, 1200);
            long nextRow = await Churn(stream, heap, 1200, 2000);
            long baseline = GC.GetTotalMemory(true);
            long pagesBefore = heap.PagesDeleted;

            await Churn(stream, heap, nextRow, 4000);
            long after = GC.GetTotalMemory(true);
            GC.KeepAlive(heap);

            Assert.Equal(live.Length, heap.RowCount);
            using var reader = heap.CreateReader();
            AssertRows(new string[] { "int" }, new int[] { 0 }, Enumerable.Range(0, live.Length).Select(x => (long?)x).ToArray(), await Gather(reader, live.Select(x => (ulong?)x).ToArray(), new int[] { 0 }));
            Assert.True(heap.PagesDeleted - pagesBefore > 20_000, $"{heap.PagesDeleted - pagesBefore} pages were deleted.");
            Assert.True(after - baseline < 512 * 1024, $"The managed memory grew by {after - baseline} bytes while {heap.PagesDeleted - pagesBefore} pages came and went.");
        }
    }
}

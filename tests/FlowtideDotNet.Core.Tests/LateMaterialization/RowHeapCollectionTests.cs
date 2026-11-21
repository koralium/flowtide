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
using static FlowtideDotNet.Core.Tests.LateMaterialization.RowHeapTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowHeapCollectionTests
    {
        internal static readonly string[] Kinds = new string[] { "int", "string" };

        internal static readonly int[] Both = new int[] { 0, 1 };

        // The positions of the rows that were appended to one page
        internal static int[] RowsOfPage(ulong[] references, long sequence)
        {
            var rows = new List<int>();
            for (int i = 0; i < references.Length; i++)
            {
                if (RowReference.Sequence(references[i]) == sequence)
                {
                    rows.Add(i);
                }
            }
            return rows.ToArray();
        }

        internal static async Task AssertReads(RowHeap heap, ulong[] references, IEnumerable<int> rows, long firstRow = 0)
        {
            using var reader = heap.CreateReader();
            var asked = rows.ToArray();
            var fetched = await Gather(reader, asked.Select(x => (ulong?)references[x]).ToArray(), Both);
            AssertRows(Kinds, Both, asked.Select(x => (long?)(firstRow + x)).ToArray(), fetched);
        }

        internal static async Task Checkpoint(RowHeapTests.Stream stream, RowHeap heap)
        {
            await heap.CheckpointAsync();
            await stream.Manager.CheckpointAsync();
        }

        [Fact]
        public async Task AReleasedRowIsCountedAndTheLastOneOfAPageTakesThePageOut()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await Append(heap, Kinds, 0, 300);
            var first = RowsOfPage(references, 1);
            long pages = heap.PageCount;
            long firstPage = PageId(heap, 1);
            long bytes = heap.StoredBytes;

            for (int i = 1; i < first.Length; i++)
            {
                heap.Release(references[first[i]]);
            }
            long rowsWithOneLeft = heap.RowCount;
            int liveWithOneLeft = RowHeap.LiveOf(heap.Directory.Get(1));
            heap.Release(references[first[0]]);
            using var reader = heap.CreateReader();
            var gone = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Gather(reader, new ulong?[] { references[first[0]] }, Both));
            var again = Assert.Throws<InvalidOperationException>(() => heap.Release(references[first[0]]));

            Assert.True(first.Length > 1 && first.Length < 300);
            Assert.Equal(300 - first.Length + 1, rowsWithOneLeft);
            Assert.Equal(1, liveWithOneLeft);
            Assert.Equal(300 - first.Length, heap.RowCount);
            Assert.Equal(first.Length, heap.RowsReleased);
            Assert.Equal(0, heap.Directory.Get(1));
            Assert.Equal(pages, heap.PageCount);
            Assert.Equal(bytes, heap.StoredBytes);
            Assert.True(stream.Manager.TryPeekCacheEntry(firstPage, out _));
            await heap.Client.DrainAsync();
            Assert.True(heap.Client.IsStored(firstPage));
            Assert.Equal("Row reference to page 1 in heap 'heap' points at no page.", gone.Message);
            Assert.Equal("Row reference to page 1 in heap 'heap' points at no page, the row cannot be released.", again.Message);
            await AssertReads(heap, references, Enumerable.Range(0, 300).Except(first));
        }

        [Fact]
        public async Task ARowCannotBeReleasedTwiceOrThroughAReferenceToNoRow()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await Append(heap, Kinds, 0, 300);
            var first = RowsOfPage(references, 1);

            heap.Release(references[first[3]]);
            var twice = Assert.Throws<InvalidOperationException>(() => heap.Release(references[first[3]]));
            var beyond = Assert.Throws<InvalidOperationException>(() => heap.Release(RowReference.Pack(1, first.Length)));
            var noPage = Assert.Throws<InvalidOperationException>(() => heap.Release(RowReference.Pack(heap.SequenceCount + 1, 0)));
            heap.Dispose();

            Assert.Equal("The row at slot 3 of page 1 in heap 'heap' was already released.", twice.Message);
            Assert.Equal($"Row reference to slot {first.Length} of page 1 in heap 'heap' is beyond the page's {first.Length} rows, the row cannot be released.", beyond.Message);
            Assert.Equal($"Row reference to page {heap.SequenceCount + 1} in heap 'heap' points at no page, the row cannot be released.", noPage.Message);
            Assert.Equal(299, heap.RowCount);
            Assert.Equal(1, heap.RowsReleased);
            Assert.Throws<ObjectDisposedException>(() => heap.Release(references[first[4]]));
        }

        [Fact]
        public async Task APageIsDeletedAtTheSecondCheckpointAfterItsLastRowIsReleased()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            using var window = heap.Client.CreateWindow();
            var references = await Append(heap, Kinds, 0, 300);
            long firstPage = PageId(heap, 1);
            long secondPage = PageId(heap, 2);
            long firstBytes = heap.StoredBytes;
            await Checkpoint(stream, heap);

            foreach (int row in RowsOfPage(references, 1))
            {
                heap.Release(references[row]);
            }
            bool cachedWhenRetired = heap.Client.TryGet(firstPage, window, out _);
            window.ReleaseAll();
            await Checkpoint(stream, heap);
            foreach (int row in RowsOfPage(references, 2))
            {
                heap.Release(references[row]);
            }
            bool cachedAfterOne = heap.Client.TryGet(firstPage, window, out _);
            window.ReleaseAll();
            bool storedAfterOne = heap.Client.IsStored(firstPage);
            long pagesAfterOne = heap.PageCount;
            await Checkpoint(stream, heap);
            bool cachedAfterTwo = heap.Client.TryGet(firstPage, window, out _);
            bool storedAfterTwo = heap.Client.IsStored(firstPage);
            bool secondStoredAfterTwo = heap.Client.IsStored(secondPage);
            long pagesAfterTwo = heap.PageCount;
            long bytesAfterTwo = heap.StoredBytes;
            await Checkpoint(stream, heap);

            Assert.True(cachedWhenRetired);
            Assert.True(cachedAfterOne);
            Assert.True(storedAfterOne);
            Assert.False(cachedAfterTwo);
            Assert.False(storedAfterTwo);
            Assert.True(secondStoredAfterTwo);
            Assert.Equal(pagesAfterOne - 1, pagesAfterTwo);
            Assert.True(bytesAfterTwo < firstBytes);
            Assert.False(heap.Client.IsStored(secondPage));
            Assert.Equal(pagesAfterOne - 2, heap.PageCount);
            Assert.Equal(2, heap.PagesDeleted);
            await AssertReads(heap, references, Enumerable.Range(0, 300).Where(x => RowReference.Sequence(references[x]) > 2));
        }

        [Fact]
        public async Task PagesWithReleasedRowsAreMergedAndTheirRowsAreReadFromTheMergedPages()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            int[] kept;
            long sequences;
            long merged;
            long storedBytes;
            long liveBytes;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                references = await Append(heap, Kinds, 0, 2000);
                sequences = heap.SequenceCount;
                await Checkpoint(stream, heap);
                long pagesBefore = heap.PageCount;
                long bytesBefore = heap.StoredBytes;
                for (int i = 0; i < references.Length; i += 2)
                {
                    heap.Release(references[i]);
                }
                kept = Enumerable.Range(0, 2000).Where(x => x % 2 == 1).ToArray();

                await heap.CompactAsync();
                merged = heap.PagesMerged;
                long rowsMerged = heap.RowsMerged;
                await AssertReads(heap, references, kept);
                using var reader = heap.CreateReader();
                var released = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Gather(reader, new ulong?[] { references[0] }, Both));
                await Checkpoint(stream, heap);
                long pagesWhileKept = heap.PageCount;
                await Checkpoint(stream, heap);

                Assert.True(merged >= sequences / 3 && merged <= sequences / 2 + 4, $"{merged} of {sequences} pages were merged.");
                Assert.Equal(merged, rowsMerged * 2 * sequences / 2000);
                Assert.Equal("Row reference to slot 0 of page 1 in heap 'heap' points at a row that was released.", released.Message);
                Assert.True(pagesWhileKept > pagesBefore);
                Assert.Equal(pagesWhileKept - merged, heap.PageCount);
                Assert.True(heap.LiveBytes >= bytesBefore * 45 / 100 && heap.LiveBytes <= bytesBefore * 60 / 100, $"{heap.LiveBytes} of {bytesBefore} bytes are live rows.");
                Assert.True(heap.StoredBytes <= heap.LiveBytes * 3 / 2 + 2 * 2000, $"{heap.StoredBytes} bytes are stored for {heap.LiveBytes} bytes of live rows.");
                Assert.True(heap.StoredBytes > heap.LiveBytes * 5 / 4);
                Assert.Equal(merged, heap.PagesDeleted);
                Assert.Equal(1000, heap.RowCount);
                storedBytes = heap.StoredBytes;
                liveBytes = heap.LiveBytes;
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(2);
            using var cold = again.CreateReader();
            var fetched = await Gather(cold, kept.Select(x => (ulong?)references[x]).ToArray(), Both);

            AssertRows(Kinds, Both, kept.Select(x => (long?)x).ToArray(), fetched);
            Assert.True(cold.ColdLoads > 0);
            Assert.Equal(storedBytes, again.StoredBytes);
            Assert.InRange(again.LiveBytes, liveBytes * 90 / 100, liveBytes * 110 / 100);
            Assert.Equal(1000, again.RowCount);
            Assert.Equal(sequences, again.SequenceCount);
        }

        [Fact]
        public async Task AMergedPageIsMergedAgainAndItsRowsAreStillFound()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            int[] kept;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                references = await Append(heap, Kinds, 0, 3000);
                for (int i = 0; i < references.Length; i++)
                {
                    if (i % 2 == 0)
                    {
                        heap.Release(references[i]);
                    }
                }
                await heap.CompactAsync();
                long mergedOnce = heap.PagesMerged;
                var pagesOnce = Enumerable.Range(1, (int)heap.SequenceCount).Select(x => PageId(heap, x)).Distinct().ToArray();
                await Checkpoint(stream, heap);
                for (int i = 0; i < references.Length; i++)
                {
                    if (i % 2 == 1 && i % 6 != 1)
                    {
                        heap.Release(references[i]);
                    }
                }
                kept = Enumerable.Range(0, 3000).Where(x => x % 6 == 1).ToArray();

                await heap.CompactAsync();
                long mergedTwice = heap.PagesMerged;
                await AssertReads(heap, references, kept);
                long entry = heap.Directory.Get(RowReference.Sequence(references[kept[0]]));
                using var window = heap.Client.CreateWindow();
                bool cached = heap.Client.TryGet(heap.Client.FirstPageId + RowHeap.PageOf(entry), window, out var page);
                int hosted = page!.Map!.Find(RowReference.Sequence(references[kept[0]]));
                await Checkpoint(stream, heap);
                await Checkpoint(stream, heap);

                Assert.True(mergedOnce > 0);
                Assert.True(mergedTwice > mergedOnce);
                Assert.True(cached);
                Assert.True(hosted >= 0);
                var pagesTwice = Enumerable.Range(1, (int)heap.SequenceCount).Where(x => heap.Directory.Get(x) != 0).Select(x => PageId(heap, x)).Distinct().ToArray();
                var left = pagesOnce.Except(pagesTwice).ToArray();
                Assert.True(left.Length > 0);
                Assert.DoesNotContain(left, heap.Client.IsStored);
                Assert.All(pagesTwice, page => Assert.True(heap.Client.IsStored(page)));
                Assert.Equal(pagesTwice.Length, heap.PageCount);
                Assert.True(heap.RowsMerged <= 2 * heap.RowsReleased);
                Assert.Equal(500, heap.RowCount);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(2);
            await AssertReads(again, references, kept);
        }

        [Theory]
        [InlineData(1.2, 4, true)]
        [InlineData(1.5, 4, false)]
        [InlineData(1.5, 2, true)]
        [InlineData(4.0, 2, false)]
        public async Task PagesAreMergedWhenTheStoredBytesPassTheSetRatio(double ratio, int releaseOneIn, bool merged)
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2, maxStoredRatio: ratio);
            var references = await Append(heap, Kinds, 0, 2000);
            for (int i = 0; i < references.Length; i++)
            {
                if (i % releaseOneIn == 0)
                {
                    heap.Release(references[i]);
                }
            }
            long storedBefore = heap.StoredBytes;

            await heap.CompactAsync();
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);

            Assert.Equal(merged, heap.PagesMerged > 0);
            Assert.Equal(merged, heap.StoredBytes < storedBefore);
            Assert.True(heap.StoredBytes <= heap.LiveBytes * ratio + 2 * 2000, $"{heap.StoredBytes} bytes are stored for {heap.LiveBytes} bytes of live rows.");
            Assert.True(!merged || heap.StoredBytes > heap.LiveBytes * (ratio - 0.15), $"{heap.StoredBytes} bytes are stored for {heap.LiveBytes} bytes of live rows.");
            await AssertReads(heap, references, Enumerable.Range(0, 2000).Where(x => x % releaseOneIn != 0));
        }

        [Theory]
        [InlineData(1.0)]
        [InlineData(1.09)]
        [InlineData(4.01)]
        [InlineData(double.NaN)]
        public async Task AHeapRefusesAStoredRatioOutsideItsRange(double ratio)
        {
            using var stream = await Open();

            var refused = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await stream.Heap(1, maxStoredRatio: ratio));

            Assert.StartsWith($"The stored bytes may grow to between 1.1 and 4 times the live rows, got {ratio}.", refused.Message);
            Assert.Equal(0, stream.Manager.CacheTable.ReservedHeldPages);
        }

        [Fact]
        public async Task ThePagesWithTheFewestLiveRowsAreMergedFirst()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await Append(heap, Kinds, 0, 3000);
            long sequences = heap.SequenceCount;
            var pagesBefore = Enumerable.Range(1, (int)sequences).Select(x => PageId(heap, x)).ToArray();
            int keptOfTheLast = 0;
            for (int i = 0; i < references.Length; i++)
            {
                long sequence = RowReference.Sequence(references[i]);
                if (sequence <= sequences / 3 && i % 2 == 0)
                {
                    heap.Release(references[i]);
                }
                else if (sequence > 2 * sequences / 3)
                {
                    if (i % 10 == 0)
                    {
                        keptOfTheLast++;
                    }
                    else
                    {
                        heap.Release(references[i]);
                    }
                }
            }

            await heap.CompactAsync();
            int halfLive = (int)(sequences / 3);
            int full = (int)(2 * sequences / 3) - halfLive;
            int movedOfTheHalfLive = Enumerable.Range(1, halfLive).Count(x => PageId(heap, x) != pagesBefore[x - 1]);
            int movedOfTheFull = Enumerable.Range(halfLive + 1, full).Count(x => PageId(heap, x) != pagesBefore[x - 1]);
            int leftOfTheLast = Enumerable.Range(halfLive + full + 1, (int)sequences - halfLive - full).Count(x => PageId(heap, x) == pagesBefore[x - 1]);

            Assert.Equal(0, movedOfTheFull);
            Assert.True(movedOfTheHalfLive == 0 || leftOfTheLast == 0, $"{movedOfTheHalfLive} half live pages were merged while {leftOfTheLast} pages with a tenth live waited.");
            Assert.True(movedOfTheHalfLive <= 2, $"{movedOfTheHalfLive} half live pages were merged to fill the last merged page.");
            Assert.True(heap.RowsMerged >= keptOfTheLast / 2 && heap.RowsMerged <= keptOfTheLast + movedOfTheHalfLive * 3000 / sequences, $"{heap.RowsMerged} rows were merged, the pages with the fewest live rows hold {keptOfTheLast}.");
            Assert.True(heap.PagesMerged >= sequences / 6 && heap.PagesMerged <= sequences / 3 + 1 + movedOfTheHalfLive, $"{heap.PagesMerged} of {sequences} pages were merged.");
            Assert.True(heap.StoredBytes - heap.PagesMerged * 2000 <= heap.LiveBytes * 3 / 2 + 2000, $"{heap.StoredBytes} bytes are stored, {heap.PagesMerged} pages wait to be deleted, for {heap.LiveBytes} live bytes.");
            Assert.True(heap.StoredBytes > heap.LiveBytes);
            await AssertReads(heap, references, Enumerable.Range(0, 3000).Where(x =>
            {
                long sequence = RowReference.Sequence(references[x]);
                return sequence <= sequences / 3 ? x % 2 == 1 : sequence <= 2 * sequences / 3 || x % 10 == 0;
            }));
        }

        [Fact]
        public async Task APageThatDoesNotHoldTheAskedSequenceIsRefused()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await Append(heap, Kinds, 0, 2000);
            long last = heap.SequenceCount;
            for (int i = 0; i < references.Length; i += 2)
            {
                heap.Release(references[i]);
            }
            await heap.CompactAsync();
            using var reader = heap.CreateReader();

            heap.Directory.Set(last, heap.Directory.Get(last - 1));
            var appended = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Gather(reader, new ulong?[] { RowReference.Pack(last, 1) }, Both));
            heap.Directory.Set(last, heap.Directory.Get(1));
            var merged = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Gather(reader, new ulong?[] { RowReference.Pack(last, 1) }, Both));

            Assert.True(heap.PagesMerged > 0);
            Assert.Equal($"Row heap 'heap' read page {last - 1} where page {last} was asked for.", appended.Message);
            Assert.Equal($"Row heap 'heap' read a merged page without the rows of page {last}.", merged.Message);
        }

        [Fact]
        public async Task AHeapNeedsRoomInTheCacheForThePageItMergesFrom()
        {
            using var stream = await Open(cachePages: 330);
            var table = stream.Manager.CacheTable;

            var refused = await Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.Heap(1));
            int reserved = table.ReservedHeldPages;
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.Heap(1));

            Assert.Equal("Row heap 'heap' cannot be opened, the cache has no room for the pages it reads when it merges.", refused.Message);
            Assert.Equal(16, table.HeldPagesBound);
            Assert.Equal(0, reserved);
            Assert.Equal(0, table.ReservedHeldPages);
        }

        [Fact]
        public async Task SmallPagesAreMergedOnceIntoFullPages()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = new ulong[400];

            for (int i = 0; i < 200; i++)
            {
                var appended = await Append(heap, Kinds, i * 2, 2);
                appended.CopyTo(references, i * 2);
                await heap.CompactAsync();
            }
            long sequences = heap.SequenceCount;
            long merged = heap.PagesMerged;
            long rowsMerged = heap.RowsMerged;
            using var window = heap.Client.CreateWindow();
            bool cached = heap.Client.TryGet(PageId(heap, 1), window, out var first);
            var map = first!.Map!;
            window.ReleaseAll();
            await heap.CompactAsync();
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);

            Assert.Equal(200, sequences);
            Assert.True(cached);
            Assert.True(map.Count > 5);
            Assert.Equal(map.Count * 24, map.ByteSize);
            Assert.Null(map.Slots(0));
            Assert.Equal(2, map.RowCount(0));
            Assert.True(merged >= 150 && merged <= 200, $"{merged} pages were merged.");
            Assert.Equal(merged * 2, rowsMerged);
            Assert.Equal(rowsMerged, heap.RowsMerged);
            Assert.True(heap.PageCount <= 200 - merged + merged / 10 + 1, $"{heap.PageCount} pages are left.");
            Assert.Equal(400, heap.RowCount);
            await AssertReads(heap, references, Enumerable.Range(0, 400));
        }

        [Fact]
        public async Task SmallPagesFromBeforeARestartAreMergedAndFoundAfterAnotherOne()
        {
            var provider = new MemoryFileProvider();
            var references = new List<ulong>();
            long firstPage;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                for (int i = 0; i < 6; i++)
                {
                    references.AddRange(await Append(heap, Kinds, references.Count, 2));
                }
                await heap.CompactAsync();
                firstPage = PageId(heap, 1);
                await Checkpoint(stream, heap);
                Assert.Equal(0, heap.PagesMerged);
            }

            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                for (int i = 0; i < 100 && heap.PagesMerged == 0; i++)
                {
                    references.AddRange(await Append(heap, Kinds, references.Count, 2));
                    await heap.CompactAsync();
                }
                long merged = heap.PagesMerged;
                long firstPageNow = PageId(heap, 1);
                await Checkpoint(stream, heap);
                await Checkpoint(stream, heap);

                Assert.True(merged > 6, $"{merged} pages were merged.");
                Assert.NotEqual(firstPage, firstPageNow);
                Assert.False(heap.Client.IsStored(firstPage));
            }

            using var last = await Open(provider);
            using var end = await last.Heap(2);
            await AssertReads(end, references.ToArray(), Enumerable.Range(0, references.Count));
        }

        [Fact]
        public async Task PagesWithReleasedRowsAreMergedAfterARestartToo()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                references = await Append(heap, Kinds, 0, 2000);
                for (int i = 0; i < references.Length; i += 2)
                {
                    heap.Release(references[i]);
                }
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(2);
            long sequences = again.SequenceCount;
            await again.CompactAsync();

            Assert.True(again.PagesMerged >= sequences / 3, $"{again.PagesMerged} of {sequences} pages were merged.");
            Assert.True(again.StoredBytes > again.LiveBytes * 5 / 4);
            await AssertReads(again, references, Enumerable.Range(0, 2000).Where(x => x % 2 == 1));
        }

        [Fact]
        public async Task AMergedPageIsKnownAsMergedAfterARestart()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            var last = new List<int>();
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                references = await Append(heap, Kinds, 0, 400);
                long sequences = heap.SequenceCount;
                for (long sequence = 1; sequence <= sequences / 2; sequence++)
                {
                    var rows = RowsOfPage(references, sequence);
                    last.Add(rows[rows.Length - 1]);
                    for (int i = 0; i < rows.Length - 1; i++)
                    {
                        heap.Release(references[rows[i]]);
                    }
                }
                await heap.CompactAsync();
                await Checkpoint(stream, heap);

                Assert.Equal(sequences / 2, heap.PagesMerged);
                Assert.Equal(last.Count, heap.RowsMerged);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(2);
            long rowsBefore = again.RowCount;
            int slot = RowReference.Slot(references[last[0]]);
            again.Release(references[last[0]]);
            await again.CompactAsync();

            Assert.True(slot > last.Count, $"The released row has slot {slot}, the merged page holds {last.Count} rows.");
            Assert.Equal(rowsBefore - 1, again.RowCount);
            Assert.Equal(0, again.PagesMerged);
            await AssertReads(again, references, last.Skip(1));
        }

        [Fact]
        public async Task ARowIsReadFromItsOldPageUntilThatPageIsDeleted()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            using var window = heap.Client.CreateWindow();
            var references = await Append(heap, Kinds, 0, 2000);
            long oldPage = PageId(heap, 1);
            for (int i = 0; i < references.Length; i += 2)
            {
                heap.Release(references[i]);
            }
            long slotBytes = heap.ReleasedSlotBytes;

            await heap.CompactAsync();
            long slotBytesAfter = heap.ReleasedSlotBytes;
            long newPage = PageId(heap, 1);
            bool oldCached = heap.Client.TryGet(oldPage, window, out var old);
            long oldSequence = old!.Sequence;
            int oldRows = old.Count;
            window.ReleaseAll();
            await Checkpoint(stream, heap);
            bool oldCachedAfterOne = heap.Client.TryGet(oldPage, window, out _);
            window.ReleaseAll();
            await Checkpoint(stream, heap);
            bool oldCachedAfterTwo = heap.Client.TryGet(oldPage, window, out _);

            Assert.NotEqual(oldPage, newPage);
            Assert.True(slotBytes > 0);
            Assert.Equal(slotBytes, slotBytesAfter);
            Assert.True(oldCached);
            Assert.Equal(1, oldSequence);
            Assert.True(oldRows > 1);
            Assert.True(oldCachedAfterOne);
            Assert.False(oldCachedAfterTwo);
            Assert.False(heap.Client.IsStored(oldPage));
            Assert.True(heap.Client.IsStored(newPage));
        }

        [Fact]
        public async Task APageHeldByAReaderOutlivesItsDelete()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            using var window = heap.Client.CreateWindow();
            var references = await Append(heap, Kinds, 0, 300);
            long firstPage = PageId(heap, 1);
            bool held = heap.Client.TryGet(firstPage, window, out var page);
            foreach (int row in RowsOfPage(references, 1))
            {
                heap.Release(references[row]);
            }

            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            bool cached = stream.Manager.TryPeekCacheEntry(firstPage, out _);
            int rents = page!.RentCount;
            long value = page.Data.Columns[0].GetValueAt(1, default).AsLong;
            window.ReleaseAll();

            Assert.True(held);
            Assert.False(cached);
            Assert.False(heap.Client.IsStored(firstPage));
            Assert.Equal(1, rents);
            Assert.Equal(4, value);
            Assert.Equal(0, page.RentCount);
        }

        [Fact]
        public async Task AReleaseAfterTheCheckpointIsForgottenByARestore()
        {
            using var stream = await Open();
            var heap = await stream.Heap(2);
            var references = await Append(heap, Kinds, 0, 2000);
            await Checkpoint(stream, heap);
            long pages = heap.PageCount;
            for (int i = 0; i < references.Length; i += 2)
            {
                heap.Release(references[i]);
            }
            await heap.CompactAsync();
            long merged = heap.PagesMerged;
            heap.Dispose();

            await stream.Manager.InitializeAsync();
            using var restored = await stream.Heap(2);

            Assert.True(merged > 0);
            Assert.Equal(2000, restored.RowCount);
            Assert.Equal(pages, restored.PageCount);
            Assert.Equal(0, restored.PagesMerged);
            await AssertReads(restored, references, Enumerable.Range(0, 2000));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ARetiredPageWaitsThroughARestartForItsCheckpoint(bool storedBefore)
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            long firstPage;
            long pages;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                references = await Append(heap, Kinds, 0, 300);
                firstPage = PageId(heap, 1);
                if (storedBefore)
                {
                    await Checkpoint(stream, heap);
                }
                foreach (int row in RowsOfPage(references, 1))
                {
                    heap.Release(references[row]);
                }
                await Checkpoint(stream, heap);
                pages = heap.PageCount;
            }

            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                bool storedWhileListed = heap.Client.IsStored(firstPage);
                long pagesWhileListed = heap.PageCount;
                long slot = heap.Directory.Get(1);
                await heap.CheckpointAsync();
                bool storedAfterTheDelete = heap.Client.IsStored(firstPage);

                Assert.Equal(0, slot);
                Assert.True(storedWhileListed);
                Assert.Equal(pages, pagesWhileListed);
                Assert.False(storedAfterTheDelete);
                Assert.Equal(pages - 1, heap.PageCount);
            }

            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                bool storedAfterTheCrash = heap.Client.IsStored(firstPage);
                long pagesAfterTheCrash = heap.PageCount;
                await Checkpoint(stream, heap);

                Assert.True(storedAfterTheCrash);
                Assert.Equal(pages, pagesAfterTheCrash);
                Assert.False(heap.Client.IsStored(firstPage));
            }

            using var last = await Open(provider);
            using var end = await last.Heap(2);
            Assert.False(end.Client.IsStored(firstPage));
            Assert.Equal(pages - 1, end.PageCount);
            Assert.Equal(300 - RowsOfPage(references, 1).Length, end.RowCount);
            await AssertReads(end, references, Enumerable.Range(0, 300).Where(x => RowReference.Sequence(references[x]) > 1));
        }

        [Fact]
        public async Task TheReleasedRowsOfAPageAreKnownAfterARestart()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            int[] first;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                references = await Append(heap, Kinds, 0, 300);
                first = RowsOfPage(references, 1);
                heap.Release(references[first[0]]);
                heap.Release(references[first[2]]);
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(2);
            var twice = Assert.Throws<InvalidOperationException>(() => again.Release(references[first[2]]));
            int live = RowHeap.LiveOf(again.Directory.Get(1));
            for (int i = 0; i < first.Length; i++)
            {
                if (i != 0 && i != 2)
                {
                    again.Release(references[first[i]]);
                }
            }

            Assert.Equal("The row at slot 2 of page 1 in heap 'heap' was already released.", twice.Message);
            Assert.Equal(first.Length - 2, live);
            Assert.Equal(0, again.Directory.Get(1));
            Assert.Equal(300 - first.Length, again.RowCount);
            Assert.Equal(0, again.ReleasedSlotBytes);
            await Checkpoint(restarted, again);
            again.Dispose();
            restarted.Dispose();

            using var last = await Open(provider);
            using var end = await last.Heap(2);
            Assert.Equal(0, end.ReleasedSlotBytes);
            Assert.Equal(0, end.Directory.Get(1));
            Assert.Equal(300 - first.Length, end.RowCount);
        }

        [Fact]
        public async Task ARowReleasedBeforeAMergeIsRefusedAfterItToo()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await Append(heap, Kinds, 0, 2000);
            var first = RowsOfPage(references, 1);
            for (int i = 0; i < references.Length; i += 2)
            {
                heap.Release(references[i]);
            }
            await heap.CompactAsync();
            bool merged = heap.Client.FirstPageId + RowHeap.PageOf(heap.Directory.Get(1)) != heap.Client.FirstPageId;
            long rows = heap.RowCount;

            var twice = Assert.Throws<InvalidOperationException>(() => heap.Release(references[0]));
            var beyond = Assert.Throws<InvalidOperationException>(() => heap.Release(RowReference.Pack(1, first.Length)));

            Assert.True(merged);
            Assert.Equal("The row at slot 0 of page 1 in heap 'heap' was already released.", twice.Message);
            Assert.Equal($"Row reference to slot {first.Length} of page 1 in heap 'heap' is beyond the page's {first.Length} rows, the row cannot be released.", beyond.Message);
            Assert.Equal(rows, heap.RowCount);
            await AssertReads(heap, references, Enumerable.Range(0, 2000).Where(x => x % 2 == 1));
        }

        [Fact]
        public async Task ARowReleasedTwiceIsRefusedWhenItsPageSequenceHasOneRowLeft()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await Append(heap, Kinds, 0, 2000);
            var first = RowsOfPage(references, 1);
            int last = first[first.Length - 1];
            for (int i = 0; i < first.Length - 1; i++)
            {
                heap.Release(references[first[i]]);
            }
            for (int i = first.Length; i < references.Length; i += 2)
            {
                heap.Release(references[i]);
            }
            await heap.CompactAsync();
            long rows = heap.RowCount;

            var twice = Assert.Throws<InvalidOperationException>(() => heap.Release(references[first[0]]));
            var beyond = Assert.Throws<InvalidOperationException>(() => heap.Release(RowReference.Pack(1, first.Length)));
            int live = RowHeap.LiveOf(heap.Directory.Get(1));
            await AssertReads(heap, references, new int[] { last });
            heap.Release(references[last]);
            var gone = Assert.Throws<InvalidOperationException>(() => heap.Release(references[last]));

            Assert.True(heap.PagesMerged > 0);
            Assert.Equal("The row at slot 0 of page 1 in heap 'heap' was already released.", twice.Message);
            Assert.EndsWith($"is beyond the page's {first.Length} rows, the row cannot be released.", beyond.Message);
            Assert.Equal(1, live);
            Assert.Equal("Row reference to page 1 in heap 'heap' points at no page, the row cannot be released.", gone.Message);
            Assert.Equal(rows - 1, heap.RowCount);
            Assert.Equal(0, heap.Directory.Get(1));
        }

        [Fact]
        public async Task PagesStoredAfterTheHeapsCheckpointAreDeletedAfterARestartAndTheirIdsAreNotUsedAgain()
        {
            var provider = new MemoryFileProvider();
            ulong[] kept;
            long[] later;
            using (var stream = await Open(provider, maxFileSize: 4096))
            {
                using var heap = await stream.Heap(2);
                kept = await Append(heap, Kinds, 0, 300);
                await heap.CheckpointAsync();
                var lost = await Append(heap, Kinds, 300, 1500);
                await heap.Client.DrainAsync();
                later = lost.Select(x => RowReference.Sequence(x)).Distinct().Select(x => PageId(heap, x)).ToArray();
                await stream.Manager.CheckpointAsync();
            }

            using var restarted = await Open(provider, maxFileSize: 4096);
            int storedBefore = later.Count(restarted.Storage.IsStored);
            using var again = await restarted.Heap(2);
            long firstFree = again.Client.NextPageId;
            var added = await Append(again, Kinds, 5000, 100);
            var addedPages = added.Select(x => RowReference.Sequence(x)).Distinct().Select(x => PageId(again, x)).ToArray();
            await Checkpoint(restarted, again);
            var storedAfter = later.Where(restarted.Storage.IsStored).ToArray();
            foreach (long page in addedPages)
            {
                restarted.Manager.DeleteFromCache(page);
            }

            Assert.True(storedBefore > 0 && storedBefore < later.Length, $"{storedBefore} of {later.Length} later pages were stored.");
            Assert.Equal(later[0] + storedBefore, firstFree);
            Assert.Equal(firstFree, addedPages[0]);
            Assert.Equal(addedPages, storedAfter);
            Assert.Equal(400, again.RowCount);
            await AssertReads(again, kept, Enumerable.Range(0, 300));
            await AssertReads(again, added, Enumerable.Range(0, 100), 5000);
        }

        [Fact]
        public async Task PagesWrittenAfterARestartAreKeptWhenTheDeletesOfTheOldOnesAreCommitted()
        {
            var provider = new MemoryFileProvider();
            ulong[] kept;
            ulong[] added;
            long[] addedPages;
            int storedAfter;
            using (var stream = await Open(provider, maxFileSize: 4096))
            {
                using var heap = await stream.Heap(2);
                kept = await Append(heap, Kinds, 0, 300);
                await heap.CheckpointAsync();
                await Append(heap, Kinds, 300, 1500);
                await heap.Client.DrainAsync();
                await stream.Manager.CheckpointAsync();
            }
            using (var restarted = await Open(provider, maxFileSize: 4096))
            {
                using var heap = await restarted.Heap(2);
                await heap.CheckpointAsync();
                added = await Append(heap, Kinds, 5000, 400);
                await heap.Client.DrainAsync();
                await restarted.Manager.CheckpointAsync();
                await Checkpoint(restarted, heap);
                addedPages = added.Select(x => RowReference.Sequence(x)).Distinct().Select(x => PageId(heap, x)).ToArray();
                storedAfter = addedPages.Count(restarted.Storage.IsStored);
            }
            using var reopened = await Open(provider, maxFileSize: 4096);
            using var last = await reopened.Heap(2);

            Assert.Equal(addedPages.Length, storedAfter);
            await AssertReads(last, kept, Enumerable.Range(0, 300));
            await AssertReads(last, added, Enumerable.Range(0, 400), 5000);
        }

        [Fact]
        public async Task AFixedSetOfRowsThatKeepsChangingKeepsTheHeapBounded()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var random = new Random(7);
            const int live = 3000;
            var references = await Append(heap, Kinds, 0, live);
            var rows = Enumerable.Range(0, live).Select(x => (long)x).ToArray();
            await Checkpoint(stream, heap);
            long liveBytes = heap.StoredBytes;
            long bytesPerRow = liveBytes / live;
            long nextRow = live;
            double worstBefore = 0;
            double worstAfter = 0;
            long memoryEarly = 0;
            long memoryHighest = 0;
            int lowestBudget = int.MaxValue;

            for (int round = 0; round < 40; round++)
            {
                var changed = Enumerable.Range(0, live).Where(x => random.Next(10) < 3).ToArray();
                var appended = await Append(heap, Kinds, nextRow, changed.Length);
                for (int i = 0; i < changed.Length; i++)
                {
                    heap.Release(references[changed[i]]);
                    references[changed[i]] = appended[i];
                    rows[changed[i]] = nextRow + i;
                }
                nextRow += changed.Length;
                await heap.CompactAsync();
                lowestBudget = Math.Min(lowestBudget, round < 2 ? int.MaxValue : heap.CompactBudget);
                long changedBytes = changed.Length * bytesPerRow;
                worstBefore = Math.Max(worstBefore, (heap.StoredBytes - heap.LiveBytes * 1.5) / changedBytes);
                await Checkpoint(stream, heap);
                worstAfter = Math.Max(worstAfter, (heap.StoredBytes - heap.LiveBytes * 1.5) / changedBytes);
                long memory = heap.DirectoryBytes + heap.PageTableBytes + heap.MergeQueueBytes + heap.ReleasedSlotBytes;
                memoryHighest = Math.Max(memoryHighest, memory);
                if (round == 9)
                {
                    memoryEarly = memoryHighest;
                }
            }
            using var reader = heap.CreateReader();
            var fetched = await Gather(reader, references.Select(x => (ulong?)x).ToArray(), Both);
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            int pagesInUse = references.Select(x => RowHeap.PageOf(heap.Directory.Get(RowReference.Sequence(x)))).Distinct().Count();

            AssertRows(Kinds, Both, rows.Select(x => (long?)x).ToArray(), fetched);
            Assert.Equal(pagesInUse, heap.PageCount);
            Assert.Equal(live, heap.RowCount);
            Assert.True(worstAfter <= 2, $"After a checkpoint the heap stored its live rows one and a half times and {worstAfter} rounds of changes.");
            Assert.True(worstBefore <= 3.75, $"Before a checkpoint the heap stored its live rows one and a half times and {worstBefore} rounds of changes.");
            Assert.True(memoryHighest <= memoryEarly + 4096, $"The heap's own memory grew from {memoryEarly} to {memoryHighest} bytes.");
            Assert.True(heap.RowsMerged <= heap.RowsReleased, $"{heap.RowsMerged} rows were merged for {heap.RowsReleased} released.");
            Assert.True(lowestBudget > 0, $"A round used up what it may read between two checkpoints.");
            Assert.True(heap.PagesDeleted > 0);
        }

        [Fact]
        public async Task AMergedPageThatCannotTakeAPageIdFreesItsColumns()
        {
            var allocator = new CountingAllocator();
            var stream = await Open();
            var heap = await RowHeap.CreateAsync(stream.Client, "heap", 2, new RowHeapOptions() { PageTargetBytes = 2000 }, allocator);
            var references = await Append(heap, Kinds, 0, 2000);
            for (int i = 0; i < references.Length; i += 2)
            {
                heap.Release(references[i]);
            }
            heap.Client.NextPageId = heap.Client.LastReservedPageId + 1;

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await heap.CompactAsync());
            var disposal = heap.Client;
            heap.Dispose();
            stream.Dispose();
            await disposal.DisposalTask.WaitAsync(Limit);

            Assert.Equal(0, allocator.BytesFreedByFinalizers);
            Assert.Equal(0, allocator.OutstandingBytes);
        }

        [Fact]
        public async Task FullRowCapPagesAreNotMergedIntoThemselves()
        {
            var kinds = new string[] { "int" };
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 600_000);
            using var batch = Batch(kinds, 0, RowReference.MaxRowsPerPage);
            for (int i = 0; i < 3; i++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, batch.Count, references);
            }

            await heap.CompactAsync();

            Assert.Equal(0, heap.PagesMerged);
            Assert.Equal(0, heap.RowsMerged);
        }

        [Fact]
        public async Task GarbageFromIntegerTruncationDoesNotAllocateEmptyMerges()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 70);
            var kinds = new string[] { "string" };
            for (int page = 0; page < 50; page++)
            {
                using var batch = Batch(kinds, 8, 3);
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 3, references);
                heap.Release((ulong)references.GetValueAt(0, default).AsLong);
            }
            await heap.CompactAsync();
            long before = GC.GetAllocatedBytesForCurrentThread();

            for (int i = 0; i < 50; i++)
            {
                await heap.CompactAsync();
            }
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            Assert.Equal(0, heap.PagesMerged);
            Assert.Equal(0, allocated);
        }

        [Fact]
        public async Task PagesAppendedAfterTheCheckpointFollowingARestartKeepTheirStoredPages()
        {
            var provider = new MemoryFileProvider();
            ulong[] kept;
            using (var stream = await Open(provider, maxFileSize: 4096))
            {
                using var heap = await stream.Heap(2);
                kept = await Append(heap, Kinds, 0, 300);
                await heap.CheckpointAsync();
                await Append(heap, Kinds, 300, 1500);
                await heap.Client.DrainAsync();
                await stream.Manager.CheckpointAsync();
            }

            ulong[] beforeBarrier;
            ulong[] afterBarrier;
            using (var restarted = await Open(provider, maxFileSize: 4096))
            {
                using var again = await restarted.Heap(2);
                beforeBarrier = await Append(again, Kinds, 1000, 100);
                await again.CheckpointAsync();
                afterBarrier = await Append(again, Kinds, 2000, 1000);
                await again.Client.DrainAsync();
                await restarted.Manager.CheckpointAsync();
                await Checkpoint(restarted, again);
            }

            using var third = await Open(provider, maxFileSize: 4096);
            using var end = await third.Heap(2);
            await AssertReads(end, kept, Enumerable.Range(0, 300), 0);
            await AssertReads(end, beforeBarrier, Enumerable.Range(0, 100), 1000);
            await AssertReads(end, afterBarrier, Enumerable.Range(0, 1000), 2000);
        }

        [Fact]
        public async Task RestoringAHeapThatUsedItsLastPageIdDoesNotDeleteItsIndexTree()
        {
            var provider = new MemoryFileProvider();
            ulong[] first;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2);
                first = await Append(heap, Kinds, 0, 10);
                await Checkpoint(stream, heap);
                var state = await stream.Client.GetOrCreateObjectStateAsync<RowHeapState>("heap_state");
                state.Value!.NextPage = FlowtideDotNet.Storage.StateManager.Internal.Sync.ImmutablePageIds.RangeSize;
                await state.Commit();
                await stream.Manager.CheckpointAsync();
            }

            using (var restarted = await Open(provider))
            {
                using var again = await restarted.Heap(2);
                await again.Client.CommitAsync();
                await restarted.Manager.CheckpointAsync();
            }

            using var third = await Open(provider);
            using var end = await third.Heap(2);
            await AssertReads(end, first, Enumerable.Range(0, 10), 0);
        }

        [Fact]
        public async Task AReaderThatFoundItsPagesBeforeAMergeReadsThemUntilTheSecondCheckpoint()
        {
            using var stream = await Open();
            var gate = new Gate();
            using var heap = await stream.Heap(2, new RowHeapOptions() { PageTargetBytes = 2000, PagesResolvedHookForTests = gate.Pass });
            var references = await Append(heap, Kinds, 0, 2000);
            await Checkpoint(stream, heap);
            var kept = Enumerable.Range(0, 2000).Where(x => x % 4 == 1).ToArray();
            foreach (int row in Enumerable.Range(0, 2000).Except(kept))
            {
                heap.Release(references[row]);
            }
            var oldPages = Enumerable.Range(1, (int)heap.SequenceCount).Select(x => PageId(heap, x)).ToArray();
            using var reader = heap.CreateReader();

            var read = Task.Run(async () => await Gather(reader, kept.Select(x => (ulong?)references[x]).ToArray(), Both));
            Assert.True(gate.Entered.Wait(Limit));
            while (await heap.CompactAsync())
            {
            }
            var newPages = Enumerable.Range(1, oldPages.Length).Select(x => PageId(heap, x)).ToArray();
            await Checkpoint(stream, heap);
            foreach (long page in oldPages)
            {
                stream.Manager.DeleteFromCache(page);
            }
            int storedAfterOne = oldPages.Count(heap.Client.IsStored);
            gate.Release.Set();
            var fetched = await read.WaitAsync(Limit);
            long coldLoads = reader.ColdLoads;
            await Checkpoint(stream, heap);
            int storedAfterTwo = oldPages.Count(heap.Client.IsStored);
            int moved = oldPages.Where((page, index) => page != newPages[index]).Count();

            AssertRows(Kinds, Both, kept.Select(x => (long?)x).ToArray(), fetched);
            Assert.True(moved >= oldPages.Length / 2, $"{moved} of {oldPages.Length} page sequences were moved while the reader waited.");
            Assert.Equal(oldPages.Length, storedAfterOne);
            Assert.Equal(oldPages.Length, coldLoads);
            Assert.Equal(oldPages.Length - moved, storedAfterTwo);
            await AssertReads(heap, references, kept);
        }

        [Fact]
        public async Task AMergedPageIsReadOnceForAllItsRowsInAGather()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await Append(heap, Kinds, 0, 3000);
            int sequences = (int)heap.SequenceCount;
            var kept = new List<int>();
            for (int i = 0; i < references.Length; i++)
            {
                int slot = RowReference.Slot(references[i]);
                if (RowReference.Sequence(references[i]) % 3 == 0 ? slot % 8 == 0 : slot % 2 == 0)
                {
                    kept.Add(i);
                }
                else
                {
                    heap.Release(references[i]);
                }
            }
            while (await heap.CompactAsync())
            {
            }
            await Checkpoint(stream, heap);
            var pages = Enumerable.Range(1, sequences).Select(x => PageId(heap, x)).ToArray();
            int distinct = pages.Distinct().Count();
            bool ordered = pages.Zip(pages.Skip(1), (first, second) => first <= second).All(x => x);
            foreach (long page in pages.Distinct())
            {
                stream.Manager.DeleteFromCache(page);
            }
            using var reader = heap.CreateReader();
            using var asked = References(kept.Select(x => (ulong?)references[x]).ToArray());
            using var numbers = Column.Create(GlobalMemoryManager.Instance);
            var rowOrder = new int[kept.Count];

            await reader.GatherAsync(asked, kept.Count, new int[] { 0 }, new Column[] { numbers }, rowOrder);
            int runs = 1;
            for (int i = 1; i < rowOrder.Length; i++)
            {
                if (PageId(heap, RowReference.Sequence(references[kept[rowOrder[i]]])) != PageId(heap, RowReference.Sequence(references[kept[rowOrder[i - 1]]])))
                {
                    runs++;
                }
            }

            Assert.True(heap.PagesMerged > 0);
            Assert.False(ordered);
            Assert.True(distinct < sequences);
            Assert.Equal(distinct, runs);
            Assert.Equal(distinct, reader.ColdLoads);
            Assert.Equal(1, reader.HeldPages == 0 ? 1 : 0);
            for (int i = 0; i < rowOrder.Length; i++)
            {
                Assert.Equal((long)kept[rowOrder[i]] * 7 - 3, numbers.GetValueAt(i, default).AsLong);
            }
        }

        [Fact]
        public async Task ACallMergesNoMoreThanItsShareAndSaysThatMoreIsLeft()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2, new RowHeapOptions() { PageTargetBytes = 2000, CompactPagesPerCall = 2 });
            var references = await Append(heap, Kinds, 0, 2000);
            long sequences = heap.SequenceCount;
            for (int i = 0; i < references.Length; i += 2)
            {
                heap.Release(references[i]);
            }

            bool more = await heap.CompactAsync();
            bool moreWithItsBudgetUsed = more;
            long readInOneCall = heap.PagesRead;
            await Checkpoint(stream, heap);
            int calls = 1;
            more = true;
            while (more)
            {
                long before = heap.PagesRead;
                more = await heap.CompactAsync();
                readInOneCall = Math.Max(readInOneCall, heap.PagesRead - before);
                calls++;
            }
            bool moreAtTheEnd = await heap.CompactAsync();

            Assert.False(moreWithItsBudgetUsed);
            Assert.Equal(2, readInOneCall);
            Assert.True(calls > 10, $"The pages were merged in {calls} calls.");
            Assert.True(heap.CompactBudget > 0);
            Assert.False(moreAtTheEnd);
            Assert.True(heap.PagesMerged >= sequences / 3, $"{heap.PagesMerged} of {sequences} pages were merged.");
            await AssertReads(heap, references, Enumerable.Range(0, 2000).Where(x => x % 2 == 1));
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            Assert.True(heap.PageCount * 2000 * 125 / 100 >= heap.StoredBytes, $"{heap.PageCount} pages hold {heap.StoredBytes} bytes, more than a quarter above their target.");
        }

        [Fact]
        public async Task ReleasedRowsOfALargePageAreKnownThroughAMergeAndRestarts()
        {
            var provider = new MemoryFileProvider();
            var kinds = new string[] { "int" };
            var column = new int[] { 0 };
            ulong[] references;
            int[] first;
            var released = new List<int>();
            long slotBytes;
            long slotBytesAtTheEnd;
            long rows;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, pageTargetBytes: 4000);
                references = await Append(heap, kinds, 0, 20_000);
                first = RowsOfPage(references, 1);
                for (int i = 0; i < references.Length; i++)
                {
                    if (RowReference.Slot(references[i]) % 3 != 0)
                    {
                        heap.Release(references[i]);
                        released.Add(i);
                    }
                }
                await heap.CompactAsync();
                Assert.True(first.Length > 128, $"The first page has {first.Length} rows.");
                Assert.True(heap.PagesMerged > 0);
                await Checkpoint(stream, heap);
                int lastKept = first.Last(x => RowReference.Slot(references[x]) % 3 == 0);
                heap.Release(references[first[0]]);
                heap.Release(references[lastKept]);
                released.Add(first[0]);
                released.Add(lastKept);
                await Checkpoint(stream, heap);
                slotBytes = heap.ReleasedSlotBytes;
                rows = heap.RowCount;
            }

            using (var restarted = await Open(provider))
            {
                using var again = await restarted.Heap(1, pageTargetBytes: 4000);
                long slotBytesAfter = again.ReleasedSlotBytes;
                int refused = 0;
                foreach (int row in released)
                {
                    var twice = Assert.Throws<InvalidOperationException>(() => again.Release(references[row]));
                    refused += twice.Message.EndsWith("was already released.") ? 1 : 0;
                }
                var kept = Enumerable.Range(0, 20_000).Except(released).ToArray();
                using var reader = again.CreateReader();
                var fetched = await Gather(reader, kept.Select(x => (ulong?)references[x]).ToArray(), column);
                AssertRows(kinds, column, kept.Select(x => (long?)x).ToArray(), fetched);
                foreach (int row in first.Except(released))
                {
                    again.Release(references[row]);
                }
                long entry = again.Directory.Get(1);
                await Checkpoint(restarted, again);
                slotBytesAtTheEnd = again.ReleasedSlotBytes;

                Assert.Equal(slotBytes, slotBytesAfter);
                Assert.Equal(released.Count, refused);
                Assert.Equal(rows, kept.Length);
                Assert.Equal(0, entry);
                Assert.True(slotBytesAtTheEnd < slotBytes);
            }

            using var last = await Open(provider);
            using var end = await last.Heap(1, pageTargetBytes: 4000);
            var gone = Assert.Throws<InvalidOperationException>(() => end.Release(references[first[3]]));

            Assert.Equal(slotBytesAtTheEnd, end.ReleasedSlotBytes);
            Assert.Equal(0, end.Directory.Get(1));
            Assert.Equal(rows - first.Count(x => !released.Contains(x)), end.RowCount);
            Assert.EndsWith("points at no page, the row cannot be released.", gone.Message);
        }

        [Fact]
        public async Task OneCallReadsNoMoreThanItsPagesWhenOneRowIsLeftInEach()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2, new RowHeapOptions() { PageTargetBytes = 2000, CompactPagesPerCall = 3 });
            var references = await Append(heap, Kinds, 0, 6000);
            long sequences = heap.SequenceCount;
            for (int i = 0; i < references.Length; i++)
            {
                if (RowReference.Slot(references[i]) != 0)
                {
                    heap.Release(references[i]);
                }
            }

            await heap.CompactAsync();
            long readBeforeTheCheckpoint = heap.PagesRead;
            await Checkpoint(stream, heap);
            bool more = await heap.CompactAsync();

            Assert.True(sequences > 50, $"{sequences} pages were appended.");
            Assert.Equal(3, readBeforeTheCheckpoint);
            Assert.True(more);
            Assert.Equal(6, heap.PagesRead);
            Assert.InRange(heap.PagesMerged, 2, 6);
        }

        [Fact]
        public async Task AMergeOfSmallPagesStaysAtItsTargetWithItsSlotMap()
        {
            var kinds = new string[] { "int" };
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 2000);
            for (int i = 0; i < 1000; i++)
            {
                await Append(heap, kinds, i, 1);
            }
            await Checkpoint(stream, heap);
            long storedBefore = heap.StoredBytes;

            while (await heap.CompactAsync())
            {
            }
            long mergedPages = heap.PageCount - 1000;
            long mergedBytes = heap.StoredBytes - storedBefore;
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            var all = Enumerable.Range(1, 1000).Select(x => (ulong?)RowReference.Pack(x, 0)).ToArray();
            using var reader = heap.CreateReader();
            var fetched = await Gather(reader, all, new int[] { 0 });

            Assert.True(heap.PagesMerged > 300, $"{heap.PagesMerged} pages were merged.");
            Assert.True(mergedPages >= 5, $"The small pages were merged into {mergedPages}.");
            Assert.True(mergedBytes <= mergedPages * 2030, $"{mergedPages} merged pages hold {mergedBytes} bytes for a target of 2000.");
            Assert.True(mergedBytes >= heap.PagesMerged * 24, $"{mergedBytes} bytes of merged pages do not hold the slot maps of {heap.PagesMerged} page sequences.");
            AssertRows(kinds, new int[] { 0 }, Enumerable.Range(0, 1000).Select(x => (long?)x).ToArray(), fetched);
        }

        [Fact]
        public async Task TwoPagesThatDoNotFitOnePageAreMergedEachOnItsOwn()
        {
            const int target = 32 * 1024;
            using var stream = await Open();
            using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target, MaxStoredRatio = 1.2 });
            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            for (int i = 0; i < 100; i++)
            {
                batch.Columns[0].Add(new StringValue(new string('z', 316)));
            }
            var kept = new List<ulong?>();
            for (int page = 0; page < 24; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 100, references);
                for (int slot = 0; slot < 100; slot++)
                {
                    ulong reference = (ulong)references.GetValueAt(slot, default).AsLong;
                    if (slot < 25)
                    {
                        heap.Release(reference);
                    }
                    else
                    {
                        kept.Add(reference);
                    }
                }
            }
            await Checkpoint(stream, heap);
            long pagesBefore = heap.PageCount;
            long storedBefore = heap.StoredBytes;

            while (await heap.CompactAsync())
            {
            }
            long mergedPages = heap.PageCount - pagesBefore;
            long mergedBytes = heap.StoredBytes - storedBefore;
            using var reader = heap.CreateReader();
            var fetched = await Gather(reader, kept.ToArray(), new int[] { 0 });
            using var result = fetched[0];

            Assert.Equal(24, pagesBefore);
            Assert.True(heap.PagesMerged >= 4, $"{heap.PagesMerged} pages were merged.");
            Assert.Equal(heap.PagesMerged, mergedPages);
            Assert.True(mergedBytes <= mergedPages * (target + target / 4), $"{mergedPages} merged pages hold {mergedBytes} bytes for a target of {target}.");
            Assert.Equal(kept.Count, result.Count);
        }

        [Fact]
        public async Task AThirdSmallPageThatPassesTheRowCapIsNotReadForTheMerge()
        {
            var kinds = new string[] { "int" };
            using var stream = await Open();
            using var heap = await stream.Heap(1, pageTargetBytes: 320_000);
            for (int page = 0; page < 4; page++)
            {
                await Append(heap, kinds, page * 30_000L, 30_000);
            }
            long pagesBefore = heap.PageCount;
            long storedBefore = heap.StoredBytes;

            await heap.CompactAsync();

            Assert.Equal(4, pagesBefore);
            Assert.True(storedBefore >= 320_000 && storedBefore / 2 < 320_000, $"Four pages of 30,000 rows are {storedBefore} bytes.");
            Assert.Equal(2, heap.PagesMerged);
            Assert.Equal(2, heap.PagesRead);
        }

        [Fact]
        public async Task ARestoredHeapMayReadWhatItFindsDue()
        {
            const int target = 32 * 1024;
            var one = new string[] { "wide" };
            var provider = new MemoryFileProvider();
            long pageBytes;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target, CompactPagesPerCall = 8 });
                for (int i = 0; i < 5; i++)
                {
                    await Append(heap, one, i, 1);
                }
                long smallBytes = heap.StoredBytes;
                using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
                for (int i = 0; i < 100; i++)
                {
                    batch.Columns[0].Add(new StringValue(new string('z', 200)));
                }
                for (int page = 0; page < 20; page++)
                {
                    using var references = Column.Create(GlobalMemoryManager.Instance);
                    await heap.AppendAsync(batch, 0, 100, references);
                    for (int slot = 0; slot < 60; slot++)
                    {
                        heap.Release((ulong)references.GetValueAt(slot, default).AsLong);
                    }
                }
                pageBytes = (heap.StoredBytes - smallBytes) / 20;
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, new RowHeapOptions() { PageTargetBytes = target, CompactPagesPerCall = 8 });
            double excess = again.StoredBytes - again.LiveBytes * 1.5;
            int due = 5 + (int)Math.Ceiling(excess * 25 / again.StoredBytes * 1.5 / (1.5 - 1));

            Assert.True(pageBytes > target / 2 && pageBytes < target, $"A page is {pageBytes} bytes.");
            Assert.True(due >= 5 + 12, $"{due} pages are due for {excess} bytes above the ratio.");
            Assert.Equal(8 + due, again.CompactBudget);
        }

        [Fact]
        public async Task ThePagesOfAMergeThatFailedAreMergedLater()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2);
            var references = await Append(heap, Kinds, 0, 2000);
            int sequences = (int)heap.SequenceCount;
            var kept = Enumerable.Range(0, 2000).Where(x => x % 20 == 1).ToArray();
            foreach (int row in Enumerable.Range(0, 2000).Except(kept))
            {
                heap.Release(references[row]);
            }
            long nextPageId = heap.Client.NextPageId;
            heap.Client.NextPageId = heap.Client.LastReservedPageId + 1;
            int budgetBefore = heap.CompactBudget;

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await heap.CompactAsync());
            long mergedByTheFailedCall = heap.PagesMerged;
            long readByTheFailedCall = heap.PagesRead;
            int budgetAfter = heap.CompactBudget;
            heap.Client.NextPageId = nextPageId;
            while (await heap.CompactAsync())
            {
            }
            int notMoved = Enumerable.Range(1, sequences).Count(x => heap.Directory.Get(x) != 0 && PageId(heap, x) < nextPageId);

            Assert.Equal(0, mergedByTheFailedCall);
            Assert.True(readByTheFailedCall >= 2, $"The call that failed read {readByTheFailedCall} pages.");
            Assert.Equal(readByTheFailedCall, budgetBefore - budgetAfter);
            Assert.True(notMoved <= 2, $"{notMoved} of {sequences} pages were not merged after the call that failed.");
            await AssertReads(heap, references, kept);
        }

        [Fact]
        public async Task MergedPagesAreNotMergedAsSmallPagesAfterARestart()
        {
            var provider = new MemoryFileProvider();
            ulong[] references;
            var kept = new List<int>();
            long merged;
            long mergedPages;
            long rowsMerged;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(2, new RowHeapOptions() { PageTargetBytes = 2000, CompactPagesPerCall = 2 });
                references = await Append(heap, Kinds, 0, 4000);
                for (int i = 0; i < references.Length; i++)
                {
                    if (RowReference.Slot(references[i]) == 0)
                    {
                        kept.Add(i);
                    }
                    else
                    {
                        heap.Release(references[i]);
                    }
                }
                long before = heap.PageCount;
                await Checkpoint(stream, heap);
                while (await heap.CompactAsync())
                {
                }
                merged = heap.PagesMerged;
                mergedPages = heap.PageCount - before;
                rowsMerged = heap.RowsMerged;
                await Checkpoint(stream, heap);
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(2);
            long liveBytes = again.LiveBytes;
            bool more = await again.CompactAsync();

            Assert.True(mergedPages >= 20, $"{merged} pages were merged into {mergedPages}.");
            Assert.True(rowsMerged <= kept.Count, $"{rowsMerged} rows were copied for {kept.Count} rows that were left.");
            Assert.True(liveBytes >= 2000, $"The merged pages hold {liveBytes} bytes.");
            Assert.False(more);
            Assert.Equal(0, again.PagesMerged);
            await AssertReads(again, references, kept);
        }

        [Fact]
        public async Task ABurstOfReleasesIsMergedAtTheRateTheCheckpointsBeforeItSaw()
        {
            using var stream = await Open();
            using var heap = await stream.Heap(2, new RowHeapOptions() { PageTargetBytes = 2000, CompactPagesPerCall = 4 });
            var references = await Append(heap, Kinds, 0, 6000);
            int sequences = (int)heap.SequenceCount;
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            int budgetAtRest = heap.CompactBudget;
            long storedBefore = heap.StoredBytes;
            var kept = Enumerable.Range(0, 6000).Where(x => x % 20 == 1).ToArray();
            foreach (int row in Enumerable.Range(0, 6000).Except(kept))
            {
                heap.Release(references[row]);
            }

            while (await heap.CompactAsync())
            {
            }
            long readInTheBurst = heap.PagesRead;
            int budgetLeft = heap.CompactBudget;
            bool moreWithoutABudget = await heap.CompactAsync();
            await Checkpoint(stream, heap);
            int budgetAfter = heap.CompactBudget;
            while (await heap.CompactAsync())
            {
            }
            long readAfter = heap.PagesRead - readInTheBurst;
            int notMoved = Enumerable.Range(1, sequences).Count(x => heap.Directory.Get(x) != 0 && PageId(heap, x) < heap.Client.FirstPageId + sequences);

            Assert.Equal(4, budgetAtRest);
            Assert.Equal(4, readInTheBurst);
            Assert.Equal(0, budgetLeft);
            Assert.False(moreWithoutABudget);
            Assert.True(budgetAfter >= 3 * storedBefore * 9 / 10 / 2000, $"The checkpoint after the burst lets {budgetAfter} pages be read for {storedBefore} bytes that were stored.");
            Assert.True(readAfter > sequences / 2, $"{readAfter} pages were read after the checkpoint.");
            Assert.True(notMoved <= 2, $"{notMoved} of {sequences} pages were not merged.");
            await AssertReads(heap, references, kept);
        }

        [Fact]
        public async Task PagesOfRowsOfOneSizeAreMergedAfterARestart()
        {
            var provider = new MemoryFileProvider();
            var kinds = new string[] { "wide" };
            var column = new int[] { 0 };
            ulong[] references;
            using (var stream = await Open(provider))
            {
                using var heap = await stream.Heap(1, pageTargetBytes: 32 * 1024);
                references = await Append(heap, kinds, 0, 4000);
                for (int i = 0; i < references.Length; i++)
                {
                    if (i % 4 != 1)
                    {
                        heap.Release(references[i]);
                    }
                }
                await Checkpoint(stream, heap);
            }

            using var restarted = await Open(provider);
            using var again = await restarted.Heap(1, pageTargetBytes: 32 * 1024);
            long sequences = again.SequenceCount;
            long waiting = again.MergeQueueBytes / sizeof(long);
            await again.CompactAsync();
            var kept = Enumerable.Range(0, 4000).Where(x => x % 4 == 1).ToArray();
            using var reader = again.CreateReader();
            var fetched = await Gather(reader, kept.Select(x => (ulong?)references[x]).ToArray(), column);

            Assert.InRange(waiting, sequences - 1, sequences);
            Assert.Equal(again.PagesMerged, again.PagesRead);
            Assert.True(again.PagesMerged >= sequences / 2, $"{again.PagesMerged} of {sequences} pages were merged.");
            AssertRows(kinds, column, kept.Select(x => (long?)x).ToArray(), fetched);
        }

        [Fact]
        public async Task ACheckpointSetsTheBudgetFromThePagesThatCameToNeedARead()
        {
            const int target = 32 * 1024;
            var one = new string[] { "wide" };
            using var stream = await Open();
            using var heap = await stream.Heap(1, new RowHeapOptions() { PageTargetBytes = target, CompactPagesPerCall = 8 });
            int atTheStart = heap.CompactBudget;
            for (int i = 0; i < 5; i++)
            {
                await Append(heap, one, i, 1);
            }
            await Checkpoint(stream, heap);
            int afterSmallPages = heap.CompactBudget;

            using var batch = new EventBatchData(new IColumn[] { Column.Create(GlobalMemoryManager.Instance) });
            batch.Columns[0].Add(new StringValue(new string('z', 20_000)));
            for (int i = 0; i < 5; i++)
            {
                batch.Columns[0].Add(new StringValue(new string('y', 100)));
            }
            var smallRows = new List<ulong>();
            long storedBefore = heap.StoredBytes;
            for (int page = 0; page < 6; page++)
            {
                using var references = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(batch, 0, 6, references);
                smallRows.Add((ulong)references.GetValueAt(1, default).AsLong);
            }
            long pageBytes = (heap.StoredBytes - storedBefore) / 6;
            await Checkpoint(stream, heap);
            await Checkpoint(stream, heap);
            int atRest = heap.CompactBudget;
            long liveBefore = heap.LiveBytes;
            foreach (ulong reference in smallRows)
            {
                heap.Release(reference);
            }
            long released = liveBefore - heap.LiveBytes;
            await Checkpoint(stream, heap);
            int afterReleases = heap.CompactBudget;
            await Checkpoint(stream, heap);
            int oneCheckpointLater = heap.CompactBudget;
            await Checkpoint(stream, heap);
            int twoCheckpointsLater = heap.CompactBudget;

            Assert.Equal(8, atTheStart);
            Assert.Equal(8 + 5, afterSmallPages);
            Assert.Equal(8, atRest);
            Assert.True(released > 6 * 3000, $"The six rows were taken as {released} bytes.");
            Assert.Equal(8 + 6 + (int)Math.Ceiling(3.0 * released / pageBytes), afterReleases);
            Assert.Equal(afterReleases, oneCheckpointLater);
            Assert.Equal(8, twoCheckpointsLater);
        }
    }
}

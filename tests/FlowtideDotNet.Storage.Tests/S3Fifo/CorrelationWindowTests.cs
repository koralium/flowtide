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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using Xunit;

namespace FlowtideDotNet.Storage.Tests.S3Fifo
{
    /// <summary>
    /// Tests for the correlation window on the small queue.
    /// A hit while an entry is young in the small queue must not count toward promotion.
    /// MaxSize 100 gives small target 10, window 5 and cleanup threshold 70.
    /// </summary>
    public class CorrelationWindowTests
    {
        private class FixedMemoryStats : IMemoryAllocationStats
        {
            public long AllocatedMemory { get; set; }

            public long GetAllocatedMemory() => AllocatedMemory;
        }

        private static void Touch(S3FifoTableSync table, long key, int times)
        {
            for (var i = 0; i < times; i++)
            {
                Assert.True(table.TryGetValue(key, out var cacheObject));
                cacheObject!.Return();
            }
        }

        [Fact]
        public async Task WindowIsHalfTheSmallQueueTarget()
        {
            using var large = await S3FifoTestHelpers.CreateStoppedTable(100);
            Assert.Equal(5, large.CorrelationWindowSizeForTests);

            // MaxSize below 20 gives window 0, so the filter is disabled.
            // This keeps small caches and the CachePageCount 0 config on the old behavior.
            using var tiny = await S3FifoTestHelpers.CreateStoppedTable(10);
            Assert.Equal(0, tiny.CorrelationWindowSizeForTests);
        }

        [Fact]
        public async Task CorrelatedHitsDoNotCountTowardPromotion()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100);
            var handler = new TestEvictHandler();

            // Insert and immediately touch twice, both hits are inside the window.
            // They are correlated and must not earn promotion.
            var obj = new TestCacheObject(0);
            table.Add(0, obj, handler);
            Touch(table, 0, 2);

            for (var i = 1; i <= 100; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }

            await table.ForceCleanup();

            // Key 0 reached the small queue head with frequency 0, so it goes to ghost
            // instead of being promoted to main.
            Assert.False(table.TryGetValue(0, out _));
            Assert.True(table.IsInGhostForTests(0));
            Assert.True(obj.Disposed);
        }

        [Fact]
        public async Task HitsAfterAgingPastTheWindowStillPromote()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100);
            var handler = new TestEvictHandler();

            table.Add(0, new TestCacheObject(0), handler);

            // Age key 0 past the window with 6 newer enqueues.
            for (var i = 1; i <= 6; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }

            // These hits are outside the window and count.
            Touch(table, 0, 2);

            for (var i = 7; i <= 100; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }

            await table.ForceCleanup();

            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
            Assert.False(table.IsInGhostForTests(0));
        }

        /// <summary>
        /// The scenario the window exists for.
        /// A scan whose pages are each touched in a burst must not displace the reused set.
        /// </summary>
        [Fact]
        public async Task CorrelatedScanDoesNotPolluteMainQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100);
            var handler = new TestEvictHandler();

            // Hot working set, inserted, aged past the window, then reused.
            for (var i = 0; i < 5; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            for (var i = 5; i < 10; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            for (var i = 0; i < 5; i++)
            {
                Touch(table, i, 2);
            }

            // Scan, every page touched twice right after insert.
            for (var i = 10; i < 100; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
                Touch(table, i, 2);
            }

            await table.ForceCleanup();

            // Only the hot set reached main. The scan touches earned nothing, so the main
            // queue holds exactly the 5 hot entries.
            for (var i = 0; i < 5; i++)
            {
                Assert.True(table.TryPeekEntryForTests(i, out var entry));
                Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
            }
            var counts = table.GetQueueCountsForTests();
            Assert.Equal(5, counts.MainCount);
        }

        /// <summary>
        /// The memory-adaptive resize recomputes maxSize from the allocation per entry.
        /// The window follows maxSize on both a shrink and a growth.
        /// MaxSize 1000 gives window 50, the shrink drops it to 10 and the growth raises it to 20.
        /// </summary>
        [Fact]
        public async Task MemoryAdaptiveResizeUpdatesTheWindowInBothDirections()
        {
            var stats = new FixedMemoryStats();
            using var table = new S3FifoTableSync(new CacheTableOptions("", NullLogger.Instance, new Meter(Guid.NewGuid().ToString()), stats)
            {
                MaxSize = 1000,
                MinSize = 0,
                MaxMemoryUsageInBytes = 8 * 1024 * 1024
            });
            await table.StopCleanupTask();
            var handler = new TestEvictHandler();

            Assert.Equal(50, table.CorrelationWindowSizeForTests);

            // Shrink, entries report as large so the budget fits fewer pages.
            long key = 0;
            for (; key < 701; key++)
            {
                table.Add(key, new TestCacheObject(key), handler);
            }
            stats.AllocatedMemory = 701 * 32 * 1024;
            await table.ForceCleanup();
            Assert.Equal(10, table.CorrelationWindowSizeForTests);

            // Growth, entries report as small so the budget fits more pages and the window
            // widens with the small queue target.
            while (table.Count < 200)
            {
                table.Add(key++, new TestCacheObject(key), handler);
            }
            stats.AllocatedMemory = 200 * 16 * 1024;
            await table.ForceCleanup();
            Assert.Equal(20, table.CorrelationWindowSizeForTests);
        }

        /// <summary>
        /// Small-queue-head outcome counters.
        /// A correlated scan earns no promotions, so the head objects go to ghost.
        /// </summary>
        [Fact]
        public async Task CountersReportSmallQueueOutcomes()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100);
            var handler = new TestEvictHandler();

            for (var i = 0; i < 100; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
                Touch(table, i, 2); // both touches are inside the window
            }

            Assert.Equal(0, table.SmallQueuePromotionsForTests);
            Assert.Equal(0, table.SmallQueueEvictionsForTests);

            await table.ForceCleanup();

            // Nothing earned promotion, the head objects went to ghost.
            Assert.Equal(0, table.SmallQueuePromotionsForTests);
            Assert.True(table.SmallQueueEvictionsForTests > 0);
        }

        /// <summary>
        /// The one-hit-wonder counter.
        /// Objects added once, never promoted, then aged out of ghost without a re-admission.
        /// </summary>
        [Fact]
        public async Task OneHitWonderCounterCountsObjectsThatAgeOutOfGhostUnused()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();

            // Ghost capacity is 9. Churn distinct never-touched keys so each cleanup evicts the
            // oldest small entries to ghost. Once more than 9 are ghosted the earliest age out.
            long key = 0;
            for (var round = 0; round < 10; round++)
            {
                while (table.Count < 10)
                {
                    table.Add(key, new TestCacheObject(key), handler);
                    key++;
                }
                await table.ForceCleanup();
            }

            Assert.Equal(0, table.SmallQueuePromotionsForTests);
            Assert.True(table.OneHitWondersForTests > 0, $"expected one-hit-wonders, got {table.OneHitWondersForTests}");
        }

        /// <summary>
        /// Guards the fallback.
        /// With the window disabled immediate double touches still promote.
        /// </summary>
        [Fact]
        public async Task TinyCachesKeepPlainS3FifoCounting()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();

            table.Add(0, new TestCacheObject(0), handler);
            Touch(table, 0, 2);

            for (var i = 1; i < 10; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }

            await table.ForceCleanup();

            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
        }
    }
}

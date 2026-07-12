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
    /// Tests for the 2Q-style correlation window on the small queue: hits while an entry is
    /// still in the young half of the small queue are correlated references (same logical
    /// operation as the insert) and must not count toward promotion to the main queue.
    ///
    /// Size math: MaxSize = 100 gives a small queue target of 10, a correlation window of
    /// 10 / 2 = 5 small-queue enqueues, and a cleanup threshold of ceil(100 * 0.7) = 70, so a
    /// cleanup at 101 entries evicts 31.
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

            // MaxSize < 20 => window 0 => the filter is disabled (plain S3-FIFO counting),
            // which keeps small caches and the CachePageCount = 0 torture configuration on
            // the exact previous behavior.
            using var tiny = await S3FifoTestHelpers.CreateStoppedTable(10);
            Assert.Equal(0, tiny.CorrelationWindowSizeForTests);
        }

        [Fact]
        public async Task CorrelatedHitsDoNotCountTowardPromotion()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100);
            var handler = new TestEvictHandler();

            // Insert and immediately touch twice: age 0 < window 5, so both hits are
            // correlated references and must not earn promotion.
            var obj = new TestCacheObject(0);
            table.Add(0, obj, handler);
            Touch(table, 0, 2);

            for (var i = 1; i <= 100; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }

            await table.ForceCleanup();

            // Key 0 was at the head of the small queue with an (uncounted) frequency of 0,
            // so it is evicted to the ghost queue instead of being promoted to main.
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

            // Age key 0 past the window: 6 newer small-queue enqueues >= window 5.
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
        /// The scenario the window exists for: a one-pass scan whose pages are each touched
        /// several times in a burst (a B+ tree operation reads and updates the same page
        /// within one logical operation) must not displace the genuinely reused working set
        /// from the main queue.
        /// </summary>
        [Fact]
        public async Task CorrelatedScanDoesNotPolluteMainQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100);
            var handler = new TestEvictHandler();

            // Hot working set: inserted, aged past the window, then genuinely reused.
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

            // Scan: every page touched twice immediately after insert (correlated burst).
            for (var i = 10; i < 100; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
                Touch(table, i, 2);
            }

            await table.ForceCleanup();

            // Only the hot set earned main-queue residency; the scan's correlated double
            // touches earned nothing, so the main queue holds exactly the 5 hot entries.
            for (var i = 0; i < 5; i++)
            {
                Assert.True(table.TryPeekEntryForTests(i, out var entry));
                Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
            }
            var counts = table.GetQueueCountsForTests();
            Assert.Equal(5, counts.MainCount);
        }

        /// <summary>
        /// The memory-adaptive resize (MaxMemoryUsageInBytes) recomputes maxSize from the
        /// observed allocation per entry; the small queue target follows maxSize, so the
        /// correlation window must follow both a shrink (RAM pressure) and a growth
        /// (entries smaller than expected).
        ///
        /// Size math: MaxSize = 1000 gives window 1000/10/2 = 50 and cleanup threshold 700.
        /// Shrink pass: 701 entries at a reported 32 KiB each against an 8 MiB budget gives
        /// idealMaxSize = floor(8 MiB * 0.8 / 32 KiB) = 204, so the window becomes
        /// 204/10/2 = 10. Growth pass: the average clamps at the 16 KiB floor, giving
        /// idealMaxSize = floor(8 MiB * 0.8 / 16 KiB) = 409 and window 409/10/2 = 20.
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

            // Shrink: entries report as large, so the same memory budget fits fewer pages.
            long key = 0;
            for (; key < 701; key++)
            {
                table.Add(key, new TestCacheObject(key), handler);
            }
            stats.AllocatedMemory = 701 * 32 * 1024;
            await table.ForceCleanup();
            Assert.Equal(10, table.CorrelationWindowSizeForTests);

            // Growth: entries report as small (clamped at the 16 KiB floor), so the budget
            // fits more pages and the window widens with the small queue target.
            while (table.Count < 200)
            {
                table.Add(key++, new TestCacheObject(key), handler);
            }
            stats.AllocatedMemory = 200 * 16 * 1024;
            await table.ForceCleanup();
            Assert.Equal(20, table.CorrelationWindowSizeForTests);
        }

        /// <summary>
        /// The observability counters: a correlated scan suppresses hits (they never count
        /// toward promotion) and the objects that reach the small-queue head are evicted to
        /// ghost rather than promoted to main.
        /// </summary>
        [Fact]
        public async Task CountersReportSuppressedHitsAndSmallQueueOutcomes()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100);
            var handler = new TestEvictHandler();

            for (var i = 0; i < 100; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
                Touch(table, i, 2); // both touches are inside the window (age 0)
            }

            // Every scan touch was correlated, so it was suppressed instead of counted.
            Assert.Equal(200, table.CorrelatedHitsSuppressedForTests);
            Assert.Equal(0, table.SmallQueuePromotionsForTests);
            Assert.Equal(0, table.SmallQueueEvictionsForTests);

            await table.ForceCleanup();

            // Nothing earned promotion; the small-queue-head objects went to ghost.
            Assert.Equal(0, table.SmallQueuePromotionsForTests);
            Assert.True(table.SmallQueueEvictionsForTests > 0);
        }

        /// <summary>
        /// Guards the fallback: with the window disabled (tiny cache) immediate double
        /// touches still promote, i.e. the exact pre-window S3-FIFO behavior.
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

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

using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;

namespace FlowtideDotNet.Storage.Tests.S3Fifo
{
    public class S3FifoStaleSlotTests
    {
        private const int Cache = 64;
        private const int WorkingSet = 66;
        private const int ReadsPerPass = 22;
        private const int ChurnPerPass = 8;
        private const int CompactionMinimumStaleCount = 1024;

        private sealed record PassSample(
            int CountBefore,
            int CleanupStart,
            int SmallStaleBefore,
            int MainStaleBefore,
            long SmallEvictions,
            long SmallPromotions,
            int SmallSlots,
            int SmallStale,
            int MainSlots,
            int MainStale,
            int GhostRecords,
            int GhostKeys,
            int SmallCapacity,
            int MainCapacity,
            int GhostCapacity)
        {
            public int Stale => SmallStale + MainStale;

            public int Live => SmallSlots + MainSlots - Stale;

            public bool Evicting => CountBefore > CleanupStart;

            // Main first through either trigger, the small queue is never scanned
            public bool MainFirst => SmallEvictions == 0 && SmallPromotions == 0;
        }

        /// <summary>
        /// A reader cycles a working set larger than the cache while a churner inserts and deletes around one live page.
        /// </summary>
        private static async Task<(List<PassSample> Samples, int Deletes)> RunMixedChurn(string name, bool ephemeralChurner, int passes)
        {
            using var harness = await EphemeralHarness.Create(name, cachePageCount: Cache);
            var table = harness.Manager.CacheTable;
            var reader = await EphemeralStateClientTests.CreateClient(harness, "reader", ephemeral: false);
            var churner = await EphemeralStateClientTests.CreateClient(harness, "churner", ephemeralChurner);

            // The reader's re-reads come back through ghost hits into main.
            var readerKeys = new List<long>();
            for (int i = 0; i < WorkingSet; i++)
            {
                var key = reader.GetNewPageId();
                reader.AddOrUpdate(key, new TestPage(i));
                readerKeys.Add(key);
                if (i % 8 == 0)
                {
                    await table.ForceCleanup();
                }
            }
            var liveKey = churner.GetNewPageId();
            churner.AddOrUpdate(liveKey, new TestPage(0));

            var samples = new List<PassSample>(passes);
            var cursor = 0;
            var deletes = 0;
            for (int p = 0; p < passes; p++)
            {
                for (int r = 0; r < ReadsPerPass; r++)
                {
                    var page = await reader.GetValue(readerKeys[cursor]);
                    Assert.NotNull(page);
                    Assert.Equal(cursor, page!.Value);
                    page.Return();
                    cursor = (cursor + 1) % readerKeys.Count;
                }
                for (int c = 0; c < ChurnPerPass; c++)
                {
                    var key = churner.GetNewPageId();
                    churner.AddOrUpdate(key, new TestPage(c));
                    churner.Delete(liveKey);
                    deletes++;
                    liveKey = key;
                }

                var countBefore = table.Count;
                var shapeBefore = table.GetQueueShapeForTests();
                var countsBefore = table.GetQueueCountsForTests();
                var evictionsBefore = table.SmallQueueEvictionsForTests;
                var promotionsBefore = table.SmallQueuePromotionsForTests;
                await table.ForceCleanup();
                var counts = table.GetQueueCountsForTests();
                var shape = table.GetQueueShapeForTests();
                samples.Add(new PassSample(
                    countBefore,
                    shapeBefore.CleanupStart,
                    countsBefore.SmallStale,
                    countsBefore.MainStale,
                    table.SmallQueueEvictionsForTests - evictionsBefore,
                    table.SmallQueuePromotionsForTests - promotionsBefore,
                    counts.SmallCount,
                    counts.SmallStale,
                    counts.MainCount,
                    counts.MainStale,
                    counts.GhostCount,
                    shape.GhostKeys,
                    shape.SmallCapacity,
                    shape.MainCapacity,
                    shape.GhostCapacity));
            }
            return (samples, deletes);
        }

        private static void AssertBoundedTrend<T>(List<T> samples, int deletes, string quantity, Func<T, int> select, bool nonVacuity)
        {
            var passes = samples.Count;
            var early = samples.Skip(passes / 10).Take(passes / 10).Max(select);
            var late = samples.Skip(passes - passes / 10).Max(select);
            var bound = 1.25 * early + 1024;
            Assert.True(late <= bound, $"{quantity} grew from a peak of {early} to {late} over {deletes} deletes, the bound is {bound}");
            if (nonVacuity)
            {
                // One retained slot per delete would break the bound twofold.
                Assert.True(deletes >= 2 * bound, $"{deletes} deletes cannot break the {quantity} bound {bound} twofold");
            }
        }

        [Fact]
        public async Task StaleSmallSlotsStayBoundedUnderSustainedEvictionPressure()
        {
            var (samples, deletes) = await RunMixedChurn("stale_slot_repro", ephemeralChurner: false, passes: 3000);

            Assert.All(samples, s => Assert.True(s.Evicting, "a pass did not start above cleanupStart"));
            for (int p = 0; p < samples.Count; p++)
            {
                var sample = samples[p];
                // Live never exceeds the threshold here, so the compaction check caps stale at it plus the deletes since.
                var bound = Math.Max(CompactionMinimumStaleCount, sample.Live) + ChurnPerPass;
                Assert.True(sample.Stale <= bound, $"pass {p}: {sample.Stale} stale slots, the bound is {bound}");
            }
            AssertBoundedTrend(samples, deletes, "small stale slots", s => s.SmallStale, nonVacuity: true);
        }

        [Fact]
        public async Task SharedQueuesStayBoundedUnderMixedEphemeralChurn()
        {
            var (samples, deletes) = await RunMixedChurn("g1", ephemeralChurner: true, passes: 3000);

            var measured = samples.Skip(samples.Count / 10).ToList();
            Assert.True(measured.Count >= 200);
            for (int p = 0; p < measured.Count; p++)
            {
                Assert.True(measured[p].Evicting, $"measured pass {p} did not start above cleanupStart");
                Assert.True(measured[p].MainFirst, $"measured pass {p} scanned the small queue, {measured[p].SmallEvictions} evictions and {measured[p].SmallPromotions} promotions");
            }

            AssertBoundedTrend(samples, deletes, "small stale slots", s => s.SmallStale, nonVacuity: true);
            // No main-resident page is deleted here, main tombstones are reaped by the main scans.
            AssertBoundedTrend(samples, deletes, "main stale slots", s => s.MainStale, nonVacuity: false);
            AssertBoundedTrend(samples, deletes, "ghost records", s => s.GhostRecords, nonVacuity: true);
            AssertBoundedTrend(samples, deletes, "ghost keys", s => s.GhostKeys, nonVacuity: true);
            AssertBoundedTrend(samples, deletes, "small queue capacity", s => s.SmallCapacity, nonVacuity: false);
            AssertBoundedTrend(samples, deletes, "main queue capacity", s => s.MainCapacity, nonVacuity: false);
            AssertBoundedTrend(samples, deletes, "ghost queue capacity", s => s.GhostCapacity, nonVacuity: false);
        }

        private sealed class AcceptAllHandler : ICacheEvictHandler
        {
            public int LastVictimCount;

            public Task<int> Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
            {
                LastVictimCount = valuesToEvict.Count;
                return Task.FromResult(valuesToEvict.Count);
            }
        }

        [Fact]
        public async Task GhostsStayBoundedWhenHeldPagesExhaustTheTrimBudget()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(32);
            var handler = new AcceptAllHandler();
            var cleanupStart = table.GetQueueShapeForTests().CleanupStart;
            // 255 held victims per pass are requeued first and spend all but the one unit the ghost insert takes.
            var held = new List<TestCacheObject>();
            long nextKey = 0;
            for (int i = 0; i < cleanupStart + 255; i++)
            {
                var page = new TestCacheObject(nextKey);
                Assert.True(page.TryRent());
                held.Add(page);
                table.Add(nextKey++, page, handler);
            }
            var samples = new List<(int Keys, int Records)>();
            try
            {
                for (int churn = 0; churn < 3000; churn++)
                {
                    var fresh = new TestCacheObject(nextKey);
                    table.Add(nextKey++, fresh, handler);
                    for (int pass = 0; !fresh.RemovedFromCache && pass < 4; pass++)
                    {
                        await table.ForceCleanup();
                        Assert.Equal(256, handler.LastVictimCount);
                    }
                    Assert.True(fresh.RemovedFromCache);
                    Assert.Equal(held.Count, table.Count);
                    samples.Add((table.GetQueueShapeForTests().GhostKeys, table.GetQueueCountsForTests().GhostCount));
                }
            }
            finally
            {
                foreach (var page in held)
                {
                    page.Return();
                }
            }
            AssertBoundedTrend(samples, 3000, "ghost keys", s => s.Keys, nonVacuity: true);
            AssertBoundedTrend(samples, 3000, "ghost records", s => s.Records, nonVacuity: true);
        }

        [Fact]
        public async Task SharedGhostsStayBoundedWhenOrdinaryClientsHoldPages()
        {
            using var harness = await EphemeralHarness.Create("ghost_held_pages", cachePageCount: 32);
            var table = harness.Manager.CacheTable;
            var churner = await EphemeralStateClientTests.CreateClient(harness, "churner", ephemeral: true);
            // Ordinary clients hold pages within their MaxHeldPages allowance across every pass.
            var held = new List<TestPage>();
            var heldCount = table.GetQueueShapeForTests().CleanupStart + 255;
            var ordinaryClientCount = 0;
            while (held.Count < heldCount)
            {
                var ordinary = await EphemeralStateClientTests.CreateClient(harness, $"ordinary_{ordinaryClientCount++}", ephemeral: false);
                var count = Math.Min(heldCount - held.Count, ordinary.MaxHeldPages);
                Assert.InRange(count, 1, ordinary.MaxHeldPages);
                for (int i = 0; i < count; i++)
                {
                    var key = ordinary.GetNewPageId();
                    ordinary.AddOrUpdate(key, new TestPage(held.Count));
                    held.Add((await ordinary.GetValue(key))!);
                }
            }
            var samples = new List<(int Keys, int Records)>();
            try
            {
                for (int churn = 0; churn < 3000; churn++)
                {
                    var key = churner.GetNewPageId();
                    var fresh = new TestPage(churn);
                    churner.AddOrUpdate(key, fresh);
                    for (int pass = 0; !fresh.RemovedFromCache && pass < 4; pass++)
                    {
                        await table.ForceCleanup();
                    }
                    Assert.True(fresh.RemovedFromCache);
                    churner.Delete(key);
                    Assert.Equal(heldCount, table.Count);
                    Assert.Empty(EphemeralStateClientTests.Sync(churner).ModifiedKeysForTests);
                    Assert.Empty(EphemeralStateClientTests.Sync(churner).SpillVersionKeysForTests);
                    samples.Add((table.GetQueueShapeForTests().GhostKeys, table.GetQueueCountsForTests().GhostCount));
                }
            }
            finally
            {
                foreach (var page in held)
                {
                    page.Return();
                }
            }
            // Inspected after every rent is returned, eventual trimming cannot hide the growth.
            Assert.All(held, page => Assert.Equal(1, page.RentCount));
            Assert.Equal(samples[^1].Keys, table.GetQueueShapeForTests().GhostKeys);
            AssertBoundedTrend(samples, 3000, "ghost keys", s => s.Keys, nonVacuity: true);
            AssertBoundedTrend(samples, 3000, "ghost records", s => s.Records, nonVacuity: true);
        }

        [Fact]
        public async Task EvictingPassBelowTheThresholdDoesNotCompact()
        {
            // 127 passes of 8 deletes end just under the 1024 floor.
            var (samples, _) = await RunMixedChurn("stale_slot_threshold", ephemeralChurner: false, passes: (CompactionMinimumStaleCount - 1) / ChurnPerPass);

            var checkedPasses = 0;
            foreach (var sample in samples)
            {
                if (sample.Evicting && sample.MainFirst && sample.SmallStaleBefore > 0 && sample.SmallStaleBefore + sample.MainStaleBefore < CompactionMinimumStaleCount)
                {
                    Assert.Equal(sample.SmallStaleBefore, sample.SmallStale);
                    checkedPasses++;
                }
            }
            Assert.True(checkedPasses >= 20, $"only {checkedPasses} main first passes below the threshold");
            Assert.True(samples.Max(s => s.SmallStaleBefore + s.MainStaleBefore) > CompactionMinimumStaleCount - 2 * ChurnPerPass, "the run did not reach the floor");
        }

        [Fact]
        public async Task EvictingPassKeepsStaleSlotsBelowTheLiveCount()
        {
            // Past the 1024 floor but under the live count, so the evicting pass must not walk the queues.
            using var table = await S3FifoTestHelpers.CreateStoppedTable(4000);
            var handler = new TestEvictHandler();
            long key = 0;
            for (; key < 3000; key++)
            {
                table.Add(key, new TestCacheObject(key), handler);
            }
            for (int i = 0; i < 1500; i++, key++)
            {
                table.Add(key, new TestCacheObject(key), handler);
                table.Delete(key);
            }
            Assert.True(table.Count > table.GetQueueShapeForTests().CleanupStart);
            Assert.Equal(1500, table.GetQueueCountsForTests().SmallStale);

            await table.ForceCleanup();

            // Small first, the victims come off the live head and the stale tail is never reached.
            Assert.True(table.Count < 3000, "the pass did not evict");
            Assert.Equal(1500, table.GetQueueCountsForTests().SmallStale);

            for (int i = 0; i < 1500; i++, key++)
            {
                table.Add(key, new TestCacheObject(key), handler);
                table.Delete(key);
            }
            await table.ForceCleanup();

            // Stale now at least live, the next pass compacts.
            Assert.Equal(0, table.GetQueueCountsForTests().SmallStale);
        }

        // Handles a set number of victims and declines the rest
        private sealed class LimitedEvictHandler : ICacheEvictHandler
        {
            public int Accept = int.MaxValue;
            public int LastVictimCount;

            public Task<int> Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
            {
                LastVictimCount = valuesToEvict.Count;
                return Task.FromResult(Math.Min(Accept, valuesToEvict.Count));
            }
        }

        [Fact]
        public async Task GhostInsertWithoutBudgetIsTrimmedWithinThePass()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(32);
            var handler = new LimitedEvictHandler();
            var capacity = table.GhostCapacityForTests;
            long nextKey = 0;

            // Fills the ghost to its capacity.
            for (int i = 0; i < 100; i++)
            {
                table.Add(nextKey, new TestCacheObject(nextKey), handler);
                nextKey++;
            }
            await table.ForceCleanup();
            Assert.Equal(capacity, table.GetQueueShapeForTests().GhostKeys);

            // 256 victims, one handled and 255 declined: the requeues spend all but one unit, so its ghost insert has none left to trim.
            var cleanupStart = table.GetQueueShapeForTests().CleanupStart;
            while (table.Count < cleanupStart + 256)
            {
                table.Add(nextKey, new TestCacheObject(nextKey), handler);
                nextKey++;
            }
            var countBefore = table.Count;
            handler.Accept = 1;
            await table.ForceCleanup();
            Assert.Equal(256, handler.LastVictimCount);
            Assert.Equal(countBefore - 1, table.Count);

            // The pass finishes the trim the insert could not pay for.
            var shape = table.GetQueueShapeForTests();
            var records = table.GetQueueCountsForTests().GhostCount;
            Assert.True(shape.GhostKeys <= capacity, $"{shape.GhostKeys} ghost keys after the starved insert, capacity {capacity}");
            Assert.True(records <= 2 * capacity, $"{records} ghost records after the starved insert, capacity {capacity}");
        }

        [Fact]
        public async Task GhostInsertsOfAThrowingPassAreTrimmedWithinThePass()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(32);
            var handler = new TestEvictHandler();
            var capacity = table.GhostCapacityForTests;
            var objects = new Dictionary<long, TestCacheObject>();
            long nextKey = 0;
            void Add()
            {
                var obj = new TestCacheObject(nextKey);
                objects[nextKey] = obj;
                table.Add(nextKey++, obj, handler);
            }

            // Fills the ghost to its capacity, the survivors are the oldest keys left in small.
            for (int i = 0; i < 100; i++)
            {
                Add();
            }
            await table.ForceCleanup();
            Assert.Equal(capacity, table.GetQueueShapeForTests().GhostKeys);
            var firstVictim = objects.Keys.Where(k => table.TryPeekEntry(k, out _)).Min();

            // 255 victims: the first is removed, the second throws, and the salvage spends 255 units before the ghost insert.
            var cleanupStart = table.GetQueueShapeForTests().CleanupStart;
            while (table.Count < cleanupStart + 255)
            {
                Add();
            }
            objects[firstVictim + 1].OnTryReclaimForEviction = () => throw new InvalidOperationException("reclaim failed");
            await Assert.ThrowsAsync<InvalidOperationException>(() => table.ForceCleanup());
            Assert.True(table.IsInGhostForTests(firstVictim));

            var shape = table.GetQueueShapeForTests();
            var records = table.GetQueueCountsForTests().GhostCount;
            Assert.True(shape.GhostKeys <= capacity, $"{shape.GhostKeys} ghost keys after the salvage, capacity {capacity}");
            Assert.True(records <= 2 * capacity, $"{records} ghost records after the salvage, capacity {capacity}");
        }

        [Fact]
        public async Task DeadGhostRecordsAreTrimmedWhenHeldPagesExhaustTheBudget()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(32);
            var handler = new AcceptAllHandler();
            var capacity = table.GhostCapacityForTests;
            var cleanupStart = table.GetQueueShapeForTests().CleanupStart;
            var held = new List<TestCacheObject>();
            long nextKey = 0;
            for (int i = 0; i < cleanupStart + 255; i++)
            {
                var page = new TestCacheObject(nextKey);
                Assert.True(page.TryRent());
                held.Add(page);
                table.Add(nextKey++, page, handler);
            }
            try
            {
                for (int churn = 0; churn < 600; churn++)
                {
                    var key = nextKey++;
                    var fresh = new TestCacheObject(key);
                    table.Add(key, fresh, handler);
                    for (int pass = 0; !fresh.RemovedFromCache && pass < 4; pass++)
                    {
                        await table.ForceCleanup();
                        Assert.Equal(256, handler.LastVictimCount);
                    }
                    Assert.True(fresh.RemovedFromCache);
                    Assert.True(table.IsInGhostForTests(key));

                    // Coming back drops the membership, its record stays behind dead.
                    table.Add(key, new TestCacheObject(key), handler);
                    table.Delete(key);
                    Assert.False(table.IsInGhostForTests(key));

                    var shape = table.GetQueueShapeForTests();
                    var records = table.GetQueueCountsForTests().GhostCount;
                    Assert.True(shape.GhostKeys <= capacity, $"churn {churn}: {shape.GhostKeys} ghost keys, capacity {capacity}");
                    Assert.True(records <= 2 * capacity, $"churn {churn}: {records} ghost records with {shape.GhostKeys} live keys, capacity {capacity}");
                }
            }
            finally
            {
                foreach (var page in held)
                {
                    page.Return();
                }
            }
        }
    }
}

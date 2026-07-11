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

using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using Xunit;

namespace FlowtideDotNet.Storage.Tests.S3Fifo
{
    /// <summary>
    /// Functional tests for the S3-FIFO cache table. All tests run with the background
    /// cleanup task stopped and drive eviction through ForceCleanup, so behavior is
    /// deterministic.
    ///
    /// Size math used throughout: with MaxSize = 10 the cleanup threshold is
    /// ceil(10 * 0.7) = 7 and the small queue target is max(1, 10/10) = 1, so a cleanup
    /// at 10 entries evicts 3. With MaxSize = 4 the threshold is 3 and one entry is evicted.
    /// </summary>
    public class S3FifoTableSyncTests
    {
        [Fact]
        public async Task AddAndGetValueRentsAndReturnsValue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            var obj = new TestCacheObject(1);

            Assert.False(table.Add(1, obj, handler));
            Assert.Equal(1, table.Count);

            Assert.True(table.TryGetValue(1, out var cacheObject));
            Assert.Same(obj, cacheObject);
            // One reference held by the cache, one by this test.
            Assert.Equal(2, obj.RentCount);
            obj.Return();
            Assert.Equal(1, obj.RentCount);

            Assert.False(table.TryGetValue(2, out _));
        }

        [Fact]
        public async Task NewEntriesEnterSmallQueueAndEvictInFifoOrder()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[10];
            for (var i = 0; i < 10; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
                Assert.True(table.TryPeekEntryForTests(i, out var newEntry));
                Assert.Equal(S3FifoQueueLocation.Small, newEntry.Location);
            }

            await table.ForceCleanup();

            // The three oldest never-promoted entries are evicted in insertion order.
            Assert.Equal(new List<long> { 0, 1, 2 }, handler.EvictedKeys);
            Assert.Equal(7, table.Count);
            for (var i = 0; i < 3; i++)
            {
                Assert.False(table.TryGetValue(i, out _));
                Assert.True(objects[i].RemovedFromCache);
                // The cache's reference was the only one, so the object is disposed.
                Assert.Equal(0, objects[i].RentCount);
                Assert.Equal(1, objects[i].DisposeCount);
                // Evicted from the small queue, so the key is remembered in the ghost queue.
                Assert.True(table.IsInGhostForTests(i));
            }
        }

        [Fact]
        public async Task GhostHitOnReAddGoesDirectlyToMainQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 10; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            await table.ForceCleanup();
            Assert.True(table.IsInGhostForTests(0));

            // Key 0 was recently evicted from the small queue: a re-add is admitted to main.
            table.Add(0, new TestCacheObject(0), handler);
            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
            Assert.False(table.IsInGhostForTests(0));

            // A brand new key still goes to the small queue.
            table.Add(100, new TestCacheObject(100), handler);
            Assert.True(table.TryPeekEntryForTests(100, out var freshEntry));
            Assert.Equal(S3FifoQueueLocation.Small, freshEntry.Location);
        }

        [Fact]
        public async Task EntriesAccessedMoreThanOncePromoteToMainInsteadOfEvicting()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[10];
            for (var i = 0; i < 10; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }

            // Access keys 0..2 twice each, giving them frequency 2 (> 1 promotes).
            for (var round = 0; round < 2; round++)
            {
                for (var i = 0; i < 3; i++)
                {
                    Assert.True(table.TryGetValue(i, out var cacheObject));
                    cacheObject!.Return();
                }
            }

            await table.ForceCleanup();

            // 0..2 were promoted, so the next-oldest small entries were evicted instead.
            Assert.Equal(new List<long> { 3, 4, 5 }, handler.EvictedKeys);
            for (var i = 0; i < 3; i++)
            {
                Assert.True(table.TryPeekEntryForTests(i, out var entry));
                Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
            }
            Assert.Equal(7, table.Count);
        }

        [Fact]
        public async Task EntryAccessedOnceIsStillEvictedFromSmallQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 10; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // A single access gives frequency 1, which does not promote (needs > 1).
            Assert.True(table.TryGetValue(0, out var cacheObject));
            cacheObject!.Return();

            await table.ForceCleanup();

            Assert.Contains(0, handler.EvictedKeys);
        }

        [Fact]
        public async Task MainQueueGivesSecondChancesAndDoesNotGhostItsEvictions()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(4);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[4];
            for (var i = 0; i < 4; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }
            // Give every entry frequency 2 so the small-queue scan promotes all of them.
            for (var round = 0; round < 2; round++)
            {
                for (var i = 0; i < 4; i++)
                {
                    Assert.True(table.TryGetValue(i, out var cacheObject));
                    cacheObject!.Return();
                }
            }

            await table.ForceCleanup();

            // All four promoted to main; the scan then decrements frequencies in FIFO
            // passes until the oldest entry (key 0) reaches frequency 0 and is evicted.
            Assert.Equal(new List<long> { 0 }, handler.EvictedKeys);
            Assert.Equal(3, table.Count);
            for (var i = 1; i < 4; i++)
            {
                Assert.True(table.TryPeekEntryForTests(i, out var entry));
                Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
            }

            // Main-queue evictions do not enter the ghost queue, so a re-add of key 0
            // starts over in the small queue.
            Assert.False(table.IsInGhostForTests(0));
            table.Add(0, new TestCacheObject(0), handler);
            Assert.True(table.TryPeekEntryForTests(0, out var readdedEntry));
            Assert.Equal(S3FifoQueueLocation.Small, readdedEntry.Location);
        }

        [Fact]
        public async Task ValueModifiedDuringEvictionStaysCached()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(4);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[4];

            // Simulates a state client modifying the page while the evict handler is
            // serializing it: the version bump must prevent the removal.
            handler.OnEvict = (values, _) =>
            {
                foreach (var value in values)
                {
                    if (value.Item1.Key == 0)
                    {
                        table.Add(0, value.Item1.Value, handler);
                    }
                }
            };

            for (var i = 0; i < 4; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }

            await table.ForceCleanup();

            // Key 0 was selected as the victim, but its version changed during eviction.
            Assert.Equal(new List<long> { 0 }, handler.EvictedKeys);
            Assert.Equal(4, table.Count);
            Assert.False(objects[0].Disposed);
            Assert.False(objects[0].RemovedFromCache);
            Assert.True(table.TryGetValue(0, out var cacheObject));
            cacheObject!.Return();
            // The survivor is requeued into the main queue.
            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
            Assert.False(table.IsInGhostForTests(0));
        }

        [Fact]
        public async Task DeleteReturnsCacheReferenceAndLeavesSkippableStaleSlot()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            var obj0 = new TestCacheObject(0);
            table.Add(0, obj0, handler);
            table.Add(1, new TestCacheObject(1), handler);

            table.Delete(0);

            Assert.Equal(0, obj0.RentCount);
            Assert.Equal(1, obj0.DisposeCount);
            Assert.False(table.TryGetValue(0, out _));
            Assert.Equal(1, table.Count);
            var counts = table.GetQueueCountsForTests();
            Assert.Equal(1, counts.SmallStale);

            // A second delete of the same key is a no-op.
            table.Delete(0);
            Assert.Equal(1, obj0.DisposeCount);

            // Fill up and clean: the stale slot for key 0 must be skipped, so the
            // victims are the oldest live entries.
            for (var i = 2; i <= 10; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            await table.ForceCleanup();
            Assert.Equal(new List<long> { 1, 2, 3 }, handler.EvictedKeys);
        }

        [Fact]
        public async Task AddReportsFullWhenCountExceedsMaxSize()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(2);
            var handler = new TestEvictHandler();
            Assert.False(table.Add(0, new TestCacheObject(0), handler));
            Assert.False(table.Add(1, new TestCacheObject(1), handler));
            Assert.False(table.Add(2, new TestCacheObject(2), handler));
            // Count is now 3 > MaxSize 2, so the caller is told to wait.
            Assert.True(table.Add(3, new TestCacheObject(3), handler));
        }

        [Fact]
        public async Task EvictedObjectStillRentedCanBeReAdded()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(4);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[4];
            for (var i = 0; i < 4; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }

            // Hold a reference to key 0 across its eviction, like a B+ tree traversal would.
            Assert.True(table.TryGetValue(0, out var rented));
            Assert.Same(objects[0], rented);

            await table.ForceCleanup();

            Assert.Contains(0, handler.EvictedKeys);
            Assert.True(objects[0].RemovedFromCache);
            Assert.False(objects[0].Disposed);
            Assert.Equal(1, objects[0].RentCount);
            Assert.False(table.TryGetValue(0, out _));

            // Re-adding the same object takes a fresh cache-owned rent and clears the flag.
            table.Add(0, objects[0], handler);
            Assert.False(objects[0].RemovedFromCache);
            Assert.Equal(2, objects[0].RentCount);

            Assert.True(table.TryGetValue(0, out var again));
            Assert.Same(objects[0], again);
            again!.Return();
            rented!.Return();
            Assert.Equal(1, objects[0].RentCount);
        }

        [Fact]
        public async Task AddingSameObjectAgainOnlyBumpsVersion()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            var obj = new TestCacheObject(0);
            table.Add(0, obj, handler);
            table.Add(0, obj, handler);
            table.Add(0, obj, handler);

            Assert.Equal(1, table.Count);
            Assert.Equal(1, obj.RentCount);
            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(2, entry.Version);
        }

        [Fact]
        public async Task AddingDifferentObjectForExistingKeyThrows()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            table.Add(0, new TestCacheObject(0), handler);
            Assert.Throws<InvalidOperationException>(() => table.Add(0, new TestCacheObject(0), handler));
        }

        [Fact]
        public async Task NoCacheHitsForALongTimeTriggersDeepCleanup()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10, minSize: 0);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 5; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }

            // 5 entries is below the cleanup threshold of 7, so nothing is evicted until
            // the no-hits counter reaches its limit, after which everything is dropped.
            for (var i = 0; i < 1001 && table.Count > 0; i++)
            {
                await table.ForceCleanup();
            }

            Assert.Equal(0, table.Count);
            Assert.Equal(5, handler.Evictions.Count);
            Assert.All(handler.Evictions, e => Assert.True(e.IsCleanup));
        }

        [Fact]
        public async Task ClearEmptiesTheTable()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 5; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }

            table.Clear();

            Assert.Equal(0, table.Count);
            Assert.False(table.TryGetValue(0, out _));
            var counts = table.GetQueueCountsForTests();
            Assert.Equal(0, counts.SmallCount);
            Assert.Equal(0, counts.MainCount);
            Assert.Equal(0, counts.GhostCount);
        }

        [Fact]
        public async Task WaitCompletesWhenNoCleanupIsRunning()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var wait = table.Wait();
            var completed = await Task.WhenAny(wait, Task.Delay(5000));
            Assert.Same(wait, completed);
        }

        [Fact]
        public async Task DeleteHeavyChurnCompactsStaleQueueSlots()
        {
            // Large max size so eviction pressure never kicks in; only add/delete churn.
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1_000_000);
            var handler = new TestEvictHandler();

            for (var i = 0; i < 1500; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
                table.Delete(i);
            }

            var before = table.GetQueueCountsForTests();
            Assert.Equal(1500, before.SmallCount);
            Assert.Equal(1500, before.SmallStale);

            // The maintenance pass inside cleanup compacts the queues once stale slots dominate.
            await table.ForceCleanup();

            var after = table.GetQueueCountsForTests();
            Assert.Equal(0, after.SmallCount);
            Assert.Equal(0, after.SmallStale);
            Assert.Equal(0, table.Count);
        }

        [Fact]
        public async Task DisposeReturnsAllCacheReferences()
        {
            var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[5];
            for (var i = 0; i < 5; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }

            table.Dispose();

            for (var i = 0; i < 5; i++)
            {
                Assert.Equal(0, objects[i].RentCount);
                Assert.Equal(1, objects[i].DisposeCount);
            }
        }
    }
}

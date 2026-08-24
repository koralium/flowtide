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
    /// Functional tests for the S3-FIFO cache table.
    /// All tests stop the cleanup task and drive eviction through ForceCleanup.
    /// MaxSize 10 gives cleanup threshold 7 and small target 1, so a cleanup at 10 evicts 3.
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
                // The cache's reference was the only one, so the object is disposed.
                Assert.Equal(0, objects[i].RentCount);
                Assert.Equal(1, objects[i].DisposeCount);
                // Evicted from the small queue, so the key is remembered in the ghost queue.
                Assert.True(table.IsInGhostForTests(i));
            }
        }

        [Fact]
        public async Task GhostHitRoutesByRecordedReuse()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 10; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // Key 0 counts one reuse before eviction, key 1 none.
            Assert.True(table.TryGetValue(0, out var hit));
            hit!.Return();
            await table.ForceCleanup();
            Assert.True(table.IsInGhostForTests(0));
            Assert.True(table.IsInGhostForTests(1));

            // One counted reuse plus the re-reference is two events, straight to main.
            table.Add(0, new TestCacheObject(0), handler);
            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
            Assert.False(table.IsInGhostForTests(0));

            // No counted reuse, the re-reference is the first event and is banked as frequency.
            table.Add(1, new TestCacheObject(1), handler);
            Assert.True(table.TryPeekEntryForTests(1, out var coldEntry));
            Assert.Equal(S3FifoQueueLocation.Small, coldEntry.Location);
            Assert.Equal(1, coldEntry.Frequency);
            Assert.False(table.IsInGhostForTests(1));

            // One counted hit completes the banked pair, frequency 2 is what the scan promotes on.
            Assert.True(table.TryGetValue(1, out var pairHit));
            pairHit!.Return();
            Assert.Equal(2, coldEntry.Frequency);

            // A brand new key still goes to the small queue with nothing banked.
            table.Add(100, new TestCacheObject(100), handler);
            Assert.True(table.TryPeekEntryForTests(100, out var freshEntry));
            Assert.Equal(S3FifoQueueLocation.Small, freshEntry.Location);
            Assert.Equal(0, freshEntry.Frequency);
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
        public async Task EntryAccessedOnceIsNotPromotedFromSmallQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 10; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // One counted access is only the first reuse event, main admission needs two.
            // The page leaves through the ghost queue with the reuse recorded instead.
            Assert.True(table.TryGetValue(0, out var cacheObject));
            cacheObject!.Return();

            await table.ForceCleanup();

            Assert.Contains(0, handler.EvictedKeys);
            Assert.True(table.IsInGhostForTests(0));
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

            // All four promoted to main, then the scan decrements frequencies in FIFO
            // passes until the oldest entry reaches 0 and is evicted.
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

            // Simulates a state client modifying the page while it is being serialized.
            // The version bump must prevent the removal.
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
        public async Task LargeEvictionBatchIsSelectedInChunks()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            const int total = 5000;
            var objects = new TestCacheObject[total];
            for (var i = 0; i < total; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }

            // 5000 entries over a threshold of 7, so one cleanup selects 4993 victims.
            // These must span many queue-lock acquisitions and keep FIFO order and accounting.
            var acquisitionsBefore = table.SelectionLockAcquisitionsForTests;
            await table.ForceCleanup();
            var acquisitions = table.SelectionLockAcquisitionsForTests - acquisitionsBefore;

            Assert.True(acquisitions > 1, $"Selection used {acquisitions} lock acquisition(s); large batches must be chunked");
            Assert.Equal(7, table.Count);
            Assert.Equal(total - 7, handler.Evictions.Count);
            // FIFO order preserved across chunks, the oldest evicted and the newest survived.
            Assert.Equal(0, handler.EvictedKeys.First());
            Assert.Equal(total - 8, handler.EvictedKeys.Last());
            for (var i = total - 7; i < total; i++)
            {
                Assert.True(table.TryGetValue(i, out var cached));
                cached!.Return();
            }
            for (var i = 0; i < total - 7; i++)
            {
                Assert.Equal(0, objects[i].RentCount);
                Assert.Equal(1, objects[i].DisposeCount);
            }
        }

        [Fact]
        public async Task EvictHandlerFailureKeepsVictimsCachedAndEvictable()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10, minSize: 0);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[10];
            for (var i = 0; i < 10; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }

            // Storage failure while serializing the victims, the cleanup pass must fail loudly
            // but the victims must stay cached and evictable.
            handler.OnEvict = (_, _) => throw new IOException("temporary storage failure");
            await Assert.ThrowsAsync<IOException>(() => table.ForceCleanup());

            Assert.Equal(10, table.Count);
            Assert.True(table.TryGetValue(0, out var stillCached));
            stillCached!.Return();

            // Handler recovers, every entry including the failed victims must be evictable
            // all the way down to an empty table.
            handler.OnEvict = null;
            for (var i = 0; i < 2001 && table.Count > 0; i++)
            {
                await table.ForceCleanup();
            }

            Assert.Equal(0, table.Count);
            foreach (var obj in objects)
            {
                Assert.Equal(0, obj.RentCount);
                Assert.Equal(1, obj.DisposeCount);
            }
        }

        [Fact]
        public async Task VictimModifiedAndDeletedDuringEvictionIsNotResurrected()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(4);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[4];
            handler.OnEvict = (values, _) =>
            {
                foreach (var value in values)
                {
                    if (value.Item1.Key == 0)
                    {
                        // The page is modified and then deleted while being serialized.
                        // The delete must win and the entry must not be resurrected.
                        table.Add(0, value.Item1.Value, handler);
                        table.Delete(0);
                    }
                }
            };
            for (var i = 0; i < 4; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }

            await table.ForceCleanup();

            Assert.Equal(3, table.Count);
            Assert.False(table.TryGetValue(0, out _));
            Assert.Equal(0, objects[0].RentCount);
            Assert.Equal(1, objects[0].DisposeCount);
            // The deleted victim must not occupy any queue slot.
            var counts = table.GetQueueCountsForTests();
            Assert.Equal(0, counts.MainCount);
            Assert.Equal(0, counts.MainStale);
        }

        [Fact]
        public async Task StaleEntryReferenceReadsAsMissAfterRemoval()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            var obj = new TestCacheObject(0);
            table.Add(0, obj, handler);
            Assert.True(table.TryPeekEntryForTests(0, out var entry));

            table.Delete(0);

            // A reader holding a stale entry reference (like a state client lookup slot)
            // must observe a miss on the lock-free read path, not a throw, and must not rent.
            Assert.False(entry!.TryRentValue());
            Assert.Equal(0, obj.RentCount);

            // Same when the object survives removal because another holder still rents it.
            var held = new TestCacheObject(1);
            Assert.True(held.TryRent());
            table.Add(1, held, handler);
            Assert.True(table.TryPeekEntryForTests(1, out var heldEntry));
            table.Delete(1);
            Assert.False(heldEntry!.TryRentValue());
            Assert.Equal(1, held.RentCount);
            held.Return();
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

            // Fill up and clean, the stale slot for key 0 must be skipped so the victims
            // are the oldest live entries.
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

        /// <summary>
        /// A page that anything still references must never be removed from the cache.
        /// Removing it lets a traversal reload a second object for the same key and the two
        /// copies diverge. Seen when a B+ tree iterator holds a leaf under eviction pressure.
        /// </summary>
        [Fact]
        public async Task HeldPagesAreNotEvictedSoTheirIdentityIsStable()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(4);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[4];
            for (var i = 0; i < 4; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }

            // Hold a reference to key 0 across a cleanup, like a B+ tree iterator would.
            // Rent directly on the object, a table read would now count as a promoting hit.
            Assert.True(objects[0].TryRent());

            await table.ForceCleanup();

            // The held page was selected and serialized but must stay cached.
            // A later read must return the same object, not a reloaded copy.
            Assert.Contains(0, handler.EvictedKeys);
            Assert.True(table.TryGetValue(0, out var again));
            Assert.Same(objects[0], again);
            again!.Return();
            Assert.False(objects[0].RemovedFromCache);
            Assert.False(objects[0].Disposed);
            Assert.Equal(4, table.Count);
            // It was requeued into the queue it came from for a later retry, being held is not
            // the proven reuse the main queue asks for.
            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Small, entry!.Location);

            // Accounting is unharmed by the skipped eviction, cache share plus our rent.
            Assert.Equal(2, objects[0].RentCount);
            objects[0].Return();
            Assert.Equal(1, objects[0].RentCount);

            // A second cleanup finds an unreferenced victim instead.
            await table.ForceCleanup();
            Assert.Equal(3, table.Count);
            Assert.Equal(1, objects[1].DisposeCount);
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

        /// <summary>
        /// The deep clean drops to MinSize, not to empty. Every other test runs with a floor
        /// of zero, where the two are indistinguishable.
        /// </summary>
        [Fact]
        public async Task DeepCleanupStopsAtMinSize()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10, minSize: 3);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 5; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }

            // Below the cleanup threshold, so nothing goes until the no-hits counter trips.
            for (var i = 0; i < 1001 && table.Count > 3; i++)
            {
                await table.ForceCleanup();
            }

            Assert.Equal(3, table.Count);
            Assert.Equal(2, handler.Evictions.Count);
            Assert.All(handler.Evictions, e => Assert.True(e.IsCleanup));

            // The floor holds, further cleanups must not drain it.
            for (var i = 0; i < 100; i++)
            {
                await table.ForceCleanup();
            }
            Assert.Equal(3, table.Count);
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

        /// <summary>
        /// The deep clean shrinks the cache to MinSize, and it should pay with pages nothing is
        /// reading rather than the ones that proved reuse.
        /// MaxSize 100 gives threshold 70 and window 2, MinSize 10 is the deep clean target.
        /// </summary>
        [Fact]
        public async Task DeepCleanupKeepsReusedPagesOverUnprovenOnes()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100, minSize: 10);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 71; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // Keys 0..4 count two reuses in separate windows, so the scan promotes them.
            for (var i = 0; i < 5; i++)
            {
                Assert.True(table.TryGetValue(i, out var reused));
                reused!.Return();
            }
            table.Add(71, new TestCacheObject(71), handler);
            table.Add(72, new TestCacheObject(72), handler);
            for (var i = 0; i < 5; i++)
            {
                Assert.True(table.TryGetValue(i, out var reusedAgain));
                reusedAgain!.Return();
            }

            // Over the threshold, so this pass evicts and promotes the reused keys on the way.
            await table.ForceCleanup();

            var afterEviction = table.GetQueueCountsForTests();
            Assert.Equal(5, afterEviction.MainCount);
            for (var i = 0; i < 5; i++)
            {
                Assert.True(table.TryPeekEntryForTests(i, out var promoted));
                Assert.Equal(S3FifoQueueLocation.Main, promoted.Location);
            }

            // Idle long enough to trip the deep clean, which drops the cache to MinSize.
            for (var i = 0; i < 1001 && table.Count > 10; i++)
            {
                await table.ForceCleanup();
            }

            Assert.Equal(10, table.Count);
            // The reused pages are what the floor is for, the never-reused small queue tail is
            // what the deep clean pays with.
            var afterDeepClean = table.GetQueueCountsForTests();
            Assert.Equal(5, afterDeepClean.MainCount);
            Assert.Equal(5, afterDeepClean.SmallCount);
        }

        /// <summary>
        /// A page a caller still holds survives eviction, and goes back to the queue it came
        /// from. Handing it the main queue would let being held buy main admission without the
        /// two reuse events the policy asks for.
        /// </summary>
        [Fact]
        public async Task HeldVictimRequeuesToItsOriginQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[10];
            for (var i = 0; i < 10; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }
            // Hold the small queue head directly, a table read would also count as a reuse.
            Assert.True(objects[0].TryRent());

            await table.ForceCleanup();

            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Small, entry.Location);
            Assert.False(table.IsInGhostForTests(0));
            objects[0].Return();
        }

        /// <summary>
        /// A cache below its eviction threshold keeps what it has. Trading resident pages for
        /// queue shares throws away capacity the cache was configured to use.
        /// </summary>
        [Fact]
        public async Task CacheBelowThresholdKeepsEverything()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 60; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // A hit keeps the cache off the idle deep clean, which owns the no-hits case.
            Assert.True(table.TryGetValue(0, out var hit));
            hit!.Return();

            await table.ForceCleanup();

            // 60 is well over the small queue's 10% share but well under the 70 threshold.
            Assert.Empty(handler.Evictions);
            Assert.Equal(60, table.Count);
            Assert.Equal(60, table.GetQueueCountsForTests().SmallCount);
        }

        /// <summary>
        /// A main queue page that stops being read loses its frequency as the cache turns over,
        /// and once it is aged out eviction takes it before the small queue's head.
        /// </summary>
        [Fact]
        public async Task AgedOutMainPagesAreEvictedBeforeSmallQueuePages()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100, minSize: 0);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 71; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // Key 0 earns two spaced reuses and is promoted by the first pass.
            Assert.True(table.TryGetValue(0, out var first));
            first!.Return();
            table.Add(71, new TestCacheObject(71), handler);
            table.Add(72, new TestCacheObject(72), handler);
            Assert.True(table.TryGetValue(0, out var second));
            second!.Return();

            await table.ForceCleanup();
            Assert.True(table.TryPeekEntryForTests(0, out var promoted));
            Assert.Equal(S3FifoQueueLocation.Main, promoted.Location);

            // Nothing reads key 0 again. Turning the cache over ages it down to zero, and once
            // there it is the first thing eviction takes.
            for (var round = 0; round < 40 && !handler.EvictedKeys.Contains(0); round++)
            {
                for (var i = 0; i < 100; i++)
                {
                    table.Add(1000 + (round * 100) + i, new TestCacheObject(0), handler);
                }
                Assert.True(table.TryGetValue(1000 + (round * 100), out var keepAlive));
                keepAlive!.Return();
                await table.ForceCleanup();
            }

            Assert.Contains(0, handler.EvictedKeys);
            Assert.Equal(0, table.GetQueueCountsForTests().MainCount);
        }

        /// <summary>
        /// Aging is paced by how much the cache turned over, so a cache nothing is inserting into
        /// does not age its own resident set away.
        /// </summary>
        [Fact]
        public async Task AgingDoesNotRunWithoutInsertions()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100, minSize: 0);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 71; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            Assert.True(table.TryGetValue(0, out var first));
            first!.Return();
            table.Add(71, new TestCacheObject(71), handler);
            table.Add(72, new TestCacheObject(72), handler);
            Assert.True(table.TryGetValue(0, out var second));
            second!.Return();
            await table.ForceCleanup();

            Assert.True(table.TryPeekEntryForTests(0, out var promoted));
            Assert.Equal(S3FifoQueueLocation.Main, promoted.Location);
            var frequencyAfterPromotion = promoted.Frequency;

            // No inserts, so no turnover, so the hand does not move however often cleanup runs.
            for (var i = 0; i < 50; i++)
            {
                await table.ForceCleanup();
            }

            Assert.Equal(frequencyAfterPromotion, promoted.Frequency);
            Assert.Equal(S3FifoQueueLocation.Main, promoted.Location);
        }

        /// <summary>
        /// With the opt in drain on, the small queue is held at its share even below the
        /// eviction threshold.
        /// </summary>
        [Fact]
        public async Task EarlyDrainHoldsSmallQueueAtItsTargetWhenEnabled()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100, drainSmallQueueEarly: true);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 60; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // A hit keeps the cache off the idle deep clean, which owns the no-hits case.
            Assert.True(table.TryGetValue(0, out var hit));
            hit!.Return();

            await table.ForceCleanup();

            Assert.Equal(10, table.GetQueueCountsForTests().SmallCount);
            Assert.NotEmpty(handler.Evictions);
        }

        /// <summary>
        /// Draining the small queue promotes its reused heads into main, so the drain has to be
        /// able to take main's aged out pages as well. Without that, main only ever grows while
        /// the cache stays below the eviction threshold.
        /// </summary>
        [Fact]
        public async Task EarlyDrainAlsoEvictsAgedOutMainPages()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100, drainSmallQueueEarly: true);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 60; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // Key 0 earns two spaced reuses, so the first drain promotes it into main.
            Assert.True(table.TryGetValue(0, out var first));
            first!.Return();
            table.Add(60, new TestCacheObject(60), handler);
            table.Add(61, new TestCacheObject(61), handler);
            Assert.True(table.TryGetValue(0, out var second));
            second!.Return();

            await table.ForceCleanup();
            Assert.True(table.TryPeekEntryForTests(0, out var promoted));
            Assert.Equal(S3FifoQueueLocation.Main, promoted.Location);

            // Nothing reads key 0 again, so the cache turning over ages it out and the drain
            // takes it instead of only ever paying from the small queue.
            for (var round = 0; round < 40 && !handler.EvictedKeys.Contains(0); round++)
            {
                for (var i = 0; i < 100; i++)
                {
                    table.Add(1000 + (round * 100) + i, new TestCacheObject(0), handler);
                }
                Assert.True(table.TryGetValue(1000 + (round * 100), out var keepAlive));
                keepAlive!.Return();
                await table.ForceCleanup();
            }

            Assert.Contains(0, handler.EvictedKeys);
            Assert.Equal(0, table.GetQueueCountsForTests().MainCount);
        }

        /// <summary>
        /// Re-adds a key only when it is no longer cached. Adding a second instance for a key the
        /// cache still holds is a caller error the table rejects on purpose.
        /// </summary>
        private static void ReAddIfEvicted(S3FifoTableSync table, long key, TestEvictHandler handler)
        {
            if (!table.TryPeekEntryForTests(key, out _))
            {
                table.Add(key, new TestCacheObject(key), handler);
            }
        }

        /// <summary>
        /// Off by default, the small queue keeps the fixed share whatever the ghost queue sees.
        /// </summary>
        [Fact]
        public async Task SmallQueueShareIsFixedUnlessAdaptationIsEnabled()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100);
            var handler = new TestEvictHandler();
            for (var round = 0; round < 5; round++)
            {
                for (var i = 0; i < 100; i++)
                {
                    table.Add((round * 100) + i, new TestCacheObject(i), handler);
                }
                await table.ForceCleanup();
                // Re-reference evicted keys, which would move an adaptive split.
                for (var i = 0; i < 20; i++)
                {
                    ReAddIfEvicted(table, (round * 100) + i, handler);
                }
            }

            Assert.Equal(100, table.SmallTargetPermilleForTests);
        }

        /// <summary>
        /// Aging is paced by what it takes to replace the resident pages, not by the configured
        /// ceiling. The early drain holds this cache near 1000 pages against a ceiling of 10000,
        /// so pacing off the ceiling would age the main queue ten times slower than the cache
        /// really turns over, and over this many rounds not at all.
        /// </summary>
        [Fact]
        public async Task AgingIsPacedByResidentPagesNotTheConfiguredCeiling()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10000, drainSmallQueueEarly: true);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 2000; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // Two hits spaced past the 250 wide correlation window promote key 0, so the main
            // queue has something to age.
            Assert.True(table.TryGetValue(0, out var first));
            first!.Return();
            for (var i = 2000; i < 2400; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            Assert.True(table.TryGetValue(0, out var second));
            second!.Return();

            await table.ForceCleanup();
            Assert.True(table.TryPeekEntryForTests(0, out var promoted));
            Assert.Equal(S3FifoQueueLocation.Main, promoted.Location);

            var stepsBefore = table.AgingStepsForTests;
            var residentBefore = table.Count;

            // Turn the resident pages over several times. The drain keeps residency near the
            // small queue target, far below the ceiling.
            for (var round = 0; round < 6; round++)
            {
                for (var i = 0; i < 1000; i++)
                {
                    table.Add(10000 + (round * 1000) + i, new TestCacheObject(i), handler);
                }
                // A hit on some other key keeps the cache off the idle path, which returns before
                // anything ages. Reading key 0 would pump its frequency back up instead.
                Assert.True(table.TryGetValue(10000 + (round * 1000), out var keepAlive));
                keepAlive!.Return();
                await table.ForceCleanup();
            }

            Assert.True(residentBefore < 3000, $"expected the drain to hold residency low, saw {residentBefore}");
            Assert.True(table.AgingStepsForTests > stepsBefore,
                "the main queue never aged, so aging is paced off the ceiling rather than the resident pages");
        }

        /// <summary>
        /// Passes that skip aging leave the insertions they saw owed to the next pass that runs
        /// one, so a long quiet stretch can owe a sweep of many laps. A page loses at most
        /// MaxFrequency points, so the laps past that cannot change a single frequency and only
        /// hold the queue lock, which Add and Delete wait behind.
        /// </summary>
        [Fact]
        public void AnOwedAgingSweepIsCutToTheLapsThatCanStillChangeSomething()
        {
            // Three laps of a 70000 page main queue is every point every page could lose, so an
            // owed sweep of a hundred laps is cut to those three.
            Assert.Equal(210_000, S3FifoTableSync.UsefulAgingSteps(stepsToRun: 7_800_000, liveMain: 70_000));

            // Exactly at the bound is kept whole.
            Assert.Equal(210_000, S3FifoTableSync.UsefulAgingSteps(stepsToRun: 210_000, liveMain: 70_000));

            // An ordinary pass earns a fraction of a lap and is left alone.
            Assert.Equal(140, S3FifoTableSync.UsefulAgingSteps(stepsToRun: 140, liveMain: 70_000));

            // Nothing queued to sweep.
            Assert.Equal(0, S3FifoTableSync.UsefulAgingSteps(stepsToRun: 5_000, liveMain: 0));
        }

        /// <summary>
        /// The ghost queue is sized from the cache size alone, never from the queue shares. The
        /// ghost is the evidence the adaptive split reads, and sizing it from the split would feed
        /// the split its own output, a loop that collapses the ghost and makes every hit look like
        /// it landed on the newest entry.
        /// </summary>
        [Fact]
        public async Task GhostCapacityFollowsTheCacheSizeOnly()
        {
            using var small = await S3FifoTestHelpers.CreateStoppedTable(100, adaptiveSmallQueueSize: true);
            Assert.Equal(50, small.GhostCapacityForTests);

            using var large = await S3FifoTestHelpers.CreateStoppedTable(10000);
            Assert.Equal(5000, large.GhostCapacityForTests);
        }

        /// <summary>
        /// The split moves a bounded amount per cleanup pass however much evidence arrives, so no
        /// burst of ghost hits can carry it across its range in one go.
        /// </summary>
        [Fact]
        public async Task SplitMovementPerPassIsBounded()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000, adaptiveSmallQueueSize: true);
            var handler = new TestEvictHandler();

            var key = 1000000L;
            var previous = table.SmallTargetPermilleForTests;
            for (var round = 0; round < 15; round++)
            {
                for (var i = 0; i < 1000; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                // Bring back a batch of just evicted keys, a burst of evidence in one pass.
                for (var i = 1; i <= 200; i++)
                {
                    ReAddIfEvicted(table, key - i, handler);
                }
                await table.ForceCleanup();

                var now = table.SmallTargetPermilleForTests;
                Assert.True(Math.Abs(now - previous) <= 8,
                    $"split moved from {previous} to {now} in one pass, the slew rate is not bounded");
                previous = now;
            }
        }

        /// <summary>
        /// Off by default, the split keeps the fixed share whatever the ghost queue sees.
        /// </summary>
        [Fact]
        public async Task SplitIsFixedUnlessAdaptationIsEnabled()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000);
            var handler = new TestEvictHandler();

            var key = 0L;
            for (var round = 0; round < 10; round++)
            {
                for (var i = 0; i < 1000; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                for (var i = 1; i <= 200; i++)
                {
                    ReAddIfEvicted(table, key - i, handler);
                }
                await table.ForceCleanup();
            }

            Assert.Equal(100, table.SmallTargetPermilleForTests);
        }

        /// <summary>
        /// Ghost entries that age out unused are the signal that the small queue is holding pages
        /// nobody comes back for, so its share shrinks. This has to work with an empty main queue,
        /// which is what a workload with no reuse produces, and it is the case that matters most:
        /// with the early drain on, the share is what the cache shrinks back to.
        /// </summary>
        [Fact]
        public async Task UnusedGhostEntriesShrinkTheSmallQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000, adaptiveSmallQueueSize: true);
            var handler = new TestEvictHandler();
            var startPermille = table.SmallTargetPermilleForTests;

            // Every key is touched once and never again, so everything the small queue evicts
            // expires in the ghost queue unused.
            var key = 0L;
            for (var round = 0; round < 25; round++)
            {
                for (var i = 0; i < 1000; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                await table.ForceCleanup();
            }

            var evidence = table.AdaptEvidenceForTests;
            Assert.Equal(0, table.GetQueueCountsForTests().MainCount);
            Assert.True(evidence.SmallEvictions > 0, $"the small queue never evicted anything: {evidence}");
            Assert.True(table.SmallTargetPermilleForTests < startPermille,
                $"share stayed at {table.SmallTargetPermilleForTests} with evidence {evidence}");
        }

        /// <summary>
        /// The mirror: when the pages the small queue discards keep being wanted again, it is
        /// throwing them away too soon and its share grows.
        /// </summary>
        [Fact]
        public async Task GhostHitsGrowTheSmallQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000, adaptiveSmallQueueSize: true);
            var handler = new TestEvictHandler();
            var startPermille = table.SmallTargetPermilleForTests;

            var key = 0L;
            for (var round = 0; round < 25; round++)
            {
                var roundStart = key;
                for (var i = 0; i < 1000; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                await table.ForceCleanup();
                // Most of what was just evicted is wanted again.
                for (var i = 0; i < 600; i++)
                {
                    ReAddIfEvicted(table, roundStart + i, handler);
                }
                await table.ForceCleanup();
            }

            var evidence = table.AdaptEvidenceForTests;
            Assert.True(evidence.SmallHits > 0, $"no eviction was ever wanted back: {evidence}");
            Assert.True(table.SmallTargetPermilleForTests > startPermille,
                $"share stayed at {table.SmallTargetPermilleForTests} with evidence {evidence}");
        }

        /// <summary>
        /// Growth is a claim on space nothing else is asking for, so a full cache must not honour
        /// it. Whatever adaptation has grown the target to, the share defended when one of the
        /// queues has to give up a page stays at the paper's, and the extra only ever caps an
        /// early drain. Shrink below it is still honoured, since that only makes the small queue
        /// give up sooner.
        /// </summary>
        [Fact]
        public async Task GrowthAboveThePaperShareIsNotDefendedUnderPressure()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000, adaptiveSmallQueueSize: true);
            var handler = new TestEvictHandler();

            var key = 0L;
            for (var round = 0; round < 25; round++)
            {
                var roundStart = key;
                for (var i = 0; i < 1000; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                await table.ForceCleanup();
                for (var i = 0; i < 600; i++)
                {
                    ReAddIfEvicted(table, roundStart + i, handler);
                }
                await table.ForceCleanup();
            }

            var grown = table.SmallTargetPermilleForTests;
            Assert.True(grown > 100, $"the workload never grew the target, it is at {grown}");
            Assert.Equal(100, table.SmallQueuePressureShareForTests(1000));
        }

        /// <summary>
        /// The main queue holds pages already proven to be reused, and a scan for a main victim
        /// grinds frequencies down until one reaches zero, so sending it to main first does not
        /// merely reorder the choice, it destroys the evidence main is built on. With the small
        /// queue holding more than the paper's share, a grown target must not send it there.
        /// </summary>
        [Fact]
        public async Task AGrownTargetDoesNotDrainTheMainQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000, adaptiveSmallQueueSize: true);
            var handler = new TestEvictHandler();

            var key = 0L;
            for (var round = 0; round < 25; round++)
            {
                var roundStart = key;
                for (var i = 0; i < 1000; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                await table.ForceCleanup();
                for (var i = 0; i < 600; i++)
                {
                    ReAddIfEvicted(table, roundStart + i, handler);
                }
                await table.ForceCleanup();
            }
            var grown = table.SmallTargetPermilleForTests;

            // A hot set that earns its way into main and is read every round after, so its head
            // is never aged out and the only reason to take from it is the grown target.
            const int HotCount = 600;
            var hotStart = key;
            for (var i = 0; i < HotCount; i++)
            {
                table.Add(key++, new TestCacheObject(i), handler);
            }
            for (var touch = 0; touch < 2; touch++)
            {
                for (var i = 0; i < HotCount; i++)
                {
                    if (table.TryGetValue(hotStart + i, out var hot))
                    {
                        hot!.Return();
                    }
                }
            }
            await table.ForceCleanup();

            // Each round leaves the small queue above the paper's share and below the grown
            // target, which is the only band where the two disagree.
            var before = table.AdaptEvidenceForTests;
            for (var round = 0; round < 10; round++)
            {
                for (var i = 0; i < 200; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                for (var i = 0; i < HotCount; i++)
                {
                    if (table.TryGetValue(hotStart + i, out var hot))
                    {
                        hot!.Return();
                    }
                }
                await table.ForceCleanup();
            }
            var after = table.AdaptEvidenceForTests;

            var smallGaveUp = after.SmallEvictions - before.SmallEvictions;
            var mainGaveUp = after.MainEvictions - before.MainEvictions;
            Assert.True(smallGaveUp > 0, "nothing was evicted, so the rule was never exercised");
            Assert.True(mainGaveUp == 0,
                $"main gave up {mainGaveUp} proven pages while the small queue gave up " +
                $"{smallGaveUp} and was above the paper's share the whole time, target {grown}");
        }

        /// <summary>
        /// The small queue is where every new page enters, so it evicts far more often than the
        /// main queue and would win any contest counted in raw events. A regret from the queue
        /// that rarely evicts has to weigh proportionally more, otherwise the main queue can never
        /// argue for space however well its own evictions were doing. This is ARC's |B2|/|B1|.
        /// </summary>
        [Fact]
        public void RareQueueRegretsWeighProportionallyMore()
        {
            // Holding a tenth of the ghost queue means evicting a tenth as often over the horizon
            // it covers, so each of that queue's regrets is worth ten.
            Assert.Equal(10, S3FifoTableSync.GhostHitWeightForTests(otherGhostEntries: 10000, ownGhostEntries: 1000));

            // The queue filling the ghost queue keeps its regrets at a plain step.
            Assert.Equal(1, S3FifoTableSync.GhostHitWeightForTests(otherGhostEntries: 1000, ownGhostEntries: 10000));

            // Capped, so a queue barely represented cannot swamp the other, and a queue with no
            // memberships left at all is the extreme of that rather than a divide by zero.
            Assert.Equal(16, S3FifoTableSync.GhostHitWeightForTests(otherGhostEntries: 10000000, ownGhostEntries: 100));
            Assert.Equal(16, S3FifoTableSync.GhostHitWeightForTests(otherGhostEntries: 10000, ownGhostEntries: 0));

            // And nothing is trusted until the ghost queue holds enough for its shares to mean
            // something, so a near empty one cannot magnify the first regret into a large move.
            Assert.Equal(1, S3FifoTableSync.GhostHitWeightForTests(otherGhostEntries: 40, ownGhostEntries: 4));
        }

        /// <summary>
        /// The two shares stand in for how often each queue evicts, so they have to track what the
        /// ghost queue actually remembers. A membership ends when the key is wanted again, when it
        /// ages out, or when a later eviction of the same key replaces it, and every one of those
        /// has to be counted or the shares drift away from the queues they describe.
        /// </summary>
        [Fact]
        public async Task GhostMembershipCountsFollowTheGhostQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000, adaptiveSmallQueueSize: true);
            var handler = new TestEvictHandler();

            for (var i = 0; i < 1000; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            await table.ForceCleanup();

            var evicted = table.GhostMembershipForTests;
            Assert.True(evicted.Small > 0, "small queue evictions were not remembered");
            Assert.Equal(evicted.Remembered, evicted.Small + evicted.Main);

            // Wanting the evicted keys back ends those memberships.
            foreach (var evictedKey in handler.EvictedKeys.ToList())
            {
                ReAddIfEvicted(table, evictedKey, handler);
            }
            var afterHits = table.GhostMembershipForTests;
            Assert.Equal(afterHits.Remembered, afterHits.Small + afterHits.Main);

            // So does churning until the oldest memberships age out, and re-evicting keys the
            // ghost queue already remembers, which replaces a membership instead of adding one.
            var key = 100000L;
            for (var round = 0; round < 20; round++)
            {
                for (var i = 0; i < 1000; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                await table.ForceCleanup();
                for (var i = 0; i < 300; i++)
                {
                    ReAddIfEvicted(table, key - 1000 + i, handler);
                }
                await table.ForceCleanup();
                var counts = table.GhostMembershipForTests;
                Assert.Equal(counts.Remembered, counts.Small + counts.Main);
            }
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

        /// <summary>
        /// Clear must not mark entries removed and must not return the cache reference.
        /// SyncStateClient caches these handles and short circuits on !Removed, so a page that
        /// only lives in memory stays reachable across a Clear. Marking them removed instead
        /// sends the client to persistent storage for a page that was never written there,
        /// which shows up far away as ExchangeOperatorTests.TestPullBucketOutput failing with
        /// "Segment not found".
        /// </summary>
        [Fact]
        public async Task ClearKeepsEntryHandlesRentableForTheClientLookupSlots()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            var values = new List<TestCacheObject>();
            for (var i = 0; i < 5; i++)
            {
                var value = new TestCacheObject(i);
                values.Add(value);
                table.Add(i, value, handler);
            }
            Assert.True(table.TryGetCacheValue(2, out var entry));

            table.Clear();

            Assert.False(Volatile.Read(ref entry.Removed));
            Assert.True(entry.TryRentValue());
            entry.Value.Return();
            Assert.All(values, v => Assert.Equal(0, v.DisposeCount));
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
            // Large max size so eviction never kicks in, only add and delete churn.
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

        /// <summary>
        /// A delete landing between a victims requeue decision and the requeue block must not
        /// resurrect the dead entry into a queue, that slot would carry no stale count and
        /// drift the compaction accounting.
        /// </summary>
        [Fact]
        public async Task DeleteDuringVictimRequeueDoesNotResurrectDeadEntry()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();

            var gateEntered = new SemaphoreSlim(0);
            var gateRelease = new ManualResetEventSlim(false);

            var objects = new TestCacheObject[10];
            for (var i = 0; i < 10; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }

            // Key 0 is held so its reclaim fails and it lands on the requeue list.
            // Rent directly on the object, a table read would now count as a promoting hit.
            Assert.True(objects[0].TryRent());

            // Key 1 holds the removal phase open after key 0's requeue decision was made.
            objects[1].OnTryReclaimForEviction = () =>
            {
                gateEntered.Release();
                gateRelease.Wait();
            };

            var cleanup = Task.Run(() => table.ForceCleanup());
            Assert.True(await gateEntered.WaitAsync(5000));

            // Delete key 0 in the window between its requeue decision and the requeue block.
            table.Delete(0);

            gateRelease.Set();
            await cleanup;

            // The dead entry must not be resurrected into the main queue.
            var counts = table.GetQueueCountsForTests();
            Assert.Equal(0, counts.MainCount);
            Assert.Equal(0, counts.MainStale);
            Assert.Equal(7, table.Count);

            // The delete already returned the cache reference, this is the held rent.
            objects[0].Return();
            Assert.True(objects[0].Disposed);
        }

        /// <summary>
        /// Hits and misses are counted per path.
        /// TryGetCacheValue is the read path and TryGetValue is the commit path.
        /// </summary>
        [Fact]
        public async Task HitAndMissCountersAreSplitByPath()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();
            table.Add(1, new TestCacheObject(1), handler);

            Assert.True(table.TryGetCacheValue(1, out var entry));
            entry!.Value.Return();
            Assert.False(table.TryGetCacheValue(2, out _));

            Assert.True(table.TryGetValue(1, out var obj));
            obj!.Return();
            Assert.True(table.TryGetValue(1, out obj));
            obj!.Return();
            Assert.False(table.TryGetValue(3, out _));

            Assert.Equal(1, table.ReadCacheHitsForTests);
            Assert.Equal(1, table.ReadCacheMissesForTests);
            Assert.Equal(2, table.CommitCacheHitsForTests);
            Assert.Equal(1, table.CommitCacheMissesForTests);
        }

        /// <summary>
        /// Stopping the cleanup task must complete even while a commit or recovery holds the
        /// eviction pause, the parked wait must observe the cancellation.
        /// </summary>
        [Fact]
        public async Task StopCleanupTaskCompletesWhileEvictionIsPaused()
        {
            using var table = S3FifoTestHelpers.CreateRunningTable(10);
            await table.PauseEvictionAsync();

            // Let the timer tick so the loop parks on the eviction pause lock.
            await Task.Delay(100);

            var stop = table.StopCleanupTask();
            var finished = await Task.WhenAny(stop, Task.Delay(2000)) == stop;
            table.ResumeEviction();

            Assert.True(finished, "cleanup task did not observe cancellation while eviction was paused");
            await stop;
        }
    }
}

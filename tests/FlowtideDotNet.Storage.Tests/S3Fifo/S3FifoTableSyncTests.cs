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
    /// Tests stop the cleanup task and drive eviction through ForceCleanup.
    /// MaxSize 10 gives threshold 7 and small target 1.
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
                // The cache held the only reference, so it is disposed.
                Assert.Equal(0, objects[i].RentCount);
                Assert.Equal(1, objects[i].DisposeCount);
                // Evicted from small, so the key is remembered in ghost.
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

            // One counted reuse plus the re-reference, straight to main.
            table.Add(0, new TestCacheObject(0), handler);
            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
            Assert.False(table.IsInGhostForTests(0));

            // No counted reuse, so the re-reference is banked as frequency.
            table.Add(1, new TestCacheObject(1), handler);
            Assert.True(table.TryPeekEntryForTests(1, out var coldEntry));
            Assert.Equal(S3FifoQueueLocation.Small, coldEntry.Location);
            Assert.Equal(1, coldEntry.Frequency);
            Assert.False(table.IsInGhostForTests(1));

            // One counted hit completes the pair, the scan promotes on 2.
            Assert.True(table.TryGetValue(1, out var pairHit));
            pairHit!.Return();
            Assert.Equal(2, coldEntry.Frequency);

            // A new key goes to small with nothing banked.
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

            // Access keys 0..2 twice, frequency 2 promotes.
            for (var round = 0; round < 2; round++)
            {
                for (var i = 0; i < 3; i++)
                {
                    Assert.True(table.TryGetValue(i, out var cacheObject));
                    cacheObject!.Return();
                }
            }

            await table.ForceCleanup();

            // 0..2 promoted, so the next oldest went instead.
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
            // One access is the first event, main needs two.
            // It leaves through ghost with the reuse recorded.
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
            // Frequency 2 on every entry, so the scan promotes them all.
            for (var round = 0; round < 2; round++)
            {
                for (var i = 0; i < 4; i++)
                {
                    Assert.True(table.TryGetValue(i, out var cacheObject));
                    cacheObject!.Return();
                }
            }

            await table.ForceCleanup();

            // All four promoted, then the scan decrements until the oldest reaches 0.
            Assert.Equal(new List<long> { 0 }, handler.EvictedKeys);
            Assert.Equal(3, table.Count);
            for (var i = 1; i < 4; i++)
            {
                Assert.True(table.TryPeekEntryForTests(i, out var entry));
                Assert.Equal(S3FifoQueueLocation.Main, entry.Location);
            }

            // Main evictions skip ghost, so a re-add starts over in small.
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

            // A client modifying the page while it serializes.
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

            // Key 0 was the victim, but its version changed.
            Assert.Equal(new List<long> { 0 }, handler.EvictedKeys);
            Assert.Equal(4, table.Count);
            Assert.False(objects[0].Disposed);
            Assert.False(objects[0].RemovedFromCache);
            Assert.True(table.TryGetValue(0, out var cacheObject));
            cacheObject!.Return();
            // Back where it came from, a write is not reuse.
            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Small, entry.Location);
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

            // 5000 entries over a threshold of 7 selects 4993 victims.
            // They must span many lock acquisitions and keep FIFO order.
            var acquisitionsBefore = table.SelectionLockAcquisitionsForTests;
            await table.ForceCleanup();
            var acquisitions = table.SelectionLockAcquisitionsForTests - acquisitionsBefore;

            Assert.True(acquisitions > 1, $"Selection used {acquisitions} lock acquisition(s); large batches must be chunked");
            Assert.Equal(7, table.Count);
            Assert.Equal(total - 7, handler.Evictions.Count);
            // FIFO order across chunks, oldest out and newest kept.
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

            // Storage fails while serializing, the pass must fail loudly.
            // The victims must stay cached and evictable.
            handler.OnEvict = (_, _) => throw new IOException("temporary storage failure");
            await Assert.ThrowsAsync<IOException>(() => table.ForceCleanup());

            Assert.Equal(10, table.Count);
            Assert.True(table.TryGetValue(0, out var stillCached));
            stillCached!.Return();

            // Handler recovers, everything must evict down to empty.
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
                        // Modified then deleted while serializing.
                        // The delete must win, no resurrection.
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

            // A stale entry reference must miss, not throw, and must not rent.
            Assert.False(entry!.TryRentValue());
            Assert.Equal(0, obj.RentCount);

            // Same when another holder still rents the object.
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

            // Fill and clean, the stale slot must be skipped.
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
            // Count 3 over MaxSize 2, the caller is told to wait.
            Assert.True(table.Add(3, new TestCacheObject(3), handler));
        }

        /// <summary>
        /// A referenced page must never be removed from the cache.
        /// A reload would make a second copy that diverges.
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

            // Hold key 0 across a cleanup, like a tree iterator would.
            // Rent on the object, a table read would count as a hit.
            Assert.True(objects[0].TryRent());

            await table.ForceCleanup();

            // Selected and serialized but it must stay cached.
            // A later read returns the same object, not a copy.
            Assert.Contains(0, handler.EvictedKeys);
            Assert.True(table.TryGetValue(0, out var again));
            Assert.Same(objects[0], again);
            again!.Return();
            Assert.False(objects[0].RemovedFromCache);
            Assert.False(objects[0].Disposed);
            Assert.Equal(4, table.Count);
            // Requeued where it came from, being held is not proven reuse.
            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Small, entry!.Location);

            // Accounting survives the skipped eviction, cache plus our rent.
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
        /// The deep clean drops to MinSize, not to empty.
        /// Every other test runs with a floor of zero.
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

            // Below the threshold, nothing goes until the no-hits counter trips.
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

            // 5 entries is below the threshold of 7.
            // Nothing goes until the no-hits counter trips, then all of it does.
            for (var i = 0; i < 1001 && table.Count > 0; i++)
            {
                await table.ForceCleanup();
            }

            Assert.Equal(0, table.Count);
            Assert.Equal(5, handler.Evictions.Count);
            Assert.All(handler.Evictions, e => Assert.True(e.IsCleanup));
        }

        /// <summary>
        /// The deep clean should pay with unread pages, not proven ones.
        /// MaxSize 100 gives threshold 70 and window 2, MinSize 10 is the target.
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
            // Keys 0..4 count two spaced reuses, so the scan promotes them.
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

            // Over the threshold, so this pass evicts and promotes.
            await table.ForceCleanup();

            var afterEviction = table.GetQueueCountsForTests();
            Assert.Equal(5, afterEviction.MainCount);
            for (var i = 0; i < 5; i++)
            {
                Assert.True(table.TryPeekEntryForTests(i, out var promoted));
                Assert.Equal(S3FifoQueueLocation.Main, promoted.Location);
            }

            // Idle long enough for the deep clean to drop to MinSize.
            for (var i = 0; i < 1001 && table.Count > 10; i++)
            {
                await table.ForceCleanup();
            }

            Assert.Equal(10, table.Count);
            // The floor is for the reused pages, the small tail pays.
            var afterDeepClean = table.GetQueueCountsForTests();
            Assert.Equal(5, afterDeepClean.MainCount);
            Assert.Equal(5, afterDeepClean.SmallCount);
        }

        /// <summary>
        /// A held page survives eviction and returns to its own queue.
        /// Sending it to main would buy admission without the two reuse events.
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
            // Hold the small head directly, a table read counts as reuse.
            Assert.True(objects[0].TryRent());

            await table.ForceCleanup();

            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(S3FifoQueueLocation.Small, entry.Location);
            Assert.False(table.IsInGhostForTests(0));
            objects[0].Return();
        }

        /// <summary>
        /// A cache below its threshold keeps what it has.
        /// Trading resident pages for queue shares throws away capacity.
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
            // A hit keeps it off the idle deep clean.
            Assert.True(table.TryGetValue(0, out var hit));
            hit!.Return();

            await table.ForceCleanup();

            // 60 is over the 10% share but under the 70 threshold.
            Assert.Empty(handler.Evictions);
            Assert.Equal(60, table.Count);
            Assert.Equal(60, table.GetQueueCountsForTests().SmallCount);
        }

        /// <summary>
        /// A main page that stops being read loses frequency on turnover.
        /// Once aged out, eviction takes it before the small head.
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
            // Key 0 earns two spaced reuses and is promoted.
            Assert.True(table.TryGetValue(0, out var first));
            first!.Return();
            table.Add(71, new TestCacheObject(71), handler);
            table.Add(72, new TestCacheObject(72), handler);
            Assert.True(table.TryGetValue(0, out var second));
            second!.Return();

            await table.ForceCleanup();
            Assert.True(table.TryPeekEntryForTests(0, out var promoted));
            Assert.Equal(S3FifoQueueLocation.Main, promoted.Location);

            // Nothing reads key 0 again, so turnover ages it to zero.
            // At zero it is the first thing eviction takes.
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
        /// Aging is paced by turnover, no inserts means no aging.
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

            // No inserts, no turnover, so the hand does not move.
            for (var i = 0; i < 50; i++)
            {
                await table.ForceCleanup();
            }

            Assert.Equal(frequencyAfterPromotion, promoted.Frequency);
            Assert.Equal(S3FifoQueueLocation.Main, promoted.Location);
        }

        /// <summary>
        /// With the drain on, small is held at its share below the threshold.
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
            // A hit keeps it off the idle deep clean.
            Assert.True(table.TryGetValue(0, out var hit));
            hit!.Return();

            await table.ForceCleanup();

            Assert.Equal(10, table.GetQueueCountsForTests().SmallCount);
            Assert.NotEmpty(handler.Evictions);
        }

        /// <summary>
        /// The drain promotes into main, so it must take its aged out pages too.
        /// Otherwise main only ever grows below the threshold.
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
            // Key 0 earns two spaced reuses, the first drain promotes it.
            Assert.True(table.TryGetValue(0, out var first));
            first!.Return();
            table.Add(60, new TestCacheObject(60), handler);
            table.Add(61, new TestCacheObject(61), handler);
            Assert.True(table.TryGetValue(0, out var second));
            second!.Return();

            await table.ForceCleanup();
            Assert.True(table.TryPeekEntryForTests(0, out var promoted));
            Assert.Equal(S3FifoQueueLocation.Main, promoted.Location);

            // Nothing reads key 0 again, so turnover ages it out.
            // The drain takes it instead of paying from small.
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
        /// Re-adds a key only when it is no longer cached.
        /// A second instance for a cached key is a caller error.
        /// </summary>
        private static void ReAddIfEvicted(S3FifoTableSync table, long key, TestEvictHandler handler)
        {
            if (!table.TryPeekEntryForTests(key, out _))
            {
                table.Add(key, new TestCacheObject(key), handler);
            }
        }

        /// <summary>
        /// Off by default, the small queue keeps the fixed share.
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
        /// Aging is paced off the resident pages, not the ceiling.
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
            // Two hits past the 250 wide window promote key 0.
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
                // A hit on another key keeps it off the idle path.
                // Reading key 0 would pump its frequency back up.
                Assert.True(table.TryGetValue(10000 + (round * 1000), out var keepAlive));
                keepAlive!.Return();
                await table.ForceCleanup();
            }

            Assert.True(residentBefore < 3000, $"expected the drain to hold residency low, saw {residentBefore}");
            Assert.True(table.AgingStepsForTests > stepsBefore,
                "the main queue never aged, so aging is paced off the ceiling rather than the resident pages");
        }

        /// <summary>
        /// An owed sweep is cut to the laps that matter.
        /// </summary>
        [Fact]
        public void AnOwedAgingSweepIsCutToTheLapsThatCanStillChangeSomething()
        {
            // Three laps of 70000 pages is every point they could lose.
            // An owed sweep of a hundred laps is cut to those three.
            Assert.Equal(210_000, S3FifoTableSync.UsefulAgingSteps(stepsToRun: 7_800_000, liveMain: 70_000));

            // Exactly at the bound is kept whole.
            Assert.Equal(210_000, S3FifoTableSync.UsefulAgingSteps(stepsToRun: 210_000, liveMain: 70_000));

            // An ordinary pass earns a fraction of a lap and is left alone.
            Assert.Equal(140, S3FifoTableSync.UsefulAgingSteps(stepsToRun: 140, liveMain: 70_000));

            // Nothing queued to sweep.
            Assert.Equal(0, S3FifoTableSync.UsefulAgingSteps(stepsToRun: 5_000, liveMain: 0));
        }

        /// <summary>
        /// The aging hand rotates, a hot head shields nothing behind it.
        /// </summary>
        [Fact]
        public async Task AHotMainHeadDoesNotShieldTheAbandonedPagesBehindIt()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000);
            var handler = new TestEvictHandler();

            // Fill past the cleanup threshold so a pass actually scans the small queue.
            for (var i = 0; i < 1000; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // Warm 0..199 twice. Only inserts advance the clock.
            // The two reads must straddle a window of them to both count.
            for (var i = 0; i < 200; i++)
            {
                if (table.TryGetValue(i, out var v)) { v!.Return(); }
            }
            for (var i = 0; i < 100; i++)
            {
                table.Add(50000 + i, new TestCacheObject(i), handler);
            }
            for (var i = 0; i < 200; i++)
            {
                if (table.TryGetValue(i, out var v)) { v!.Return(); }
            }
            await table.ForceCleanup();

            var inMain = 0;
            for (var i = 0; i < 200; i++)
            {
                if (table.TryPeekEntryForTests(i, out var e) && e.Location == S3FifoQueueLocation.Main) { inMain++; }
            }
            var countsAfterPromote = table.GetQueueCountsForTests();

            // Only key 0 is read now, 1..199 are abandoned in main.
            var key = 100000L;
            for (var round = 0; round < 40; round++)
            {
                for (var i = 0; i < 500; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                if (table.TryGetValue(0, out var hot)) { hot!.Return(); }
                await table.ForceCleanup();
            }

            table.TryPeekEntryForTests(0, out var head);
            var survivors = 0;
            var zeroFreq = 0;
            for (var i = 1; i < 200; i++)
            {
                if (table.TryPeekEntryForTests(i, out var e))
                {
                    survivors++;
                    if (e.Frequency == 0) { zeroFreq++; }
                }
            }
            var counts = table.GetQueueCountsForTests();

            Assert.True(inMain > 100, $"the hot set never reached main, only {inMain} of 200 promoted");
            Assert.True(table.AgingStepsForTests > 0, "the aging hand never moved");

            // The one page still being read kept its place and its frequency.
            Assert.NotNull(head);
            Assert.Equal(S3FifoQueueLocation.Main, head!.Location);
            Assert.True(head.Frequency > 0, "the page being read every round still aged out");

            // Everything abandoned behind it drained to zero and was taken.
            Assert.Equal(0, survivors);
            Assert.Equal(0, zeroFreq);
            Assert.True(counts.MainCount < 10,
                $"main kept {counts.MainCount} pages when only one was still being read");
        }

        /// <summary>
        /// Ghost size follows the cache size only, never the queue shares.
        /// Sizing it from the split would feed the split its own output.
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
        /// The split moves a bounded amount per pass, whatever evidence arrives.
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
                // Bring back just evicted keys, a burst of evidence.
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
        /// The share rests on the reuse ratio, not on raw expiry counts.
        /// Hits forgive expiries, so a stream with modest reuse holds its share,
        /// and once the reuse stops the forgiveness drains and shrink resumes.
        /// </summary>
        [Fact]
        public async Task ModestReuseForgivesExpiriesUntilItStops()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000, adaptiveSmallQueueSize: true);
            var handler = new TestEvictHandler();
            var start = table.SmallTargetPermilleForTests;

            // Each round evicts about a thousand and wants twelve back, one in
            // eighty, above the resting ratio.
            var key = 0L;
            for (var round = 0; round < 40; round++)
            {
                var roundStart = key;
                for (var i = 0; i < 1000; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                await table.ForceCleanup();
                for (var i = 0; i < 12; i++)
                {
                    ReAddIfEvicted(table, roundStart + i, handler);
                }
                await table.ForceCleanup();
            }
            Assert.True(table.SmallTargetPermilleForTests >= start,
                $"a reused stream shrank the share to {table.SmallTargetPermilleForTests}");

            // The reuse stops, the junk stream shrinks the share.
            var held = table.SmallTargetPermilleForTests;
            for (var round = 0; round < 60; round++)
            {
                for (var i = 0; i < 1000; i++)
                {
                    table.Add(key++, new TestCacheObject(i), handler);
                }
                await table.ForceCleanup();
            }
            Assert.True(table.SmallTargetPermilleForTests < held,
                $"the share stayed at {table.SmallTargetPermilleForTests} after the reuse stopped");
        }

        /// <summary>
        /// Clear wipes the ghost queue, so movement earned from it goes too.
        /// The next pass must not move the split on discarded evidence.
        /// </summary>
        [Fact]
        public async Task ClearDropsPendingAdaptationEvidence()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000, adaptiveSmallQueueSize: true);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 1000; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            await table.ForceCleanup();

            // Ghost hits bank pending growth that no pass has applied yet.
            var banked = 0;
            for (var i = 0; i < 1000 && banked < 20; i++)
            {
                if (!table.TryPeekEntryForTests(i, out _) && table.IsInGhostForTests(i))
                {
                    table.Add(i, new TestCacheObject(i), handler);
                    banked++;
                }
            }
            Assert.True(banked >= 8, $"only {banked} ghost hits were banked");
            var before = table.SmallTargetPermilleForTests;

            table.Clear();

            // Activity so the next pass reaches the adaptation step.
            for (var i = 0; i < 10; i++)
            {
                table.Add(5000 + i, new TestCacheObject(i), handler);
            }
            Assert.True(table.TryGetValue(5000, out var hit));
            hit!.Return();
            await table.ForceCleanup();

            Assert.Equal(before, table.SmallTargetPermilleForTests);
        }

        /// <summary>
        /// Off by default, the split keeps the fixed share.
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
        /// Ghost entries aging out unused shrink the small queue share.
        /// Must work with an empty main queue, no reuse produces that.
        /// </summary>
        [Fact]
        public async Task UnusedGhostEntriesShrinkTheSmallQueue()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(1000, adaptiveSmallQueueSize: true);
            var handler = new TestEvictHandler();
            var startPermille = table.SmallTargetPermilleForTests;

            // Every key is touched once, so every eviction expires unused.
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
        /// The mirror, evictions wanted back again grow the small queue share.
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
        /// A full cache does not honour growth above the paper's share.
        /// Shrink below it still counts, that only makes small give up sooner.
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
        /// A grown target must not send eviction to main.
        /// A main scan grinds frequencies down and destroys main's evidence.
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

            // A hot set in main, read every round so it never ages out.
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

            // Keep small between the paper share and the grown target.
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
        /// A hit from the queue that rarely evicts weighs more, ARC's |B2|/|B1|.
        /// Counted in raw events the small queue would win everything.
        /// </summary>
        [Fact]
        public void RareQueueRegretsWeighProportionallyMore()
        {
            // A tenth of the ghost queue makes its hit worth ten.
            Assert.Equal(10, S3FifoTableSync.GhostHitWeightForTests(otherGhostEntries: 10000, ownGhostEntries: 1000));

            // The queue filling the ghost queue keeps a plain step.
            Assert.Equal(1, S3FifoTableSync.GhostHitWeightForTests(otherGhostEntries: 1000, ownGhostEntries: 10000));

            // Capped, and no memberships left is the extreme, not a divide by zero.
            Assert.Equal(16, S3FifoTableSync.GhostHitWeightForTests(otherGhostEntries: 10000000, ownGhostEntries: 100));
            Assert.Equal(16, S3FifoTableSync.GhostHitWeightForTests(otherGhostEntries: 10000, ownGhostEntries: 0));

            // A near empty ghost queue is not trusted to weight anything.
            Assert.Equal(1, S3FifoTableSync.GhostHitWeightForTests(otherGhostEntries: 40, ownGhostEntries: 4));
        }

        /// <summary>
        /// The shares must match what the ghost queue remembers.
        /// Wanted again, aged out and replaced all end a membership.
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

            // So does churning until the oldest age out, and re-evicting remembered keys.
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
        /// Clear must not mark entries removed or return the cache reference.
        /// Clients short circuit on !Removed to reach memory only pages.
        /// Marking them removed shows up as TestPullBucketOutput, "Segment not found".
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
        /// A delete between the requeue decision and the requeue must not resurrect it.
        /// That slot would carry no stale count and drift the accounting.
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

            // Key 0 is held, so its reclaim fails and it is requeued.
            // Rent on the object, a table read would count as a hit.
            Assert.True(objects[0].TryRent());

            // Key 1 holds the removal phase open after key 0 was requeued.
            objects[1].OnTryReclaimForEviction = () =>
            {
                gateEntered.Release();
                gateRelease.Wait();
            };

            var cleanup = Task.Run(() => table.ForceCleanup());
            Assert.True(await gateEntered.WaitAsync(5000));

            // Delete key 0 between its requeue decision and the requeue.
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
        /// Stopping the cleanup task must complete while eviction is paused.
        /// The parked wait must observe the cancellation.
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

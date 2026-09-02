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

namespace FlowtideDotNet.Storage.Tests.S3Fifo
{
    /// <summary>
    /// Regressions from the cache review, each fails before its fix.
    /// </summary>
    public class S3FifoRegressionTests
    {
        /// <summary>
        /// A tombstone at the main head must not open the eviction gate.
        /// </summary>
        [Fact]
        public async Task TombstoneAtMainHeadDoesNotDrainLiveMainPages()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100, drainSmallQueueEarly: true);
            var handler = new TestEvictHandler();

            // Two pages earn promotion into main, the rest are filler for the small queue.
            for (var i = 0; i < 22; i++)
            {
                var obj = new TestCacheObject(i);
                table.Add(i, obj, handler);
                if (i < 2)
                {
                    Assert.True(table.TryPeekEntryForTests(i, out var promoted));
                    // Two counted reuses, what TryEvictOneFromSmall requires to promote.
                    Volatile.Write(ref promoted!.Frequency, 2);
                }
            }

            Assert.True(table.TryGetValue(0, out var warm));
            warm!.Return();
            await table.ForceCleanup();

            var afterPromotion = table.GetQueueCountsForTests();
            Assert.Equal(2, afterPromotion.MainCount);
            Assert.True(table.TryPeekEntryForTests(0, out var head));
            Assert.True(table.TryPeekEntryForTests(1, out var hot));
            Assert.Equal(S3FifoQueueLocation.Main, hot!.Location);

            // Both main pages are now hot at max frequency.
            Volatile.Write(ref head!.Frequency, S3FifoCacheEntry.MaxFrequency);
            Volatile.Write(ref hot.Frequency, S3FifoCacheEntry.MaxFrequency);

            // The oldest main page is freed, so its tombstone sits at the main head.
            table.Delete(0);
            Assert.Equal(1, table.GetQueueCountsForTests().MainStale);

            // Sized so aging earns no steps and cannot reap the tombstone.
            for (var i = 100; i < 112; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }

            var agingBefore = table.AgingStepsForTests;
            Assert.True(table.TryGetValue(1, out var warm2));
            warm2!.Return();
            await table.ForceCleanup();
            Assert.Equal(agingBefore, table.AgingStepsForTests);

            // The hot main page survived, the drain paid from small.
            Assert.True(table.TryPeekEntryForTests(1, out var survivor));
            Assert.Equal(S3FifoQueueLocation.Main, survivor!.Location);
            Assert.Equal(S3FifoCacheEntry.MaxFrequency, Volatile.Read(ref survivor.Frequency));
            Assert.DoesNotContain(1L, handler.EvictedKeys);

            // Still reaped, so it does not park at the head.
            Assert.Equal(0, table.GetQueueCountsForTests().MainStale);
        }

        /// <summary>
        /// A page that came back during the pass gets no ghost record.
        /// </summary>
        [Fact]
        public async Task GhostRecordIsNotWrittenForAKeyThatCameBackDuringTheSamePass()
        {
            // Ghost horizon must outlast the batch, or the record is trimmed first.
            using var table = await S3FifoTestHelpers.CreateStoppedTable(40);
            var handler = new TestEvictHandler();
            var objects = new TestCacheObject[40];

            for (var i = 0; i < 40; i++)
            {
                objects[i] = new TestCacheObject(i);
                table.Add(i, objects[i], handler);
            }
            // 12 victims against a ghost capacity of 20, so nothing is trimmed this pass.
            Assert.True(table.GhostCapacityForTests > 12);

            // A later victim re-adds key 0, inside the ghost insert window.
            var readded = new TestCacheObject(0);
            var hookFired = false;
            var keyZeroWasRemoved = false;
            objects[1].OnTryReclaimForEviction = () =>
            {
                hookFired = true;
                if (!table.TryPeekEntryForTests(0, out _))
                {
                    keyZeroWasRemoved = true;
                    table.Add(0, readded, handler);
                }
            };

            await table.ForceCleanup();

            Assert.True(hookFired, "key 1 was never reclaimed, so the race window never opened");
            Assert.True(keyZeroWasRemoved, "key 0 was still resident when the later victim was reclaimed");
            // Resident again, so not remembered as evicted.
            Assert.True(table.TryPeekEntryForTests(0, out _));
            Assert.False(table.IsInGhostForTests(0));
        }

        /// <summary>
        /// Dead ghost records must not eat the horizon.
        /// </summary>
        [Fact]
        public async Task ReAdmittedGhostRecordsDoNotConsumeTheGhostHorizon()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(40);
            var handler = new TestEvictHandler();
            var capacity = table.GhostCapacityForTests;

            // Runs a pass and reports which keys left.
            async Task<List<long>> EvictPass()
            {
                var before = handler.EvictedKeys.Count;
                await table.ForceCleanup();
                return handler.EvictedKeys.Skip(before).ToList();
            }

            // An old cohort sits at the ghost head, never touched again.
            for (var i = 0; i < 32; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            var oldCohort = await EvictPass();
            Assert.NotEmpty(oldCohort);
            Assert.All(oldCohort, k => Assert.True(table.IsInGhostForTests(k)));

            // Push over the threshold once, re-admitting keeps it there.
            // Each re-admission drops a membership and strands its record.
            for (var i = 0; i < 4; i++)
            {
                table.Add(1000 + i, new TestCacheObject(1000 + i), handler);
            }
            for (var cycle = 0; cycle < 6; cycle++)
            {
                foreach (var key in await EvictPass())
                {
                    if (!table.TryPeekEntryForTests(key, out _))
                    {
                        table.Add(key, new TestCacheObject(key), handler);
                    }
                }
            }

            var membership = table.GhostMembershipForTests;
            Assert.Equal(membership.Remembered, (int)(membership.Small + membership.Main));
            // The horizon is not full, so no expiry was justified.
            Assert.True(
                membership.Remembered < capacity,
                $"live memberships {membership.Remembered} reached the horizon {capacity}, the scenario no longer isolates dead records");

            var survivingOld = oldCohort.Count(k => table.IsInGhostForTests(k));
            Assert.True(
                survivingOld == oldCohort.Count,
                $"{oldCohort.Count - survivingOld} of {oldCohort.Count} old ghost keys were expired to make room for dead records, with only {membership.Remembered} live memberships against a horizon of {capacity}");
        }

        /// <summary>
        /// Delete drops the rent, so it must flag the value.
        /// </summary>
        [Fact]
        public async Task DeleteFlagsTheValueSoAReAddTakesAFreshRent()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();

            var obj = new TestCacheObject(0);
            Assert.True(obj.TryRent());
            Assert.Equal(2, obj.RentCount);
            table.Add(0, obj, handler);

            table.Delete(0);
            Assert.True(obj.RemovedFromCache);
            Assert.Equal(1, obj.RentCount);
            Assert.False(obj.Disposed);

            // The same object comes back. The cache must take its own rent again.
            table.Add(0, obj, handler);
            Assert.Equal(2, obj.RentCount);
            Assert.False(obj.RemovedFromCache);

            // The holder lets go. The page stays alive because the cache holds a rent.
            obj.Return();
            Assert.False(obj.Disposed);
            Assert.Equal(0, obj.NegativeRentViolations);
            Assert.Equal(0, obj.RentAfterDisposeViolations);
        }

        /// <summary>
        /// A failed rent must withdraw the entry, not poison the key.
        /// </summary>
        [Fact]
        public async Task AddWithdrawsThePublishedEntryWhenTheRentFails()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(10);
            var handler = new TestEvictHandler();

            var dead = new TestCacheObject(0)
            {
                RemovedFromCache = true
            };
            dead.Return();
            Assert.True(dead.Disposed);

            Assert.Throws<InvalidOperationException>(() => table.Add(0, dead, handler));

            // The key must be free, not left holding an unrentable, unevictable entry.
            Assert.False(table.TryPeekEntryForTests(0, out _));
            Assert.Equal(0, table.Count);

            var fresh = new TestCacheObject(0);
            table.Add(0, fresh, handler);
            Assert.True(table.TryGetCacheValue(0, out var entry));
            entry!.Value.Return();
            Assert.Equal(1, table.Count);
        }

        /// <summary>
        /// A grow must not evict down to the old threshold.
        /// </summary>
        [Fact]
        public async Task MemoryDrivenGrowDoesNotEvictDownToTheOldThreshold()
        {
            // 1 GB budget with tiny pages, so the recomputed capacity is far above maxSize.
            var stats = new FixedMemoryStats(90_000);
            using var table = await S3FifoTestHelpers.CreateStoppedTable(
                maxSize: 1000,
                maxMemoryUsageInBytes: 1_000_000_000,
                memoryStats: stats);
            var handler = new TestEvictHandler();

            for (var i = 0; i < 900; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            // Above the old cleanupStart of 700, so a pass would plan 200 evictions.
            Assert.Equal(900, table.Count);

            await table.ForceCleanup();

            // The pass grew the capacity, so it must not evict against the threshold it replaced.
            Assert.Empty(handler.EvictedKeys);
            Assert.Equal(900, table.Count);
        }

        /// <summary>
        /// An idle cache already at MinSize must not collect on every re-armed deep clean.
        /// </summary>
        [Fact]
        public async Task IdleCacheAtMinSizeDoesNotCollectOnEveryReArmedDeepClean()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100, minSize: 10);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 20; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }

            // The first deep clean frees pages, so it may collect.
            for (var i = 0; i < 1001 && table.Count > 10; i++)
            {
                await table.ForceCleanup();
            }
            Assert.Equal(10, table.Count);
            var collectsAfterFirstDeepClean = table.CollectCallsForTests;
            Assert.Equal(1, collectsAfterFirstDeepClean);

            // Still idle at the floor, the next re-arm frees nothing.
            var evictionsAfterFirstDeepClean = handler.Evictions.Count;
            for (var i = 0; i < 1001; i++)
            {
                await table.ForceCleanup();
            }

            Assert.Equal(10, table.Count);
            Assert.Equal(evictionsAfterFirstDeepClean, handler.Evictions.Count);
            Assert.Equal(collectsAfterFirstDeepClean, table.CollectCallsForTests);
        }

        /// <summary>
        /// A checkpoint commit reads every dirty page, that is not reuse by the query.
        /// </summary>
        [Fact]
        public async Task CommitPathRentDoesNotCountAsReuse()
        {
            // Window of two, so the filler below opens it for key 0.
            using var table = await S3FifoTestHelpers.CreateStoppedTable(100);
            var handler = new TestEvictHandler();
            for (var i = 0; i < 4; i++)
            {
                table.Add(i, new TestCacheObject(i), handler);
            }
            Assert.True(table.TryPeekEntryForTests(0, out var entry));
            Assert.Equal(0, entry!.Frequency);

            Assert.True(table.TryGetValue(0, out var committed));
            committed!.Return();
            Assert.Equal(0, entry.Frequency);
            Assert.Equal(1, table.CommitCacheHitsForTests);

            // The read path still counts, so the window really was open.
            Assert.True(table.TryGetCacheValue(0, out var read));
            read!.Value.Return();
            Assert.Equal(1, entry.Frequency);
        }

        /// <summary>
        /// Disposing the table must complete callers parked on the eviction gate.
        /// </summary>
        [Fact]
        public async Task ParkedGateWaitersCompleteWhenTheTableIsDisposed()
        {
            var table = await S3FifoTestHelpers.CreateStoppedTable(2);
            var handler = new TestEvictHandler();
            for (long key = 1; key <= 3; key++)
            {
                table.Add(key, new TestCacheObject(key), handler);
            }
            // Over capacity, so Wait parks instead of returning early.
            Assert.True(table.Count > 2);

            // Stands in for a cleanup pass holding the gate at dispose time.
            var holder = table.PauseEvictionAsync();
            Assert.True(holder.IsCompletedSuccessfully);

            var parkedWait = table.Wait();
            var parkedPause = table.PauseEvictionAsync();
            Assert.False(parkedWait.IsCompleted);
            Assert.False(parkedPause.IsCompleted);

            table.Dispose();

            // Any completion counts, a cancelled park is fine, a park that never wakes is not.
            var timeout = Task.Delay(TimeSpan.FromSeconds(5));
            Assert.True(await Task.WhenAny(parkedWait, timeout) == parkedWait, "Wait stayed parked after Dispose");
            Assert.True(await Task.WhenAny(parkedPause, timeout) == parkedPause, "PauseEvictionAsync stayed parked after Dispose");
        }
    }
}

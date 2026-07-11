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
using System.Collections.Concurrent;
using System.Diagnostics;
using Xunit;

namespace FlowtideDotNet.Storage.Tests.S3Fifo
{
    /// <summary>
    /// Race condition tests for the S3-FIFO cache table.
    ///
    /// The concurrency contract being verified:
    /// * Reads (TryGetValue/TryGetCacheValue) may race freely with each other, with
    ///   eviction, with deletes and with adds of other keys.
    /// * Mutations of a single key (Add/Delete) are externally serialized per key, which
    ///   the tests respect by giving every key exactly one owner task.
    /// * Rent/return accounting must balance exactly: at the end every object must have a
    ///   rent count of 0, be disposed exactly once, and never have been rented after
    ///   disposal or returned below zero.
    /// </summary>
    public class S3FifoTableSyncConcurrencyTests
    {
        private static readonly TimeSpan StormDuration = TimeSpan.FromSeconds(2);

        private static void AssertCleanAccounting(IEnumerable<TestCacheObject> objects)
        {
            foreach (var obj in objects)
            {
                Assert.Equal(0, obj.RentCount);
                Assert.Equal(1, obj.DisposeCount);
                Assert.Equal(0, obj.RentAfterDisposeViolations);
                Assert.Equal(0, obj.NegativeRentViolations);
            }
        }

        private static async Task RunAll(ConcurrentQueue<Exception> failures, params Func<Task>[] bodies)
        {
            var tasks = bodies.Select(body => Task.Run(async () =>
            {
                try
                {
                    await body();
                }
                catch (Exception e)
                {
                    failures.Enqueue(e);
                }
            })).ToArray();
            await Task.WhenAll(tasks);
            if (!failures.IsEmpty)
            {
                throw new AggregateException(failures);
            }
        }

        /// <summary>
        /// Readers hammer the cache while owners reload evicted keys with fresh objects and
        /// two cleanup drivers (the built-in 10ms task plus a tight ForceCleanup loop) evict
        /// under them. Verifies the removed-check + rent handoff: a successful get must never
        /// observe a disposed object, and accounting must balance when everything stops.
        /// </summary>
        [Fact]
        public async Task ReadersRaceEvictionAndReload()
        {
            const int keyCount = 256;
            const int ownerCount = 4;
            const int readerCount = 4;

            using var table = S3FifoTestHelpers.CreateRunningTable(maxSize: 64);
            var handler = new TestEvictHandler
            {
                // Widen the eviction window so removal races are actually exercised.
                OnEvict = (_, _) => Thread.SpinWait(200)
            };
            var allObjects = new ConcurrentQueue<TestCacheObject>();
            var failures = new ConcurrentQueue<Exception>();
            var stop = new CancellationTokenSource(StormDuration);

            Func<Task> Owner(int ownerIndex) => async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    for (var key = ownerIndex; key < keyCount; key += ownerCount)
                    {
                        if (table.TryGetValue(key, out var cacheObject))
                        {
                            if (((TestCacheObject)cacheObject!).Disposed)
                            {
                                throw new InvalidOperationException($"Rented a disposed object for key {key}");
                            }
                            cacheObject.Return();
                        }
                        else
                        {
                            // Cache miss: reload, like a state client reading from storage.
                            var obj = new TestCacheObject(key);
                            allObjects.Enqueue(obj);
                            if (table.Add(key, obj, handler))
                            {
                                await table.Wait();
                            }
                        }
                    }
                }
            };

            Func<Task> Reader() => () =>
            {
                var random = new Random(Environment.CurrentManagedThreadId);
                while (!stop.IsCancellationRequested)
                {
                    var key = random.Next(keyCount);
                    if (random.Next(2) == 0)
                    {
                        if (table.TryGetValue(key, out var cacheObject))
                        {
                            if (((TestCacheObject)cacheObject!).Disposed)
                            {
                                throw new InvalidOperationException($"Rented a disposed object for key {key}");
                            }
                            cacheObject.Return();
                        }
                    }
                    else
                    {
                        // Same path the state client lookup tables use.
                        if (table.TryGetCacheValue(key, out var entry))
                        {
                            if (((TestCacheObject)entry!.Value).Disposed)
                            {
                                throw new InvalidOperationException($"Rented a disposed object for key {key}");
                            }
                            entry.Value.Return();
                        }
                    }
                }
                return Task.CompletedTask;
            };

            Func<Task> CleanupHammer() => async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await table.ForceCleanup();
                }
            };

            var bodies = new List<Func<Task>>();
            for (var i = 0; i < ownerCount; i++)
            {
                bodies.Add(Owner(i));
            }
            for (var i = 0; i < readerCount; i++)
            {
                bodies.Add(Reader());
            }
            bodies.Add(CleanupHammer());

            await RunAll(failures, bodies.ToArray());

            table.Dispose();
            Assert.True(allObjects.Count > 0);
            AssertCleanAccounting(allObjects);
        }

        /// <summary>
        /// Owners keep their own rented reference to every object and re-add the same object
        /// after it is evicted, exercising the RemovedFromCache re-rent handshake under
        /// reader and eviction races. Also verifies a reader can never receive a different
        /// object than the one the key's owner installed.
        /// </summary>
        [Fact]
        public async Task EvictedObjectsAreReAddedWhileStillRented()
        {
            const int keyCount = 128;
            const int ownerCount = 2;
            const int readerCount = 2;

            using var table = S3FifoTestHelpers.CreateRunningTable(maxSize: 32);
            var handler = new TestEvictHandler
            {
                OnEvict = (_, _) => Thread.SpinWait(200)
            };
            var objectsByKey = new TestCacheObject[keyCount];
            var failures = new ConcurrentQueue<Exception>();
            var stop = new CancellationTokenSource(StormDuration);

            for (var key = 0; key < keyCount; key++)
            {
                var obj = new TestCacheObject(key);
                // The owner holds its own reference for the whole test, on top of the
                // initial reference that the cache takes ownership of on Add.
                Assert.True(obj.TryRent());
                objectsByKey[key] = obj;
                table.Add(key, obj, handler);
            }

            Func<Task> Owner(int ownerIndex) => async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    for (var key = ownerIndex; key < keyCount; key += ownerCount)
                    {
                        if (table.TryGetValue(key, out var cacheObject))
                        {
                            if (!ReferenceEquals(cacheObject, objectsByKey[key]))
                            {
                                throw new InvalidOperationException($"Key {key} returned a foreign object");
                            }
                            cacheObject!.Return();
                        }
                        else
                        {
                            // Evicted while we still hold a reference: re-add the same object.
                            if (table.Add(key, objectsByKey[key], handler))
                            {
                                await table.Wait();
                            }
                        }
                    }
                }
            };

            Func<Task> Reader() => () =>
            {
                var random = new Random(Environment.CurrentManagedThreadId);
                while (!stop.IsCancellationRequested)
                {
                    var key = random.Next(keyCount);
                    if (table.TryGetValue(key, out var cacheObject))
                    {
                        if (((TestCacheObject)cacheObject!).Disposed)
                        {
                            throw new InvalidOperationException($"Rented a disposed object for key {key}");
                        }
                        cacheObject.Return();
                    }
                }
                return Task.CompletedTask;
            };

            Func<Task> CleanupHammer() => async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await table.ForceCleanup();
                }
            };

            var bodies = new List<Func<Task>>();
            for (var i = 0; i < ownerCount; i++)
            {
                bodies.Add(Owner(i));
            }
            for (var i = 0; i < readerCount; i++)
            {
                bodies.Add(Reader());
            }
            bodies.Add(CleanupHammer());

            await RunAll(failures, bodies.ToArray());

            // No object may have died while its owner held a reference.
            foreach (var obj in objectsByKey)
            {
                Assert.False(obj.Disposed);
                obj.Return();
            }

            table.Dispose();
            AssertCleanAccounting(objectsByKey);
        }

        /// <summary>
        /// Add/delete churn with readers and a cleanup loop, with a max size high enough that
        /// eviction pressure never triggers. Verifies stale queue slots left behind by deletes
        /// are compacted instead of growing without bound, and that delete accounting stays
        /// exact under reader races.
        /// </summary>
        [Fact]
        public async Task DeleteChurnDoesNotLeakQueueSlots()
        {
            const int keysPerOwner = 100;
            const int ownerCount = 4;
            const int readerCount = 2;

            using var table = S3FifoTestHelpers.CreateRunningTable(maxSize: 1_000_000);
            var handler = new TestEvictHandler();
            var allObjects = new ConcurrentQueue<TestCacheObject>();
            var failures = new ConcurrentQueue<Exception>();
            var stop = new CancellationTokenSource(StormDuration);
            long churnedKeys = 0;

            Func<Task> Owner(int ownerIndex) => () =>
            {
                var firstKey = ownerIndex * keysPerOwner;
                while (!stop.IsCancellationRequested)
                {
                    for (var key = firstKey; key < firstKey + keysPerOwner; key++)
                    {
                        var obj = new TestCacheObject(key);
                        allObjects.Enqueue(obj);
                        table.Add(key, obj, handler);
                        table.Delete(key);
                        Interlocked.Increment(ref churnedKeys);
                    }
                }
                return Task.CompletedTask;
            };

            Func<Task> Reader() => () =>
            {
                var random = new Random(Environment.CurrentManagedThreadId);
                while (!stop.IsCancellationRequested)
                {
                    var key = random.Next(ownerCount * keysPerOwner);
                    if (table.TryGetValue(key, out var cacheObject))
                    {
                        if (((TestCacheObject)cacheObject!).Disposed)
                        {
                            throw new InvalidOperationException($"Rented a disposed object for key {key}");
                        }
                        cacheObject.Return();
                    }
                }
                return Task.CompletedTask;
            };

            Func<Task> CleanupHammer() => async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await table.ForceCleanup();
                }
            };

            var bodies = new List<Func<Task>>();
            for (var i = 0; i < ownerCount; i++)
            {
                bodies.Add(Owner(i));
            }
            for (var i = 0; i < readerCount; i++)
            {
                bodies.Add(Reader());
            }
            bodies.Add(CleanupHammer());

            await RunAll(failures, bodies.ToArray());

            // Far more churn happened than any bounded queue could retain by accident.
            Assert.True(Interlocked.Read(ref churnedKeys) > 10_000, $"Only {churnedKeys} keys churned, test too short to be meaningful");

            // Compact whatever the final in-flight iterations left behind.
            await table.ForceCleanup();
            var counts = table.GetQueueCountsForTests();
            var totalSlots = counts.SmallCount + counts.MainCount;
            Assert.True(totalSlots < 4096, $"Queues retained {totalSlots} slots after churn of {churnedKeys} keys");

            table.Dispose();
            AssertCleanAccounting(allObjects);
        }

        /// <summary>
        /// The evict handler concurrently bumps versions of half the victims while they are
        /// being evicted (the same shape as a state client modifying a page during
        /// serialization), racing readers and reloads. Bumped victims must survive, and
        /// accounting must balance at the end.
        /// </summary>
        [Fact]
        public async Task VersionBumpsDuringEvictionRaceReadersAndReloads()
        {
            const int keyCount = 64;
            const int readerCount = 3;

            using var table = S3FifoTestHelpers.CreateRunningTable(maxSize: 32);
            var failures = new ConcurrentQueue<Exception>();
            var allObjects = new ConcurrentQueue<TestCacheObject>();
            var stop = new CancellationTokenSource(StormDuration);

            var handler = new TestEvictHandler();
            handler.OnEvict = (values, _) =>
            {
                foreach (var value in values)
                {
                    // Bump every other victim, marking its serialized copy stale.
                    if (value.Item1.Key % 2 == 0)
                    {
                        table.Add(value.Item1.Key, value.Item1.Value, handler);
                    }
                }
            };

            Func<Task> Owner() => async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    for (var key = 0; key < keyCount; key++)
                    {
                        if (table.TryGetValue(key, out var cacheObject))
                        {
                            cacheObject!.Return();
                        }
                        else
                        {
                            var obj = new TestCacheObject(key);
                            allObjects.Enqueue(obj);
                            if (table.Add(key, obj, handler))
                            {
                                await table.Wait();
                            }
                        }
                    }
                }
            };

            Func<Task> Reader() => () =>
            {
                var random = new Random(Environment.CurrentManagedThreadId);
                while (!stop.IsCancellationRequested)
                {
                    var key = random.Next(keyCount);
                    if (table.TryGetValue(key, out var cacheObject))
                    {
                        if (((TestCacheObject)cacheObject!).Disposed)
                        {
                            throw new InvalidOperationException($"Rented a disposed object for key {key}");
                        }
                        cacheObject.Return();
                    }
                }
                return Task.CompletedTask;
            };

            Func<Task> CleanupHammer() => async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await table.ForceCleanup();
                }
            };

            var bodies = new List<Func<Task>> { Owner(), CleanupHammer() };
            for (var i = 0; i < readerCount; i++)
            {
                bodies.Add(Reader());
            }

            await RunAll(failures, bodies.ToArray());

            table.Dispose();
            Assert.True(allObjects.Count > 0);
            AssertCleanAccounting(allObjects);
        }

        /// <summary>
        /// Writers race large chunked victim selections: owners continuously add fresh keys
        /// to a table whose eviction batches exceed the selection operation budget, so
        /// Add/Delete interleave with selection at chunk boundaries. Verifies chunked
        /// selection keeps exact rent accounting under concurrent mutation.
        /// </summary>
        [Fact]
        public async Task LargeEvictionBatchesRaceWriters()
        {
            const int ownerCount = 3;

            // cleanupStart = 1400, so each cleanup under pressure selects hundreds of
            // victims, spanning multiple 256-operation selection chunks.
            using var table = S3FifoTestHelpers.CreateRunningTable(maxSize: 2000);
            var handler = new TestEvictHandler();
            var allObjects = new ConcurrentQueue<TestCacheObject>();
            var failures = new ConcurrentQueue<Exception>();
            var stop = new CancellationTokenSource(StormDuration);

            Func<Task> Owner(int ownerIndex) => () =>
            {
                // Each owner adds an endless stream of fresh keys in its own key space.
                var key = (long)ownerIndex * 1_000_000_000;
                while (!stop.IsCancellationRequested)
                {
                    var obj = new TestCacheObject(key);
                    allObjects.Enqueue(obj);
                    table.Add(key, obj, handler);
                    key++;
                    // Delete a fraction shortly after adding to mix stale slots into the
                    // queues that selection must skip over.
                    if ((key & 7) == 0)
                    {
                        table.Delete(key - 1);
                    }
                }
                return Task.CompletedTask;
            };

            Func<Task> CleanupHammer() => async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await table.ForceCleanup();
                }
            };

            var bodies = new List<Func<Task>>();
            for (var i = 0; i < ownerCount; i++)
            {
                bodies.Add(Owner(i));
            }
            bodies.Add(CleanupHammer());

            await RunAll(failures, bodies.ToArray());

            Assert.True(table.SelectionLockAcquisitionsForTests > 1, "Storm never exercised chunked selection");
            table.Dispose();
            Assert.True(allObjects.Count > 1000);
            AssertCleanAccounting(allObjects);
        }

        /// <summary>
        /// Deterministic two-thread interleaving storm on the removed-check + rent handoff:
        /// one thread continuously evicts a single key through cleanup while another reads
        /// it and re-adds it on miss, crossing the tight window between the removed check
        /// and TryRent many times. The reader loop is adaptive: it runs until enough
        /// evict/re-add cycles have actually been observed (a fixed iteration count can
        /// finish before the thread pool ever schedules the evictor).
        /// </summary>
        [Fact]
        public async Task SingleKeyEvictReadStorm()
        {
            using var table = await S3FifoTestHelpers.CreateStoppedTable(maxSize: 0);
            var handler = new TestEvictHandler();
            var allObjects = new ConcurrentQueue<TestCacheObject>();
            var failures = new ConcurrentQueue<Exception>();
            const long key = 42;
            const int targetEvictReAddCycles = 200;
            var stop = false;

            // maxSize 0 makes cleanupStart 0, so every cleanup evicts everything present.
            Func<Task> Evictor() => async () =>
            {
                while (!Volatile.Read(ref stop))
                {
                    await table.ForceCleanup();
                }
            };

            Func<Task> OwnerReader() => () =>
            {
                var stopwatch = Stopwatch.StartNew();
                var iteration = 0L;
                while (true)
                {
                    if (table.TryGetValue(key, out var cacheObject))
                    {
                        if (((TestCacheObject)cacheObject!).Disposed)
                        {
                            throw new InvalidOperationException("Rented a disposed object");
                        }
                        cacheObject.Return();
                    }
                    else
                    {
                        var obj = new TestCacheObject(key);
                        allObjects.Enqueue(obj);
                        table.Add(key, obj, handler);
                    }
                    iteration++;
                    if ((iteration & 1023) == 0 &&
                        (allObjects.Count > targetEvictReAddCycles || stopwatch.Elapsed > TimeSpan.FromSeconds(15)))
                    {
                        break;
                    }
                }
                Volatile.Write(ref stop, true);
                return Task.CompletedTask;
            };

            await RunAll(failures, Evictor(), OwnerReader());

            table.Dispose();
            Assert.True(allObjects.Count > 1, "The key was never evicted and re-added, storm did not exercise the race");
            AssertCleanAccounting(allObjects);
        }
    }
}

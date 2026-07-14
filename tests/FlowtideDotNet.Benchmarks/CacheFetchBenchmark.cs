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

using BenchmarkDotNet.Attributes;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Benchmarks
{
    /// <summary>
    /// Measures the cost of a cache hit on the S3-FIFO table.
    /// The production read paths are lock-free, the Model benchmarks keep the old designs.
    /// All benchmarks are single-threaded, which matches the engine today.
    /// Compare LockEnterExit vs InterlockedIncrement, the two TableHit paths and the two FastPaths.
    /// </summary>
    [InProcess]
    [MemoryDiagnoser]
    public class CacheFetchBenchmark
    {
        private sealed class BenchCacheObject : ICacheObject
        {
            private int _rentCount = 1;

            public bool RemovedFromCache { get; set; }

            public bool TryRent()
            {
                var local = Volatile.Read(ref _rentCount);
                while (true)
                {
                    if (local == 0)
                    {
                        return false;
                    }
                    var observed = Interlocked.CompareExchange(ref _rentCount, local + 1, local);
                    if (observed == local)
                    {
                        return true;
                    }
                    local = observed;
                }
            }

            public int RentCount => Volatile.Read(ref _rentCount);

            public void Return()
            {
                Interlocked.Decrement(ref _rentCount);
            }

            public bool TryReclaimForEviction()
            {
                return Interlocked.CompareExchange(ref _rentCount, 0, 1) == 1;
            }

            public void EnterWriteLock() => Monitor.Enter(this);

            public void ExitWriteLock() => Monitor.Exit(this);
        }

        private sealed class NoopEvictHandler : ICacheEvictHandler
        {
            public void Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
            {
            }
        }

        private sealed class NoMemoryStats : IMemoryAllocationStats
        {
            public long GetAllocatedMemory() => 0;
        }

        private struct PairSlot
        {
            public long Key;
            public S3FifoCacheEntry? Value;
        }

        private const long Key = 42;

        private readonly object _plainLock = new object();
        private readonly object _clientLock = new object();
        private S3FifoTableSync _table = null!;
        private S3FifoCacheEntry _entry = null!;
        private PairSlot[] _pairSlots = null!;
        private S3FifoCacheEntry?[] _refSlots = null!;
        private long _hits;
        private long _plainCounter;

        [GlobalSetup]
        public void Setup()
        {
            _table = new S3FifoTableSync(new CacheTableOptions("", NullLogger.Instance, new Meter(Guid.NewGuid().ToString()), new NoMemoryStats())
            {
                MaxSize = 100_000,
                MinSize = 0
            });
            _table.StopCleanupTask().GetAwaiter().GetResult();
            _table.Add(Key, new BenchCacheObject(), new NoopEvictHandler());
            if (!_table.TryPeekEntryForTests(Key, out var entry))
            {
                throw new InvalidOperationException("Setup failed");
            }
            _entry = entry!;

            // Replica of SyncStateClient._lookupTable (key + reference pair, needs the client lock).
            _pairSlots = new PairSlot[1009];
            _pairSlots[Key % 1009] = new PairSlot { Key = Key, Value = _entry };

            // Single-reference slot design, atomically readable without any lock.
            // The key is validated on the entry object itself.
            _refSlots = new S3FifoCacheEntry?[1024];
            _refSlots[Key & (_refSlots.Length - 1)] = _entry;
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _table.Dispose();
        }

        [Benchmark(Baseline = true)]
        public long LockEnterExit()
        {
            lock (_plainLock)
            {
                return _plainCounter;
            }
        }

        [Benchmark]
        public long InterlockedIncrement()
        {
            return Interlocked.Increment(ref _plainCounter);
        }

        [Benchmark]
        public bool TableHit_Production()
        {
            if (_table.TryGetValue(Key, out var cacheObject))
            {
                cacheObject!.Return();
                return true;
            }
            return false;
        }

        [Benchmark]
        public bool TableHit_ManualLockFreeModel()
        {
            // Same dictionary lookup, but the removed-check + rent handoff and the
            // frequency bump run without the entry lock.
            if (_table.TryPeekEntryForTests(Key, out var entry))
            {
                if (!Volatile.Read(ref entry!.Removed) && entry.Value.TryRent())
                {
                    BumpFrequency(entry);
                    Interlocked.Increment(ref _hits);
                    entry.Value.Return();
                    return true;
                }
            }
            return false;
        }

        [Benchmark]
        public bool FastPath_LegacyLockedModel()
        {
            // Faithful replica of the SyncStateClient.GetValue fast path.
            S3FifoCacheEntry? rented = null;
            lock (_clientLock)
            {
                var modLookup = Key % _pairSlots.Length;
                if (_pairSlots[modLookup].Key == Key)
                {
                    var entry = _pairSlots[modLookup].Value!;
                    lock (entry)
                    {
                        if (!entry.Removed)
                        {
                            if (!entry.Value.TryRent())
                            {
                                throw new InvalidOperationException("Could not rent value from cache");
                            }
                            entry.Frequency = Math.Min(entry.Frequency + 1, S3FifoCacheEntry.MaxFrequency);
                            rented = entry;
                        }
                    }
                }
            }
            if (rented != null)
            {
                rented.Value.Return();
                return true;
            }
            return false;
        }

        [Benchmark]
        public bool FastPath_Production()
        {
            var entry = Volatile.Read(ref _refSlots[Key & (_refSlots.Length - 1)]);
            if (entry != null && entry.Key == Key)
            {
                if (!Volatile.Read(ref entry.Removed) && entry.Value.TryRent())
                {
                    BumpFrequency(entry);
                    entry.Value.Return();
                    return true;
                }
            }
            return false;
        }

        private static void BumpFrequency(S3FifoCacheEntry entry)
        {
            // Capped CAS loop. On a hot entry the frequency saturates, so the cost is one
            // volatile read.
            var current = Volatile.Read(ref entry.Frequency);
            while (current < S3FifoCacheEntry.MaxFrequency)
            {
                var observed = Interlocked.CompareExchange(ref entry.Frequency, current + 1, current);
                if (observed == current)
                {
                    break;
                }
                current = observed;
            }
        }
    }
}

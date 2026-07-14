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
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Tests.S3Fifo
{
    /// <summary>
    /// Cache object with the same rent semantics as a B+ tree node.
    /// Rent count starts at 1, TryRent fails at 0, Return disposes at 0.
    /// Tracks invariant violations so concurrency tests can assert on them.
    /// </summary>
    internal class TestCacheObject : ICacheObject
    {
        private int _rentCount = 1;
        private int _disposeCount;
        private int _rentAfterDisposeCount;
        private int _negativeRentCount;

        public TestCacheObject(long id)
        {
            Id = id;
        }

        public long Id { get; }

        public int RentCount => Volatile.Read(ref _rentCount);

        public int DisposeCount => Volatile.Read(ref _disposeCount);

        public bool Disposed => DisposeCount > 0;

        /// <summary>
        /// Number of times a rent succeeded on an already disposed object. Must stay 0.
        /// </summary>
        public int RentAfterDisposeViolations => Volatile.Read(ref _rentAfterDisposeCount);

        /// <summary>
        /// Number of times the rent count went below zero (more returns than rents). Must stay 0.
        /// </summary>
        public int NegativeRentViolations => Volatile.Read(ref _negativeRentCount);

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
                    if (Volatile.Read(ref _disposeCount) > 0)
                    {
                        Interlocked.Increment(ref _rentAfterDisposeCount);
                    }
                    return true;
                }
                local = observed;
            }
        }

        public void Return()
        {
            var value = Interlocked.Decrement(ref _rentCount);
            if (value == 0)
            {
                Interlocked.Increment(ref _disposeCount);
            }
            else if (value < 0)
            {
                Interlocked.Increment(ref _negativeRentCount);
            }
        }

        public bool TryReclaimForEviction()
        {
            if (Interlocked.CompareExchange(ref _rentCount, 0, 1) == 1)
            {
                Interlocked.Increment(ref _disposeCount);
                return true;
            }
            return false;
        }

        public void EnterWriteLock()
        {
            Monitor.Enter(this);
        }

        public void ExitWriteLock()
        {
            Monitor.Exit(this);
        }
    }

    internal class TestEvictHandler : ICacheEvictHandler
    {
        public ConcurrentQueue<(long Key, bool IsCleanup)> Evictions { get; } = new();

        /// <summary>
        /// Invoked on the eviction thread before evictions are recorded.
        /// Mirrors where a state client would serialize the value.
        /// </summary>
        public Action<List<(S3FifoCacheEntry, long)>, bool>? OnEvict { get; set; }

        public List<long> EvictedKeys => Evictions.Select(e => e.Key).ToList();

        public void Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
        {
            OnEvict?.Invoke(valuesToEvict, isCleanup);
            foreach (var value in valuesToEvict)
            {
                Evictions.Enqueue((value.Item1.Key, isCleanup));
            }
        }
    }

    internal class ZeroMemoryStats : IMemoryAllocationStats
    {
        public long GetAllocatedMemory()
        {
            return 0;
        }
    }

    internal static class S3FifoTestHelpers
    {
        public static S3FifoTableSync CreateRunningTable(int maxSize, int minSize = 0)
        {
            return new S3FifoTableSync(new CacheTableOptions("", NullLogger.Instance, new Meter(Guid.NewGuid().ToString()), new ZeroMemoryStats())
            {
                MaxSize = maxSize,
                MinSize = minSize
            });
        }

        /// <summary>
        /// Creates a table with the background cleanup task stopped, so tests drive
        /// eviction deterministically through ForceCleanup.
        /// </summary>
        public static async Task<S3FifoTableSync> CreateStoppedTable(int maxSize, int minSize = 0)
        {
            var table = CreateRunningTable(maxSize, minSize);
            await table.StopCleanupTask();
            return table;
        }
    }
}

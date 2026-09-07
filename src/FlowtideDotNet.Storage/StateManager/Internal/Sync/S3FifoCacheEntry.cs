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

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    /// <summary>
    /// Which queue an entry is in. Guarded by the queue lock.
    /// </summary>
    internal enum S3FifoQueueLocation : byte
    {
        /// <summary>
        /// Not in any queue, just created or picked as a victim.
        /// </summary>
        None = 0,
        Small = 1,
        Main = 2
    }

    /// <summary>
    /// Stable handle for a value in the cache table.
    /// State clients cache these, so one instance must represent a key while it is cached.
    /// Reads are lock-free through TryRentValue. Frequency uses Volatile and Interlocked.
    /// Version and Removed are guarded by lock(entry), Location by the queue lock.
    /// An entry lock may be taken inside the queue lock, never the reverse.
    /// </summary>
    internal sealed class S3FifoCacheEntry
    {
        /// <summary>
        /// Max access frequency per entry.
        /// With spaced counting each point is one window of real reuse. Kept shallow so an
        /// abandoned page drains in a few scan laps.
        /// </summary>
        public const int MaxFrequency = 3;

        private readonly S3FifoCorrelationClock _correlationClock;

        /// <summary>
        /// Clock value at the insertion or the last counted hit.
        /// A hit within the window of this stamp is correlated and does not count.
        /// Always valid, the constructor stamps it so a hit landing before the insert
        /// finishes its bookkeeping is treated as part of the insertion burst.
        /// Written at insertion and by RecordAccess itself, read lock-free on the hit path.
        /// </summary>
        private long _lastCountedStamp;

        public S3FifoCacheEntry(long key, ICacheObject value, ICacheEvictHandler evictHandler, S3FifoCorrelationClock correlationClock)
        {
            Key = key;
            Value = value;
            EvictHandler = evictHandler;
            _correlationClock = correlationClock;
            _lastCountedStamp = correlationClock.CurrentSequence();
        }

        public long Key { get; }

        public ICacheObject Value { get; }

        public ICacheEvictHandler EvictHandler { get; }

        /// <summary>
        /// Bumped when the value is modified. Eviction snapshots it to skip a stale removal.
        /// Guarded by lock(entry).
        /// </summary>
        public long Version;

        /// <summary>
        /// Access counter from 0 to MaxFrequency, bumped on every hit.
        /// Uses Volatile and Interlocked, no lock.
        /// </summary>
        public int Frequency;

        /// <summary>
        /// Set true before the cache reference is returned. Never resets.
        /// </summary>
        public bool Removed;

        /// <summary>
        /// Which queue the entry sits in. Guarded by the queue lock.
        /// </summary>
        public S3FifoQueueLocation Location;

        /// <summary>
        /// Lock-free rent and access record. False is treated as a cache miss.
        /// A rent only fails once the count reaches zero, which is when eviction claimed the
        /// sole reference, so the caller reloads as the new owner. Count corruption is caught
        /// in Add instead.
        /// </summary>
        public bool TryRentValue()
        {
            if (!TryRentValueWithoutAccess())
            {
                return false;
            }
            RecordAccess();
            return true;
        }

        /// <summary>
        /// The same rent for the commit path. A checkpoint reads every dirty page, that is not reuse.
        /// </summary>
        public bool TryRentValueWithoutAccess()
        {
            if (Volatile.Read(ref Removed))
            {
                return false;
            }
            return Value.TryRent();
        }

        /// <summary>
        /// Stamps the entry at insertion, so the insertion burst does not count as reuse.
        /// </summary>
        public void SetCountStamp(long sequence)
        {
            Volatile.Write(ref _lastCountedStamp, sequence);
        }

        /// <summary>
        /// Saturating lock-free frequency bump with spaced counting.
        /// A hit only counts when the entry has aged past the window since its insertion or
        /// its last counted hit, so a burst of accesses close together counts as one reuse
        /// event instead of saturating the frequency instantly. The stamp only moves on a
        /// counted hit, a hammered entry still earns frequency once per window.
        /// One window for all entries. Racing counted hits can both pass the check, that is
        /// accepted heuristic noise.
        /// </summary>
        public void RecordAccess()
        {
            var current = Volatile.Read(ref Frequency);
            if (current >= MaxFrequency)
            {
                return;
            }
            if (_correlationClock.IsCorrelated(Volatile.Read(ref _lastCountedStamp)))
            {
                return;
            }
            while (current < MaxFrequency)
            {
                var observed = Interlocked.CompareExchange(ref Frequency, current + 1, current);
                if (observed == current)
                {
                    Volatile.Write(ref _lastCountedStamp, _correlationClock.CurrentSequence());
                    break;
                }
                current = observed;
            }
        }
    }
}

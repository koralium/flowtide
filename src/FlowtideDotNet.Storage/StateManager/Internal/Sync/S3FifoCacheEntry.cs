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
        /// Max access frequency per entry, 2 bits.
        /// </summary>
        public const int MaxFrequency = 3;

        /// <summary>
        /// Set when the entry is not in the small queue, so every hit counts.
        /// </summary>
        private const long NotInSmallQueue = -1;

        private readonly S3FifoCorrelationClock _correlationClock;

        /// <summary>
        /// Small queue enqueue sequence, or NotInSmallQueue.
        /// Written under the queue lock, read lock-free on the hit path.
        /// </summary>
        private long _smallQueueStamp = NotInSmallQueue;

        public S3FifoCacheEntry(long key, ICacheObject value, ICacheEvictHandler evictHandler, S3FifoCorrelationClock correlationClock)
        {
            Key = key;
            Value = value;
            EvictHandler = evictHandler;
            _correlationClock = correlationClock;
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
            if (Volatile.Read(ref Removed))
            {
                return false;
            }
            if (!Value.TryRent())
            {
                return false;
            }
            RecordAccess();
            return true;
        }

        /// <summary>
        /// Stamps the entry when it enters the small queue.
        /// Called under the queue lock on insert and on the failed-evict requeue.
        /// </summary>
        public void SetSmallQueueStamp(long sequence)
        {
            Volatile.Write(ref _smallQueueStamp, sequence);
        }

        /// <summary>
        /// Clears the stamp when the entry leaves the small queue.
        /// From then on every hit counts.
        /// </summary>
        public void ClearSmallQueueStamp()
        {
            Volatile.Write(ref _smallQueueStamp, NotInSmallQueue);
        }

        /// <summary>
        /// Saturating lock-free frequency bump.
        /// Correlated hits while young in the small queue do not count, so an insertion burst
        /// cannot earn promotion. Hot entries return before the stamp read, cost unchanged.
        /// </summary>
        public void RecordAccess()
        {
            var current = Volatile.Read(ref Frequency);
            if (current >= MaxFrequency)
            {
                return;
            }
            var stamp = Volatile.Read(ref _smallQueueStamp);
            if (stamp != NotInSmallQueue && _correlationClock.IsCorrelated(stamp))
            {
                return;
            }
            while (current < MaxFrequency)
            {
                var observed = Interlocked.CompareExchange(ref Frequency, current + 1, current);
                if (observed == current)
                {
                    break;
                }
                current = observed;
            }
        }
    }
}

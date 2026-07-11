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
    /// Which S3-FIFO queue an entry currently resides in.
    /// Guarded by the cache table's queue lock.
    /// </summary>
    internal enum S3FifoQueueLocation : byte
    {
        /// <summary>
        /// Not present in any queue, either because it was just created,
        /// or because it was dequeued as an eviction candidate.
        /// </summary>
        None = 0,
        Small = 1,
        Main = 2
    }

    /// <summary>
    /// A stable handle for a value stored in <see cref="S3FifoTableSync"/>.
    /// State clients cache references to these entries in their local lookup tables,
    /// so the same instance must represent a key for the entire time it is cached.
    ///
    /// Concurrency rules:
    /// * Reads are lock-free: <see cref="TryRentValue"/> is the only correct way to rent
    ///   through an entry. It relies on <see cref="ICacheObject.TryRent"/> failing once the
    ///   count reaches zero, and on every code path that returns the cache's reference
    ///   setting <see cref="Removed"/> (volatile) before calling Return, so a failed rent
    ///   on a non-removed entry can only mean reference-count corruption.
    /// * <see cref="Frequency"/> is accessed with Volatile/Interlocked only.
    /// * <see cref="Version"/> is guarded by lock(entry); only mutators and eviction touch it.
    /// * <see cref="Removed"/> is written under lock(entry) with a volatile write, and may be
    ///   read either under the lock or with a volatile read.
    /// * <see cref="Location"/> is guarded by the table's queue lock.
    /// * An entry lock may be taken while holding the queue lock, never the reverse.
    /// </summary>
    internal sealed class S3FifoCacheEntry
    {
        /// <summary>
        /// Maximum access frequency tracked per entry, as in the S3-FIFO paper (2 bits).
        /// </summary>
        public const int MaxFrequency = 3;

        public S3FifoCacheEntry(long key, ICacheObject value, ICacheEvictHandler evictHandler)
        {
            Key = key;
            Value = value;
            EvictHandler = evictHandler;
        }

        public long Key { get; }

        public ICacheObject Value { get; }

        public ICacheEvictHandler EvictHandler { get; }

        /// <summary>
        /// Incremented every time the value behind the key is modified (AddOrUpdate on an existing key).
        /// Eviction snapshots the version when selecting a victim and skips the removal if the
        /// version changed while the value was being serialized, so a newer modification is never lost.
        /// Guarded by lock(entry).
        /// </summary>
        public long Version;

        /// <summary>
        /// Saturating access counter in [0, <see cref="MaxFrequency"/>].
        /// Incremented on every cache hit, consumed by the eviction scan
        /// (promotion out of the small queue, reinsertion in the main queue).
        /// Accessed with Volatile/Interlocked only; readers bump it without any lock.
        /// </summary>
        public int Frequency;

        /// <summary>
        /// Set to true before the cache's reference is returned, with a volatile write under
        /// lock(entry). Once true it never becomes false again; a re-add of the key creates
        /// a new entry.
        /// </summary>
        public bool Removed;

        /// <summary>
        /// The queue this entry currently sits in. Guarded by the table's queue lock.
        /// </summary>
        public S3FifoQueueLocation Location;

        /// <summary>
        /// Lock-free read handoff: rents the value and records the access. Returns false when
        /// the entry is being or has been evicted, which callers treat as a cache miss.
        ///
        /// Safety: every path that returns the cache's reference (eviction removal, delete,
        /// dispose) sets <see cref="Removed"/> before calling Return. A failed TryRent means
        /// the count reached zero, which is only possible after such a Return, so Removed must
        /// be observable by then (the failed CAS is a full fence). A failed rent on a
        /// non-removed entry therefore indicates reference-count corruption and throws
        /// instead of being silently treated as a miss.
        /// </summary>
        public bool TryRentValue()
        {
            if (Volatile.Read(ref Removed))
            {
                return false;
            }
            if (!Value.TryRent())
            {
                if (!Volatile.Read(ref Removed))
                {
                    throw new InvalidOperationException("Could not rent value from cache");
                }
                return false;
            }
            RecordAccess();
            return true;
        }

        /// <summary>
        /// Saturating lock-free frequency bump. On a hot entry the counter stays at the cap,
        /// so the steady-state cost is a single volatile read.
        /// </summary>
        public void RecordAccess()
        {
            var current = Volatile.Read(ref Frequency);
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

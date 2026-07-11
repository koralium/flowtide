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
    /// Locking rules:
    /// * <see cref="Version"/>, <see cref="Frequency"/> and <see cref="Removed"/> are guarded
    ///   by locking the entry itself (lock(entry)). Checking <see cref="Removed"/> and calling
    ///   <see cref="ICacheObject.TryRent"/> must happen inside the same lock scope so that a
    ///   rent can never succeed after eviction has returned the cache's reference.
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
        /// Guarded by lock(entry).
        /// </summary>
        public int Frequency;

        /// <summary>
        /// Set to true, under lock(entry), before the cache's reference is returned.
        /// Readers must check this flag inside lock(entry) before renting.
        /// Once true it never becomes false again; a re-add of the key creates a new entry.
        /// </summary>
        public bool Removed;

        /// <summary>
        /// The queue this entry currently sits in. Guarded by the table's queue lock.
        /// </summary>
        public S3FifoQueueLocation Location;
    }
}

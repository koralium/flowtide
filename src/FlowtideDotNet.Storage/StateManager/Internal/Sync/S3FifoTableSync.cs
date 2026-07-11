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
using FlowtideDotNet.Storage.Mimalloc;
using FlowtideDotNet.Storage.Utils;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Runtime.ExceptionServices;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    /// <summary>
    /// In-memory page cache shared by all state clients in a stream, using the S3-FIFO
    /// eviction algorithm ("FIFO queues are all you need for cache eviction", SOSP'23)
    /// in place of the previous LRU/CLOCK hybrid.
    ///
    /// Structure:
    /// * A concurrent dictionary provides lock-free key lookup on the read path.
    /// * A small FIFO queue (~10% of max size) receives all newly inserted keys and
    ///   filters out one-hit wonders quickly.
    /// * A main FIFO queue holds entries promoted from the small queue (accessed more
    ///   than once before reaching the small queue's head) and entries whose key was
    ///   found in the ghost queue on insert.
    /// * A ghost queue remembers only the keys recently evicted from the small queue,
    ///   so a quickly re-inserted key is admitted directly into the main queue.
    ///
    /// Reads never touch the queues (lazy promotion): a hit only bumps the entry's
    /// saturating frequency counter, so the hot fetch path is a dictionary read plus a
    /// short entry lock. All queue maintenance happens on insert and inside the
    /// background cleanup task.
    ///
    /// Locking protocol (required to avoid deadlocks):
    /// * lock(entry) guards Removed/Frequency/Version and makes the removed-check plus
    ///   TryRent handoff atomic against eviction.
    /// * m_queueLock guards the three queues, the stale counters and entry.Location.
    /// * An entry lock may be taken while holding m_queueLock, never the reverse.
    ///
    /// Mutations of a single key (Add/Delete) must be externally serialized per key;
    /// state clients guarantee this with their client lock. Reads may race freely with
    /// everything, including eviction.
    /// </summary>
    internal class S3FifoTableSync : IDisposable
    {
        private readonly struct GhostRecord
        {
            public GhostRecord(long key, long sequence)
            {
                Key = key;
                Sequence = sequence;
            }

            public long Key { get; }

            /// <summary>
            /// Identifies which insertion into the ghost queue this record belongs to, so a
            /// stale ring slot (key re-added and evicted again) does not remove the newer
            /// membership record when it reaches the head.
            /// </summary>
            public long Sequence { get; }
        }

        private readonly struct EvictionCandidate
        {
            public EvictionCandidate(S3FifoCacheEntry entry, long version, bool fromSmallQueue)
            {
                Entry = entry;
                Version = version;
                FromSmallQueue = fromSmallQueue;
            }

            public S3FifoCacheEntry Entry { get; }
            public long Version { get; }

            /// <summary>
            /// Only entries evicted from the small queue enter the ghost queue,
            /// as prescribed by the S3-FIFO algorithm.
            /// </summary>
            public bool FromSmallQueue { get; }
        }

        /// <summary>
        /// When the number of stale (deleted) slots in the FIFO queues exceeds both this value and
        /// the number of live slots, the queues are compacted. Deletions leave their queue slot in
        /// place (a ring buffer cannot remove from the middle), so without compaction a delete-heavy
        /// workload that never triggers eviction would grow the queues without bound.
        /// </summary>
        private const int CompactionMinimumStaleCount = 1024;

        private readonly ConcurrentDictionary<long, S3FifoCacheEntry> m_cache;

        private readonly object m_queueLock = new object();
        private readonly Queue<S3FifoCacheEntry> m_smallQueue;
        private readonly Queue<S3FifoCacheEntry> m_mainQueue;
        private readonly Queue<GhostRecord> m_ghostQueue;
        private readonly Dictionary<long, long> m_ghostKeys;
        private long m_ghostSequence;
        private int m_smallStaleCount;
        private int m_mainStaleCount;

        private Task? m_cleanupTask;
        private int maxSize;
        private readonly ILogger logger;
        private readonly Meter meter;
        private readonly string m_streamName;
        private readonly long maxMemoryUsageInBytes;
        private int cleanupStart;
        private readonly SemaphoreSlim _fullLock;
        private int m_count;
        private long m_cacheHits;
        private long m_cacheMisses;
        private long m_lastSeenCacheHits;
        private int m_sameCacheHitsCount;

        private long m_metrics_lastSeenTotal;
        private long m_metrics_lastSeenHits;
        private float m_metrics_lastSentPercentage;

        private bool m_disposedValue;
        private readonly CancellationTokenSource m_cleanupTokenSource;
        private readonly CacheTableOptions tableOptions;
        private readonly IMemoryAllocationStats _memoryAllocationStats;

        public S3FifoTableSync(CacheTableOptions tableOptions)
        {
            m_cache = new ConcurrentDictionary<long, S3FifoCacheEntry>();
            m_smallQueue = new Queue<S3FifoCacheEntry>();
            m_mainQueue = new Queue<S3FifoCacheEntry>();
            m_ghostQueue = new Queue<GhostRecord>();
            m_ghostKeys = new Dictionary<long, long>();
            this.maxSize = tableOptions.MaxSize;
            this.logger = tableOptions.Logger;
            this.meter = tableOptions.Meter;
            this.m_streamName = tableOptions.StreamName;
            this.maxMemoryUsageInBytes = tableOptions.MaxMemoryUsageInBytes;
            _memoryAllocationStats = tableOptions.MemoryAllocationStats;
            cleanupStart = (int)Math.Ceiling(maxSize * 0.7);
            _fullLock = new SemaphoreSlim(1);
            m_cleanupTokenSource = new CancellationTokenSource();
            StartCleanupTask();

            if (!string.IsNullOrEmpty(m_streamName))
            {
                // Metric names are kept from the previous LRU implementation so existing
                // dashboards keep working, even though the table is no longer an LRU.
                meter.CreateObservableGauge("flowtide_lru_table_size", () =>
                {
                    return new Measurement<int>(Volatile.Read(ref m_count), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableGauge("flowtide_lru_table_max_size", () =>
                {
                    return new Measurement<int>(Volatile.Read(ref maxSize), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableGauge("flowtide_lru_table_cleanup_start", () =>
                {
                    return new Measurement<int>(Volatile.Read(ref cleanupStart), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableGauge("flowtide_s3fifo_small_queue_size", () =>
                {
                    return new Measurement<int>(m_smallQueue.Count, new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableGauge("flowtide_s3fifo_main_queue_size", () =>
                {
                    return new Measurement<int>(m_mainQueue.Count, new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableGauge("flowtide_s3fifo_ghost_size", () =>
                {
                    return new Measurement<int>(m_ghostQueue.Count, new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableGauge("flowtide_lru_table_cache_hits_percentage", () =>
                {
                    var hit = Volatile.Read(ref m_cacheHits);
                    var misses = Volatile.Read(ref m_cacheMisses);
                    var total = hit + misses;
                    if (total > m_metrics_lastSeenTotal)
                    {
                        var newTotal = total - m_metrics_lastSeenTotal;
                        var newHits = hit - m_metrics_lastSeenHits;
                        m_metrics_lastSeenTotal = total;
                        m_metrics_lastSeenHits = hit;
                        m_metrics_lastSentPercentage = (float)newHits / newTotal;
                        return new Measurement<float>(m_metrics_lastSentPercentage, new KeyValuePair<string, object?>("stream", m_streamName));
                    }
                    else
                    {
                        return new Measurement<float>(m_metrics_lastSentPercentage, new KeyValuePair<string, object?>("stream", m_streamName));
                    }
                });
                meter.CreateObservableCounter("flowtide_lru_table_cache_hits", () =>
                {
                    return new Measurement<long>(Volatile.Read(ref m_cacheHits), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableCounter("flowtide_lru_table_cache_misses", () =>
                {
                    return new Measurement<long>(Volatile.Read(ref m_cacheMisses), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableCounter("flowtide_lru_table_cache_tries", () =>
                {
                    var hits = Volatile.Read(ref m_cacheHits);
                    var misses = Volatile.Read(ref m_cacheMisses);
                    return new Measurement<long>(hits + misses, new KeyValuePair<string, object?>("stream", m_streamName));
                });
            }

            this.tableOptions = tableOptions;
        }

        /// <summary>
        /// Number of live entries, used by unit tests and metrics.
        /// </summary>
        internal int Count => Volatile.Read(ref m_count);

        /// <summary>
        /// Drops all entries without returning the cache's references.
        /// Only called during stream initialization/restore, where the state clients are reset
        /// right after: objects still referenced by a caller repair the cache's reference on
        /// re-add, unreferenced objects are reclaimed by their finalizers. This matches the
        /// behavior of the previous LRU implementation.
        /// </summary>
        public void Clear()
        {
            lock (m_queueLock)
            {
                m_cache.Clear();
                m_smallQueue.Clear();
                m_mainQueue.Clear();
                m_ghostQueue.Clear();
                m_ghostKeys.Clear();
                m_smallStaleCount = 0;
                m_mainStaleCount = 0;
                Volatile.Write(ref m_count, 0);
            }
        }

        public void Delete(in long key)
        {
            if (m_cache.TryGetValue(key, out var entry))
            {
                lock (entry)
                {
                    if (entry.Removed)
                    {
                        return;
                    }
                    // Volatile and ordered before Return: lock-free readers rely on Removed
                    // being observable once a rent can fail (see S3FifoCacheEntry.TryRentValue).
                    Volatile.Write(ref entry.Removed, true);
                    if (m_cache.TryRemove(key, out _))
                    {
                        entry.Value.Return();
                        Interlocked.Decrement(ref m_count);
                    }
                }
                // The queue slot cannot be removed from the middle of a ring buffer, so the
                // entry stays behind as a stale slot that the eviction scan skips. Track the
                // stale count so the cleanup task can compact the queues when needed.
                lock (m_queueLock)
                {
                    if (entry.Location == S3FifoQueueLocation.Small)
                    {
                        m_smallStaleCount++;
                    }
                    else if (entry.Location == S3FifoQueueLocation.Main)
                    {
                        m_mainStaleCount++;
                    }
                }
            }
        }

        private void StartCleanupTask()
        {
            m_cleanupTask = Task.Factory.StartNew(async () =>
            {
                await CleanupTask();
            }, TaskCreationOptions.LongRunning)
                .Unwrap()
                .ContinueWith((task) =>
                {
                    if (m_cleanupTokenSource.IsCancellationRequested)
                    {
                        // Do not start a new task if we are cancelled
                        return;
                    }
                    if (task.IsFaulted)
                    {
                        logger.ExceptionInLruTableCleanup(task.Exception, m_streamName);
                    }
                    else
                    {
                        logger.CleanupTaskClosedWithoutError(m_streamName);
                    }
                    if (!task.IsCompletedSuccessfully)
                    {
                        StartCleanupTask();
                    }
                });
        }

        private async Task CleanupTask()
        {
            while (true)
            {
                m_cleanupTokenSource.Token.ThrowIfCancellationRequested();
                await Task.Delay(10);
                try
                {
                    await _fullLock.WaitAsync();
                    await Cleanup();
                }
                finally
                {
                    _fullLock.Release();
                }
            }
        }

        /// <summary>
        /// Used for unit testing
        /// </summary>
        internal async Task StopCleanupTask()
        {
            m_cleanupTokenSource.Cancel();
            await m_cleanupTask!;
        }

        public bool TryGetCacheValue(long key, [NotNullWhen(true)] out S3FifoCacheEntry? entry)
        {
            // Lock-free read: the rent handoff and frequency bump live in TryRentValue.
            // A rent failure means the entry is being evicted right now and is treated
            // as a miss; the caller reloads from temporary or persistent storage.
            if (m_cache.TryGetValue(key, out entry) && entry.TryRentValue())
            {
                Interlocked.Increment(ref m_cacheHits);
                return true;
            }
            return false;
        }

        public bool TryGetValue(long key, out ICacheObject? cacheObject)
        {
            if (m_cache.TryGetValue(key, out var entry))
            {
                if (entry.TryRentValue())
                {
                    cacheObject = entry.Value;
                    Interlocked.Increment(ref m_cacheHits);
                    return true;
                }
                cacheObject = default;
                return false;
            }
            Interlocked.Increment(ref m_cacheMisses);
            cacheObject = default;
            return false;
        }

        public async Task Wait()
        {
            logger.LruTableIsFull(m_streamName);
            await _fullLock.WaitAsync().ConfigureAwait(false);
            _fullLock.Release();
            logger.LruTableNoLongerFull(m_streamName);
        }

        public bool Add(long key, ICacheObject value, ICacheEvictHandler evictHandler)
        {
            bool full = Volatile.Read(ref m_count) > Volatile.Read(ref maxSize);

            while (true)
            {
                if (m_cache.TryGetValue(key, out var existing))
                {
                    lock (existing)
                    {
                        if (!existing.Removed)
                        {
                            if (!value.Equals(existing.Value))
                            {
                                throw new InvalidOperationException("Cannot add a new value to the cache with the same key.");
                            }
                            existing.Version = existing.Version + 1;
                            return full;
                        }
                    }
                    // The entry was removed concurrently by eviction. Removal deletes the key
                    // from the dictionary in the same entry-lock scope that sets Removed, so a
                    // retry observes the removal and inserts a fresh entry.
                    continue;
                }

                var entry = new S3FifoCacheEntry(key, value, evictHandler);
                if (m_cache.TryAdd(key, entry))
                {
                    if (value.RemovedFromCache)
                    {
                        // Re-add of an object that was evicted while a caller still held a
                        // reference: take a new cache-owned rent. The caller holds its own
                        // rent (documented invariant), so the object cannot die concurrently.
                        if (!value.TryRent())
                        {
                            throw new InvalidOperationException("Already disposed");
                        }
                        value.RemovedFromCache = false;
                    }

                    lock (m_queueLock)
                    {
                        if (m_ghostKeys.Remove(key))
                        {
                            // Recently evicted from the small queue: admit directly to main.
                            entry.Location = S3FifoQueueLocation.Main;
                            m_mainQueue.Enqueue(entry);
                        }
                        else
                        {
                            entry.Location = S3FifoQueueLocation.Small;
                            m_smallQueue.Enqueue(entry);
                        }
                    }

                    Interlocked.Increment(ref m_count);
                    return full;
                }
                // Lost an insert race for the key, retry and treat it as an update.
            }
        }

        /// <summary>
        /// Used for testing only
        /// </summary>
        internal Task ForceCleanup()
        {
            return Cleanup();
        }

        private int SmallQueueTargetSize()
        {
            // The small queue gets ~10% of the cache capacity, as in the S3-FIFO paper.
            return Math.Max(1, Volatile.Read(ref maxSize) / 10);
        }

        private int GhostCapacity()
        {
            // The ghost queue tracks as many keys as the main queue holds objects.
            return Math.Max(1, Volatile.Read(ref maxSize) - SmallQueueTargetSize());
        }

        private async Task Cleanup()
        {
            var currentCount = Volatile.Read(ref m_count);
            int cleanupStartLocal = cleanupStart;
            bool isCleanup = false;
            if (currentCount <= cleanupStartLocal)
            {
                var cacheHitsLocal = Volatile.Read(ref m_cacheHits);
                if (m_lastSeenCacheHits == cacheHitsLocal)
                {
                    m_sameCacheHitsCount++;
                    if (m_sameCacheHitsCount >= 1000 && currentCount > 0)
                    {
                        // No cache hits during a long time, clear the entire cache
                        isCleanup = true;
                        cleanupStartLocal = tableOptions.MinSize;
                        m_sameCacheHitsCount = 0;
                    }
                    else
                    {
                        CompactQueuesIfNeeded();
                        return;
                    }
                }
                else
                {
                    m_lastSeenCacheHits = cacheHitsLocal;
                    m_sameCacheHitsCount = 0;
                    CompactQueuesIfNeeded();
                    return;
                }
            }

            var toBeRemovedCount = currentCount - cleanupStartLocal;
            if (maxMemoryUsageInBytes > 0 && !isCleanup && currentCount > 0)
            {
                var usedMemory = _memoryAllocationStats.GetAllocatedMemory();

                if (usedMemory > 0)
                {
                    var avgItemSizeBytes = Math.Max(16 * 1024.0, (double)usedMemory / currentCount);
                    var targetMemoryBytes = maxMemoryUsageInBytes * 0.80;

                    var rawIdealMaxSize = (int)Math.Floor(targetMemoryBytes / avgItemSizeBytes);

                    var minAllowedSize = 100;
                    var idealMaxSize = Math.Max(minAllowedSize, rawIdealMaxSize);

                    var tolerance = idealMaxSize * 0.20;

                    if (Math.Abs(maxSize - idealMaxSize) > tolerance)
                    {
                        Volatile.Write(ref maxSize, idealMaxSize);

                        var rawCleanupSize = (int)Math.Ceiling(idealMaxSize * 0.70);

                        var cleanupSize = Math.Max(1, rawCleanupSize);
                        Volatile.Write(ref cleanupStart, cleanupSize);

                        if (currentCount > idealMaxSize)
                        {
                            toBeRemovedCount = currentCount - cleanupSize;
                        }
                    }
                }
            }

            if (toBeRemovedCount <= 0)
            {
                CompactQueuesIfNeeded();
                return;
            }

            var victims = new List<EvictionCandidate>();
            lock (m_queueLock)
            {
                SelectVictims(victims, toBeRemovedCount);
            }

            if (victims.Count == 0)
            {
                return;
            }

            Dictionary<ICacheEvictHandler, List<(S3FifoCacheEntry, long)>> groupedValues = new Dictionary<ICacheEvictHandler, List<(S3FifoCacheEntry, long)>>();
            foreach (var candidate in victims)
            {
                if (!groupedValues.TryGetValue(candidate.Entry.EvictHandler, out var list))
                {
                    list = new List<(S3FifoCacheEntry, long)>();
                    groupedValues.Add(candidate.Entry.EvictHandler, list);
                }
                list.Add((candidate.Entry, candidate.Version));
            }

            List<Task> evictTasks = new List<Task>();
            List<List<(S3FifoCacheEntry, long)>> evictTaskGroups = new List<List<(S3FifoCacheEntry, long)>>();
            foreach (var group in groupedValues)
            {
                evictTaskGroups.Add(group.Value);
                evictTasks.Add(Task.Factory.StartNew(() =>
                {
                    group.Key.Evict(group.Value, isCleanup);
                }));
            }

            Exception? evictException = null;
            try
            {
                await Task.WhenAll(evictTasks);
            }
            catch (Exception e)
            {
                evictException = e;
            }

            HashSet<S3FifoCacheEntry>? failedVictims = null;
            if (evictException != null)
            {
                // A failed evict handler did not serialize its victims, so they must not be
                // removed from memory. The victims were already dequeued at selection, so
                // without a requeue they would be stranded outside every queue: still cached
                // and counted, but unreachable by any future eviction. Collect them here so
                // they go back into their queues and are retried by a later cleanup.
                failedVictims = new HashSet<S3FifoCacheEntry>();
                for (int i = 0; i < evictTasks.Count; i++)
                {
                    if (!evictTasks[i].IsCompletedSuccessfully)
                    {
                        foreach (var value in evictTaskGroups[i])
                        {
                            failedVictims.Add(value.Item1);
                        }
                    }
                }
            }

            List<S3FifoCacheEntry>? requeueToSmall = null;
            List<S3FifoCacheEntry>? requeueToMain = null;
            List<long>? ghostInserts = null;
            foreach (var candidate in victims)
            {
                var entry = candidate.Entry;
                lock (entry)
                {
                    if (entry.Removed)
                    {
                        // Deleted while eviction was in progress: the delete already removed
                        // it from the dictionary and returned the cache's reference, and the
                        // slot was dropped at selection. Requeuing it (even on a version
                        // mismatch) would resurrect a dead, uncounted stale slot.
                        continue;
                    }
                    if (failedVictims != null && failedVictims.Contains(entry))
                    {
                        // Not serialized because its evict handler failed: keep it cached and
                        // put it back where it came from so a later cleanup retries it.
                        if (candidate.FromSmallQueue)
                        {
                            (requeueToSmall ??= new List<S3FifoCacheEntry>()).Add(entry);
                        }
                        else
                        {
                            (requeueToMain ??= new List<S3FifoCacheEntry>()).Add(entry);
                        }
                        continue;
                    }
                    if (candidate.Version != entry.Version)
                    {
                        // Modified while it was being serialized: the serialized copy is stale,
                        // keep the value cached. It goes back into the main queue since it is
                        // clearly being used.
                        (requeueToMain ??= new List<S3FifoCacheEntry>()).Add(entry);
                        continue;
                    }
                    // Volatile and ordered before Return: lock-free readers rely on Removed
                    // being observable once a rent can fail (see S3FifoCacheEntry.TryRentValue).
                    Volatile.Write(ref entry.Removed, true);
                    // RemovedFromCache must be set before the dictionary removal. A concurrent
                    // re-add of the same object can only succeed its TryAdd after this TryRemove
                    // (same dictionary bucket lock), and that ordering is what guarantees the
                    // re-adder observes the flag and takes a new cache-owned rent. Written the
                    // other way around, the re-adder can miss the flag and the cache's reference
                    // count goes one short, disposing the object while it is still cached.
                    entry.Value.RemovedFromCache = true;
                    if (m_cache.TryRemove(entry.Key, out _))
                    {
                        entry.Value.Return();
                        Interlocked.Decrement(ref m_count);
                        if (candidate.FromSmallQueue)
                        {
                            (ghostInserts ??= new List<long>()).Add(entry.Key);
                        }
                    }
                }
            }

            if (requeueToSmall != null || requeueToMain != null || ghostInserts != null)
            {
                lock (m_queueLock)
                {
                    if (requeueToSmall != null)
                    {
                        foreach (var entry in requeueToSmall)
                        {
                            entry.Location = S3FifoQueueLocation.Small;
                            m_smallQueue.Enqueue(entry);
                        }
                    }
                    if (requeueToMain != null)
                    {
                        foreach (var entry in requeueToMain)
                        {
                            entry.Location = S3FifoQueueLocation.Main;
                            m_mainQueue.Enqueue(entry);
                        }
                    }
                    if (ghostInserts != null)
                    {
                        foreach (var key in ghostInserts)
                        {
                            AddToGhost(key);
                        }
                    }
                }
            }

            if (evictException != null)
            {
                // Rethrow after the victims have been rehomed so the failure is still logged
                // and the cleanup task restarts, matching the previous implementation where
                // an evict failure propagated but left the victims evictable.
                ExceptionDispatchInfo.Capture(evictException).Throw();
            }

            if (isCleanup)
            {
                FlowtideMemoryAllocation.Collect();
            }
        }

        /// <summary>
        /// Runs the S3-FIFO eviction scans until enough victims have been collected or no
        /// evictable entry remains. Must be called under the queue lock.
        ///
        /// Victims are only dequeued here, not removed from the cache: they stay readable
        /// until the removal phase after their content has been serialized by the evict
        /// handlers, mirroring the previous implementation.
        /// </summary>
        private void SelectVictims(List<EvictionCandidate> victims, int toBeRemovedCount)
        {
            var smallTarget = SmallQueueTargetSize();
            while (victims.Count < toBeRemovedCount)
            {
                var liveSmall = m_smallQueue.Count - m_smallStaleCount;
                var liveMain = m_mainQueue.Count - m_mainStaleCount;
                if (liveSmall <= 0 && liveMain <= 0)
                {
                    break;
                }
                if (liveSmall > smallTarget || liveMain <= 0)
                {
                    if (!TryEvictOneFromSmall(victims) && !TryEvictOneFromMain(victims))
                    {
                        break;
                    }
                }
                else
                {
                    if (!TryEvictOneFromMain(victims) && !TryEvictOneFromSmall(victims))
                    {
                        break;
                    }
                }
            }
        }

        private bool TryEvictOneFromSmall(List<EvictionCandidate> victims)
        {
            while (m_smallQueue.Count > 0)
            {
                var entry = m_smallQueue.Dequeue();
                lock (entry)
                {
                    if (entry.Removed)
                    {
                        entry.Location = S3FifoQueueLocation.None;
                        if (m_smallStaleCount > 0)
                        {
                            m_smallStaleCount--;
                        }
                        continue;
                    }
                    if (Volatile.Read(ref entry.Frequency) > 1)
                    {
                        // Accessed more than once while in the small queue: promote to main.
                        entry.Location = S3FifoQueueLocation.Main;
                        m_mainQueue.Enqueue(entry);
                        continue;
                    }
                    entry.Location = S3FifoQueueLocation.None;
                    victims.Add(new EvictionCandidate(entry, entry.Version, fromSmallQueue: true));
                    return true;
                }
            }
            return false;
        }

        private bool TryEvictOneFromMain(List<EvictionCandidate> victims)
        {
            // Terminates: every pass either evicts, drops a stale slot, or decrements a
            // frequency, and the total frequency in the queue is finite.
            while (m_mainQueue.Count > 0)
            {
                var entry = m_mainQueue.Dequeue();
                lock (entry)
                {
                    if (entry.Removed)
                    {
                        entry.Location = S3FifoQueueLocation.None;
                        if (m_mainStaleCount > 0)
                        {
                            m_mainStaleCount--;
                        }
                        continue;
                    }
                    if (Volatile.Read(ref entry.Frequency) > 0)
                    {
                        // Second chance: reinsert at the tail with a decremented frequency.
                        // Cannot underflow: only one scan holds a dequeued entry at a time and
                        // concurrent lock-free readers only increment.
                        Interlocked.Decrement(ref entry.Frequency);
                        m_mainQueue.Enqueue(entry);
                        continue;
                    }
                    entry.Location = S3FifoQueueLocation.None;
                    victims.Add(new EvictionCandidate(entry, entry.Version, fromSmallQueue: false));
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Must be called under the queue lock.
        /// </summary>
        private void AddToGhost(long key)
        {
            var sequence = ++m_ghostSequence;
            m_ghostKeys[key] = sequence;
            m_ghostQueue.Enqueue(new GhostRecord(key, sequence));
            var capacity = GhostCapacity();
            while (m_ghostQueue.Count > capacity)
            {
                var oldest = m_ghostQueue.Dequeue();
                if (m_ghostKeys.TryGetValue(oldest.Key, out var storedSequence) && storedSequence == oldest.Sequence)
                {
                    m_ghostKeys.Remove(oldest.Key);
                }
            }
        }

        private void CompactQueuesIfNeeded()
        {
            lock (m_queueLock)
            {
                var stale = m_smallStaleCount + m_mainStaleCount;
                if (stale < CompactionMinimumStaleCount)
                {
                    return;
                }
                var live = (m_smallQueue.Count + m_mainQueue.Count) - stale;
                if (stale < live)
                {
                    return;
                }
                CompactQueue(m_smallQueue);
                CompactQueue(m_mainQueue);
                m_smallStaleCount = 0;
                m_mainStaleCount = 0;
            }
        }

        /// <summary>
        /// Removes stale (deleted) slots from a queue while preserving FIFO order.
        /// Must be called under the queue lock.
        /// </summary>
        private static void CompactQueue(Queue<S3FifoCacheEntry> queue)
        {
            var count = queue.Count;
            for (int i = 0; i < count; i++)
            {
                var entry = queue.Dequeue();
                bool removed;
                lock (entry)
                {
                    removed = entry.Removed;
                }
                if (!removed)
                {
                    queue.Enqueue(entry);
                }
                else
                {
                    entry.Location = S3FifoQueueLocation.None;
                }
            }
        }

        #region Test helpers

        /// <summary>
        /// Looks up the entry for a key without renting. Used for unit test assertions only.
        /// </summary>
        internal bool TryPeekEntryForTests(long key, [NotNullWhen(true)] out S3FifoCacheEntry? entry)
        {
            return m_cache.TryGetValue(key, out entry);
        }

        internal (int SmallCount, int MainCount, int GhostCount, int SmallStale, int MainStale) GetQueueCountsForTests()
        {
            lock (m_queueLock)
            {
                return (m_smallQueue.Count, m_mainQueue.Count, m_ghostQueue.Count, m_smallStaleCount, m_mainStaleCount);
            }
        }

        internal bool IsInGhostForTests(long key)
        {
            lock (m_queueLock)
            {
                return m_ghostKeys.ContainsKey(key);
            }
        }

        #endregion

        private void DisposeEntries()
        {
            lock (m_queueLock)
            {
                DrainQueueOnDispose(m_smallQueue);
                DrainQueueOnDispose(m_mainQueue);
                m_ghostQueue.Clear();
                m_ghostKeys.Clear();
                m_smallStaleCount = 0;
                m_mainStaleCount = 0;
            }
        }

        private void DrainQueueOnDispose(Queue<S3FifoCacheEntry> queue)
        {
            while (queue.Count > 0)
            {
                var entry = queue.Dequeue();
                lock (entry)
                {
                    entry.Location = S3FifoQueueLocation.None;
                    if (!entry.Removed)
                    {
                        Volatile.Write(ref entry.Removed, true);
                        m_cache.TryRemove(entry.Key, out _);
                        entry.Value.Return();
                        Interlocked.Decrement(ref m_count);
                    }
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!m_disposedValue)
            {
                if (disposing)
                {
                    m_cleanupTokenSource.Cancel();
                    if (m_cleanupTask != null)
                    {
                        m_cleanupTask.Wait();
                        m_cleanupTask.Dispose();
                    }
                    DisposeEntries();
                    m_cleanupTokenSource.Dispose();
                    meter.Dispose();
                    _fullLock.Dispose();
                }
                m_disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

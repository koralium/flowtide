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
    /// In-memory page cache shared by all state clients in a stream.
    /// Uses the S3-FIFO eviction algorithm instead of the old LRU/CLOCK hybrid.
    ///
    /// A concurrent dictionary gives lock-free key lookup.
    /// A small FIFO queue takes new keys and filters one-hit wonders.
    /// A main FIFO queue holds promoted entries and ghost re-admissions.
    /// A ghost queue remembers keys recently evicted from the small queue.
    ///
    /// Reads never touch the queues, a hit only bumps the entry frequency.
    /// Queue maintenance happens on insert and in the background cleanup task.
    /// A 2Q style correlation window stops insertion bursts from being promoted, see S3FifoCorrelationClock.
    ///
    /// lock(entry) guards Removed, Frequency and Version.
    /// m_queueLock guards the queues, the stale counters and entry.Location.
    /// An entry lock may be taken inside the queue lock, never the reverse.
    /// Add and Delete on one key must be serialized by the caller, reads race freely.
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
            /// Which insertion into the ghost queue this record belongs to.
            /// Stops a stale ring slot from removing a newer membership record.
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
        /// Compact the queues when stale slots pass both this value and the live count.
        /// Deletes leave a stale slot behind, so a delete-heavy workload would grow unbounded.
        /// </summary>
        private const int CompactionMinimumStaleCount = 1024;

        private readonly ConcurrentDictionary<long, S3FifoCacheEntry> m_cache;

        private readonly object m_queueLock = new object();
        private readonly Queue<S3FifoCacheEntry> m_smallQueue;
        private readonly Queue<S3FifoCacheEntry> m_mainQueue;
        private readonly Queue<GhostRecord> m_ghostQueue;
        private readonly Dictionary<long, long> m_ghostKeys;
        private readonly S3FifoCorrelationClock m_correlationClock = new S3FifoCorrelationClock();
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
        // Written only on the cleanup thread, read lock-free by the metric callbacks.
        // Small-queue-head outcomes and one-hit-wonders, see the metric registrations.
        private long m_smallQueuePromotions;
        private long m_smallQueueEvictions;
        private long m_oneHitWonders;
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
            m_correlationClock.SetWindowSize(CorrelationWindowSize());
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
                // promotions went to main, evictions went to the ghost queue.
                // one_hit_wonders aged out of ghost unused, added once and never promoted.
                meter.CreateObservableCounter("flowtide_s3fifo_small_queue_promotions", () =>
                {
                    return new Measurement<long>(Volatile.Read(ref m_smallQueuePromotions), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableCounter("flowtide_s3fifo_small_queue_evictions", () =>
                {
                    return new Measurement<long>(Volatile.Read(ref m_smallQueueEvictions), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableCounter("flowtide_s3fifo_one_hit_wonders", () =>
                {
                    return new Measurement<long>(Volatile.Read(ref m_oneHitWonders), new KeyValuePair<string, object?>("stream", m_streamName));
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
        /// Drops all entries without returning the cache references.
        /// Only called on init or restore, where the clients are reset right after.
        /// Referenced objects repair the reference on re-add, the rest are finalized.
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
                    // Write Removed before Return so lock-free readers see it once a rent fails.
                    Volatile.Write(ref entry.Removed, true);
                    if (m_cache.TryRemove(key, out _))
                    {
                        entry.Value.Return();
                        Interlocked.Decrement(ref m_count);
                    }
                }
                // The ring buffer cannot remove from the middle, so the slot stays behind as
                // stale and the scan skips it. Track the count so cleanup can compact later.
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
            // PeriodicTimer is allocation-free per tick. Task.Delay here allocated every 10ms
            // forever and made a visible GC sawtooth on idle streams.
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(10));
            while (true)
            {
                m_cleanupTokenSource.Token.ThrowIfCancellationRequested();
                await timer.WaitForNextTickAsync(m_cleanupTokenSource.Token);
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
            // Lock-free read, the rent handoff lives in TryRentValue.
            // A rent failure means the entry is being evicted and is treated as a miss.
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

        /// <summary>
        /// Blocks the background eviction task until ResumeEviction is called.
        /// A commit and an eviction serialize pages through the same client serializer, which is
        /// not thread-safe, so a concurrent serialize corrupts the persisted bytes.
        /// Recovery also clears the cache that an in-flight eviction would write through.
        /// Uses the lock the cleanup task holds, so acquiring it drains any in-flight eviction.
        /// </summary>
        internal Task PauseEvictionAsync()
        {
            return _fullLock.WaitAsync();
        }

        internal void ResumeEviction()
        {
            _fullLock.Release();
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
                    // The entry was removed concurrently by eviction.
                    // Retry to observe the removal and insert a fresh entry.
                    continue;
                }

                var entry = new S3FifoCacheEntry(key, value, evictHandler, m_correlationClock);
                if (m_cache.TryAdd(key, entry))
                {
                    if (value.RemovedFromCache)
                    {
                        // Defensive, the cache no longer produces objects with this flag set.
                        // Take a new cache-owned rent, the caller still holds its own rent.
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
                            // Recently evicted from the small queue, admit directly to main.
                            // Never stamped, so every hit counts.
                            entry.Location = S3FifoQueueLocation.Main;
                            m_mainQueue.Enqueue(entry);
                        }
                        else
                        {
                            // The entry is already in the dictionary, so a hit can land before
                            // this stamp and count. The gap is tiny and the filter is a heuristic.
                            entry.SetSmallQueueStamp(m_correlationClock.NextSequence());
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

        /// <summary>
        /// Correlation window width, half the small queue target.
        /// The target is used instead of the live length, which races and counts stale slots.
        /// Integer division makes the window 0 for MaxSize below 20, disabling the filter.
        /// </summary>
        private int CorrelationWindowSize()
        {
            return SmallQueueTargetSize() / 2;
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
                        // The small queue target follows maxSize, so the correlation window
                        // must follow it too.
                        m_correlationClock.SetWindowSize(CorrelationWindowSize());

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

            // Large batches are selected in chunks, releasing the queue lock between them so
            // Add and Delete are not stalled. Selected victims are dequeued and owned here.
            var victims = new List<EvictionCandidate>();
            while (true)
            {
                bool finished;
                lock (m_queueLock)
                {
                    m_selectionLockAcquisitions++;
                    finished = TrySelectVictims(victims, toBeRemovedCount, SelectionOperationBudget);
                }
                if (finished)
                {
                    break;
                }
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
                // A failed evict handler did not serialize its victims, so keep them cached.
                // Collect them here so they go back into their queues and are retried later.
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
                        // Deleted during eviction, already removed and returned.
                        // Requeuing it would resurrect a dead stale slot.
                        continue;
                    }
                    if (failedVictims != null && failedVictims.Contains(entry))
                    {
                        // Its evict handler failed, keep it cached and put it back so a later
                        // cleanup retries it.
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
                        // Modified while being serialized, so the copy is stale.
                        // Keep it cached and put it in the main queue since it is being used.
                        (requeueToMain ??= new List<S3FifoCacheEntry>()).Add(entry);
                        continue;
                    }
                    // Only evict pages nothing else references. Claim the cache reference if it
                    // is the sole one, otherwise a held page stays cached. Evicting a held page
                    // would let a reload create a second diverging copy of the same key.
                    // The claim disposes on success, so a racing reader reloads as the new owner
                    // and a racing re-add blocks on lock(entry) and sees Removed once we release.
                    if (!entry.Value.TryReclaimForEviction())
                    {
                        (requeueToMain ??= new List<S3FifoCacheEntry>()).Add(entry);
                        continue;
                    }
                    Volatile.Write(ref entry.Removed, true);
                    entry.Value.RemovedFromCache = true;
                    if (m_cache.TryRemove(entry.Key, out _))
                    {
                        Interlocked.Decrement(ref m_count);
                        if (candidate.FromSmallQueue)
                        {
                            (ghostInserts ??= new List<long>()).Add(entry.Key);
                            m_smallQueueEvictions++;
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
                            // Re-enters the small queue at the tail, so the window restarts.
                            entry.SetSmallQueueStamp(m_correlationClock.NextSequence());
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
                // Rethrow after the victims are rehomed, so the failure is still logged and the
                // cleanup task restarts.
                ExceptionDispatchInfo.Capture(evictException).Throw();
            }

            if (isCleanup)
            {
                FlowtideMemoryAllocation.Collect();
            }
        }

        /// <summary>
        /// Max queue operations under one queue-lock acquisition during selection.
        /// Bounds how long a lock hold can stall Add and Delete.
        /// </summary>
        private const int SelectionOperationBudget = 256;

        /// <summary>
        /// Number of queue-lock acquisitions spent on victim selection, used by unit tests
        /// to verify that large batches are actually chunked.
        /// </summary>
        private long m_selectionLockAcquisitions;

        internal long SelectionLockAcquisitionsForTests => Volatile.Read(ref m_selectionLockAcquisitions);

        /// <summary>
        /// Runs the eviction scans until enough victims, nothing evictable, or budget spent.
        /// Must be called under the queue lock.
        /// Victims are only dequeued here, they stay readable until the removal phase.
        /// </summary>
        /// <returns>
        /// True when selection is complete, false when the budget ran out and the caller
        /// should reacquire the lock for another chunk.
        /// </returns>
        private bool TrySelectVictims(List<EvictionCandidate> victims, int toBeRemovedCount, int operationBudget)
        {
            var smallTarget = SmallQueueTargetSize();
            while (victims.Count < toBeRemovedCount)
            {
                if (operationBudget <= 0)
                {
                    return false;
                }
                var liveSmall = m_smallQueue.Count - m_smallStaleCount;
                var liveMain = m_mainQueue.Count - m_mainStaleCount;
                if (liveSmall <= 0 && liveMain <= 0)
                {
                    return true;
                }
                bool foundVictim;
                if (liveSmall > smallTarget || liveMain <= 0)
                {
                    foundVictim = TryEvictOneFromSmall(victims, ref operationBudget) || TryEvictOneFromMain(victims, ref operationBudget);
                }
                else
                {
                    foundVictim = TryEvictOneFromMain(victims, ref operationBudget) || TryEvictOneFromSmall(victims, ref operationBudget);
                }
                if (!foundVictim && operationBudget > 0)
                {
                    // Both queues were scanned without finding an evictable entry.
                    return true;
                }
            }
            return true;
        }

        private bool TryEvictOneFromSmall(List<EvictionCandidate> victims, ref int operationBudget)
        {
            while (m_smallQueue.Count > 0 && operationBudget > 0)
            {
                operationBudget--;
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
                        // Accessed more than once while in the small queue, promote to main.
                        // With the window only aged hits count, so this means two aged hits.
                        entry.ClearSmallQueueStamp();
                        entry.Location = S3FifoQueueLocation.Main;
                        m_mainQueue.Enqueue(entry);
                        m_smallQueuePromotions++;
                        continue;
                    }
                    // Leaving the small queue as a victim, later hits should count.
                    entry.ClearSmallQueueStamp();
                    entry.Location = S3FifoQueueLocation.None;
                    victims.Add(new EvictionCandidate(entry, entry.Version, fromSmallQueue: true));
                    return true;
                }
            }
            return false;
        }

        private bool TryEvictOneFromMain(List<EvictionCandidate> victims, ref int operationBudget)
        {
            // Every pass evicts, drops a stale slot, or decrements a frequency.
            // Readers can pump frequencies back up, so the budget bounds one lock hold.
            while (m_mainQueue.Count > 0 && operationBudget > 0)
            {
                operationBudget--;
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
                        // Second chance, reinsert at the tail with a decremented frequency.
                        // Cannot underflow, only one scan holds an entry and readers only increment.
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
                    // A live ghost membership ages out, this key was evicted and never
                    // re-admitted. Added once, never promoted, and now gone. A one-hit-wonder.
                    m_ghostKeys.Remove(oldest.Key);
                    m_oneHitWonders++;
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

        internal int CorrelationWindowSizeForTests => m_correlationClock.WindowSizeForTests;

        internal long OneHitWondersForTests => Volatile.Read(ref m_oneHitWonders);

        internal long SmallQueuePromotionsForTests => Volatile.Read(ref m_smallQueuePromotions);

        internal long SmallQueueEvictionsForTests => Volatile.Read(ref m_smallQueueEvictions);

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
                    var cleanupTask = m_cleanupTask;
                    while (cleanupTask != null)
                    {
                        try
                        {
                            cleanupTask.Wait();
                        }
                        catch
                        {
                            // A faulted or cancelled cleanup task rethrows on Wait, disposal swallows it.
                        }
                        var successor = m_cleanupTask;
                        if (ReferenceEquals(successor, cleanupTask))
                        {
                            break;
                        }
                        cleanupTask = successor;
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

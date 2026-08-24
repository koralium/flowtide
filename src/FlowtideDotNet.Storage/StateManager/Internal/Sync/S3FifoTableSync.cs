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
    /// A main FIFO queue holds entries that proved two reuse events, either two counted
    /// hits or one counted hit plus a ghost re-reference.
    /// A ghost queue remembers keys recently evicted from the small queue along with
    /// whether they counted a reuse; a re-referenced key without one re-enters the small
    /// queue with the re-reference banked as frequency.
    ///
    /// Reads never touch the queues, a hit only bumps the entry frequency.
    /// Queue maintenance happens on insert and in the background cleanup task.
    /// The cleanup task ages the main queue every pass, paced by how much the cache turned over,
    /// so a page there keeps its place only while it is still being read. Eviction then takes the
    /// aged out head first, which lets the main queue shrink back and hand the space to the small
    /// queue instead of holding a fixed share of the cache.
    /// Frequency uses spaced counting, a hit only counts when the entry aged past the
    /// correlation window since its insertion or last counted hit, so a burst counts as
    /// one reuse event, see S3FifoCorrelationClock.
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
        private readonly struct GhostValue
        {
            public GhostValue(long sequence, bool reused, bool fromMain)
            {
                Sequence = sequence;
                Reused = reused;
                FromMain = fromMain;
            }

            public long Sequence { get; }

            /// <summary>
            /// The entry counted a reuse before it was evicted, so a re-reference is its
            /// second reuse event and admits it straight to main.
            /// </summary>
            public bool Reused { get; }

            /// <summary>
            /// The entry was evicted from the main queue rather than the small one. A hit on it
            /// says main was too small, the mirror of a hit on a small queue evictee, and it is
            /// the evidence the small queue's own ghost entries can never give.
            /// Only recorded while the adaptive split is on.
            /// </summary>
            public bool FromMain { get; }
        }

        private readonly Queue<GhostRecord> m_ghostQueue;
        private readonly Dictionary<long, GhostValue> m_ghostKeys;
        private readonly S3FifoCorrelationClock m_correlationClock = new S3FifoCorrelationClock();
        private long m_ghostSequence;
        private int m_smallStaleCount;
        private int m_mainStaleCount;

        /// <summary>
        /// The small queue's share of the cache in permille. Adapted from ghost hits when the
        /// adaptive split is on, left at the default share otherwise.
        /// Written under the queue lock, read with Volatile off it.
        /// </summary>
        private int m_smallTargetPermille = DefaultSmallTargetPermille;

        private Task? m_cleanupTask;
        private int maxSize;
        private readonly ILogger logger;
        // Owned by the state manager and shared with the state clients, never disposed here.
        private readonly Meter meter;
        private readonly string m_streamName;
        private readonly long maxMemoryUsageInBytes;
        private int cleanupStart;
        private readonly SemaphoreSlim _fullLock;
        private int m_count;

        private long m_readCacheHits;
        private long m_readCacheMisses;
        private long m_commitCacheHits;
        private long m_commitCacheMisses;
        // Client lookup-table fast paths count hits the table never sees. The idle check reads
        // them through these providers so a fast-path-only stream is not judged idle and wiped.
        private readonly List<Func<long>> m_externalHitCounters = new List<Func<long>>();
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
            m_ghostKeys = new Dictionary<long, GhostValue>();
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
                    var hit = TotalCacheHits();
                    var misses = TotalCacheMisses();
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
                    return new Measurement<long>(TotalCacheHits(), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableCounter("flowtide_lru_table_cache_misses", () =>
                {
                    return new Measurement<long>(TotalCacheMisses(), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableCounter("flowtide_lru_table_cache_tries", () =>
                {
                    return new Measurement<long>(TotalCacheHits() + TotalCacheMisses(), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                // Split by path. Read numbers measure cache quality for query processing,
                // commit numbers measure dirty pages surviving until the checkpoint.
                meter.CreateObservableCounter("flowtide_cache_read_hits", () =>
                {
                    return new Measurement<long>(Volatile.Read(ref m_readCacheHits), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableCounter("flowtide_cache_read_misses", () =>
                {
                    return new Measurement<long>(Volatile.Read(ref m_readCacheMisses), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableCounter("flowtide_cache_commit_hits", () =>
                {
                    return new Measurement<long>(Volatile.Read(ref m_commitCacheHits), new KeyValuePair<string, object?>("stream", m_streamName));
                });
                meter.CreateObservableCounter("flowtide_cache_commit_misses", () =>
                {
                    return new Measurement<long>(Volatile.Read(ref m_commitCacheMisses), new KeyValuePair<string, object?>("stream", m_streamName));
                });
            }

            this.tableOptions = tableOptions;
        }

        /// <summary>
        /// Number of live entries, used by unit tests and metrics.
        /// </summary>
        internal int Count => Volatile.Read(ref m_count);

        private long TotalCacheHits()
        {
            return Volatile.Read(ref m_readCacheHits) + Volatile.Read(ref m_commitCacheHits);
        }

        private long TotalCacheMisses()
        {
            return Volatile.Read(ref m_readCacheMisses) + Volatile.Read(ref m_commitCacheMisses);
        }

        internal void RegisterExternalHitCounter(Func<long> hitCounter)
        {
            lock (m_externalHitCounters)
            {
                m_externalHitCounters.Add(hitCounter);
            }
        }

        private long ExternalCacheHits()
        {
            long sum = 0;
            lock (m_externalHitCounters)
            {
                foreach (var counter in m_externalHitCounters)
                {
                    sum += counter();
                }
            }
            return sum;
        }

        /// <summary>
        /// Drops all entries without returning the cache references or marking them removed.
        /// Only for ClearCache, where the clients keep their lookup handles and must go on
        /// serving through them, a Removed mark would route those reads to persistent storage
        /// where a dirty page was never written.
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

        /// <summary>
        /// Drops all entries and returns the cache-owned rent on each, so pages are disposed
        /// deterministically instead of waiting for the finalizer.
        /// Values are flagged removed-from-cache so a surviving holder re-adds with a fresh rent.
        /// Callers must reset every state client afterwards, the entries are marked Removed and
        /// fail the clients' lookup handles.
        /// </summary>
        public void ClearAndReturnRents()
        {
            lock (m_queueLock)
            {
                DrainQueueOnDispose(m_smallQueue);
                DrainQueueOnDispose(m_mainQueue);
                m_cache.Clear();
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
                // One hold over both locks, split phases let the scan consume a stale count
                // this delete had not published yet, understating the live queue length.
                lock (m_queueLock)
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
                        // The ring buffer cannot remove from the middle, so the slot stays behind as
                        // stale and the scan skips it. Track the count so cleanup can compact later.
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
                // Acquire outside the try so a failed wait cannot release without a matching
                // acquire, and pass the token so disposal can wake a parked wait.
                await _fullLock.WaitAsync(m_cleanupTokenSource.Token);
                try
                {
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
            if (m_cache.TryGetValue(key, out entry))
            {
                if (entry.TryRentValue())
                {
                    Interlocked.Increment(ref m_readCacheHits);
                    return true;
                }
                return false;
            }
            Interlocked.Increment(ref m_readCacheMisses);
            return false;
        }

        /// <summary>
        /// Rents a value only when it is already cached, never touching storage.
        /// Lets a caller hold pages it knows it will read without pulling in the ones that would
        /// cost a read. A hit counts, a caller holding the page will use it; a miss counts
        /// nothing, the page is fetched on the normal path later and counted there.
        /// </summary>
        public bool TryRentCached(long key, [NotNullWhen(true)] out S3FifoCacheEntry? entry)
        {
            if (m_cache.TryGetValue(key, out entry) && entry.TryRentValue())
            {
                Interlocked.Increment(ref m_readCacheHits);
                return true;
            }
            entry = null;
            return false;
        }

        public bool TryGetValue(long key, out ICacheObject? cacheObject)
        {
            if (m_cache.TryGetValue(key, out var entry))
            {
                if (entry.TryRentValue())
                {
                    cacheObject = entry.Value;
                    Interlocked.Increment(ref m_commitCacheHits);
                    return true;
                }
                cacheObject = default;
                return false;
            }
            Interlocked.Increment(ref m_commitCacheMisses);
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
        /// Blocks the background eviction task until ResumeEviction is called, used by recovery,
        /// which clears the cache that an in-flight eviction would write through.
        /// Uses the lock the cleanup task holds, so acquiring it drains any in-flight eviction.
        /// Commit versus eviction exclusion is per client, see SyncStateClient.
        /// </summary>
        internal Task PauseEvictionAsync()
        {
            return _fullLock.WaitAsync();
        }

        internal void ResumeEviction()
        {
            try
            {
                _fullLock.Release();
            }
            catch (ObjectDisposedException)
            {
                // Dispose won the race against an in-flight commit's resume, nothing left to release.
            }
        }

        /// <summary>
        /// How many pages one caller may hold at once while working through a batch. Held pages
        /// are not evictable, so a caller takes at most half the capacity and eviction always has
        /// the other half to work with. Holding more than the cache fits is not possible anyway.
        /// </summary>
        internal int MaxHeldPages => Math.Max(1, Volatile.Read(ref maxSize) / 2);

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
                        // The entry is already in the dictionary, so a hit can land before
                        // this stamp and count. The gap is tiny and the filter is a heuristic.
                        entry.SetCountStamp(m_correlationClock.NextSequence());
                        var inGhost = m_ghostKeys.Remove(key, out var ghostValue);
                        if (inGhost)
                        {
                            // The queue this key was evicted from could not hold it until it was
                            // wanted again. Evidence only, the split moves once per pass.
                            RecordGhostHit(ghostValue.FromMain);
                        }
                        if (inGhost && (ghostValue.Reused || ghostValue.FromMain))
                        {
                            // A counted reuse before eviction plus this re-reference makes
                            // the two events main admission requires, and a key that was in main
                            // already proved as much before it aged out.
                            entry.Location = S3FifoQueueLocation.Main;
                            m_mainQueue.Enqueue(entry);
                        }
                        else
                        {
                            if (inGhost)
                            {
                                // Re-referenced but never counted a reuse while resident.
                                // Bank this as the first event and let small ask for one more.
                                Volatile.Write(ref entry.Frequency, 1);
                            }
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

        /// <summary>
        /// The small queue's default share of the cache, 10% as in the S3-FIFO paper.
        /// </summary>
        private const int DefaultSmallTargetPermille = 100;

        /// <summary>
        /// How far the adaptive split may move the small queue's share.
        /// The floor is two correlation windows worth, the residency a page needs to earn the two
        /// spaced hits promotion asks for. The ceiling leaves the main queue a sliver so a
        /// workload that starts reusing pages again can climb back out.
        /// </summary>
        private const int MinSmallTargetPermille = 50;
        private const int MaxSmallTargetPermille = 950;

        /// <summary>
        /// A ghost hit is decisive: the page was thrown away and wanted straight back, so the
        /// queue that evicted it is too small. Each one is worth a step, which is what lets the
        /// split climb quickly when a workload clearly wants more of one queue.
        /// </summary>
        private const int AdaptPermillePerGhostHit = 1;

        /// <summary>
        /// Most evicted pages are never wanted again even in a well sized cache, so an expiry is
        /// weak evidence and only worth a step in bulk. Scaled to the cache so the trickle is a
        /// property of how much the cache turns over rather than of raw throughput: a full cache
        /// worth of evictions that nobody returns for moves the split about four steps.
        /// </summary>
        private static int ExpiriesPerPermille(int cacheSize) => Math.Max(64, cacheSize / 4);

        /// <summary>
        /// Most the split may move in one cleanup pass however much evidence arrived. Fast enough
        /// to cross its range in about a second of steady evidence, bounded so no burst can carry
        /// it there in one go.
        /// </summary>
        private const int AdaptMaxPermillePerPass = 8;

        /// <summary>
        /// Movement earned since the last pass, applied together and capped there. Growth comes
        /// from the small queue's own ghost hits, shrink from the main queue's ghost hits and from
        /// the slow trickle of small queue evictions that expired unused.
        /// Guarded by the queue lock.
        /// </summary>
        private long m_pendingGrowPermille;
        private long m_pendingShrinkPermille;
        private long m_expiryCredit;

        /// <summary>
        /// Lifetime totals, for diagnostics and tests.
        /// </summary>
        private long m_smallEvictionsSeen;
        private long m_smallGhostHits;
        private long m_mainEvictionsSeen;
        private long m_mainGhostHits;

        private int SmallQueueTargetSize()
        {
            return SmallQueueTargetSize(Volatile.Read(ref maxSize));
        }

        /// <summary>
        /// The small queue share of a given cache size. A pass that drives the cache down to a
        /// smaller size has to shrink the small queue with it. Sized against the capacity
        /// instead, a deep clean to MinSize fills the whole floor with unproven small queue
        /// entries and pays for it with the reused main queue pages, which is backwards, those
        /// are the pages the floor exists to keep.
        /// </summary>
        private int SmallQueueTargetSize(int cacheSize)
        {
            var permille = tableOptions.AdaptiveSmallQueueSize
                ? Volatile.Read(ref m_smallTargetPermille)
                : DefaultSmallTargetPermille;
            return Math.Max(1, (int)((long)cacheSize * permille / 1000));
        }

        /// <summary>
        /// Applies the movement earned since the last pass, capped so no burst of evidence can
        /// carry the split across its range at once.
        /// Growth is fast because a ghost hit is decisive, a page thrown away and wanted straight
        /// back. Shrink from expiries is deliberately a trickle, since most evicted pages are
        /// never wanted again even when the queue is sized well. The share therefore rests at the
        /// paper's ten percent and only leaves it while the evidence keeps saying so, climbing
        /// freely when the main queue is not being used and nothing pushes the other way.
        /// Must be called under the queue lock.
        /// </summary>
        private void AdaptSmallTarget()
        {
            if (!tableOptions.AdaptiveSmallQueueSize)
            {
                return;
            }
            var net = m_pendingGrowPermille - m_pendingShrinkPermille;
            m_pendingGrowPermille = 0;
            m_pendingShrinkPermille = 0;
            if (net == 0)
            {
                return;
            }
            net = Math.Clamp(net, -AdaptMaxPermillePerPass, AdaptMaxPermillePerPass);
            var updated = Math.Clamp(m_smallTargetPermille + (int)net, MinSmallTargetPermille, MaxSmallTargetPermille);
            Volatile.Write(ref m_smallTargetPermille, updated);
        }

        /// <summary>
        /// The queue this key was evicted from could not hold it until it was wanted again.
        /// Must be called under the queue lock.
        /// </summary>
        private void RecordGhostHit(bool fromMain)
        {
            if (!tableOptions.AdaptiveSmallQueueSize)
            {
                return;
            }
            if (fromMain)
            {
                m_mainGhostHits++;
                m_pendingShrinkPermille += AdaptPermillePerGhostHit;
            }
            else
            {
                m_smallGhostHits++;
                m_pendingGrowPermille += AdaptPermillePerGhostHit;
            }
        }

        /// <summary>
        /// A small queue eviction aged out of the ghost queue without ever being wanted again.
        /// Weak evidence, so it only earns a step in bulk.
        /// Must be called under the queue lock.
        /// </summary>
        private void RecordGhostExpiry()
        {
            if (!tableOptions.AdaptiveSmallQueueSize)
            {
                return;
            }
            var perPermille = ExpiriesPerPermille(Volatile.Read(ref maxSize));
            if (++m_expiryCredit >= perPermille)
            {
                m_expiryCredit -= perPermille;
                m_pendingShrinkPermille++;
            }
        }

        internal int SmallTargetPermilleForTests => Volatile.Read(ref m_smallTargetPermille);

        /// <summary>
        /// The evidence the split is read from, for tests and for reading a benchmark run.
        /// </summary>
        internal (long SmallEvictions, long SmallHits, long MainEvictions, long MainHits) AdaptEvidenceForTests
        {
            get
            {
                lock (m_queueLock)
                {
                    return (m_smallEvictionsSeen, m_smallGhostHits, m_mainEvictionsSeen, m_mainGhostHits);
                }
            }
        }


        /// <summary>
        /// How far the small queue is over its target share, used when the cache as a whole is
        /// still below the eviction threshold. The result caps an early drain, so a concurrent
        /// add storm cannot keep one drain running.
        /// </summary>
        private int SmallQueueOverflow(int currentCount)
        {
            // MinSize keeps pages resident to cut read latency, so a cache that is already at or
            // below it is left alone even when its small queue is over the target. Real pressure
            // goes through the normal path, which is allowed to cross the floor.
            var headroom = currentCount - tableOptions.MinSize;
            if (headroom <= 0)
            {
                return 0;
            }
            int liveSmall;
            lock (m_queueLock)
            {
                liveSmall = m_smallQueue.Count - m_smallStaleCount;
            }
            return Math.Min(liveSmall - SmallQueueTargetSize(), headroom);
        }

        private int CorrelationWindowSize()
        {
            return Volatile.Read(ref maxSize) / 40;
        }

        private int GhostCapacity()
        {
            // Half a cache worth of evicted keys, so a ghost hit means the key would still be
            // here if the cache were half again as large.
            // Deliberately not sized off the queue shares:
            // the ghost is the evidence the adaptive split reads, and sizing it from the split
            // would feed the split its own output. A grown small queue would shrink the ghost, a
            // short ghost only remembers the newest evictions, every hit would then look shallow,
            // and shallow hits vote to grow the small queue again.
            return Math.Max(1, Volatile.Read(ref maxSize) / 2);
        }

        internal int GhostCapacityForTests => GhostCapacity();

        private async Task Cleanup()
        {
            var currentCount = Volatile.Read(ref m_count);
            int cleanupStartLocal = cleanupStart;
            bool isCleanup = false;
            if (currentCount <= cleanupStartLocal)
            {
                // Fast-path hits included, a stream reading only through client lookup tables
                // is active and must not be deep-cleaned as idle.
                var cacheHitsLocal = TotalCacheHits() + ExternalCacheHits();
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
                        if (m_sameCacheHitsCount >= 1000)
                        {
                            // Idle with an empty cache, hand the freed pages back to the OS.
                            FlowtideMemoryAllocation.Collect();
                            m_sameCacheHitsCount = 0;
                        }
                        CompactQueuesIfNeeded();
                        return;
                    }
                }
                else
                {
                    // Falls through to the memory check. The count says the cache is small, but
                    // pages can have grown since maxSize was last derived, so the budget still
                    // has to be re-evaluated. Compaction happens on the no-eviction exit below.
                    m_lastSeenCacheHits = cacheHitsLocal;
                    m_sameCacheHitsCount = 0;
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

            // Move the split at most one step, on the evidence gathered since the last pass.
            lock (m_queueLock)
            {
                AdaptSmallTarget();
            }

            // Age the main queue whether or not this pass evicts. Aging is what makes a page
            // earn its place over time, and it used to happen only as a side effect of scanning
            // main for victims, which almost never ran.
            AgeMainQueue(currentCount);

            var smallQueueOverflow = 0;
            if (toBeRemovedCount <= 0)
            {
                CompactQueuesIfNeeded();
                if (!tableOptions.DrainSmallQueueEarly)
                {
                    // Nothing to free. Evicting here would throw away capacity the cache was
                    // configured to use, which costs far more than the queue shares are worth.
                    return;
                }
                smallQueueOverflow = SmallQueueOverflow(currentCount);
                if (smallQueueOverflow <= 0)
                {
                    return;
                }
            }

            // What the cache holds once this pass is done. The deep clean and the memory
            // adaptive resize both aim below the current threshold, so it is derived from the
            // batch size rather than read back off cleanupStart.
            var targetCacheSize = currentCount - toBeRemovedCount;

            // Large batches are selected in chunks, releasing the queue lock between them so
            // Add and Delete are not stalled. Selected victims are dequeued and owned here.
            var victims = new List<EvictionCandidate>();
            var drainSmallQueueOnly = toBeRemovedCount <= 0;
            // Readers can re-pump frequencies between chunks, at correlation window 0 faster than
            // the scan drains them, so the whole pass gets a finite budget on top of the per-hold one.
            var passBudget = ((long)m_smallQueue.Count + m_mainQueue.Count) * (S3FifoCacheEntry.MaxFrequency + 1) + SelectionOperationBudget;
            while (true)
            {
                bool finished;
                lock (m_queueLock)
                {
                    m_selectionLockAcquisitions++;
                    finished = drainSmallQueueOnly
                        ? TrySelectSmallQueueOverflowVictims(victims, smallQueueOverflow, SelectionOperationBudget)
                        : TrySelectVictims(victims, toBeRemovedCount, targetCacheSize, SelectionOperationBudget);
                }
                if (finished)
                {
                    break;
                }
                passBudget -= SelectionOperationBudget;
                if (passBudget <= 0)
                {
                    // Proceed with the victims found so far, the next pass continues the drain.
                    break;
                }
                // An immediate retake wins the unfair monitor race, yield so parked writers get a window.
                Thread.Yield();
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

            List<Task<bool>> evictTasks = new List<Task<bool>>();
            List<List<(S3FifoCacheEntry, long)>> evictTaskGroups = new List<List<(S3FifoCacheEntry, long)>>();
            foreach (var group in groupedValues)
            {
                evictTaskGroups.Add(group.Value);
                evictTasks.Add(Task.Factory.StartNew(() =>
                {
                    return group.Key.Evict(group.Value, isCleanup);
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

            // A failed evict handler did not serialize its victims and a declining one skipped
            // them (its commit is in flight), so keep both cached. Collect them here so they go
            // back into their queues and are retried later.
            HashSet<S3FifoCacheEntry>? failedVictims = null;
            for (int i = 0; i < evictTasks.Count; i++)
            {
                if (!evictTasks[i].IsCompletedSuccessfully || !evictTasks[i].Result)
                {
                    failedVictims ??= new HashSet<S3FifoCacheEntry>();
                    foreach (var value in evictTaskGroups[i])
                    {
                        failedVictims.Add(value.Item1);
                    }
                }
            }

            List<S3FifoCacheEntry>? requeueToSmall = null;
            List<S3FifoCacheEntry>? requeueToMain = null;
            List<(long Key, bool Reused, bool FromMain)>? ghostInserts = null;
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
                        // Held by a reader or a read-ahead window, which is not proven reuse,
                        // so it goes back where it came from instead of being handed the main
                        // queue for free and skipping the two reuse events admission asks for.
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
                    Volatile.Write(ref entry.Removed, true);
                    entry.Value.RemovedFromCache = true;
                    if (m_cache.TryRemove(entry.Key, out _))
                    {
                        Interlocked.Decrement(ref m_count);
                        if (candidate.FromSmallQueue)
                        {
                            // The reuse bit is read here rather than at selection, a hit landing
                            // between the two still earns the entry its ghost credit.
                            (ghostInserts ??= new List<(long, bool, bool)>()).Add((entry.Key, Volatile.Read(ref entry.Frequency) >= 1, false));
                            m_smallQueueEvictions++;
                        }
                        else if (tableOptions.AdaptiveSmallQueueSize)
                        {
                            // Only tracked for the adaptive split. A hit on one of these says the
                            // main queue was too small, which is the evidence the small queue's
                            // own ghost entries can never provide.
                            (ghostInserts ??= new List<(long, bool, bool)>()).Add((entry.Key, true, true));
                        }
                    }
                }
            }

            if (requeueToSmall != null || requeueToMain != null || ghostInserts != null)
            {
                // Chunked like selection, one hold proportional to the batch stalls Add and Delete.
                int smallIndex = 0;
                int mainIndex = 0;
                int ghostIndex = 0;
                while (true)
                {
                    lock (m_queueLock)
                    {
                        var operationBudget = SelectionOperationBudget;
                        while (requeueToSmall != null && smallIndex < requeueToSmall.Count && operationBudget > 0)
                        {
                            operationBudget--;
                            var entry = requeueToSmall[smallIndex++];
                            lock (entry)
                            {
                                // Deleted after the removal phase kept it, enqueuing it would
                                // resurrect a dead slot with no stale count.
                                if (!entry.Removed)
                                {
                                    entry.Location = S3FifoQueueLocation.Small;
                                    m_smallQueue.Enqueue(entry);
                                }
                            }
                        }
                        while (requeueToMain != null && mainIndex < requeueToMain.Count && operationBudget > 0)
                        {
                            operationBudget--;
                            var entry = requeueToMain[mainIndex++];
                            lock (entry)
                            {
                                // Same delete race as the small requeue above.
                                if (!entry.Removed)
                                {
                                    entry.Location = S3FifoQueueLocation.Main;
                                    m_mainQueue.Enqueue(entry);
                                }
                            }
                        }
                        while (ghostInserts != null && ghostIndex < ghostInserts.Count && operationBudget > 0)
                        {
                            operationBudget--;
                            var ghostInsert = ghostInserts[ghostIndex++];
                            AddToGhost(ghostInsert.Key, ghostInsert.Reused, ghostInsert.FromMain, ref operationBudget);
                        }
                    }
                    if ((requeueToSmall == null || smallIndex >= requeueToSmall.Count)
                        && (requeueToMain == null || mainIndex >= requeueToMain.Count)
                        && (ghostInserts == null || ghostIndex >= ghostInserts.Count))
                    {
                        break;
                    }
                    Thread.Yield();
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
        /// targetCacheSize is the entry count the cache is being driven down to, it sets how
        /// much of what survives the pass the small queue is allowed to hold.
        /// </summary>
        /// <returns>
        /// True when selection is complete, false when the budget ran out and the caller
        /// should reacquire the lock for another chunk.
        /// </returns>
        private bool TrySelectVictims(List<EvictionCandidate> victims, int toBeRemovedCount, int targetCacheSize, int operationBudget)
        {
            // The share is taken from the size this pass leaves behind, never more than the
            // capacity share, so a shrink drains the small queue instead of preserving it.
            var smallTarget = SmallQueueTargetSize(Math.Min(Volatile.Read(ref maxSize), targetCacheSize));
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
                // A main head that aged to zero spent every second chance and went MaxFrequency
                // cache turnovers unused, which makes it a weaker page than the small queue head,
                // and that one is at least recent. When main is still being read its head is
                // never zero, so this never rotates a hot main queue looking for a victim.
                if (MainHeadHasAgedOut() || liveSmall <= smallTarget)
                {
                    foundVictim = TryEvictOneFromMain(victims, ref operationBudget) || TryEvictOneFromSmall(victims, ref operationBudget);
                }
                else
                {
                    foundVictim = TryEvictOneFromSmall(victims, ref operationBudget) || TryEvictOneFromMain(victims, ref operationBudget);
                }
                if (!foundVictim && operationBudget > 0)
                {
                    // Both queues were scanned without finding an evictable entry.
                    return true;
                }
            }
            return true;
        }

        private bool TrySelectSmallQueueOverflowVictims(List<EvictionCandidate> victims, int overflowCount, int operationBudget)
        {
            var smallTarget = SmallQueueTargetSize();
            while (victims.Count < overflowCount)
            {
                if (operationBudget <= 0)
                {
                    return false;
                }
                // Draining the small queue promotes its reused heads into main, so main grows
                // here. Without taking its aged out pages too, main would only ever grow while
                // the cache stays below the threshold, and it is those pages, unread for
                // MainQueueTurnoversToAgeOut turnovers, that the drain should pay with first.
                if (MainHeadHasAgedOut() && TryEvictOneFromMain(victims, ref operationBudget))
                {
                    continue;
                }
                if ((m_smallQueue.Count - m_smallStaleCount) <= smallTarget)
                {
                    // The small queue is back at its share and main has nothing aged out.
                    return true;
                }
                if (!TryEvictOneFromSmall(victims, ref operationBudget))
                {
                    // Either the budget ran out mid scan, or the queue holds nothing evictable.
                    return operationBudget > 0;
                }
            }
            return true;
        }

        /// <summary>
        /// How many cache turnovers a main queue page survives without being read again, which
        /// falls out of losing one frequency point per turnover. A read earns a point back per
        /// correlation window, and there are many of those per turnover, so a page still being
        /// read stays saturated while an abandoned one drains.
        /// </summary>
        private const int MainQueueTurnoversToAgeOut = S3FifoCacheEntry.MaxFrequency;

        /// <summary>
        /// Max aging steps in one pass, so a large main queue cannot hold the queue lock while it
        /// is swept. Whatever is left over is aged on the following passes.
        /// </summary>
        private const int AgingOperationBudget = 1024;

        /// <summary>
        /// Insertion count the last aging sweep ran at, and the sweep work carried over from it.
        /// The sweep is paced by how much the cache turned over rather than by wall clock, so a
        /// quiet stream does not age its own hot set. The carry over matters, a pass usually
        /// earns a fraction of a step and dropping it would leave a small main queue never aged.
        /// </summary>
        private long m_lastAgingSequence;
        private long m_agingCredit;

        /// <summary>
        /// Aging steps taken so far, used by unit tests to pin the pacing.
        /// Written only on the cleanup thread.
        /// </summary>
        private long m_agingSteps;

        internal long AgingStepsForTests => Volatile.Read(ref m_agingSteps);

        /// <summary>
        /// Advances the aging hand over the main queue, decrementing frequencies without evicting
        /// anything. A page that stops being read drains to zero and becomes the first thing
        /// eviction takes, a page still being read is pushed back up by its hits.
        /// Freeing space is the eviction pass's job, aging only decides what has earned its place,
        /// which is why this runs even when nothing needs to be evicted.
        /// </summary>
        private void AgeMainQueue(int currentCount)
        {
            var sequence = m_correlationClock.CurrentSequence();
            var inserted = sequence - m_lastAgingSequence;
            m_lastAgingSequence = sequence;
            if (inserted <= 0)
            {
                return;
            }

            int liveMain;
            lock (m_queueLock)
            {
                liveMain = m_mainQueue.Count - m_mainStaleCount;
            }
            if (liveMain <= 0)
            {
                return;
            }

            // One sweep of the whole main queue per cache turnover, so each page loses a point
            // per turnover and an unread one is gone after MainQueueTurnoversToAgeOut of them.
            // Scaling by the main queue's own size keeps a small main queue cheap to age.
            // A turnover is what it takes to replace the pages actually resident, not the
            // configured ceiling. Paced off the ceiling, a cache running well below it, which is
            // what the early drain and a working set smaller than the cache both produce, would
            // age its main queue far slower than it really turns over.
            var turnover = Math.Max(1, currentCount);
            m_agingCredit += inserted * liveMain;
            var stepsToRun = m_agingCredit / turnover;
            m_agingCredit -= stepsToRun * turnover;
            var steps = (int)Math.Min(int.MaxValue, stepsToRun);
            while (steps > 0)
            {
                var chunk = Math.Min(steps, AgingOperationBudget);
                steps -= chunk;
                lock (m_queueLock)
                {
                    AgeMainQueueChunk(chunk);
                }
                m_agingSteps += chunk;
                if (steps > 0)
                {
                    // Give writers parked on the queue lock a window, as the selection scan does.
                    Thread.Yield();
                }
            }
        }

        /// <summary>
        /// Must be called under the queue lock.
        /// </summary>
        private void AgeMainQueueChunk(int steps)
        {
            for (int i = 0; i < steps && m_mainQueue.Count > 0; i++)
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
                        Interlocked.Decrement(ref entry.Frequency);
                    }
                    // Aged pages stay queued, eviction decides whether the space is needed.
                    m_mainQueue.Enqueue(entry);
                }
            }
        }

        /// <summary>
        /// True when the main queue head has aged out and is ready to be taken.
        /// Must be called under the queue lock.
        /// </summary>
        private bool MainHeadHasAgedOut()
        {
            if (m_mainQueue.Count == 0)
            {
                return false;
            }
            var head = m_mainQueue.Peek();
            return Volatile.Read(ref head.Removed) || Volatile.Read(ref head.Frequency) == 0;
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
                        // Two counted hits promote, the same two-reuse-events bar as the ghost
                        // path. A single counted hit leaves through the ghost queue with the
                        // reuse recorded, so a re-reference completes the pair there.
                        entry.Location = S3FifoQueueLocation.Main;
                        m_mainQueue.Enqueue(entry);
                        m_smallQueuePromotions++;
                        continue;
                    }
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
        /// The trim is bounded by the budget, a shrink can drop the capacity by the whole
        /// queue at once and the leftover excess trims on later inserts.
        /// </summary>
        private void AddToGhost(long key, bool reused, bool fromMain, ref int operationBudget)
        {
            var sequence = ++m_ghostSequence;
            m_ghostKeys[key] = new GhostValue(sequence, reused, fromMain);
            if (fromMain)
            {
                m_mainEvictionsSeen++;
            }
            else
            {
                m_smallEvictionsSeen++;
            }
            m_ghostQueue.Enqueue(new GhostRecord(key, sequence));
            var capacity = GhostCapacity();
            while (m_ghostQueue.Count > capacity && operationBudget > 0)
            {
                operationBudget--;
                var oldest = m_ghostQueue.Dequeue();
                if (m_ghostKeys.TryGetValue(oldest.Key, out var storedValue) && storedValue.Sequence == oldest.Sequence)
                {
                    // A live ghost membership ages out, this key was evicted and never
                    // re-admitted. Added once, never promoted, and now gone. A one-hit-wonder.
                    m_ghostKeys.Remove(oldest.Key);
                    if (!storedValue.FromMain)
                    {
                        m_oneHitWonders++;
                        RecordGhostExpiry();
                    }
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
            }
            CompactQueue(m_smallQueue, small: true);
            CompactQueue(m_mainQueue, small: false);
        }

        /// <summary>
        /// Removes stale (deleted) slots from a queue, chunked under the selection budget so a
        /// large compaction does not stall Add and Delete for its whole duration.
        /// Adds landing between chunks interleave with re-enqueued survivors, that FIFO drift
        /// during a rare compaction is accepted. The stale counters are decremented per dropped
        /// slot, a reset would lose counts published while the compaction is in flight.
        /// </summary>
        private void CompactQueue(Queue<S3FifoCacheEntry> queue, bool small)
        {
            int remaining;
            lock (m_queueLock)
            {
                remaining = queue.Count;
            }
            while (remaining > 0)
            {
                lock (m_queueLock)
                {
                    // remaining is an upper bound, a concurrent test-driven cleanup pass can
                    // drain the queue between chunks, so it is clamped to the live count.
                    remaining = Math.Min(remaining, queue.Count);
                    var operationBudget = SelectionOperationBudget;
                    while (remaining > 0 && operationBudget > 0)
                    {
                        operationBudget--;
                        remaining--;
                        var entry = queue.Dequeue();
                        // Removed is written under the queue lock in Delete and dequeued victims
                        // never re-enter the queues, so a volatile read is enough here.
                        if (!Volatile.Read(ref entry.Removed))
                        {
                            queue.Enqueue(entry);
                        }
                        else
                        {
                            entry.Location = S3FifoQueueLocation.None;
                            if (small)
                            {
                                if (m_smallStaleCount > 0)
                                {
                                    m_smallStaleCount--;
                                }
                            }
                            else if (m_mainStaleCount > 0)
                            {
                                m_mainStaleCount--;
                            }
                        }
                    }
                }
                if (remaining > 0)
                {
                    Thread.Yield();
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

        internal long ReadCacheHitsForTests => Volatile.Read(ref m_readCacheHits);

        internal long ReadCacheMissesForTests => Volatile.Read(ref m_readCacheMisses);

        internal long CommitCacheHitsForTests => Volatile.Read(ref m_commitCacheHits);

        internal long CommitCacheMissesForTests => Volatile.Read(ref m_commitCacheMisses);

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
                        // Flagged before the dictionary removal so a racing re-add takes a new rent.
                        entry.Value.RemovedFromCache = true;
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

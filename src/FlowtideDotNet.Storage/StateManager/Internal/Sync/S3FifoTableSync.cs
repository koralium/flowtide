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

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal partial class S3FifoTableSync : IDisposable
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
            /// Which ghost insertion this record belongs to.
            /// Stops a stale slot removing a newer membership.
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
            /// Which queue the victim came from, for requeue and ghost.
            /// </summary>
            public bool FromSmallQueue { get; }
        }

        /// <summary>
        /// Compact when stale slots pass this and the live count.
        /// Deletes leave a stale slot behind.
        /// </summary>
        private const int CompactionMinimumStaleCount = 1024;

        /// <summary>
        /// Cadence of the background cleanup pass.
        /// </summary>
        private const int CleanupIntervalMs = 10;

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
            /// Counted a reuse before eviction, so a re-reference admits it to main.
            /// </summary>
            public bool Reused { get; }

            /// <summary>
            /// Evicted from main, so a hit on it says main was too small.
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
        /// The small queue share in permille, adapted from ghost hits.
        /// Written under the queue lock, read with Volatile off it.
        /// </summary>
        private int m_smallTargetPermille = DefaultSmallTargetPermille;

        private Task? m_cleanupTask;
        private int maxSize;
        private readonly ILogger logger;
        // Owned by the state manager, never disposed here.
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
        // Client fast path hits the table never sees.
        // Read by the idle check so a fast-path-only stream is not wiped.
        private readonly List<Func<long>> m_externalHitCounters = new List<Func<long>>();
        // Written on the cleanup thread, read lock-free by the metrics.
        private long m_smallQueuePromotions;
        private long m_smallQueueEvictions;
        private long m_oneHitWonders;
        private long m_lastSeenCacheHits;
        private int m_sameCacheHitsCount;
        // Forced collections issued by this table, tests pin when a deep clean collects.
        private long m_collectCalls;
        // Pages freed since the last forced collection, an idle table with none does not collect.
        private long m_pagesFreedSinceCollect;

        // Guards the gauge state below, callbacks run per listener thread.
        private readonly object m_metricsLock = new object();
        private long m_metrics_lastSeenTotal;
        private long m_metrics_lastSeenHits;
        private float m_metrics_lastSentPercentage;


        private bool m_disposedValue;
        private readonly CancellationTokenSource m_cleanupTokenSource;
        // Cancelled by Dispose alone, it wakes waits parked on the eviction gate.
        private readonly CancellationTokenSource m_disposeTokenSource = new CancellationTokenSource();
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
            this.tableOptions = tableOptions;
            StartCleanupTask();
            RegisterMetrics();
        }

        private void RegisterMetrics()
        {
            if (!string.IsNullOrEmpty(m_streamName))
            {
                // LRU metric names kept so existing dashboards keep working.
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
                // Promotions went to main, evictions to ghost.
                // One hit wonders aged out of ghost unused.
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
                    // Carries state between collections, so the window needs a lock.
                    lock (m_metricsLock)
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
                        }
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
                // Split by path. Read is query processing, commit is dirty pages at checkpoint.
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
        /// Drops all entries without returning references or marking them removed.
        /// Only for ClearCache, the clients keep serving through their lookup handles.
        /// </summary>
        public void Clear()
        {
            lock (m_queueLock)
            {
                m_cache.Clear();
                m_smallQueue.Clear();
                m_mainQueue.Clear();
                ResetGhostState();
                m_smallStaleCount = 0;
                m_mainStaleCount = 0;
                Volatile.Write(ref m_count, 0);
            }
        }

        /// <summary>
        /// Drops all entries and returns the cache rent, disposing deterministically.
        /// Callers must reset every state client afterwards.
        /// </summary>
        public void ClearAndReturnRents()
        {
            lock (m_queueLock)
            {
                DrainQueueOnDispose(m_smallQueue);
                DrainQueueOnDispose(m_mainQueue);
                m_cache.Clear();
                ResetGhostState();
                m_smallStaleCount = 0;
                m_mainStaleCount = 0;
                Volatile.Write(ref m_count, 0);
            }
        }

        public void Delete(in long key)
        {
            if (!m_cache.TryGetValue(key, out var entry))
            {
                return;
            }
            while (true)
            {
                // One hold over both locks, or the scan reads too early.
                // Never block here, eviction holds it across a spill write.
                lock (m_queueLock)
                {
                    if (Monitor.TryEnter(entry))
                    {
                        try
                        {
                            if (entry.Removed)
                            {
                                return;
                            }
                            // Write Removed before Return so lock-free readers see it once a rent fails.
                            Volatile.Write(ref entry.Removed, true);
                            if (m_cache.TryRemove(key, out _))
                            {
                                // Flagged before removal so a racing re-add takes a new rent.
                                entry.Value.RemovedFromCache = true;
                                entry.Value.Return();
                                Interlocked.Decrement(ref m_count);
                                Interlocked.Increment(ref m_pagesFreedSinceCollect);
                            }
                            // No removal from the middle, cleanup compacts the stale slot.
                            if (entry.Location == S3FifoQueueLocation.Small)
                            {
                                m_smallStaleCount++;
                            }
                            else if (entry.Location == S3FifoQueueLocation.Main)
                            {
                                m_mainStaleCount++;
                            }
                            return;
                        }
                        finally
                        {
                            Monitor.Exit(entry);
                        }
                    }
                }
                // Wait for the entry lock with the queue lock dropped.
                lock (entry)
                {
                }
                Thread.Yield();
            }
        }

        public bool TryGetCacheValue(long key, [NotNullWhen(true)] out S3FifoCacheEntry? entry)
        {
            // Lock-free read, a failed rent means it is being evicted.
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
        /// Rents a value only when already cached, never touching storage.
        /// A miss counts nothing, the normal path fetches and counts it later.
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

        /// <summary>
        /// Commit path rent, counted separately and not recorded as an access.
        /// </summary>
        public bool TryGetValue(long key, out ICacheObject? cacheObject)
        {
            if (m_cache.TryGetValue(key, out var entry))
            {
                if (entry.TryRentValueWithoutAccess())
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

        /// <summary>
        /// Cleanup passes a producer waits before it proceeds anyway.
        /// </summary>
        private const int MaxWaitPassesWhenFull = 16;

        private bool IsOverCapacity => Volatile.Read(ref m_count) > Volatile.Read(ref maxSize);

        public async Task Wait()
        {
            logger.LruTableIsFull(m_streamName);
            try
            {
                for (int pass = 0; pass < MaxWaitPassesWhenFull; pass++)
                {
                    // Checked first, the common case costs no acquisition.
                    if (!IsOverCapacity)
                    {
                        break;
                    }
                    // Rides out the pass in flight. The token wakes a parked wait on Dispose.
                    await _fullLock.WaitAsync(m_disposeTokenSource.Token).ConfigureAwait(false);
                    _fullLock.Release();
                    if (!IsOverCapacity)
                    {
                        break;
                    }
                    if (pass + 1 >= MaxWaitPassesWhenFull)
                    {
                        // Out of passes, sleeping here would be dead time.
                        break;
                    }
                    // No room freed, let the next pass run first.
                    await Task.Delay(CleanupIntervalMs, m_cleanupTokenSource.Token).ConfigureAwait(false);
                }
            }
            catch (ObjectDisposedException)
            {
                // Disposed while parked, the stream is tearing down.
            }
            catch (OperationCanceledException)
            {
                // Cleanup cancelled, disposal is in progress.
            }
            logger.LruTableNoLongerFull(m_streamName);
        }

        /// <summary>
        /// Blocks the eviction task until ResumeEviction, used by recovery.
        /// Takes the cleanup task's lock, so it drains any in-flight eviction.
        /// Cancelled by Dispose, a parked pause must not outlive the table.
        /// </summary>
        internal Task PauseEvictionAsync()
        {
            return _fullLock.WaitAsync(m_disposeTokenSource.Token);
        }

        internal void ResumeEviction()
        {
            try
            {
                _fullLock.Release();
            }
            catch (ObjectDisposedException)
            {
                // Dispose won the race, nothing left to release.
            }
        }

        /// <summary>
        /// Pages one caller may hold at once, half the capacity.
        /// Held pages are not evictable, so eviction keeps the other half.
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
                    // Removed by eviction, retry and insert a fresh entry.
                    continue;
                }

                var entry = new S3FifoCacheEntry(key, value, evictHandler, m_correlationClock);
                if (m_cache.TryAdd(key, entry))
                {
                    if (value.RemovedFromCache)
                    {
                        // Defensive, take a new cache rent, the caller keeps its own.
                        if (!value.TryRent())
                        {
                            // Withdraw the entry, an orphan would poison the key.
                            m_cache.TryRemove(new KeyValuePair<long, S3FifoCacheEntry>(key, entry));
                            throw new InvalidOperationException("Already disposed");
                        }
                        value.RemovedFromCache = false;
                    }

                    lock (m_queueLock)
                    {
                        // A hit can land before this stamp, the filter is a heuristic.
                        entry.SetCountStamp(m_correlationClock.NextSequence());
                        var inGhost = m_ghostKeys.Remove(key, out var ghostValue);
                        if (inGhost)
                        {
                            DropGhostMembership(ghostValue.FromMain);
                            // Evidence only, the split moves once per pass.
                            RecordGhostHit(ghostValue.FromMain);
                        }
                        if (inGhost && (ghostValue.Reused || ghostValue.FromMain))
                        {
                            // Two reuse events, which is what main admission requires.
                            entry.Location = S3FifoQueueLocation.Main;
                            m_mainQueue.Enqueue(entry);
                        }
                        else
                        {
                            if (inGhost)
                            {
                                // Bank the first event, small asks for one more.
                                Volatile.Write(ref entry.Frequency, 1);
                            }
                            entry.Location = S3FifoQueueLocation.Small;
                            m_smallQueue.Enqueue(entry);
                        }
                    }

                    Interlocked.Increment(ref m_count);
                    return full;
                }
                // Lost the insert race, retry as an update.
            }
        }

        /// <summary>
        /// Used for testing only
        /// </summary>
        internal Task ForceCleanup()
        {
            return Cleanup();
        }

        #region Test helpers

        /// <summary>
        /// Looks up an entry without renting, for test assertions only.
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

        internal long CollectCallsForTests => Volatile.Read(ref m_collectCalls);

        #endregion

        private void DisposeEntries()
        {
            lock (m_queueLock)
            {
                DrainQueueOnDispose(m_smallQueue);
                DrainQueueOnDispose(m_mainQueue);
                ResetGhostState();
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
                        // Flagged before removal so a racing re-add takes a new rent.
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
                    // Parked gate waits wake now.
                    m_disposeTokenSource.Cancel();
                    var cleanupTask = m_cleanupTask;
                    while (cleanupTask != null)
                    {
                        try
                        {
                            cleanupTask.Wait();
                        }
                        catch
                        {
                            // A faulted task rethrows on Wait, disposal swallows it.
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
                    m_disposeTokenSource.Dispose();
                    // The gate is never disposed. A cancelled wait still unlinks itself from it
                    // asynchronously, and a disposed semaphore drops that waiter so it never wakes.
                    // It owns no handle, so there is nothing to release.
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

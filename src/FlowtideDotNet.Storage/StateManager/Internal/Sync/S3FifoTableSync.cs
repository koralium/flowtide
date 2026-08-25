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
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
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
            /// Only small queue evictions enter the ghost queue.
            /// </summary>
            public bool FromSmallQueue { get; }
        }

        /// <summary>
        /// Compact when stale slots pass this and the live count.
        /// Deletes leave a stale slot behind.
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
                m_ghostQueue.Clear();
                m_ghostKeys.Clear();
                m_ghostSmallEntries = 0;
                m_ghostMainEntries = 0;
                // Pending movement came from the wiped ghost queue, it goes too.
                m_pendingGrowPermille = 0;
                m_pendingShrinkPermille = 0;
                m_expiryCredit = 0;
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
                m_ghostQueue.Clear();
                m_ghostKeys.Clear();
                m_ghostSmallEntries = 0;
                m_ghostMainEntries = 0;
                // Pending movement came from the wiped ghost queue, it goes too.
                m_pendingGrowPermille = 0;
                m_pendingShrinkPermille = 0;
                m_expiryCredit = 0;
                m_smallStaleCount = 0;
                m_mainStaleCount = 0;
                Volatile.Write(ref m_count, 0);
            }
        }

        public void Delete(in long key)
        {
            if (m_cache.TryGetValue(key, out var entry))
            {
                // One hold over both locks, or the scan reads too early.
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
                        // No removal from the middle, cleanup compacts the stale slot.
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
            // PeriodicTimer allocates nothing per tick, Task.Delay made a GC sawtooth.
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(10));
            while (true)
            {
                m_cleanupTokenSource.Token.ThrowIfCancellationRequested();
                await timer.WaitForNextTickAsync(m_cleanupTokenSource.Token);
                // Acquire outside the try, a failed wait must not release.
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
        /// Blocks the eviction task until ResumeEviction, used by recovery.
        /// Takes the cleanup task's lock, so it drains any in-flight eviction.
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

        /// <summary>
        /// The default small queue share, 10% as in the paper.
        /// </summary>
        private const int DefaultSmallTargetPermille = 100;

        /// <summary>
        /// How far the adaptive split may move the small queue share.
        /// </summary>
        private const int MinSmallTargetPermille = 50;
        private const int MaxSmallTargetPermille = 950;

        /// <summary>
        /// Smallest move one ghost hit is worth.
        /// </summary>
        private const int AdaptPermillePerGhostHit = 1;

        /// <summary>
        /// Cap on how much a rare queue's hit is magnified.
        /// </summary>
        private const int AdaptMaxHitWeight = 16;

        /// <summary>
        /// Ghost entries needed before the shares are trusted.
        /// </summary>
        private const int AdaptMinimumEvidence = 64;

        /// <summary>
        /// Expiries needed to shrink the share one step.
        /// </summary>
        private static int ExpiriesPerPermille(int cacheSize) => Math.Max(64, cacheSize / 4);

        /// <summary>
        /// Most the split may move in one cleanup pass.
        /// </summary>
        private const int AdaptMaxPermillePerPass = 8;

        /// <summary>
        /// Movement earned since the last pass. Guarded by the queue lock.
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

        private long m_ghostSmallEntries;
        private long m_ghostMainEntries;

        private int SmallQueueTargetSize()
        {
            return SmallQueueTargetSize(Volatile.Read(ref maxSize));
        }

        /// <summary>
        /// Small queue share of a cache size, as the early drain caps it.
        /// Sized off what the pass leaves behind, not the capacity.
        /// </summary>
        private int SmallQueueTargetSize(int cacheSize)
        {
            var permille = tableOptions.AdaptiveSmallQueueSize
                ? Volatile.Read(ref m_smallTargetPermille)
                : DefaultSmallTargetPermille;
            return Math.Max(1, (int)((long)cacheSize * permille / 1000));
        }

        /// <summary>
        /// The small queue share to defend when the cache is full.
        /// Never above the paper's share, adaptation may still take it below.
        /// </summary>
        private int SmallQueuePressureShare(int cacheSize)
        {
            var permille = tableOptions.AdaptiveSmallQueueSize
                ? Math.Min(Volatile.Read(ref m_smallTargetPermille), DefaultSmallTargetPermille)
                : DefaultSmallTargetPermille;
            return Math.Max(1, (int)((long)cacheSize * permille / 1000));
        }

        /// <summary>
        /// Applies the movement earned since the last pass, capped on the net.
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
        /// The queue that evicted this key could not hold it.
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
                m_pendingShrinkPermille += GhostHitWeight(m_ghostSmallEntries, m_ghostMainEntries);
            }
            else
            {
                m_smallGhostHits++;
                m_pendingGrowPermille += GhostHitWeight(m_ghostMainEntries, m_ghostSmallEntries);
            }
        }

        internal static long GhostHitWeightForTests(long otherGhostEntries, long ownGhostEntries)
            => GhostHitWeight(otherGhostEntries, ownGhostEntries);

        private static long GhostHitWeight(long otherGhostEntries, long ownGhostEntries)
        {
            if (otherGhostEntries + ownGhostEntries < AdaptMinimumEvidence)
            {
                return AdaptPermillePerGhostHit;
            }
            return Math.Clamp(otherGhostEntries / Math.Max(1, ownGhostEntries), AdaptPermillePerGhostHit, AdaptMaxHitWeight);
        }

        /// <summary>
        /// A small queue eviction aged out of ghost unused.
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

        internal int SmallQueuePressureShareForTests(int cacheSize) => SmallQueuePressureShare(cacheSize);

        internal (long Small, long Main, int Remembered) GhostMembershipForTests
        {
            get
            {
                lock (m_queueLock)
                {
                    return (m_ghostSmallEntries, m_ghostMainEntries, m_ghostKeys.Count);
                }
            }
        }

        /// <summary>
        /// The evidence the split is read from.
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
        /// How far the small queue is over its share, below the threshold.
        /// Caps an early drain so an add storm cannot keep one running.
        /// </summary>
        private int SmallQueueOverflow(int currentCount)
        {
            // A cache at MinSize is left alone, real pressure crosses the floor instead.
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
            // Half a cache worth of evicted keys.
            // Never sized off the shares, that would feed the split its own output.
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
                // Fast path hits count, that stream is active and not idle.
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
                    // Falls through to the memory check, pages may have grown since maxSize.
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
                        // The correlation window follows maxSize too.
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

            // Move the split at most one step per pass.
            lock (m_queueLock)
            {
                AdaptSmallTarget();
            }

            // Age main whether or not this pass evicts.
            AgeMainQueue(currentCount);

            var smallQueueOverflow = 0;
            if (toBeRemovedCount <= 0)
            {
                CompactQueuesIfNeeded();
                if (!tableOptions.DrainSmallQueueEarly)
                {
                    // Nothing to free, evicting here throws away configured capacity.
                    return;
                }
                smallQueueOverflow = SmallQueueOverflow(currentCount);
                if (smallQueueOverflow <= 0)
                {
                    return;
                }
            }

            // What the cache holds once this pass is done.
            var targetCacheSize = currentCount - toBeRemovedCount;

            // Selected in chunks so Add and Delete are not stalled.
            var victims = new List<EvictionCandidate>();
            var drainSmallQueueOnly = toBeRemovedCount <= 0;
            // Readers re-pump frequencies between chunks, so the pass gets its own budget.
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
                // An immediate retake wins the monitor race, yield instead.
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

            // Failed and declined victims were never serialized, so keep them cached.
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
                        // Deleted during eviction, requeuing would resurrect a dead slot.
                        continue;
                    }
                    if (failedVictims != null && failedVictims.Contains(entry))
                    {
                        // Its handler failed, put it back for a later retry.
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
                        // Modified while serializing, so the copy is stale. It is being used.
                        (requeueToMain ??= new List<S3FifoCacheEntry>()).Add(entry);
                        continue;
                    }
                    // Only evict pages nothing else references.
                    // Evicting a held page lets a reload make a second diverging copy.
                    if (!entry.Value.TryReclaimForEviction())
                    {
                        // Being held is not proven reuse, back where it came from.
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
                            // Read the reuse bit here, a late hit still earns ghost credit.
                            (ghostInserts ??= new List<(long, bool, bool)>()).Add((entry.Key, Volatile.Read(ref entry.Frequency) >= 1, false));
                            m_smallQueueEvictions++;
                        }
                        else if (tableOptions.AdaptiveSmallQueueSize)
                        {
                            // Only for the adaptive split, a hit says main was too small.
                            (ghostInserts ??= new List<(long, bool, bool)>()).Add((entry.Key, true, true));
                        }
                    }
                }
            }

            if (requeueToSmall != null || requeueToMain != null || ghostInserts != null)
            {
                // Chunked like selection, one long hold stalls Add and Delete.
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
                                // Deleted after the removal phase, enqueuing resurrects a dead slot.
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
                // Rethrow after the victims are rehomed, so cleanup restarts.
                ExceptionDispatchInfo.Capture(evictException).Throw();
            }

            if (isCleanup)
            {
                FlowtideMemoryAllocation.Collect();
            }
        }

        /// <summary>
        /// Max queue operations per lock hold during selection.
        /// </summary>
        private const int SelectionOperationBudget = 256;

        /// <summary>
        /// Lock acquisitions spent on selection, tests check batches are chunked.
        /// </summary>
        private long m_selectionLockAcquisitions;

        internal long SelectionLockAcquisitionsForTests => Volatile.Read(ref m_selectionLockAcquisitions);

        /// <summary>
        /// Scans until enough victims, nothing evictable, or budget spent.
        /// Victims are only dequeued here, they stay readable until removal.
        /// Must be called under the queue lock.
        /// </summary>
        /// <returns>
        /// True when selection is complete, false when the budget ran out
        /// and the caller should reacquire the lock.
        /// </returns>
        private bool TrySelectVictims(List<EvictionCandidate> victims, int toBeRemovedCount, int targetCacheSize, int operationBudget)
        {
            // Share off what the pass leaves behind, so a shrink drains small too.
            var smallTarget = SmallQueuePressureShare(Math.Min(Volatile.Read(ref maxSize), targetCacheSize));
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
                // The aged out main head is weaker than the small head.
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
                // The drain grows main, so pay with its aged out pages first.
                if (MainHeadHasAgedOut() && TryEvictOneFromMain(victims, ref operationBudget))
                {
                    continue;
                }
                if ((m_smallQueue.Count - m_smallStaleCount) <= smallTarget)
                {
                    // Small is back at its share, main has nothing aged out.
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
        /// Cache turnovers an unread main page survives.
        /// </summary>
        private const int MainQueueTurnoversToAgeOut = S3FifoCacheEntry.MaxFrequency;

        /// <summary>
        /// Max aging steps per lock hold, the rest ages on later passes.
        /// </summary>
        private const int AgingOperationBudget = 1024;

        /// <summary>
        /// Insertion count the last sweep ran at, and its carried over work.
        /// A pass earns a fraction of a step, so it carries.
        /// </summary>
        private long m_lastAgingSequence;
        private long m_agingCredit;

        /// <summary>
        /// Aging steps so far, tests pin the pacing on it.
        /// Written only on the cleanup thread.
        /// </summary>
        private long m_agingSteps;

        internal long AgingStepsForTests => Volatile.Read(ref m_agingSteps);

        internal static long UsefulAgingSteps(long stepsToRun, int liveMain)
        {
            return Math.Min(stepsToRun, (long)MainQueueTurnoversToAgeOut * Math.Max(0, liveMain));
        }

        /// <summary>
        /// Advances the aging hand over main, decrementing without evicting.
        /// Runs even when nothing needs freeing.
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

            // One sweep of main per cache turnover.
            // Turnover is the resident pages, not the ceiling.
            var turnover = Math.Max(1, currentCount);
            m_agingCredit += inserted * liveMain;
            var stepsToRun = m_agingCredit / turnover;
            m_agingCredit -= stepsToRun * turnover;
            var steps = (int)Math.Min(int.MaxValue, UsefulAgingSteps(stepsToRun, liveMain));
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
                    // Give parked writers a window, as selection does.
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
        /// True when the main head has aged out.
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
                        // Two counted hits promote, the same bar as the ghost path.
                        // One hit leaves through ghost and completes the pair there.
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
                        // Second chance, back to the tail with one point less.
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
        /// The trim is bounded by the budget, the excess trims on later inserts.
        /// Must be called under the queue lock.
        /// </summary>
        private void AddToGhost(long key, bool reused, bool fromMain, ref int operationBudget)
        {
            var sequence = ++m_ghostSequence;
            ref var membership = ref CollectionsMarshal.GetValueRefOrAddDefault(m_ghostKeys, key, out var alreadyRemembered);
            if (alreadyRemembered)
            {
                // Evicted again while the earlier membership is still live, so replace it.
                DropGhostMembership(membership.FromMain);
            }
            membership = new GhostValue(sequence, reused, fromMain);
            if (fromMain)
            {
                m_ghostMainEntries++;
                m_mainEvictionsSeen++;
            }
            else
            {
                m_ghostSmallEntries++;
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
                    // Aged out never re-admitted, a one hit wonder.
                    m_ghostKeys.Remove(oldest.Key);
                    DropGhostMembership(storedValue.FromMain);
                    if (!storedValue.FromMain)
                    {
                        m_oneHitWonders++;
                        RecordGhostExpiry();
                    }
                }
            }
        }

        /// <summary>
        /// A remembered ghost key is gone, wanted again, aged out or replaced.
        /// Must be called under the queue lock.
        /// </summary>
        private void DropGhostMembership(bool fromMain)
        {
            if (fromMain)
            {
                if (m_ghostMainEntries > 0)
                {
                    m_ghostMainEntries--;
                }
            }
            else if (m_ghostSmallEntries > 0)
            {
                m_ghostSmallEntries--;
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
        /// Removes stale slots from a queue, chunked so it does not stall writers.
        /// Adds between chunks drift the FIFO order, which is accepted here.
        /// Stale counters drop per slot, a reset would lose in flight counts.
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
                    // An upper bound, a concurrent pass can drain the queue between chunks.
                    remaining = Math.Min(remaining, queue.Count);
                    var operationBudget = SelectionOperationBudget;
                    while (remaining > 0 && operationBudget > 0)
                    {
                        operationBudget--;
                        remaining--;
                        var entry = queue.Dequeue();
                        // Removed is written under this lock, a volatile read is enough.
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

        #endregion

        private void DisposeEntries()
        {
            lock (m_queueLock)
            {
                DrainQueueOnDispose(m_smallQueue);
                DrainQueueOnDispose(m_mainQueue);
                m_ghostQueue.Clear();
                m_ghostKeys.Clear();
                m_ghostSmallEntries = 0;
                m_ghostMainEntries = 0;
                // Pending movement came from the wiped ghost queue, it goes too.
                m_pendingGrowPermille = 0;
                m_pendingShrinkPermille = 0;
                m_expiryCredit = 0;
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

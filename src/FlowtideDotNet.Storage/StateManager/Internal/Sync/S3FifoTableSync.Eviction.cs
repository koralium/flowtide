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
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    // The background cleanup pass, victim selection and the ghost queue.
    internal partial class S3FifoTableSync
    {
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
            if (!TryPlanEviction(currentCount, out var toBeRemovedCount, out var isCleanup))
            {
                return;
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

            // Rent the scratch set so the 10ms cadence does not allocate.
            var scratch = Interlocked.Exchange(ref m_cleanupScratch, null) ?? new CleanupScratch();
            try
            {
                SelectVictims(scratch, toBeRemovedCount, currentCount - toBeRemovedCount, smallQueueOverflow);
                if (scratch.Victims.Count == 0)
                {
                    return;
                }

                var (failedVictims, evictException) = await RunEvictHandlers(scratch, isCleanup);
                RemoveOrRequeueVictims(scratch, failedVictims);

                if (evictException != null)
                {
                    // Rethrow after the victims are rehomed, so cleanup restarts.
                    ExceptionDispatchInfo.Capture(evictException).Throw();
                }
            }
            finally
            {
                scratch.Reset();
                Volatile.Write(ref m_cleanupScratch, scratch);
            }

            if (isCleanup)
            {
                FlowtideMemoryAllocation.Collect();
            }
        }

        /// <summary>
        /// The collections one cleanup pass works through, reused between passes.
        /// A concurrent test driven pass rents nothing and builds a fresh set.
        /// </summary>
        private sealed class CleanupScratch
        {
            public readonly List<EvictionCandidate> Victims = new List<EvictionCandidate>();
            public readonly Dictionary<ICacheEvictHandler, List<(S3FifoCacheEntry, long)>> VictimsByHandler = new Dictionary<ICacheEvictHandler, List<(S3FifoCacheEntry, long)>>();
            public readonly List<Task<bool>> EvictTasks = new List<Task<bool>>();
            public readonly List<List<(S3FifoCacheEntry, long)>> EvictTaskGroups = new List<List<(S3FifoCacheEntry, long)>>();
            public readonly List<S3FifoCacheEntry> RequeueToSmall = new List<S3FifoCacheEntry>();
            public readonly List<S3FifoCacheEntry> RequeueToMain = new List<S3FifoCacheEntry>();
            public readonly List<(long Key, bool Reused, bool FromMain)> GhostInserts = new List<(long, bool, bool)>();
            public readonly Stack<List<(S3FifoCacheEntry, long)>> HandlerListPool = new Stack<List<(S3FifoCacheEntry, long)>>();

            public void Reset()
            {
                Victims.Clear();
                GhostInserts.Clear();
                RequeueToSmall.Clear();
                RequeueToMain.Clear();
                EvictTasks.Clear();
                EvictTaskGroups.Clear();
                foreach (var list in VictimsByHandler.Values)
                {
                    list.Clear();
                    HandlerListPool.Push(list);
                }
                VictimsByHandler.Clear();
                // A rare deep clean must not pin its huge backing arrays forever.
                if (Victims.Capacity > 65536)
                {
                    Victims.TrimExcess();
                }
                if (GhostInserts.Capacity > 65536)
                {
                    GhostInserts.TrimExcess();
                }
            }
        }

        /// <summary>
        /// The parked scratch set, exchanged out by the pass that runs.
        /// </summary>
        private CleanupScratch? m_cleanupScratch = new CleanupScratch();

        /// <summary>
        /// Decides how much this pass evicts.
        /// Covers the idle deep clean and the memory driven resize.
        /// False when the pass has nothing left to do.
        /// </summary>
        private bool TryPlanEviction(int currentCount, out int toBeRemovedCount, out bool isCleanup)
        {
            int cleanupStartLocal = cleanupStart;
            isCleanup = false;
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
                        toBeRemovedCount = 0;
                        return false;
                    }
                }
                else
                {
                    // Falls through to the memory check, pages may have grown since maxSize.
                    m_lastSeenCacheHits = cacheHitsLocal;
                    m_sameCacheHitsCount = 0;
                }
            }

            toBeRemovedCount = currentCount - cleanupStartLocal;
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

            return true;
        }

        /// <summary>
        /// Runs the eviction scans in chunks, filling scratch with the owned victims.
        /// targetCacheSize is what the cache holds once this pass is done.
        /// </summary>
        private void SelectVictims(CleanupScratch scratch, int toBeRemovedCount, int targetCacheSize, int smallQueueOverflow)
        {
            // Selected in chunks so Add and Delete are not stalled.
            var victims = scratch.Victims;
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
        }

        /// <summary>
        /// Fans the victims out to their evict handlers and awaits them.
        /// Returns the victims whose handler failed or declined, and the failure.
        /// </summary>
        private async Task<(HashSet<S3FifoCacheEntry>? FailedVictims, Exception? EvictException)> RunEvictHandlers(CleanupScratch scratch, bool isCleanup)
        {
            var groupedValues = scratch.VictimsByHandler;
            foreach (var candidate in scratch.Victims)
            {
                if (!groupedValues.TryGetValue(candidate.Entry.EvictHandler, out var list))
                {
                    list = scratch.HandlerListPool.Count > 0
                        ? scratch.HandlerListPool.Pop()
                        : new List<(S3FifoCacheEntry, long)>();
                    groupedValues.Add(candidate.Entry.EvictHandler, list);
                }
                list.Add((candidate.Entry, candidate.Version));
            }

            var evictTasks = scratch.EvictTasks;
            var evictTaskGroups = scratch.EvictTaskGroups;
            foreach (var group in groupedValues)
            {
                evictTaskGroups.Add(group.Value);
                // The state overload spares a closure per handler.
                evictTasks.Add(Task.Factory.StartNew(
                    static state =>
                    {
                        var (handler, victimsForHandler, cleanup) = ((ICacheEvictHandler, List<(S3FifoCacheEntry, long)>, bool))state!;
                        return handler.Evict(victimsForHandler, cleanup);
                    },
                    (group.Key, group.Value, isCleanup)));
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
            return (failedVictims, evictException);
        }

        /// <summary>
        /// Removes each victim or puts it back, then applies the requeues and
        /// ghost inserts in chunks.
        /// </summary>
        private void RemoveOrRequeueVictims(CleanupScratch scratch, HashSet<S3FifoCacheEntry>? failedVictims)
        {
            var requeueToSmall = scratch.RequeueToSmall;
            var requeueToMain = scratch.RequeueToMain;
            var ghostInserts = scratch.GhostInserts;
            foreach (var candidate in scratch.Victims)
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
                            requeueToSmall.Add(entry);
                        }
                        else
                        {
                            requeueToMain.Add(entry);
                        }
                        continue;
                    }
                    if (candidate.Version != entry.Version)
                    {
                        // Modified while serializing, so the copy is stale. It is being used.
                        requeueToMain.Add(entry);
                        continue;
                    }
                    // Only evict pages nothing else references.
                    // Evicting a held page lets a reload make a second diverging copy.
                    if (!entry.Value.TryReclaimForEviction())
                    {
                        // Being held is not proven reuse, back where it came from.
                        if (candidate.FromSmallQueue)
                        {
                            requeueToSmall.Add(entry);
                        }
                        else
                        {
                            requeueToMain.Add(entry);
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
                            ghostInserts.Add((entry.Key, Volatile.Read(ref entry.Frequency) >= 1, false));
                            m_smallQueueEvictions++;
                        }
                        else if (tableOptions.AdaptiveSmallQueueSize)
                        {
                            // Only for the adaptive split, a hit says main was too small.
                            ghostInserts.Add((entry.Key, true, true));
                        }
                    }
                }
            }

            if (requeueToSmall.Count > 0 || requeueToMain.Count > 0 || ghostInserts.Count > 0)
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
                        while (smallIndex < requeueToSmall.Count && operationBudget > 0)
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
                        while (mainIndex < requeueToMain.Count && operationBudget > 0)
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
                        while (ghostIndex < ghostInserts.Count && operationBudget > 0)
                        {
                            operationBudget--;
                            var ghostInsert = ghostInserts[ghostIndex++];
                            AddToGhost(ghostInsert.Key, ghostInsert.Reused, ghostInsert.FromMain, ref operationBudget);
                        }
                    }
                    if (smallIndex >= requeueToSmall.Count
                        && mainIndex >= requeueToMain.Count
                        && ghostIndex >= ghostInserts.Count)
                    {
                        break;
                    }
                    Thread.Yield();
                }
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
        /// Forgets the ghost queue and every piece of evidence built on it.
        /// Must be called under the queue lock.
        /// </summary>
        private void ResetGhostState()
        {
            m_ghostQueue.Clear();
            m_ghostKeys.Clear();
            m_ghostSmallEntries = 0;
            m_ghostMainEntries = 0;
            m_pendingGrowPermille = 0;
            m_pendingShrinkPermille = 0;
            m_expiryCredit = 0;
            m_expiryForgiveness = 0;
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

    }
}

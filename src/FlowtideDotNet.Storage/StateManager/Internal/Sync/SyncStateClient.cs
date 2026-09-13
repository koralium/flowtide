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

using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Utils;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.ExceptionServices;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class SyncStateClient<V, TMetadata> : StateClient, IStateClient<V, TMetadata>, ICacheEvictHandler, IStateSerializerInitializeReader, IStateSerializerCheckpointWriter
        where V : ICacheObject
        where TMetadata : class, IStorageMetadata
    {
        /// <summary>
        /// A modified page: its write sequence, -1 for a delete.
        /// </summary>
        private readonly record struct Modified(long Sequence);

        /// <summary>
        /// One Commit's worth of pages, shared by the walk and on-fetch writes. Reused only
        /// after the previous worker, including its temporary-allocation cleanup, has finished.
        /// </summary>
        private sealed class CommitGeneration
        {
            /// <summary>
            /// Pending writes and the currently claimed key, guarded by the client's m_lock.
            /// The walk and fetch share these so each snapshot is written once.
            /// </summary>
            public Dictionary<long, Modified> Pending = new Dictionary<long, Modified>();
            public long WritingKey = -1;
            public long[] Keys = Array.Empty<long>();
            public readonly List<long> HeldKeys = new List<long>();
            public int Count;
            private int _smallGenerations;

            /// <summary>
            /// Net page count change, deletes in the walk take from it.
            /// </summary>
            public long NewPages;

            public bool PreviousCommitedOnce;
            public bool PreviousMetadataUpdated;
            public HashSet<long>? DeletedKeys;

            /// <summary>
            /// Pages whose original checkpoint copy was spilled before replacing their cache entry.
            /// Guarded by m_lock and retained even if the replacement is deleted again.
            /// </summary>
            public HashSet<long>? ReplacedKeys;

            /// <summary>
            /// The first preparation, write or cleanup failure. Cleared only by reset.
            /// </summary>
            public Exception? Failure;

            public void Capture(ref Dictionary<long, Modified> modified, long newPages)
            {
                Debug.Assert(Pending.Count == 0 && WritingKey == -1 && Failure == null);
                (Pending, modified) = (modified, Pending);
                Count = Pending.Count;
                NewPages = newPages;
                DeletedKeys?.Clear();
                ReplacedKeys?.Clear();
                if (Keys.Length < Count)
                {
                    Keys = new long[Count + (Count >> 1) + 16];
                    _smallGenerations = 0;
                }
                else if (Count != 0)
                {
                    // An idle checkpoint says nothing about the size the client needs.
                    if (Keys.Length > Math.Max(4 * Count, 1024))
                    {
                        if (++_smallGenerations >= 8)
                        {
                            Keys = new long[Count + (Count >> 1) + 16];
                            // Release peak-sized snapshot sets along with the oversized buffer.
                            DeletedKeys = null;
                            ReplacedKeys = null;
                            _smallGenerations = 0;
                        }
                    }
                    else
                    {
                        _smallGenerations = 0;
                    }
                }
                Pending.Keys.CopyTo(Keys, 0);
                foreach (var kv in Pending)
                {
                    if (kv.Value.Sequence == -1)
                    {
                        (DeletedKeys ??= new HashSet<long>()).Add(kv.Key);
                    }
                }
            }
        }

        /// <summary>
        /// How long an eviction pass waits for the lock before it declines, one page write on most stores.
        /// </summary>
        private static readonly TimeSpan EvictWaitBudget = TimeSpan.FromMilliseconds(2);

        private bool disposedValue;
        private readonly StateManagerSync stateManager;
        private readonly string name;
        private readonly long metadataId;
        private StateClientMetadata<TMetadata> metadata;
        private readonly IPersistentStorageSession session;
        private readonly StateClientOptions<V> options;
        private readonly bool useReadCache;
        private readonly bool m_backgroundCommit;
        private readonly int m_bplusTreePageSize;
        private readonly int m_bplusTreePageSizeBytes;
        private readonly IMemoryAllocator memoryAllocator;

        /// <summary>
        /// Pages written since the last Commit, guarded by m_lock. Swapped with the generation at Commit.
        /// </summary>
        private Dictionary<long, Modified> m_modified;

        /// <summary>
        /// Reusable snapshot storage, including the last commit's failure until reset.
        /// </summary>
        private readonly CommitGeneration m_commit = new CommitGeneration();

        /// <summary>
        /// The generation in flight, null when none, guarded by m_lock and read under m_commitEvictLock by the writers.
        /// </summary>
        private CommitGeneration? m_generation;
        // The worker can outlive the active snapshot during cleanup or a timed-out stop.
        // Keep its physical lifetime separate; disposal and recovery must join the actual task.
        private Task? m_commitTask;
        private Task? m_disposalTask;
        private bool m_commitTaskObserved;

        /// <summary>
        /// Set by a stop or an abandoned drain, the walk gives up at its next page. Cleared by the recovery pause.
        /// </summary>
        private bool m_stopRequested;

        private readonly object m_lock = new object();

        /// <summary>
        /// Per-page lock between the walk, the on-fetch writes and Evict, never disposed.
        /// </summary>
        private readonly SemaphoreSlim m_commitEvictLock = new SemaphoreSlim(1, 1);

        /// <summary>
        /// One session call at a time for a session that is not thread-safe, the walk writes
        /// while the operator reads. Taken inside m_commitEvictLock, never the other way.
        /// </summary>
        private readonly SemaphoreSlim? m_sessionLock;
        private readonly FlowtideDotNet.Storage.FileCache.IFileCache m_fileCache;
        private readonly ConcurrentDictionary<long, long> m_fileCacheVersion;

        /// <summary>
        /// Monotonic write generation, guarded by m_lock and never reset.
        /// The eviction dedup compares it against m_fileCacheVersion. Resetting it per commit
        /// would let a straddling eviction collide with a later write and drop a modified page.
        /// </summary>
        private long m_writeSequence;
        private readonly Histogram<float>? m_persistenceReadMsHistogram;
        private readonly Histogram<float>? m_temporaryReadMsHistogram;
        private readonly Histogram<float>? m_temporaryWriteMsHistogram;
        private readonly TagList tagList;

        /// <summary>
        /// Direct mapped cache of entry references in front of the shared table.
        /// Slots are single references read atomically, the key is validated on the entry.
        /// Writes happen under m_lock, the GetValue fast path reads lock-free.
        /// </summary>
        private readonly S3FifoCacheEntry?[] _lookupTable;

        /// <summary>
        /// A constant divisor, so the hot path modulo is a multiply instead of a 64-bit divide.
        /// </summary>
        private const int LookupTableSize = 1009;

        /// <summary>
        /// Value of how many pages have changed since last commit.
        /// </summary>
        private long newPages;
        private long cacheMisses;

        /// <summary>
        /// Hits served by the lock-free lookup table, which bypass the shared table's hit
        /// counter. Plain increment, the client read path runs on one thread. Read with
        /// Volatile by the metric callback.
        /// </summary>
        private long m_lookupTableHits;


        public long CacheMisses => cacheMisses;

        public override long MetadataId => metadataId;

        public SyncStateClient(
            StateManagerSync stateManager,
            string name,
            long metadataId,
            StateClientMetadata<TMetadata> metadata,
            IPersistentStorageSession session,
            StateClientOptions<V> options,
            IFileCacheFactory fileCacheFactory,
            Meter meter,
            bool useReadCache,
            bool backgroundCommit,
            int bplusTreePageSize,
            int bplusTreePageSizeBytes,
            IMemoryAllocator memoryAllocator)
        {
            this.stateManager = stateManager;
            this.name = name;
            this.metadataId = metadataId;
            this.metadata = metadata;
            this.session = session;
            this.options = options;
            this.useReadCache = useReadCache;
            this.m_backgroundCommit = backgroundCommit;
            this.m_bplusTreePageSize = bplusTreePageSize;
            this.m_bplusTreePageSizeBytes = bplusTreePageSizeBytes;
            this.memoryAllocator = memoryAllocator;
            m_fileCache = fileCacheFactory.Create(name, memoryAllocator);
            m_sessionLock = session.SupportsConcurrentReads ? null : new SemaphoreSlim(1, 1);
            m_modified = new Dictionary<long, Modified>();
            m_fileCacheVersion = new ConcurrentDictionary<long, long>();
            if (!string.IsNullOrEmpty(name))
            {
                m_persistenceReadMsHistogram = meter.CreateHistogram<float>("flowtide_persistence_read_ms");
                m_temporaryReadMsHistogram = meter.CreateHistogram<float>("flowtide_temporary_read_ms");
                m_temporaryWriteMsHistogram = meter.CreateHistogram<float>("flowtide_temporary_write_ms");
            }
            tagList = options.TagList;
            tagList.Add("state_client", name);

            if (!string.IsNullOrEmpty(name))
            {
                meter.CreateObservableCounter("flowtide_state_client_lookup_hits", () =>
                {
                    return new Measurement<long>(Volatile.Read(ref m_lookupTableHits), tagList);
                });
            }

            _lookupTable = new S3FifoCacheEntry?[LookupTableSize];
            // The fast path bypasses the table's hit counters, feed its count to the idle check.
            stateManager.RegisterExternalHitCounter(() => Volatile.Read(ref m_lookupTableHits));
        }

        public TMetadata? Metadata
        {
            get
            {
                return metadata.Metadata;
            }
            set
            {
                metadata.Metadata = value;
            }
        }

        public int BPlusTreePageSize => m_bplusTreePageSize;

        public int BPlusTreePageSizeBytes => m_bplusTreePageSizeBytes;

        public bool AddOrUpdate(in long key, V value)
        {
            lock (m_lock)
            {
                if (m_generation == null ||
                    !m_modified.TryGetValue(key, out var modified) || modified.Sequence != -1 ||
                    !m_commit.Pending.TryGetValue(key, out var pending) || pending.Sequence <= 0 ||
                    (m_generation.ReplacedKeys?.Contains(key) ?? false))
                {
                    return AddOrUpdate_NoLock(key, value);
                }
            }

            // Replacement spills share the existing serialization lock.
            m_commitEvictLock.Wait();
            try
            {
                lock (m_lock)
                {
                    return AddOrUpdate_NoLock(key, value);
                }
            }
            finally
            {
                m_commitEvictLock.Release();
            }
        }

        private bool AddOrUpdate_NoLock(in long key, V value)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            if (m_generation != null && OwesCheckpointWrite_NoLock(key))
            {
                if (m_modified.TryGetValue(key, out var modified))
                {
                    if (modified.Sequence == -1)
                    {
                        if (m_commit.Pending.TryGetValue(key, out var pending) && pending.Sequence > 0 &&
                            !(m_generation.ReplacedKeys?.Contains(key) ?? false))
                        {
                            if (stateManager.TryPeekCacheEntry(key, out var oldEntry))
                            {
                                Debug.Assert(options.ValueSerializer != null);
                                // Spill old version before replacing cache entry.
                                m_fileCacheVersion[key] = pending.Sequence;
                                try
                                {
                                    m_fileCache.Write(key, new SerializableObject(oldEntry.Value, options.ValueSerializer));
                                }
                                catch
                                {
                                    // Failed spills must not advance the stored version.
                                    m_fileCacheVersion.TryRemove(new KeyValuePair<long, long>(key, pending.Sequence));
                                    throw;
                                }
                            }
                            // Replacements preserve the original page's existing spill.
                            (m_generation.ReplacedKeys ??= new HashSet<long>()).Add(key);
                        }
                        stateManager.DeleteFromCache(key);
                    }
                    // Replaced page version already spilled to file cache.
                }
                else if (m_commit.Pending.TryGetValue(key, out var pending) && pending.Sequence == -1)
                {
                    // Deleted in pending generation, does not conflict with SessionDelete.
                    stateManager.DeleteFromCache(key);
                }
                else
                {
                    // Pages retained across commits require a rent.
                    throw new InvalidOperationException($"Page '{key}' on state client '{name}' was written before its checkpoint write, a page kept across Commit must be rented.");
                }
            }
            if (m_fileCacheVersion.TryGetValue(key, out var ver) && ver == -2)
            {
                // Invalidate obsolete read cache before adding updated page.
                FreeSpill(key);
            }
            m_modified[key] = new Modified(++m_writeSequence);

            var modLookup = key % LookupTableSize;
            var lookupEntry = _lookupTable[modLookup];
            if (lookupEntry != null && lookupEntry.Key == key)
            {
                lock (lookupEntry)
                {
                    if (!ReferenceEquals(lookupEntry.Value, value))
                    {
                        stateManager.DeleteFromCache(key);
                        Volatile.Write(ref _lookupTable[modLookup], null);
                    }
                    else
                    {
                        lookupEntry.Version = lookupEntry.Version + 1;
                        // Live entries already belong to the cache.
                        if (!lookupEntry.Removed)
                        {
                            return stateManager.IsOverCapacity;
                        }
                    }
                }
            }
            else if (stateManager.TryPeekCacheEntry(key, out var existingEntry))
            {
                lock (existingEntry)
                {
                    if (!ReferenceEquals(existingEntry.Value, value))
                    {
                        stateManager.DeleteFromCache(key);
                    }
                }
            }

            var full = stateManager.AddOrUpdate(key, value, this, out var entry);
            Volatile.Write(ref _lookupTable[modLookup], entry);
            return full || stateManager.IsOverCapacity;
        }

        public Task WaitForNotFullAsync()
        {
            return stateManager.WaitForNotFullAsync();
        }

        /// <summary>
        /// Highest number of pages the caller may hold at once while working through a batch.
        /// Held pages cannot be evicted, so the cache decides how many it can spare.
        /// </summary>
        public int MaxHeldPages => stateManager.MaxHeldPages;

        /// <summary>
        /// Rents the page only when it is already cached, so a caller can hold pages it is
        /// certain to read without paying a read for the ones that are not there.
        /// </summary>
        public bool TryGetCachedValue(in long key, out V? value)
        {
            // A page owing its checkpoint write is handed out by GetValue only, which writes it first.
            var modLookup = key % LookupTableSize;
            var entry = Volatile.Read(ref _lookupTable[modLookup]);
            if (entry != null && entry.Key == key)
            {
                if (Volatile.Read(ref entry.OwesCheckpointWrite))
                {
                    value = default;
                    return false;
                }
                if (entry.TryRentValue())
                {
                    m_lookupTableHits++;
                    value = (V)entry.Value;
                    return true;
                }
            }
            // Lock-free probe, a miss never waits behind a commit.
            if (stateManager.TryRentCachedValue(key, out var cached))
            {
                if (Volatile.Read(ref cached.OwesCheckpointWrite))
                {
                    cached.Value.Return();
                    value = default;
                    return false;
                }
                // Publish under the lock like every other slot write.
                // A delete racing this leaves a stale slot the fast path already tolerates.
                lock (m_lock)
                {
                    Volatile.Write(ref _lookupTable[modLookup], cached);
                }
                value = (V)cached.Value;
                return true;
            }
            value = default;
            return false;
        }

        internal long LookupTableHitsForTests => Volatile.Read(ref m_lookupTableHits);

        private bool OwesCheckpointWrite_NoLock(long key)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            return m_commit.Pending.ContainsKey(key) || m_commit.WritingKey == key;
        }

        private Task EnterSessionAsync()
        {
            return m_sessionLock?.WaitAsync() ?? Task.CompletedTask;
        }

        private void ExitSession()
        {
            m_sessionLock?.Release();
        }

        private async Task SessionWrite(long key, SerializableObject value)
        {
            await EnterSessionAsync();
            try
            {
                await session.Write(key, value);
            }
            finally
            {
                ExitSession();
            }
        }

        private async Task SessionDelete(long key)
        {
            await EnterSessionAsync();
            try
            {
                await session.Delete(key);
            }
            finally
            {
                ExitSession();
            }
        }

        private async ValueTask<ReadOnlyMemory<byte>> SessionRead(long key)
        {
            await EnterSessionAsync();
            try
            {
                return await session.Read(key);
            }
            finally
            {
                ExitSession();
            }
        }

        private async ValueTask<T> SessionRead<T>(long key, IStateSerializer<T> serializer)
            where T : ICacheObject
        {
            await EnterSessionAsync();
            try
            {
                return await session.Read(key, serializer);
            }
            finally
            {
                ExitSession();
            }
        }

        private async Task SessionCommit()
        {
            await EnterSessionAsync();
            try
            {
                await session.Commit();
            }
            finally
            {
                ExitSession();
            }
        }

        private async Task JoinQuietlyAsync(Task? commitTask, TimeSpan timeout)
        {
            if (commitTask == null)
            {
                return;
            }
            // A walk wedged on storage fails the recovery instead of hanging it, the retry joins again.
            if (!commitTask.IsCompleted && await Task.WhenAny(commitTask, Task.Delay(timeout)).ConfigureAwait(false) != commitTask)
            {
                throw new InvalidOperationException($"State client '{name}' is still writing its commit after {timeout}, storage may be wedged.");
            }
            try
            {
                await commitTask.ConfigureAwait(false);
            }
            catch
            {
            }
        }

        internal override async Task PauseCommitsAsync(TimeSpan walkTimeout)
        {
            // The reset that follows is the response to a failed commit, its fault is not the caller's.
            // Commit publishes its walk under the lock, so a walk seen here after taking it is complete.
            while (true)
            {
                var commitTask = Volatile.Read(ref m_commitTask);
                await JoinQuietlyAsync(commitTask, walkTimeout);
                await m_commitEvictLock.WaitAsync();
                if (ReferenceEquals(Volatile.Read(ref m_commitTask), commitTask))
                {
                    // Consumed, the reset under this pause must not see the fault again.
                    Volatile.Write(ref m_commitTask, null);
                    Volatile.Write(ref m_commitTaskObserved, false);
                    // A stop request from an abandoned drain is done with, the client runs on after the reset.
                    Volatile.Write(ref m_stopRequested, false);
                    return;
                }
                // A commit slipped in between the join and the lock, join that one too.
                m_commitEvictLock.Release();
            }
        }

        internal override void ResumeCommits()
        {
            m_commitEvictLock.Release();
        }

        internal override bool HasCommitInFlight => Volatile.Read(ref m_commitTask) is { IsCompleted: false };
        internal override bool HasCommitFault => Volatile.Read(ref m_commit.Failure) != null;
        internal override Exception? CommitFault => Volatile.Read(ref m_commit.Failure);

        internal override Task WaitForCommitAsync()
        {
            var commitTask = Volatile.Read(ref m_commitTask);
            if (commitTask == null)
            {
                return Task.CompletedTask;
            }
            Volatile.Write(ref m_commitTaskObserved, true);
            return commitTask;
        }

        internal override void RequestStopCommits()
        {
            // The walk gives up at its next page, before the cache table goes away.
            Volatile.Write(ref m_stopRequested, true);
        }

        internal override void StopCommits(TimeSpan timeout)
        {
            RequestStopCommits();
            var commitTask = Volatile.Read(ref m_commitTask);
            if (commitTask == null)
            {
                return;
            }
            // A plain wait needs no scheduler to complete on, and a walk wedged on storage
            // must not hold the teardown, it faults against the disposed resources instead.
            try
            {
                commitTask.Wait(timeout);
            }
            catch (AggregateException)
            {
            }
        }

        internal bool CommitedOnceForTests => metadata.CommitedOnce;

        public async ValueTask Commit()
        {
            Debug.Assert(options.ValueSerializer != null);

            // One generation in flight per client, a previous commit still running is joined first.
            await WaitForCommitAsync();
            lock (m_lock)
            {
                // Refused before anything is written, a failed generation only leaves with a reset.
                if (m_generation != null || m_commit.Failure != null)
                {
                    throw new InvalidOperationException($"State client '{name}' must be reset after its last commit failed.");
                }
            }

            // The serializer checkpoint and the metadata are taken on the caller's thread, the
            // operator changes the tree metadata again as soon as this returns.
            var previousCommitedOnce = metadata.CommitedOnce;
            var previousMetadataUpdated = metadata.Metadata?.Updated ?? false;
            Task commitTask;
            await m_commitEvictLock.WaitAsync();
            try
            {
                lock (m_lock)
                {
                    if (m_generation != null || m_commit.Failure != null || HasCommitInFlight)
                    {
                        throw new InvalidOperationException(m_commit.Failure != null
                            ? $"State client '{name}' must be reset after its last commit failed."
                            : $"State client '{name}' already has a commit in flight.");
                    }
                }
                var generation = m_commit;
                generation.PreviousCommitedOnce = previousCommitedOnce;
                generation.PreviousMetadataUpdated = previousMetadataUpdated;
                try
                {
                    await options.ValueSerializer.CheckpointAsync(this, metadata);
                    await WriteMetadata();

                    lock (m_lock)
                    {
                        generation.Capture(ref m_modified, Interlocked.Exchange(ref newPages, 0));
                        m_generation = generation;
                    }

                    // A page someone still holds may be changed in place as soon as this returns, so
                    // its checkpoint copy is taken here. After this no pending page has a holder.
                    await WriteHeldPages(generation);

                    if (!m_backgroundCommit)
                    {
                        await CommitGenerationAsync(generation, lockHeld: true);
                        return;
                    }
                }
                catch (Exception e)
                {
                    // The metadata went into the session with the flag set, a failure before
                    // the session commit must take the flag back or recovery reads a page that never landed.
                    metadata.CommitedOnce = previousCommitedOnce;
                    if (metadata.Metadata != null)
                    {
                        metadata.Metadata.Updated = previousMetadataUpdated;
                    }
                    // Preparation failures use the same fault state as background and on-fetch writes.
                    generation.Failure ??= e;
                    throw;
                }

                // Published while the lock is held, so a pause that holds the lock has seen every walk.
                // Session writes mostly complete synchronously, run inline the walk would stay on the caller's thread.
                commitTask = Task.Factory.StartNew(static state =>
                {
                    var client = (SyncStateClient<V, TMetadata>)state!;
                    return client.CommitGenerationAsync(client.m_commit, lockHeld: false);
                }, this, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();
                Volatile.Write(ref m_commitTaskObserved, false);
                Volatile.Write(ref m_commitTask, commitTask);
            }
            finally
            {
                m_commitEvictLock.Release();
            }
        }

        /// <summary>
        /// Flags every cached generation page and writes the ones rented by someone other than
        /// the cache, caller holds m_commitEvictLock.
        /// </summary>
        private async Task WriteHeldPages(CommitGeneration generation)
        {
            var held = m_commit.HeldKeys;
            held.Clear();
            foreach (var kv in m_commit.Pending)
            {
                // A delete has no page, an evicted one has no entry until a reload makes a fresh one.
                if (kv.Value.Sequence == -1 || !stateManager.TryPeekCacheEntry(kv.Key, out var entry))
                {
                    continue;
                }
                // The fast paths decline the page until its write clears this.
                Volatile.Write(ref entry.OwesCheckpointWrite, true);
                if (entry.Value.RentCount > 1)
                {
                    held.Add(kv.Key);
                }
            }
            foreach (var key in held)
            {
                await WritePendingPage(key, generation);
            }
        }

        private async Task CommitGenerationAsync(CommitGeneration generation, bool lockHeld)
        {
            Debug.Assert(options.ValueSerializer != null);
            try
            {
                var hook = stateManager.PageWriteHookForTests;
                var keys = generation.Keys;
                for (int i = 0; i < generation.Count; i++)
                {
                    var key = keys[i];
                    ThrowIfStopRequested();
                    if (hook != null)
                    {
                        await hook(name, key);
                    }
                    if (!lockHeld)
                    {
                        await m_commitEvictLock.WaitAsync();
                    }
                    try
                    {
                        // Waited for the lock past a dispose, the table it reads from is gone.
                        ThrowIfStopRequested();
                        ThrowIfFailed(generation);
                        // Already written by a fetch or at Commit.
                        if (!await WritePendingPage(key, generation))
                        {
                            continue;
                        }
                    }
                    finally
                    {
                        if (!lockHeld)
                        {
                            m_commitEvictLock.Release();
                        }
                    }
                    if (!lockHeld && (i & 255) == 255)
                    {
                        // Session writes mostly complete synchronously, hand the pool thread back now and then.
                        await Task.Yield();
                    }
                }
                ThrowIfStopRequested();

                // An on-fetch write still in flight holds the lock, it lands before the commit.
                if (!lockHeld)
                {
                    await m_commitEvictLock.WaitAsync();
                }
                try
                {
                    ThrowIfStopRequested();
                    ThrowIfFailed(generation);
                    lock (m_lock)
                    {
                        Debug.Assert(m_commit.Pending.Count == 0);
                    }

                    Debug.Assert(stateManager.m_metadata != null);
                    await SessionCommit();

                    // Update metadata counters only after commit succeeds.
                    Interlocked.Add(ref stateManager.m_metadata.PageCommits, (ulong)generation.Count);
                    Interlocked.Add(ref stateManager.m_metadata.PageCount, generation.NewPages);

                    lock (m_lock)
                    {
                        m_generation = null;
                    }

                    // Worker cleanup shares the existing serialization gate.
                    m_fileCache.ClearTemporaryAllocations();
                    options.ValueSerializer.ClearTemporaryAllocations();
                }
                finally
                {
                    if (!lockHeld)
                    {
                        m_commitEvictLock.Release();
                    }
                }
            }
            catch (Exception e)
            {
                metadata.CommitedOnce = generation.PreviousCommitedOnce;
                if (metadata.Metadata != null)
                {
                    metadata.Metadata.Updated = generation.PreviousMetadataUpdated;
                }
                // A failure past the pages, the session commit, leaves the generation marked too.
                generation.Failure ??= e;
                throw;
            }
        }

        private void ThrowIfStopRequested()
        {
            // Faulted, not returned, a checkpoint waiting on this walk must not take it as landed.
            if (Volatile.Read(ref m_stopRequested))
            {
                throw new OperationCanceledException("The commit was given up on a stop request.");
            }
        }

        /// <summary>
        /// Rethrows an earlier page write failure as is, the checkpoint sees the storage error.
        /// </summary>
        private static void ThrowIfFailed(CommitGeneration generation)
        {
            if (generation.Failure != null)
            {
                ExceptionDispatchInfo.Throw(generation.Failure);
            }
        }

        /// <summary>
        /// Claims and writes one snapshot, returning false if it was already written.
        /// Caller holds m_commitEvictLock; claiming and renting also hold m_lock so a
        /// concurrent replacement cannot destroy the original. Failures restore the claim.
        /// </summary>
        private async Task<bool> WritePendingPage(long key, CommitGeneration generation)
        {
            Debug.Assert(options.ValueSerializer != null);
            long version = 0;
            S3FifoCacheEntry? entry = null;
            try
            {
                bool isReplaced;
                lock (m_lock)
                {
                    if (!generation.Pending.Remove(key, out var pending))
                    {
                        return false;
                    }
                    version = pending.Sequence;
                    generation.WritingKey = key;
                    isReplaced = generation.ReplacedKeys?.Contains(key) ?? false;
                    if (version > 0 && !isReplaced)
                    {
                        stateManager.TryRentCacheEntryForCommit(key, out entry);
                    }
                }
                if (version == -1)
                {
                    // deleted
                    await SessionDelete(key);

                    // Remove a page from the new pages counter
                    generation.NewPages--;
                    FreeSpill(key);
                }
                else if (entry != null)
                {
                    // Return the lookup's rent even when the write throws, a leaked rent keeps the page unevictable forever.
                    try
                    {
                        await SessionWrite(key, new SerializableObject(entry.Value, options.ValueSerializer));
                    }
                    finally
                    {
                        entry.Value.Return();
                    }
                    // The spill is older than what was just written.
                    FreeSpill(key);
                }
                else
                {
                    var bytes = await m_fileCache.Read(key);
                    await SessionWrite(key, new SerializableObject(bytes));

                    if (isReplaced || !useReadCache)
                    {
                        FreeSpill(key);
                    }
                    else
                    {
                        // Set version to -2 which marks that it is a read only version
                        m_fileCacheVersion[key] = -2;
                    }
                }
            }
            catch (Exception e)
            {
                // Still owed and never retried, the walk stops at its next page and reports this.
                lock (m_lock)
                {
                    generation.WritingKey = -1;
                    if (version != 0)
                    {
                        generation.Pending[key] = new Modified(version);
                    }
                }
                generation.Failure ??= e;
                throw;
            }
            lock (m_lock)
            {
                generation.WritingKey = -1;
                if (entry != null)
                {
                    // Handed out again only once the key is no longer owed, AddOrUpdate checks that under this lock.
                    Volatile.Write(ref entry.OwesCheckpointWrite, false);
                }
                if (m_modified.TryGetValue(key, out var current) && current.Sequence == -1)
                {
                    // The delete waited for this write, see Delete.
                    Delete_NoLock(key);
                }
            }
            return true;
        }

        /// <summary>
        /// Writes the page's checkpoint copy before a fetch hands it out for modification.
        /// </summary>
        private async ValueTask CommitPendingPage(long key)
        {
            await m_commitEvictLock.WaitAsync();
            try
            {
                var generation = m_generation;
                if (generation == null)
                {
                    // The walk finished first.
                    return;
                }
                ThrowIfFailed(generation);
                await WritePendingPage(key, generation);
            }
            finally
            {
                m_commitEvictLock.Release();
            }
        }

        /// <summary>
        /// Sets CommitedOnce before the write, Commit takes it back on any failure.
        /// </summary>
        private async Task WriteMetadata()
        {
            if (!metadata.CommitedOnce || (metadata.Metadata != null && metadata.Metadata.Updated))
            {
                metadata.CommitedOnce = true;
                var bytes = StateClientMetadataSerializer.Serialize(metadata);
                await SessionWrite(metadataId, new SerializableObject(bytes));
                if (metadata.Metadata != null)
                {
                    metadata.Metadata.Updated = false;
                }
            }
        }

        public void Delete(in long key)
        {
            lock (m_lock)
            {
                var owesCheckpointWrite = m_generation != null && OwesCheckpointWrite_NoLock(key);
                var hasReplacement = owesCheckpointWrite && m_modified.TryGetValue(key, out var current) && current.Sequence > 0;
                m_modified[key] = new Modified(-1);
                Volatile.Write(ref _lookupTable[key % LookupTableSize], null);
                if (owesCheckpointWrite)
                {
                    if (hasReplacement)
                    {
                        // The checkpoint's original is spilled or rented by its writer (or is
                        // a deletion). Discard the replacement without freeing that snapshot.
                        stateManager.DeleteFromCache(key);
                    }
                    // Keep the original cache entry and spill until the checkpoint write lands.
                    return;
                }
                Delete_NoLock(key);
            }
        }

        private void Delete_NoLock(long key)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            Volatile.Write(ref _lookupTable[key % LookupTableSize], null);
            FreeSpill(key);
            stateManager.DeleteFromCache(key);
        }

        /// <summary>
        /// Free before removing the version entry, a surviving entry must always point at live spill data.
        /// </summary>
        private void FreeSpill(long key)
        {
            m_fileCache.Free(key);
            m_fileCacheVersion.Remove(key, out _);
        }

        public long GetNewPageId()
        {
            // Add to the new pages counter
            Interlocked.Increment(ref newPages);
            return stateManager.GetNewPageId();
        }

        public ValueTask<V?> GetValue(in long key)
        {
            var modLookup = key % LookupTableSize;

            // Lock-free fast path. The slot is one reference, the key is validated on the entry,
            // and TryRentValue is safe against eviction. A stale slot, or a page owing its
            // checkpoint write, falls through to the lock.
            var entry = Volatile.Read(ref _lookupTable[modLookup]);
            if (entry != null && entry.Key == key && !Volatile.Read(ref entry.OwesCheckpointWrite) && entry.TryRentValue())
            {
                m_lookupTableHits++;
                return ValueTask.FromResult<V?>((V)entry.Value);
            }

            lock (m_lock)
            {
                if (m_generation != null)
                {
                    if (m_modified.TryGetValue(key, out var mod))
                    {
                        if (mod.Sequence == -1)
                        {
                            // Deleted page in modified set returns null.
                            return ValueTask.FromResult<V?>(default);
                        }
                        return GetValue_Locked(key, modLookup);
                    }
                    if (m_commit.Pending.TryGetValue(key, out var pending))
                    {
                        if (pending.Sequence == -1)
                        {
                            // Deleted page in pending generation returns null.
                            return ValueTask.FromResult<V?>(default);
                        }
                    }
                    else if (m_commit.WritingKey != key)
                    {
                        if (m_generation.DeletedKeys != null && m_generation.DeletedKeys.Contains(key))
                        {
                            // Deleted page in active generation returns null.
                            return ValueTask.FromResult<V?>(default);
                        }
                        return GetValue_Locked(key, modLookup);
                    }
                }
                else
                {
                    return GetValue_Locked(key, modLookup);
                }
            }
            // Written first, outside m_lock, the caller may modify what it gets.
            return GetValue_CommitFirst(key);
        }

        private ValueTask<V?> GetValue_Locked(long key, long modLookup)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            if (stateManager.TryGetCacheValueFromCache(key, out var cacheVal))
            {
                Volatile.Write(ref _lookupTable[modLookup], cacheVal);
                return ValueTask.FromResult<V?>((V)cacheVal.Value);
            }
            var hasModification = m_modified.TryGetValue(key, out var modified);
            if (hasModification && modified.Sequence == -1)
            {
                // Deleted page in modified set returns null.
                return ValueTask.FromResult<V?>(default);
            }
            if (!hasModification && m_generation?.DeletedKeys != null && m_generation.DeletedKeys.Contains(key))
            {
                // Deleted page in active generation returns null.
                return ValueTask.FromResult<V?>(default);
            }
            Interlocked.Increment(ref cacheMisses);
            // Read from temporary file storage
            if (m_fileCacheVersion.ContainsKey(key))
            {
                return GetValue_FromCache(key);
            }
            // Read from persistent store
            return GetValue_Persistent(key);
        }

        private async ValueTask<V?> GetValue_CommitFirst(long key)
        {
            await CommitPendingPage(key);
            return await GetValue(key);
        }

        private async ValueTask<V?> GetValue_FromCache(long key)
        {
            Debug.Assert(options.ValueSerializer != null);
            var sw = ValueStopwatch.StartNew();
            var value = await m_fileCache.Read<V>(key, options.ValueSerializer);
            if (!value.TryRent())
            {
                throw new InvalidOperationException("Could not rent value when fetched from storage.");
            }
            stateManager.AddOrUpdate(key, value, this);

            if (m_temporaryReadMsHistogram != null)
            {
                m_temporaryReadMsHistogram.Record((float)sw.GetElapsedTime().TotalMilliseconds, tagList);
            }

            return value;
        }

        private async ValueTask<V?> GetValue_Persistent(long key)
        {
            Debug.Assert(options.ValueSerializer != null);
            var sw = ValueStopwatch.StartNew();
            V? value = default;
            try
            {
                value = await SessionRead(key, options.ValueSerializer);
            }
            catch (Exception e)
            {
                throw new FlowtidePersistentStorageException($"Error reading persistent data in client '{name}' with key '{key}'", e);
            }

            // Rented before it is published to the cache. The other way round leaves a window
            // where an evictor sees the page at the cache's single rent, returns it to zero and
            // disposes it, and this rent then fails on an already dead page.
            if (!value.TryRent())
            {
                throw new InvalidOperationException("Could not rent value when fetched from storage.");
            }
            stateManager.AddOrUpdate(key, value, this);

            if (m_persistenceReadMsHistogram != null)
            {
                m_persistenceReadMsHistogram.Record((float)sw.GetElapsedTime().TotalMilliseconds, tagList);
            }
            return value;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // The manager waited as long as it will, a walk still running writes through the
                    // serializer and the session, so the last one out disposes them.
                    RequestStopCommits();
                    var commitTask = Volatile.Read(ref m_commitTask);
                    if (commitTask != null && !commitTask.IsCompleted)
                    {
                        m_disposalTask = commitTask.ContinueWith(static (t, state) => ((SyncStateClient<V, TMetadata>)state!).DisposeAfterWalk(t), this, TaskScheduler.Default);
                    }
                    else
                    {
                        // Nobody awaits a walk the stop gave up on, its fault ends here.
                        _ = commitTask?.Exception;
                        DisposeResources();
                    }
                }

                disposedValue = true;
            }
        }

        internal override Task DisposalTask => m_disposalTask ?? Task.CompletedTask;

        private void DisposeAfterWalk(Task walk)
        {
            // Nobody awaits a walk the stop gave up on, its fault ends here.
            _ = walk.Exception;
            try
            {
                DisposeResources();
            }
            catch
            {
                // A teardown fallback, there is nobody left to tell.
            }
        }

        private void DisposeResources()
        {
            // Disposal failures must not skip other owned resources.
            try
            {
                m_fileCache.Dispose();
            }
            finally
            {
                try
                {
                    options.ValueSerializer?.Dispose();
                }
                finally
                {
                    session.Dispose();
                }
            }
        }

        public override void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public override async ValueTask Reset(bool clearMetadata, bool discardErrors = false)
        {
            // A structure clearing itself between checkpoints may still have a walk in flight.
            // A failed walk surfaces here, recovery consumed it in PauseCommitsAsync already.
            var commitTask = Volatile.Read(ref m_commitTask);
            var wasObserved = Volatile.Read(ref m_commitTaskObserved);
            try
            {
                if (commitTask != null)
                {
                    await commitTask;
                }
            }
            catch when (wasObserved || discardErrors)
            {
                // A previously observed walk failure is ignored during reset.
            }
            finally
            {
                Volatile.Write(ref m_commitTask, null);
                Volatile.Write(ref m_commitTaskObserved, false);
                // Reset clears recorded commit fault.
                m_commit.Failure = null;
                lock (m_lock)
                {
                    foreach (var key in m_modified.Keys)
                    {
                        stateManager.DeleteFromCache(key);
                    }
                    if (m_generation != null)
                    {
                        // Purge generation pages written before walk failed.
                        for (int i = 0; i < m_generation.Count; i++)
                        {
                            stateManager.DeleteFromCache(m_generation.Keys[i]);
                        }
                    }
                    else
                    {
                        // Purge uncommitted pending pages from cache table.
                        foreach (var key in m_commit.Pending.Keys)
                        {
                            stateManager.DeleteFromCache(key);
                        }
                    }
                    for (int i = 0; i < _lookupTable.Length; i++)
                    {
                        Volatile.Write(ref _lookupTable[i], null);
                    }
                    m_fileCache.FreeAll(m_modified.Keys);
                    m_fileCache.FreeAll(m_commit.Pending.Keys);
                    m_modified.Clear();
                    m_commit.Pending.Clear();
                    m_commit.WritingKey = -1;
                    m_commit.DeletedKeys?.Clear();
                    m_commit.ReplacedKeys?.Clear();
                    m_commit.HeldKeys.Clear();
                    m_commit.Count = 0;
                    m_generation = null;
                    m_fileCacheVersion.Clear();
                    // Reset clears uncommitted new page counter.
                    Interlocked.Exchange(ref newPages, 0);
                }
                if (clearMetadata)
                {
                    Metadata = default;
                    metadata.CommitedOnce = false;
                }
                else if (!metadata.CommitedOnce)
                {
                    Metadata = default;
                }
                else
                {
                    var bytes = await SessionRead(metadataId);
                    metadata = StateClientMetadataSerializer.Deserialize<TMetadata>(bytes, bytes.Length);
                }
                m_fileCache.ClearTemporaryAllocations();
                if (options.ValueSerializer != null)
                {
                    options.ValueSerializer.ClearTemporaryAllocations();
                }
            }
        }

        public async Task<bool> Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
        {
            Debug.Assert(options.ValueSerializer != null);
            // One page write at most, an inline commit holds the lock throughout and lets this time out.
            if (!await m_commitEvictLock.WaitAsync(EvictWaitBudget))
            {
                return false;
            }
            var wroteAny = false;
            var canEvictAll = true;
            try
            {
                foreach (var value in valuesToEvict)
                {
                    var (canEvict, wrote) = EvictPage(value.Item1);
                    wroteAny |= wrote;
                    if (!canEvict)
                    {
                        canEvictAll = false;
                    }
                }
            }
            finally
            {
                m_commitEvictLock.Release();
            }

            if (wroteAny)
            {
                m_fileCache.Flush();
            }

            if (isCleanup)
            {
                m_fileCache.ClearTemporaryAllocations();
                if (options.ValueSerializer != null)
                {
                    options.ValueSerializer.ClearTemporaryAllocations();
                }
            }
            return canEvictAll;
        }

        private (bool canEvict, bool wrote) EvictPage(S3FifoCacheEntry entry)
        {
            Debug.Assert(options.ValueSerializer != null);
            var modLookup = entry.Key % LookupTableSize;
            bool isModified;
            long val;
            lock (m_lock)
            {
                // A page owing its checkpoint write spills at that version whatever the new
                // generation says about it, the commit reads the spill back.
                if (m_commit.Pending.TryGetValue(entry.Key, out var pending))
                {
                    if ((m_generation?.ReplacedKeys?.Contains(entry.Key) ?? false) ||
                        (m_modified.TryGetValue(entry.Key, out var mod) && mod.Sequence > 0))
                    {
                        // Replaced page cannot overwrite pending generation spill.
                        return (false, false);
                    }
                    isModified = true;
                    val = pending.Sequence;
                }
                else if (m_modified.TryGetValue(entry.Key, out var modified))
                {
                    isModified = true;
                    val = modified.Sequence;
                }
                else
                {
                    isModified = false;
                    val = default;
                }
                if (ReferenceEquals(_lookupTable[modLookup], entry))
                {
                    Volatile.Write(ref _lookupTable[modLookup], null);
                }
            }
            if (!useReadCache)
            {
                // Skip writing data if we dont use read cache and its not modified or deleted
                if (isModified == false || val == -1)
                {
                    return (true, false);
                }
            }
            else
            {
                if (isModified)
                {
                    if (val == -1)
                    {
                        // Deleted
                        return (true, false);
                    }
                }
                else
                {
                    val = -2;
                }
            }

            if (m_fileCacheVersion.TryGetValue(entry.Key, out var storedVersion) && storedVersion == val)
            {
                return (true, false);
            }
            entry.Value.EnterWriteLock();
            var sw = ValueStopwatch.StartNew();
            var wrote = false;
            try
            {
                // Must lock the cache entry here since it can be deleted and disposed
                // So we check if it is already removed from the cache, then we skip serialization
                lock (entry)
                {
                    if (!entry.Removed)
                    {
                        // Record the version entry before the spill write.
                        // A surviving version entry then always points at live spill data,
                        // so a read never hits freed data and throws Segment not found.
                        m_fileCacheVersion[entry.Key] = val;
                        try
                        {
                            m_fileCache.Write(entry.Key, new SerializableObject(entry.Value, options.ValueSerializer));
                            wrote = true;
                        }
                        catch
                        {
                            // A failed spill write must not leave a version entry behind.
                            m_fileCacheVersion.TryRemove(new KeyValuePair<long, long>(entry.Key, val));
                            throw;
                        }
                    }
                }
            }
            finally
            {
                entry.Value.ExitWriteLock();
            }
            if (m_temporaryWriteMsHistogram != null)
            {
                m_temporaryWriteMsHistogram.Record((float)sw.GetElapsedTime().TotalMilliseconds, tagList);
            }
            return (true, wrote);
        }

        public async Task InitializeSerializerAsync()
        {
            Debug.Assert(options.ValueSerializer != null);
            await options.ValueSerializer.InitializeAsync(this, metadata);
        }

        public Task<ReadOnlyMemory<byte>> ReadPage(long pageId)
        {
            return SessionRead(pageId).AsTask();
        }

        Memory<byte> IStateSerializerCheckpointWriter.RequestPageMemory(int expectedSize)
        {
            // Can be changed later to request memory from persistent storage
            return new byte[expectedSize];
        }

        Task IStateSerializerCheckpointWriter.WritePageMemory(long pageId, Memory<byte> memory)
        {
            return SessionWrite(pageId, new SerializableObject(memory));
        }

        Task IStateSerializerCheckpointWriter.RemovePage(long pageId)
        {
            return SessionDelete(pageId);
        }

    }
}

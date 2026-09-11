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
using System.Numerics;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class SyncStateClient<V, TMetadata> : StateClient, IStateClient<V, TMetadata>, ICacheEvictHandler, IStateSerializerInitializeReader, IStateSerializerCheckpointWriter
        where V : ICacheObject
        where TMetadata : class, IStorageMetadata
    {
        /// <summary>
        /// A modified page: its write sequence, -1 for a delete, and the page object as last written.
        /// </summary>
        private readonly record struct Modified(long Sequence, ICacheObject? Page);

        /// <summary>
        /// One Commit's worth of pages, shared by the walk and the on-fetch writes under m_commitEvictLock.
        /// </summary>
        private sealed class CommitGeneration
        {
            public CommitGeneration(long[] keys, long newPages, bool previousCommitedOnce)
            {
                Keys = keys;
                NewPages = newPages;
                PreviousCommitedOnce = previousCommitedOnce;
            }

            public long[] Keys { get; }

            /// <summary>
            /// Net page count change, deletes in the walk take from it.
            /// </summary>
            public long NewPages;

            public bool PreviousCommitedOnce { get; }
        }

        /// <summary>
        /// Lock-free membership pre-check for the read paths, one bit per hashed key, eight bits per key.
        /// A miss is final, a hit falls through to the locked check.
        /// </summary>
        private sealed class GenerationFilter
        {
            private readonly ulong[] _bits;
            private readonly int _shift;

            public GenerationFilter(Dictionary<long, Modified>.KeyCollection keys, int count)
            {
                var bitCount = Math.Max(64UL, BitOperations.RoundUpToPowerOf2((ulong)count * 8));
                _bits = new ulong[bitCount / 64];
                _shift = 64 - BitOperations.Log2(bitCount);
                foreach (var key in keys)
                {
                    var index = Index(key);
                    _bits[index >> 6] |= 1UL << (int)(index & 63);
                }
            }

            private ulong Index(long key)
            {
                return ((ulong)key * 0x9E3779B97F4A7C15UL) >> _shift;
            }

            public bool MayContain(long key)
            {
                var index = Index(key);
                return (_bits[index >> 6] & (1UL << (int)(index & 63))) != 0;
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
        private readonly TimeSpan m_stopCommitsTimeout;
        private readonly int m_bplusTreePageSize;
        private readonly int m_bplusTreePageSizeBytes;
        private readonly IMemoryAllocator memoryAllocator;

        /// <summary>
        /// Pages written since the last Commit, guarded by m_lock. Swapped with m_pending at Commit.
        /// </summary>
        private Dictionary<long, Modified> m_modified;

        /// <summary>
        /// Pages of the last Commit that still owe their checkpoint write, guarded by m_lock.
        /// Whoever removes a key under the lock owns its write, so a page is never written twice.
        /// </summary>
        private Dictionary<long, Modified> m_pending;

        /// <summary>
        /// The page whose checkpoint write is in progress, -1 when none, guarded by m_lock.
        /// </summary>
        private long m_pendingWriteKey = -1;

        /// <summary>
        /// Set while m_pending may hold pages, read under m_lock by the write paths.
        /// </summary>
        private int m_commitInFlight;

        /// <summary>
        /// The in-flight generation's keys, null when none. The read fast paths probe it lock-free.
        /// </summary>
        private GenerationFilter? m_generationFilter;

        private CommitGeneration? m_generation;
        private Task? m_commitTask;
        private bool m_disposeRequested;

        /// <summary>
        /// Set by an eviction pass waiting for the lock, the walk yields after its next page so the pass gets in.
        /// </summary>
        private int m_evictWaiting;
        private readonly object m_lock = new object();

        /// <summary>
        /// Serializes this client's checkpoint writes against its Evict, all go through the same
        /// non-thread-safe value serializer and file-cache version state. Taken per page, so the
        /// operator's fetches and the eviction pass slot in between the background writes.
        /// Never disposed, a commit still in flight during teardown must be able to release it.
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
            TimeSpan stopCommitsTimeout,
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
            this.m_stopCommitsTimeout = stopCommitsTimeout;
            this.m_bplusTreePageSize = bplusTreePageSize;
            this.m_bplusTreePageSizeBytes = bplusTreePageSizeBytes;
            this.memoryAllocator = memoryAllocator;
            m_fileCache = fileCacheFactory.Create(name, memoryAllocator);
            m_sessionLock = session.IsThreadSafe ? null : new SemaphoreSlim(1, 1);
            m_modified = new Dictionary<long, Modified>();
            m_pending = new Dictionary<long, Modified>();
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
                if (Volatile.Read(ref m_commitInFlight) != 0 && OwesCheckpointWrite_NoLock(key))
                {
                    // Held pages are written at Commit, so this is a reference kept without a rent.
                    throw new InvalidOperationException($"Page '{key}' on state client '{name}' was written before its checkpoint write, a page kept across Commit must be rented.");
                }
                m_modified[key] = new Modified(++m_writeSequence, value);

                var modLookup = key % LookupTableSize;
                var entry = _lookupTable[modLookup];
                if (entry != null && entry.Key == key)
                {
                    lock (entry)
                    {
                        entry.Version = entry.Version + 1;
                        // If it is not removed, we can return directly, otherwise it needs to be readded
                        if (!entry.Removed)
                        {
                            return false;
                        }
                    }
                }

                return stateManager.AddOrUpdate(key, value, this);
            }
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
            if (MayOweCheckpointWrite(key) && OwesCheckpointWrite(key))
            {
                value = default;
                return false;
            }
            var modLookup = key % LookupTableSize;
            var entry = Volatile.Read(ref _lookupTable[modLookup]);
            if (entry != null && entry.Key == key && entry.TryRentValue())
            {
                m_lookupTableHits++;
                value = (V)entry.Value;
                return true;
            }
            // Lock-free probe, a miss never waits behind a commit.
            if (stateManager.TryRentCachedValue(key, out var cached))
            {
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

        /// <summary>
        /// Lock-free pre-check, the generation's keys never grow so a miss is final.
        /// </summary>
        private bool MayOweCheckpointWrite(long key)
        {
            var filter = Volatile.Read(ref m_generationFilter);
            return filter != null && filter.MayContain(key);
        }

        private bool OwesCheckpointWrite(long key)
        {
            lock (m_lock)
            {
                return OwesCheckpointWrite_NoLock(key);
            }
        }

        private bool OwesCheckpointWrite_NoLock(long key)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            return m_pending.ContainsKey(key) || m_pendingWriteKey == key;
        }

        private Task EnterSessionAsync()
        {
            return m_sessionLock?.WaitAsync() ?? Task.CompletedTask;
        }

        private void ExitSession()
        {
            m_sessionLock?.Release();
        }

        private static async Task JoinQuietlyAsync(Task? commitTask)
        {
            if (commitTask == null)
            {
                return;
            }
            try
            {
                await commitTask.ConfigureAwait(false);
            }
            catch
            {
            }
        }

        internal override async Task PauseCommitsAsync()
        {
            // The reset that follows is the response to a failed commit, its fault is not the caller's.
            // Commit publishes its walk under the lock, so a walk seen here after taking it is complete.
            while (true)
            {
                var commitTask = Volatile.Read(ref m_commitTask);
                await JoinQuietlyAsync(commitTask);
                await m_commitEvictLock.WaitAsync();
                if (ReferenceEquals(Volatile.Read(ref m_commitTask), commitTask))
                {
                    // Consumed, the reset under this pause must not see the fault again.
                    Volatile.Write(ref m_commitTask, null);
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

        internal override Task WaitForCommitAsync()
        {
            return Volatile.Read(ref m_commitTask) ?? Task.CompletedTask;
        }

        internal override void RequestStopCommits()
        {
            // The walk gives up at its next page, before the cache table goes away.
            Volatile.Write(ref m_disposeRequested, true);
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

        /// <summary>
        /// The last generation's walk, for test assertions on how it ended.
        /// </summary>
        internal Task? CommitTaskForTests => Volatile.Read(ref m_commitTask);

        public async ValueTask Commit()
        {
            Debug.Assert(options.ValueSerializer != null);

            // One generation in flight per client, a previous commit still running is joined first.
            await WaitForCommitAsync();

            // The serializer checkpoint and the metadata are taken on the caller's thread, the
            // operator changes the tree metadata again as soon as this returns.
            var previousCommitedOnce = metadata.CommitedOnce;
            Task commitTask;
            await m_commitEvictLock.WaitAsync();
            try
            {
                await options.ValueSerializer.CheckpointAsync(this, metadata);
                await WriteMetadata();

                CommitGeneration generation;
                lock (m_lock)
                {
                    if (m_pending.Count != 0)
                    {
                        throw new InvalidOperationException($"State client '{name}' still has pages pending from a previous commit.");
                    }
                    (m_pending, m_modified) = (m_modified, m_pending);
                    var keys = new long[m_pending.Count];
                    m_pending.Keys.CopyTo(keys, 0);
                    generation = new CommitGeneration(keys, Interlocked.Exchange(ref newPages, 0), previousCommitedOnce);
                    m_generation = generation;
                    Volatile.Write(ref m_generationFilter, new GenerationFilter(m_pending.Keys, m_pending.Count));
                    Volatile.Write(ref m_commitInFlight, 1);
                }

                try
                {
                    // A page someone still holds may be changed in place as soon as this returns, so
                    // its checkpoint copy is taken here. After this no pending page has a holder.
                    await WriteHeldPages(generation);

                    if (!m_backgroundCommit)
                    {
                        await CommitGenerationAsync(generation, lockHeld: true);
                        return;
                    }
                }
                catch
                {
                    // The metadata went into the session with the flag set, a failure before
                    // the session commit must take the flag back or recovery reads a page that never landed.
                    metadata.CommitedOnce = previousCommitedOnce;
                    throw;
                }

                // Published while the lock is held, so a pause that holds the lock has seen every walk.
                // Session writes mostly complete synchronously, run inline the walk would stay on the caller's thread.
                commitTask = Task.Run(() => CommitGenerationAsync(generation, lockHeld: false));
                Volatile.Write(ref m_commitTask, commitTask);
            }
            finally
            {
                m_commitEvictLock.Release();
            }
        }

        /// <summary>
        /// Writes every generation page rented by someone other than the cache, caller holds m_commitEvictLock.
        /// </summary>
        private async Task WriteHeldPages(CommitGeneration generation)
        {
            // The page object as last written is enough for the rent count unless it left the
            // cache since, then the table has the current one.
            List<long>? held = null;
            foreach (var kv in m_pending)
            {
                var page = kv.Value.Page;
                if (page == null)
                {
                    continue;
                }
                if (page.RemovedFromCache)
                {
                    if (!stateManager.TryPeekCacheEntry(kv.Key, out var entry))
                    {
                        continue;
                    }
                    page = entry.Value;
                }
                if (page.RentCount > 1)
                {
                    held ??= new List<long>();
                    held.Add(kv.Key);
                }
            }
            if (held == null)
            {
                return;
            }
            foreach (var key in held)
            {
                if (!TryTakePending(key, out var version))
                {
                    continue;
                }
                await WritePendingPage(key, version, generation);
            }
        }

        private async Task CommitGenerationAsync(CommitGeneration generation, bool lockHeld)
        {
            Debug.Assert(options.ValueSerializer != null);
            try
            {
                var hook = stateManager.PageWriteHookForTests;
                foreach (var key in generation.Keys)
                {
                    ThrowIfDisposeRequested();
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
                        // Already written by a fetch or at Commit.
                        if (!TryTakePending(key, out var version))
                        {
                            continue;
                        }
                        await WritePendingPage(key, version, generation);
                    }
                    finally
                    {
                        if (!lockHeld)
                        {
                            m_commitEvictLock.Release();
                        }
                    }
                    if (!lockHeld && Volatile.Read(ref m_evictWaiting) != 0)
                    {
                        // An immediate retake beats the waiting pass, give it the lock.
                        await Task.Yield();
                    }
                }
                ThrowIfDisposeRequested();

                // An on-fetch write still in flight holds the lock, it lands before the commit.
                if (!lockHeld)
                {
                    await m_commitEvictLock.WaitAsync();
                }
                try
                {
                    lock (m_lock)
                    {
                        Debug.Assert(m_pending.Count == 0);
                    }

                    Debug.Assert(stateManager.m_metadata != null);
                    // Add modified page count to the page commits counter
                    Interlocked.Add(ref stateManager.m_metadata.PageCommits, (ulong)generation.Keys.Length);
                    // Modify active pages
                    Interlocked.Add(ref stateManager.m_metadata.PageCount, generation.NewPages);

                    await EnterSessionAsync();
                    try
                    {
                        await session.Commit();
                    }
                    finally
                    {
                        ExitSession();
                    }

                    lock (m_lock)
                    {
                        Volatile.Write(ref m_generationFilter, null);
                        m_generation = null;
                        Volatile.Write(ref m_commitInFlight, 0);
                    }
                }
                finally
                {
                    if (!lockHeld)
                    {
                        m_commitEvictLock.Release();
                    }
                }

                m_fileCache.ClearTemporaryAllocations();
                options.ValueSerializer.ClearTemporaryAllocations();
            }
            catch
            {
                metadata.CommitedOnce = generation.PreviousCommitedOnce;
                throw;
            }
        }

        private void ThrowIfDisposeRequested()
        {
            // Faulted, not returned, a checkpoint waiting on this walk must not take it as landed.
            if (Volatile.Read(ref m_disposeRequested))
            {
                throw new ObjectDisposedException($"State client '{name}' was disposed with a commit in flight.");
            }
        }

        /// <summary>
        /// Claims the page's checkpoint write, false when a fetch or the walk already took it.
        /// </summary>
        private bool TryTakePending(long key, out long version)
        {
            lock (m_lock)
            {
                if (!m_pending.Remove(key, out var pending))
                {
                    version = default;
                    return false;
                }
                version = pending.Sequence;
                m_pendingWriteKey = key;
                return true;
            }
        }

        /// <summary>
        /// Writes one claimed page to the session, caller holds m_commitEvictLock.
        /// </summary>
        private async Task WritePendingPage(long key, long version, CommitGeneration generation)
        {
            Debug.Assert(options.ValueSerializer != null);
            try
            {
                if (version == -1)
                {
                    // deleted
                    await EnterSessionAsync();
                    try
                    {
                        await session.Delete(key);
                    }
                    finally
                    {
                        ExitSession();
                    }

                    // Remove a page from the new pages counter
                    generation.NewPages--;
                    FreeSpill(key);
                    return;
                }
                if (stateManager.TryGetValueFromCache<V>(key, out var val))
                {
                    // Return the lookup's rent even when the write throws, a leaked rent keeps the page unevictable forever.
                    try
                    {
                        await EnterSessionAsync();
                        try
                        {
                            // Write to persistence
                            await session.Write(key, new SerializableObject(val, options.ValueSerializer));
                        }
                        finally
                        {
                            ExitSession();
                        }
                    }
                    finally
                    {
                        val.Return();
                    }
                    // The spill is older than what was just written.
                    FreeSpill(key);
                }
                else
                {
                    var bytes = await m_fileCache.Read(key);

                    await EnterSessionAsync();
                    try
                    {
                        // Write to persistence
                        await session.Write(key, new SerializableObject(bytes));
                    }
                    finally
                    {
                        ExitSession();
                    }

                    if (!useReadCache)
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
            finally
            {
                lock (m_lock)
                {
                    m_pendingWriteKey = -1;
                    if (m_modified.TryGetValue(key, out var current) && current.Sequence == -1)
                    {
                        // The delete waited for this write, see Delete.
                        Delete_NoLock(key);
                    }
                }
            }
        }

        /// <summary>
        /// Writes the page's checkpoint copy before a fetch hands it out for modification.
        /// </summary>
        private async ValueTask CommitPendingPage(long key)
        {
            await m_commitEvictLock.WaitAsync();
            try
            {
                if (!TryTakePending(key, out var version))
                {
                    return;
                }
                var generation = m_generation;
                Debug.Assert(generation != null);
                try
                {
                    await WritePendingPage(key, version, generation);
                }
                catch
                {
                    // Still owed. The walk has not passed it, it claims under this lock, so it
                    // retries the write and faults the generation if that fails too.
                    lock (m_lock)
                    {
                        m_pending[key] = new Modified(version, null);
                    }
                    throw;
                }
            }
            finally
            {
                m_commitEvictLock.Release();
            }
        }

        private async Task WriteMetadata()
        {
            if (!metadata.CommitedOnce || (metadata.Metadata != null && metadata.Metadata.Updated))
            {
                var previousCommitedOnce = metadata.CommitedOnce;
                try
                {
                    metadata.CommitedOnce = true;
                    var bytes = StateClientMetadataSerializer.Serialize(metadata);
                    await EnterSessionAsync();
                    try
                    {
                        await session.Write(metadataId, new SerializableObject(bytes));
                    }
                    finally
                    {
                        ExitSession();
                    }
                    if (metadata.Metadata != null)
                    {
                        metadata.Metadata.Updated = false;
                    }
                }
                catch (Exception)
                {
                    metadata.CommitedOnce = previousCommitedOnce;
                    throw;
                }
            }
        }

        public void Delete(in long key)
        {
            lock (m_lock)
            {
                m_modified[key] = new Modified(-1, null);
                if (Volatile.Read(ref m_commitInFlight) != 0 && OwesCheckpointWrite_NoLock(key))
                {
                    // Nobody holds a pending page, so it can stay until the walk has written it.
                    return;
                }
                Delete_NoLock(key);
            }
        }

        private void Delete_NoLock(long key)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
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
            // A page owing its checkpoint write is written first, the caller may modify what it gets.
            if (MayOweCheckpointWrite(key) && OwesCheckpointWrite(key))
            {
                return GetValue_CommitFirst(key);
            }

            var modLookup = key % LookupTableSize;

            // Lock-free fast path. The slot is one reference, the key is validated on the entry,
            // and TryRentValue is safe against eviction. A stale slot falls through to the lock.
            var entry = Volatile.Read(ref _lookupTable[modLookup]);
            if (entry != null && entry.Key == key && entry.TryRentValue())
            {
                m_lookupTableHits++;
                return ValueTask.FromResult<V?>((V)entry.Value);
            }

            lock (m_lock)
            {
                if (stateManager.TryGetCacheValueFromCache(key, out var cacheVal))
                {
                    Volatile.Write(ref _lookupTable[modLookup], cacheVal);
                    return ValueTask.FromResult<V?>((V)cacheVal.Value);
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
                await EnterSessionAsync();
                try
                {
                    value = await session.Read<V>(key, options.ValueSerializer);
                }
                finally
                {
                    ExitSession();
                }
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
                    StopCommits(m_stopCommitsTimeout);
                    m_fileCache.Dispose();
                    if (options.ValueSerializer != null)
                    {
                        options.ValueSerializer.Dispose();
                    }
                    // The client owns the session it was created with. A supplied storage
                    // outlives a stop, so an undisposed session would be stranded in it.
                    session.Dispose();
                }

                disposedValue = true;
            }
        }

        public override void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public override async ValueTask Reset(bool clearMetadata)
        {
            // A structure clearing itself between checkpoints may still have a walk in flight.
            // A failed walk surfaces here, recovery consumed it in PauseCommitsAsync already.
            var commitTask = Volatile.Read(ref m_commitTask);
            if (commitTask != null)
            {
                await commitTask;
                Volatile.Write(ref m_commitTask, null);
            }
            lock (m_lock)
            {
                foreach (var key in m_modified.Keys)
                {
                    stateManager.DeleteFromCache(key);
                }
                // A failed commit leaves its generation behind, those pages are reloaded from the recovered store.
                foreach (var key in m_pending.Keys)
                {
                    stateManager.DeleteFromCache(key);
                }
                for (int i = 0; i < _lookupTable.Length; i++)
                {
                    Volatile.Write(ref _lookupTable[i], null);
                }
                m_fileCache.FreeAll(m_modified.Keys);
                m_modified.Clear();
                m_pending.Clear();
                m_pendingWriteKey = -1;
                m_generation = null;
                Volatile.Write(ref m_generationFilter, null);
                Volatile.Write(ref m_commitInFlight, 0);
                m_fileCacheVersion.Clear();
            }
            if (clearMetadata || !metadata.CommitedOnce)
            {
                Metadata = default;
            }
            else
            {
                await EnterSessionAsync();
                try
                {
                    var bytes = await session.Read(metadataId);
                    metadata = StateClientMetadataSerializer.Deserialize<TMetadata>(bytes, bytes.Length);
                }
                finally
                {
                    ExitSession();
                }
            }
            m_fileCache.ClearTemporaryAllocations();
            if (options.ValueSerializer != null)
            {
                options.ValueSerializer.ClearTemporaryAllocations();
            }
        }

        public bool Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
        {
            Debug.Assert(options.ValueSerializer != null);
            // Waits one page write at most. The walk yields to the raised flag, an immediate
            // retake would otherwise win every time, and an inline commit lets this time out
            // so the pass never stalls behind a whole commit's I/O.
            bool acquired;
            Volatile.Write(ref m_evictWaiting, 1);
            try
            {
                acquired = m_commitEvictLock.Wait(EvictWaitBudget);
            }
            finally
            {
                Volatile.Write(ref m_evictWaiting, 0);
            }
            if (!acquired)
            {
                return false;
            }
            try
            {
                EvictInternal(valuesToEvict, isCleanup);
            }
            finally
            {
                m_commitEvictLock.Release();
            }
            return true;
        }

        private void EvictInternal(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
        {
            Debug.Assert(options.ValueSerializer != null);
            // Flush is an fsync, a batch of clean or already spilled pages must not pay for one.
            var wroteAny = false;
            foreach (var value in valuesToEvict)
            {
                var entry = value.Item1;
                var modLookup = entry.Key % LookupTableSize;
                bool isModified;
                long val;
                lock (m_lock)
                {
                    // A page owing its checkpoint write spills at that version whatever the new
                    // generation says about it, the commit reads the spill back.
                    if (m_pending.TryGetValue(entry.Key, out var pending))
                    {
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
                        continue;
                    }
                }
                else
                {
                    if (isModified)
                    {
                        if (val == -1)
                        {
                            // Deleted
                            continue;
                        }
                    }
                    else
                    {
                        val = -2;
                    }
                }

                if (m_fileCacheVersion.TryGetValue(entry.Key, out var storedVersion) && storedVersion == val)
                {
                    continue;
                }
                entry.Value.EnterWriteLock();
                var sw = ValueStopwatch.StartNew();
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
                                wroteAny = true;
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
        }

        public async Task InitializeSerializerAsync()
        {
            Debug.Assert(options.ValueSerializer != null);
            await options.ValueSerializer.InitializeAsync(this, metadata);
        }

        public async Task<ReadOnlyMemory<byte>> ReadPage(long pageId)
        {
            await EnterSessionAsync();
            try
            {
                return await session.Read(pageId);
            }
            finally
            {
                ExitSession();
            }
        }

        Memory<byte> IStateSerializerCheckpointWriter.RequestPageMemory(int expectedSize)
        {
            // Can be changed later to request memory from persistent storage
            return new byte[expectedSize];
        }

        async Task IStateSerializerCheckpointWriter.WritePageMemory(long pageId, Memory<byte> memory)
        {
            await EnterSessionAsync();
            try
            {
                await session.Write(pageId, new SerializableObject(memory));
            }
            finally
            {
                ExitSession();
            }
        }

        async Task IStateSerializerCheckpointWriter.RemovePage(long pageId)
        {
            await EnterSessionAsync();
            try
            {
                await session.Delete(pageId);
            }
            finally
            {
                ExitSession();
            }
        }

    }
}

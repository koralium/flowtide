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

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class SyncStateClient<V, TMetadata> : StateClient, IStateClient<V, TMetadata>, ICacheEvictHandler, IStateSerializerInitializeReader, IStateSerializerCheckpointWriter
        where V : ICacheObject
        where TMetadata : class, IStorageMetadata
    {
        private bool disposedValue;
        private readonly StateManagerSync stateManager;
        private readonly string name;
        private readonly long metadataId;
        private StateClientMetadata<TMetadata> metadata;
        private readonly IPersistentStorageSession session;
        private readonly StateClientOptions<V> options;
        private readonly bool useReadCache;
        private readonly int m_bplusTreePageSize;
        private readonly int m_bplusTreePageSizeBytes;
        private readonly IMemoryAllocator memoryAllocator;
        private readonly Dictionary<long, long> m_modified;
        private readonly object m_lock = new object();
        private readonly FlowtideDotNet.Storage.FileCache.IFileCache m_fileCache;
        private readonly ConcurrentDictionary<long, long> m_fileCacheVersion;

        /// <summary>
        /// Monotonic write-generation counter, guarded by m_lock and NEVER reset.
        /// m_modified records the generation of each page's latest write and the eviction
        /// dedup compares it against the generation stored in m_fileCacheVersion. An
        /// eviction that straddles a commit can re-insert its version entry after the
        /// commit cleared it; if generations restarted per commit interval, that stale
        /// entry could equal a later write's generation, making the dedup skip serializing
        /// modified content that is then dropped from memory (lost write). Monotonic
        /// generations can never collide across intervals.
        /// </summary>
        private long m_writeSequence;
        private readonly Histogram<float>? m_persistenceReadMsHistogram;
        private readonly Histogram<float>? m_temporaryReadMsHistogram;
        private readonly Histogram<float>? m_temporaryWriteMsHistogram;
        private readonly TagList tagList;

        /// <summary>
        /// Direct-mapped (key % length) cache of entry references, in front of the shared
        /// cache table. Slots are single references so they can be read atomically without
        /// any lock; the key is validated on the entry itself. All writes happen under
        /// m_lock, reads on the GetValue fast path are lock-free volatile reads.
        /// </summary>
        private S3FifoCacheEntry?[] _lookupTable;

        /// <summary>
        /// Value of how many pages have changed since last commit.
        /// </summary>
        private long newPages;
        private long cacheMisses;

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
            this.m_bplusTreePageSize = bplusTreePageSize;
            this.m_bplusTreePageSizeBytes = bplusTreePageSizeBytes;
            this.memoryAllocator = memoryAllocator;
            m_fileCache = fileCacheFactory.Create(name, memoryAllocator);
            m_modified = new Dictionary<long, long>();
            m_fileCacheVersion = new ConcurrentDictionary<long, long>();
            if (!string.IsNullOrEmpty(name))
            {
                m_persistenceReadMsHistogram = meter.CreateHistogram<float>("flowtide_persistence_read_ms");
                m_temporaryReadMsHistogram = meter.CreateHistogram<float>("flowtide_temporary_read_ms");
                m_temporaryWriteMsHistogram = meter.CreateHistogram<float>("flowtide_temporary_write_ms");
            }
            tagList = options.TagList;
            tagList.Add("state_client", name);

            _lookupTable = new S3FifoCacheEntry?[1009];
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
                m_modified[key] = ++m_writeSequence;

                var modLookup = key % _lookupTable.Length;
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

        public async ValueTask Commit()
        {
            Debug.Assert(options.ValueSerializer != null);

            foreach (var kv in m_modified)
            {
                if (kv.Value == -1)
                {
                    // deleted
                    await session.Delete(kv.Key);

                    // Remove a page from the new pages counter
                    Interlocked.Decrement(ref newPages);

                    m_fileCache.Free(kv.Key);
                    continue;
                }
                if (stateManager.TryGetValueFromCache<V>(kv.Key, out var val))
                {
                    // Write to persistence
                    await session.Write(kv.Key, new SerializableObject(val, options.ValueSerializer));

                    if (!useReadCache)
                    {
                        m_fileCache.Free(kv.Key);
                    }
                    else
                    {
                        // Remove it from file cache version and file cache
                        // This is required since the data can have been modified since it was written to the cache.
                        m_fileCacheVersion.Remove(kv.Key, out _);
                        m_fileCache.Free(kv.Key);
                    }
                    val.Return();
                    continue;
                }
                {
                    var bytes = await m_fileCache.Read(kv.Key);

                    // Write to persistence
                    await session.Write(kv.Key, new SerializableObject(bytes));

                    if (!useReadCache)
                    {
                        // Free the data from temporary storage
                        m_fileCache.Free(kv.Key);
                    }
                    else
                    {
                        // Set version to -2 which marks that it is a read only version
                        m_fileCacheVersion[kv.Key] = -2;
                    }
                }
            }

            // Checkpoint the serializer
            await options.ValueSerializer.CheckpointAsync(this, metadata);

            var modifiedPagesCount = m_modified.Count;
            Debug.Assert(stateManager.m_metadata != null);
            // Add modified page count to the page commits counter
            Interlocked.Add(ref stateManager.m_metadata.PageCommits, (ulong)modifiedPagesCount);
            // Modify active pages
            Interlocked.Add(ref stateManager.m_metadata.PageCount, newPages);
            newPages = 0;

            if (!useReadCache)
            {
                m_fileCache.FreeAll(m_modified.Keys);
                m_fileCacheVersion.Clear();
            }
            m_modified.Clear();

            await WriteMetadata();
            await session.Commit();
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
                    await session.Write(metadataId, new SerializableObject(bytes));
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
                m_modified[key] = -1;
                m_fileCacheVersion.Remove(key, out _);
                m_fileCache.Free(key);
                stateManager.DeleteFromCache(key);
            }
        }

        public long GetNewPageId()
        {
            // Add to the new pages counter
            Interlocked.Increment(ref newPages);
            return stateManager.GetNewPageId();
        }

        public ValueTask<V?> GetValue(in long key)
        {
            var modLookup = key % _lookupTable.Length;

            // Lock-free fast path: a slot is a single reference (atomic to read), the key is
            // validated on the entry, and TryRentValue makes the removed-check + rent handoff
            // safe against concurrent eviction. A stale or mid-eviction slot simply falls
            // through to the locked path, which repopulates it.
            var entry = Volatile.Read(ref _lookupTable[modLookup]);
            if (entry != null && entry.Key == key && entry.TryRentValue())
            {
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
                value = await session.Read<V>(key, options.ValueSerializer);
            }
            catch (Exception e)
            {
                throw new FlowtidePersistentStorageException($"Error reading persistent data in client '{name}' with key '{key}'", e);
            }

            // Rent for the caller BEFORE publishing to the cache, like GetValue_FromCache:
            // after AddOrUpdate the cache owns the object's only reference and a concurrent
            // eviction could dispose it before the caller's rent lands.
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
                    m_fileCache.Dispose();
                    if (options.ValueSerializer != null)
                    {
                        options.ValueSerializer.Dispose();
                    }
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
            lock (m_lock)
            {
                foreach (var kv in m_modified)
                {
                    stateManager.DeleteFromCache(kv.Key);
                    m_fileCache.Free(kv.Key);
                }
                for (int i = 0; i < _lookupTable.Length; i++)
                {
                    Volatile.Write(ref _lookupTable[i], null);
                }
                m_fileCache.FreeAll(m_modified.Keys);
                m_modified.Clear();
                m_fileCacheVersion.Clear();
            }
            if (clearMetadata || !metadata.CommitedOnce)
            {
                Metadata = default;
            }
            else
            {
                var bytes = await session.Read(metadataId);
                metadata = StateClientMetadataSerializer.Deserialize<TMetadata>(bytes, bytes.Length);
            }
        }

        public void Evict(List<(S3FifoCacheEntry, long)> valuesToEvict, bool isCleanup)
        {
            Debug.Assert(options.ValueSerializer != null);
            foreach (var value in valuesToEvict)
            {
                var entry = value.Item1;
                var modLookup = entry.Key % _lookupTable.Length;
                bool isModified;
                long val;
                lock (m_lock)
                {
                    isModified = m_modified.TryGetValue(entry.Key, out val);
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
                            m_fileCache.Write(entry.Key, new SerializableObject(entry.Value, options.ValueSerializer));
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

                m_fileCacheVersion[entry.Key] = val;
            }
            m_fileCache.Flush();

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
            return await session.Read(pageId);
        }

        Memory<byte> IStateSerializerCheckpointWriter.RequestPageMemory(int expectedSize)
        {
            // Can be changed later to request memory from persistent storage
            return new byte[expectedSize];
        }

        Task IStateSerializerCheckpointWriter.WritePageMemory(long pageId, Memory<byte> memory)
        {
            return session.Write(pageId, new SerializableObject(memory));
        }

        Task IStateSerializerCheckpointWriter.RemovePage(long pageId)
        {
            return session.Delete(pageId);
        }

    }
}

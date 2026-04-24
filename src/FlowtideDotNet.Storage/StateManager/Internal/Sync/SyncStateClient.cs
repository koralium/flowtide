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
using static FlowtideDotNet.Storage.StateManager.Internal.Sync.LruTableSync;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class SyncStateClient<V, TMetadata> : StateClient, IStateClient<V, TMetadata>, ILruEvictHandler, IStateSerializerInitializeReader, IStateSerializerCheckpointWriter
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
        private Dictionary<long, int> m_modified;
        private readonly object m_lock = new object();
        private readonly FlowtideDotNet.Storage.FileCache.IFileCache m_fileCache;
        private readonly ConcurrentDictionary<long, int> m_fileCacheVersion;
        private readonly Histogram<float>? m_persistenceReadMsHistogram;
        private readonly Histogram<float>? m_temporaryReadMsHistogram;
        private readonly Histogram<float>? m_temporaryWriteMsHistogram;
        private readonly TagList tagList;

        private CacheValue[] _lookupTable;

        /// <summary>
        /// Value of how many pages have changed since last commit.
        /// </summary>
        private long newPages;
        private long cacheMisses;

        private Dictionary<long, int> m_committing;
        private readonly ConcurrentDictionary<long, byte[]> m_committing_preserialized;
        private Task? m_commitTask;
        private long m_committingNewPages;
        private StateClientMetadata<TMetadata>? m_committingMetadata;

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
            m_modified = new Dictionary<long, int>();
            m_committing = new Dictionary<long, int>();
            m_committing_preserialized = new ConcurrentDictionary<long, byte[]>();
            m_fileCacheVersion = new ConcurrentDictionary<long, int>();
            if (!string.IsNullOrEmpty(name))
            {
                m_persistenceReadMsHistogram = meter.CreateHistogram<float>("flowtide_persistence_read_ms");
                m_temporaryReadMsHistogram = meter.CreateHistogram<float>("flowtide_temporary_read_ms");
                m_temporaryWriteMsHistogram = meter.CreateHistogram<float>("flowtide_temporary_write_ms");
            }
            tagList = options.TagList;
            tagList.Add("state_client", name);

            _lookupTable = new CacheValue[1009];
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
                if (m_modified.TryGetValue(key, out var old))
                {
                    m_modified[key] = old + 1;
                }
                else
                {
                    m_modified[key] = 0;
                }
                m_fileCacheVersion.Remove(key, out _);

                var modLookup = key % _lookupTable.Length;
                if (_lookupTable[modLookup].Key == key)
                {
                    var node = _lookupTable[modLookup].Value!;
                    lock (node)
                    {
                        node.ValueRef.version = node.ValueRef.version + 1;
                        // If it is not removed, we can return directly, otherwise it needs to be readded
                        if (!node.ValueRef.removed)
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

            if (m_commitTask != null)
            {
                await m_commitTask;
            }

            lock (m_lock)
            {
                var temp = m_modified;
                m_modified = m_committing;
                m_committing = temp;
                m_committing_preserialized.Clear();

                m_committingMetadata = new StateClientMetadata<TMetadata>()
                {
                    CommitedOnce = metadata.CommitedOnce,
                    Metadata = metadata.Metadata
                };

                m_committingNewPages = newPages;
                newPages = 0;
            }

            m_commitTask = Task.Run(CommitBackground);
        }

        private async Task CommitBackground()
        {
            foreach (var kv in m_committing)
            {
                if (kv.Value == -1)
                {
                    // deleted
                    await session.Delete(kv.Key);

                    // Remove a page from the new pages counter
                    Interlocked.Decrement(ref m_committingNewPages);

                    m_fileCache.Free(kv.Key);
                    continue;
                }

                if (m_committing_preserialized.TryGetValue(kv.Key, out var preserializedBytes))
                {
                    await session.Write(kv.Key, new SerializableObject(preserializedBytes));
                    continue;
                }


                bool loadedFromCache = false;
                lock (m_lock)
                {
                    if (m_committing_preserialized.TryGetValue(kv.Key, out preserializedBytes))
                    {
                        // Was preserialized just now
                    }
                    else if (stateManager.TryGetValueFromCache<V>(kv.Key, out var cacheVal))
                    {
                        // Found in cache, preserialize it inside the lock
                        var bufferWriter = new System.Buffers.ArrayBufferWriter<byte>();
                        options.ValueSerializer!.Serialize(bufferWriter, cacheVal);
                        preserializedBytes = bufferWriter.WrittenSpan.ToArray();
                        m_committing_preserialized.TryAdd(kv.Key, preserializedBytes);
                        
                        loadedFromCache = true;
                    }
                }

                if (preserializedBytes != null)
                {
                    await session.Write(kv.Key, new SerializableObject(preserializedBytes));

                    if (loadedFromCache)
                    {
                        if (useReadCache)
                        {
                            lock (m_lock)
                            {
                                if (!m_modified.ContainsKey(kv.Key))
                                {
                                    m_fileCacheVersion.Remove(kv.Key, out _);
                                    m_fileCache.Free(kv.Key);
                                }
                            }
                        }
                        else
                        {
                            m_fileCache.Free(kv.Key);
                        }
                    }
                    continue;
                }

                if (!loadedFromCache)
                {
                    var readOnlyBytes = await m_fileCache.Read(kv.Key);
                    var bytes = readOnlyBytes.ToArray();

                    // Write to persistence
                    await session.Write(kv.Key, new SerializableObject(bytes));

                    if (useReadCache)
                    {
                        lock(m_lock)
                        {
                            // Set version to -2 which marks that it is a read only version
                            if (!m_modified.ContainsKey(kv.Key))
                            {
                                m_fileCacheVersion[kv.Key] = -2;
                            }
                        }
                    }
                }
            }

            // Checkpoint the serializer
            await options.ValueSerializer!.CheckpointAsync(this, m_committingMetadata!);

            var modifiedPagesCount = m_committing.Count;
            Debug.Assert(stateManager.m_metadata != null);
            // Add modified page count to the page commits counter
            Interlocked.Add(ref stateManager.m_metadata.PageCommits, (ulong)modifiedPagesCount);
            // Modify active pages
            Interlocked.Add(ref stateManager.m_metadata.PageCount, m_committingNewPages);
            m_committingNewPages = 0;

            lock (m_lock)
            {
                if (!useReadCache)
                {
                    foreach (var key in m_committing.Keys)
                    {
                        if (!m_modified.ContainsKey(key))
                        {
                            m_fileCache.Free(key);
                            m_fileCacheVersion.Remove(key, out _);
                        }
                    }
                }
                m_committing.Clear();
                m_committing_preserialized.Clear();
            }

            await WriteMetadata(m_committingMetadata!);
            await session.Commit();
        }

        public override async Task WaitForCommitAsync()
        {
            if (m_commitTask != null)
            {
                await m_commitTask;
            }
        }

        private async Task WriteMetadata(StateClientMetadata<TMetadata> committingMetadata)
        {
            if (!committingMetadata.CommitedOnce || (committingMetadata.Metadata != null && committingMetadata.Metadata.Updated))
            {
                var previousCommitedOnce = metadata.CommitedOnce;
                try
                {
                    committingMetadata.CommitedOnce = true;
                    var bytes = StateClientMetadataSerializer.Serialize(committingMetadata);
                    await session.Write(metadataId, new SerializableObject(bytes));
                    
                    lock (m_lock)
                    {
                        if (committingMetadata.Metadata != null)
                        {
                            committingMetadata.Metadata.Updated = false;
                        }
                        metadata.CommitedOnce = true;
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
                // We keep it in m_committing if it's there so the commit background task can serialize its OLD version
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
            lock (m_lock)
            {
                bool needsPreserialize = false;
                if (m_committing.ContainsKey(key) && !m_committing_preserialized.ContainsKey(key))
                {
                    needsPreserialize = true;
                }

                var modLookup = key % _lookupTable.Length;

                if (_lookupTable[modLookup].Key == key)
                {
                    var node = _lookupTable[modLookup].Value!;
                    lock (node)
                    {
                        if (!node.ValueRef.removed)
                        {
                            if (needsPreserialize)
                            {
                                var bufferWriter = new System.Buffers.ArrayBufferWriter<byte>();
                                options.ValueSerializer!.Serialize(bufferWriter, (V)node.ValueRef.value);
                                m_committing_preserialized.TryAdd(key, bufferWriter.WrittenSpan.ToArray());
                            }

                            if (!node.ValueRef.value.TryRent())
                            {
                                throw new InvalidOperationException("Could not rent value from cache");
                            }
                            node.ValueRef.useCount = Math.Min(node.ValueRef.useCount + 1, 5);
                            return ValueTask.FromResult<V?>((V)_lookupTable[modLookup].Value!.ValueRef.value);
                        }
                    }
                }

                if (stateManager.TryGetCacheValueFromCache(key, out var cacheVal))
                {
                    if (needsPreserialize)
                    {
                        var bufferWriter = new System.Buffers.ArrayBufferWriter<byte>();
                        options.ValueSerializer!.Serialize(bufferWriter, (V)cacheVal.ValueRef.value);
                        m_committing_preserialized.TryAdd(key, bufferWriter.WrittenSpan.ToArray());
                    }

                    _lookupTable[modLookup] = new CacheValue { Key = key, Value = cacheVal };
                    return ValueTask.FromResult<V?>((V)cacheVal.ValueRef.value);
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

            bool needsPreserialize = false;
            lock (m_lock)
            {
                if (m_committing.ContainsKey(key) && !m_committing_preserialized.ContainsKey(key))
                {
                    needsPreserialize = true;
                }
            }

            if (needsPreserialize)
            {
                var readOnlyBytes = await m_fileCache.Read(key);
                var bytes = readOnlyBytes.ToArray();
                m_committing_preserialized.TryAdd(key, bytes);
                
                var value = options.ValueSerializer.Deserialize(new System.Buffers.ReadOnlySequence<byte>(bytes), bytes.Length);
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

            var value2 = await m_fileCache.Read<V>(key, options.ValueSerializer);
            if (!value2.TryRent())
            {
                throw new InvalidOperationException("Could not rent value when fetched from storage.");
            }
            stateManager.AddOrUpdate(key, value2, this);

            if (m_temporaryReadMsHistogram != null)
            {
                m_temporaryReadMsHistogram.Record((float)sw.GetElapsedTime().TotalMilliseconds, tagList);
            }

            return value2;
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

            stateManager.AddOrUpdate(key, value, this);
            if (!value.TryRent())
            {
                throw new InvalidOperationException("Could not rent value when fetched from storage.");
            }
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
            if (m_commitTask != null)
            {
                await m_commitTask;
            }

            lock (m_lock)
            {
                foreach (var kv in m_modified)
                {
                    stateManager.DeleteFromCache(kv.Key);
                    m_fileCache.Free(kv.Key);
                }
                for (int i = 0; i < _lookupTable.Length; i++)
                {
                    _lookupTable[i].Value = default;
                    _lookupTable[i].Key = 0;
                }
                m_fileCache.FreeAll(m_modified.Keys);
                m_modified.Clear();
                m_fileCacheVersion.Clear();
                m_committing.Clear();
                m_committing_preserialized.Clear();
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

        public void Evict(List<(LinkedListNode<LruTableSync.LinkedListValue>, long)> valuesToEvict, bool isCleanup)
        {
            Debug.Assert(options.ValueSerializer != null);
            foreach (var value in valuesToEvict)
            {
                var modLookup = value.Item1.ValueRef.key % _lookupTable.Length;
                bool isModified;
                int val;
                lock (m_lock)
                {
                    isModified = m_modified.TryGetValue(value.Item1.ValueRef.key, out val);
                    if (!isModified && m_committing.TryGetValue(value.Item1.ValueRef.key, out var commitVal))
                    {
                        isModified = true;
                        val = commitVal;
                    }
                    if (_lookupTable[modLookup].Key == value.Item1.ValueRef.key)
                    {
                        _lookupTable[modLookup] = new CacheValue { Key = -1, Value = null };
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

                if (m_fileCacheVersion.TryGetValue(value.Item1.ValueRef.key, out var storedVersion) && storedVersion == val)
                {
                    continue;
                }
                value.Item1.ValueRef.value.EnterWriteLock();
                var sw = ValueStopwatch.StartNew();
                try
                {
                    // Must lock the linked list value here since it can be deleted and disposed
                    // So we check if it is already removed from the cache, then we skip serialization
                    lock (value.Item1)
                    {
                        if (!value.Item1.ValueRef.removed)
                        {
                            m_fileCache.Write(value.Item1.ValueRef.key, new SerializableObject(value.Item1.ValueRef.value, options.ValueSerializer));
                        }
                    }
                }
                finally
                {
                    value.Item1.ValueRef.value.ExitWriteLock();
                }
                if (m_temporaryWriteMsHistogram != null)
                {
                    m_temporaryWriteMsHistogram.Record((float)sw.GetElapsedTime().TotalMilliseconds, tagList);
                }

                m_fileCacheVersion[value.Item1.ValueRef.key] = val;
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

        private struct CacheValue
        {
            public long Key;
            public LinkedListNode<LinkedListValue>? Value;
        }
    }
}

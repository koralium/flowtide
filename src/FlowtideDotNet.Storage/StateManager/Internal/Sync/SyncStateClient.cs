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

using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Utils;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class SyncStateClient<V, TMetadata> : StateClient, IStateClient<V, TMetadata>, ILruEvictHandler
        where V : ICacheObject
    {
        private bool disposedValue;
        private readonly StateManagerSync stateManager;
        private readonly long metadataId;
        private StateClientMetadata<TMetadata> metadata;
        private readonly IPersistentStorageSession session;
        private readonly StateClientOptions<V> options;
        private readonly bool useReadCache;
        private readonly int m_bplusTreePageSize;
        private readonly int m_bplusTreePageSizeBytes;
        private readonly ConcurrentDictionary<long, int> m_modified;
        private readonly object m_lock = new object();
        private readonly FlowtideDotNet.Storage.FileCache.FileCache m_fileCache;
        private readonly ConcurrentDictionary<long, int> m_fileCacheVersion;
        private readonly Histogram<float>? m_persistenceReadMsHistogram;
        private readonly Histogram<float>? m_temporaryReadMsHistogram;
        private readonly Histogram<float>? m_temporaryWriteMsHistogram;
        private readonly TagList tagList;

        // Method containers for addOrUpdate methods to skip casting to Func all the time
        private Func<long, int> addorUpdate_newValue_container;
        private Func<long, int, int> addorUpdate_existingValue_container;

        /// <summary>
        /// Value of how many pages have changed since last commit.
        /// </summary>
        private long newPages;
        private long cacheMisses;

        public long CacheMisses => cacheMisses;

        public SyncStateClient(
            StateManagerSync stateManager,
            string name,
            long metadataId,
            StateClientMetadata<TMetadata> metadata,
            IPersistentStorageSession session,
            StateClientOptions<V> options,
            FileCacheOptions fileCacheOptions,
            Meter meter,
            bool useReadCache,
            int bplusTreePageSize,
            int bplusTreePageSizeBytes)
        {
            this.stateManager = stateManager;
            this.metadataId = metadataId;
            this.metadata = metadata;
            this.session = session;
            this.options = options;
            this.useReadCache = useReadCache;
            this.m_bplusTreePageSize = bplusTreePageSize;
            this.m_bplusTreePageSizeBytes = bplusTreePageSizeBytes;
            m_fileCache = new FlowtideDotNet.Storage.FileCache.FileCache(fileCacheOptions, name);
            m_modified = new ConcurrentDictionary<long, int>();
            m_fileCacheVersion = new ConcurrentDictionary<long, int>();
            if (!string.IsNullOrEmpty(name))
            {
                m_persistenceReadMsHistogram = meter.CreateHistogram<float>("flowtide_persistence_read_ms");
                m_temporaryReadMsHistogram = meter.CreateHistogram<float>("flowtide_temporary_read_ms");
                m_temporaryWriteMsHistogram = meter.CreateHistogram<float>("flowtide_temporary_write_ms");
            }
            tagList = options.TagList;
            tagList.Add("state_client", name);
            addorUpdate_newValue_container = AddOrUpdate_NewValue;
            addorUpdate_existingValue_container = AddOrUpdate_ExistingValue;
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

        private class AddOrUpdateState
        {
            public bool isFull;
            public V? value;
        }

        private AddOrUpdateState _addOrUpdateState = new AddOrUpdateState();

        private int AddOrUpdate_NewValue(long key)
        {
            _addOrUpdateState.isFull = stateManager.AddOrUpdate(key, _addOrUpdateState.value!, this);
            return 0;
        }

        private int AddOrUpdate_ExistingValue(long key, int old)
        {
            _addOrUpdateState.isFull = stateManager.AddOrUpdate(key, _addOrUpdateState.value!, this);
            return old + 1;
        }

        public bool AddOrUpdate(in long key, V value)
        {
            lock (m_lock)
            {
                _addOrUpdateState.value = value;
                 m_modified.AddOrUpdate(key, addorUpdate_newValue_container, addorUpdate_existingValue_container);
                return _addOrUpdateState.isFull;
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
                    var bytes = options.ValueSerializer.Serialize(val, stateManager.SerializeOptions);
                    // Write to persistence
                    await session.Write(kv.Key, bytes);

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
                    var bytes = m_fileCache.Read(kv.Key);

                    if (bytes == null)
                    {
                        throw new InvalidOperationException("Data could not be found in temporary cache.");
                    }

                    // Write to persistence
                    await session.Write(kv.Key, bytes);

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
            var modifiedPagesCount = m_modified.Count;
            Debug.Assert(stateManager.m_metadata != null);
            // Add modified page count to the page commits counter
            Interlocked.Add(ref stateManager.m_metadata.PageCommits, (ulong)modifiedPagesCount);
            // Modify active pages
            Interlocked.Add(ref stateManager.m_metadata.PageCount, newPages);
            newPages = 0;

            m_modified.Clear();
            if (!useReadCache)
            {
                m_fileCache.FreeAll();
                m_fileCacheVersion.Clear();
            }
            
            {
                var bytes = StateClientMetadataSerializer.Serialize(metadata);
                await session.Write(metadataId, bytes);
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

        public ValueTask<V?> GetValue(in long key, string from)
        {
            Debug.Assert(options.ValueSerializer != null);
            lock (m_lock)
            {
                if (stateManager.TryGetValueFromCache<V>(key, out var val))
                {
                    return ValueTask.FromResult<V?>(val);
                }
                Interlocked.Increment(ref cacheMisses);
                // Read from temporary file storage
                if (m_fileCacheVersion.ContainsKey(key))
                {
                    var sw = ValueStopwatch.StartNew();
                    var bytes = m_fileCache.Read(key);
                    var value = options.ValueSerializer.Deserialize(new ByteMemoryOwner(bytes), bytes.Length, stateManager.SerializeOptions);
                    stateManager.AddOrUpdate(key, value, this);
                    if (!value.TryRent())
                    {
                        throw new InvalidOperationException("Could not rent value when fetched from storage.");
                    }
                    if (m_temporaryReadMsHistogram != null)
                    {
                        m_temporaryReadMsHistogram.Record((float)sw.GetElapsedTime().TotalMilliseconds, tagList);
                    }
                    
                    return ValueTask.FromResult<V?>(value);
                }
                // Read from persistent store
                return GetValue_Persistent(key);
            }
        }

        private async ValueTask<V?> GetValue_Persistent(long key)
        {
            Debug.Assert(options.ValueSerializer != null);
            var sw = ValueStopwatch.StartNew();
            var bytes = await session.Read(key);
            var value = options.ValueSerializer.Deserialize(new ByteMemoryOwner(bytes), bytes.Length, stateManager.SerializeOptions);
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
                foreach(var kv in m_modified)
                {
                    stateManager.DeleteFromCache(kv.Key);
                    m_fileCache.Free(kv.Key);
                }
                m_modified.Clear();
                m_fileCache.FreeAll();
                m_fileCacheVersion.Clear();
            }
            if (clearMetadata)
            {
                Metadata = default;
            }
            else
            {
                var bytes = await session.Read(metadataId);
                metadata = StateClientMetadataSerializer.Deserialize<TMetadata>(new ByteMemoryOwner(bytes), bytes.Length);
            }
        }

        public void Evict(List<(LinkedListNode<LruTableSync.LinkedListValue>, long)> valuesToEvict, bool isCleanup)
        {
            Debug.Assert(options.ValueSerializer != null);
            foreach (var value in valuesToEvict)
            {
                bool isModified = m_modified.TryGetValue(value.Item1.ValueRef.key, out var val);
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
                    var bytes = options.ValueSerializer.Serialize(value.Item1.ValueRef.value, stateManager.SerializeOptions);
                    m_fileCache.WriteAsync(value.Item1.ValueRef.key, bytes);
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
            }
        }
    }
}

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
using System.Diagnostics;

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
        private Dictionary<long, int> m_modified;
        private readonly object m_lock = new object();
        private readonly FlowtideDotNet.Storage.FileCache.FileCache m_fileCache;

        /// <summary>
        /// Value of how many pages have changed since last commit.
        /// </summary>
        private long newPages;

        public SyncStateClient(
            StateManagerSync stateManager,
            string name,
            long metadataId,
            StateClientMetadata<TMetadata> metadata,
            IPersistentStorageSession session,
            StateClientOptions<V> options,
            FileCacheOptions fileCacheOptions)
        {
            this.stateManager = stateManager;
            this.metadataId = metadataId;
            this.metadata = metadata;
            this.session = session;
            this.options = options;
            m_fileCache = new FlowtideDotNet.Storage.FileCache.FileCache(fileCacheOptions, name);
            m_modified = new Dictionary<long, int>();
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

        public ValueTask AddOrUpdate(in long key, in V value)
        {
            lock (m_lock)
            {
                m_modified[key] = 0;
                stateManager.AddOrUpdate(key, value, this);
                return ValueTask.CompletedTask;
            }
        }

        public async ValueTask Commit()
        {
            List<Task> writeTasks = new List<Task>();
            lock (m_lock)
            {
                
                foreach(var kv in m_modified)
                {
                    if (kv.Value == -1)
                    {
                        // deleted
                        writeTasks.Add(session.Delete(kv.Key));

                        // Remove a page from the new pages counter
                        Interlocked.Decrement(ref newPages);
                        continue;
                    }
                    if (stateManager.TryGetValueFromCache<V>(kv.Key, out var val))
                    {
                        var bytes = options.ValueSerializer.Serialize(val, stateManager.SerializeOptions);
                        // Write to persistence
                        writeTasks.Add(session.Write(kv.Key, bytes));
                        continue;
                    }
                    {
                        var bytes = m_fileCache.Read(kv.Key);

                        if (bytes == null)
                        {
                            throw new Exception();
                        }

                        // Write to persistence
                        writeTasks.Add(session.Write(kv.Key, bytes));

                        // Free the data from temporary storage
                        m_fileCache.Free(kv.Key);
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
                {
                    var bytes = StateClientMetadataSerializer.Instance.Serialize(metadata);
                    writeTasks.Add(session.Write(metadataId, bytes));
                }
            }
            await Task.WhenAll(writeTasks);
        }

        public void Delete(in long key)
        {
            lock (m_lock)
            {
                m_modified[key] = -1;
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
            lock (m_lock)
            {
                if (stateManager.TryGetValueFromCache<V>(key, out var val))
                {
                    return ValueTask.FromResult(val);
                }
                // Read from temporary file storage
                if (m_modified.ContainsKey(key))
                {
                    var bytes = m_fileCache.Read(key);
                    var value = options.ValueSerializer.Deserialize(new ByteMemoryOwner(bytes), bytes.Length, stateManager.SerializeOptions);
                    stateManager.AddOrUpdate(key, value, this);
                    return ValueTask.FromResult<V?>(value);
                }
                // Read from persistent store
                return GetValue_Persistent(key);
            }
        }

        private async ValueTask<V?> GetValue_Persistent(long key)
        {
            var bytes = await session.Read(key);
            var value = options.ValueSerializer.Deserialize(new ByteMemoryOwner(bytes), bytes.Length, stateManager.SerializeOptions);
            stateManager.AddOrUpdate(key, value, this);
            return value;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)x
                    m_fileCache.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
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
            }
            if (clearMetadata)
            {
                Metadata = default;
            }
            else
            {
                var bytes = await session.Read(metadataId);
                metadata = StateClientMetadataSerializer.Instance.Deserialize<TMetadata>(new ByteMemoryOwner(bytes), bytes.Length);
            }
        }

        public void Evict(List<(LinkedListNode<LruTableSync.LinkedListValue>, long)> valuesToEvict, bool isCleanup)
        {
            foreach (var value in valuesToEvict)
            {
                value.Item1.ValueRef.value.EnterWriteLock();
                var bytes = options.ValueSerializer.Serialize(value.Item1.ValueRef.value, stateManager.SerializeOptions);
                m_fileCache.WriteAsync(value.Item1.ValueRef.key, bytes);
                value.Item1.ValueRef.value.ExitWriteLock();
            }
            m_fileCache.Flush();

            if (isCleanup)
            {
                m_fileCache.ClearTemporaryAllocations();
            }
        }
    }
}

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
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Storage.Persistence.CacheStorage
{
    public class FileCachePersistentStorage : IPersistentStorage
    {
        private readonly bool _ignoreDispose;
        private readonly FileCacheOptions _fileCacheOptions;
        private readonly HashSet<long> _uncheckpointedPages = new HashSet<long>();
        private readonly HashSet<long> _checkpointedPages = new HashSet<long>();
        private readonly object _lock = new object();
        private long _version;
        internal FlowtideDotNet.Storage.FileCache.FileCache m_fileCache;

        public FileCachePersistentStorage(FileCacheOptions fileCacheOptions, bool ignoreDispose = false)
        {
            _fileCacheOptions = fileCacheOptions;
            // Start at version 1, since version 0 is reserved for empty state
            _version = 1;
            m_fileCache = new FlowtideDotNet.Storage.FileCache.FileCache(fileCacheOptions, "persitent", GlobalMemoryManager.Instance);
            this._ignoreDispose = ignoreDispose;
        }

        internal void OnKeyWritten(long key)
        {
            lock (_lock)
            {
                _uncheckpointedPages.Add(key);
            }
        }

        internal void OnKeyDeleted(long key)
        {
            lock (_lock)
            {
                _uncheckpointedPages.Remove(key);
                _checkpointedPages.Remove(key);
            }
        }

        public long CurrentVersion => _version;

        public virtual async ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            await Write(1, metadata);
            lock (_lock)
            {
                foreach (var page in _uncheckpointedPages)
                {
                    _checkpointedPages.Add(page);
                }
                _uncheckpointedPages.Clear();
                _version++;
            }
        }

        public virtual ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount)
        {
            return ValueTask.CompletedTask;
        }

        public virtual IPersistentStorageSession CreateSession()
        {
            return new FileCachePersistentSession(this, m_fileCache);
        }

        public void Dispose()
        {
            if (!_ignoreDispose)
            {
                lock (_lock)
                {
                    _uncheckpointedPages.Clear();
                    _checkpointedPages.Clear();
                    m_fileCache.Dispose();
                }
            }
        }

        /// <summary>
        /// Force dispose even if ignoreDispose is true.
        /// </summary>
        public void ForceDispose()
        {
            lock (_lock)
            {
                _uncheckpointedPages.Clear();
                _checkpointedPages.Clear();
                m_fileCache.Dispose();
            }
        }

        public virtual Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            return Task.CompletedTask;
        }

        public virtual ValueTask RecoverAsync(long checkpointVersion)
        {
            return ValueTask.CompletedTask;
        }

        public virtual ValueTask ResetAsync()
        {
            lock (_lock)
            {
                _uncheckpointedPages.Clear();
                _checkpointedPages.Clear();
                // Reset file cache to an empty state.
                m_fileCache.Dispose();
                m_fileCache = new FlowtideDotNet.Storage.FileCache.FileCache(_fileCacheOptions, "persitent", GlobalMemoryManager.Instance);
                _version = 1;
            }
            return ValueTask.CompletedTask;
        }

        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value)
        {
            if (m_fileCache.Exists(key))
            {
                value = m_fileCache.ReadSync(key);
                return true;
            }
            value = default;
            return false;
        }

        public virtual ValueTask Write(long key, byte[] value)
        {
            OnKeyWritten(key);
            m_fileCache.Write(key, new SerializableObject(value));
            m_fileCache.Flush();
            return ValueTask.CompletedTask;
        }

        public void ClearForRestore()
        {
            lock (_lock)
            {
                // Retain previously checkpointed pages across clear for restore.
                foreach (var page in _uncheckpointedPages)
                {
                    if (!_checkpointedPages.Contains(page))
                    {
                        m_fileCache.Free(page);
                    }
                }
                _uncheckpointedPages.Clear();
                m_fileCache.ClearTemporaryAllocations();
            }
        }
    }
}

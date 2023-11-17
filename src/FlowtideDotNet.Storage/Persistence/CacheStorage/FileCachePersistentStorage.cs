﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Storage.Persistence.CacheStorage
{
    public class FileCachePersistentStorage : IPersistentStorage
    {
        private readonly bool _ignoreDispose;
        private long _version;
        private FlowtideDotNet.Storage.FileCache.FileCache m_fileCache;

        public FileCachePersistentStorage(FileCacheOptions fileCacheOptions, bool ignoreDispose = false)
        {
            m_fileCache = new FlowtideDotNet.Storage.FileCache.FileCache(fileCacheOptions, "persitent");
            this._ignoreDispose = ignoreDispose;
        }

        public long CurrentVersion => _version;

        public ValueTask CheckpointAsync(byte[] metadata)
        {
            Write(1, metadata);
            _version++;
            return ValueTask.CompletedTask;
        }

        public ValueTask CompactAsync()
        {
            return ValueTask.CompletedTask;
        }

        public IPersistentStorageSession CreateSession()
        {
            return new FileCachePersistentSession(m_fileCache);
        }

        public void Dispose()
        {
            if (!_ignoreDispose)
            {
                m_fileCache.Dispose();
            }
        }

        /// <summary>
        /// Force dispose even if ignoreDispose is true.
        /// </summary>
        public void ForceDispose()
        {
            m_fileCache.Dispose();
        }

        public Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        public ValueTask RecoverAsync(long checkpointVersion)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask ResetAsync()
        {
            return ValueTask.CompletedTask;
        }

        public bool TryGetValue(long key, [NotNullWhen(true)] out byte[]? value)
        {
            if (m_fileCache.Exists(key))
            {
                value = m_fileCache.Read(key);
                return true;
            }
            value = default;
            return false;
        }

        public void Write(long key, byte[] value)
        {
            m_fileCache.WriteAsync(key, value);
            m_fileCache.Flush();
        }
    }
}

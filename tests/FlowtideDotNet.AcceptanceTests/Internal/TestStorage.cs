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

using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.CacheStorage;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    class TestStorage : FileCachePersistentStorage
    {
        private HashSet<long> _writtenKeys = new HashSet<long>();

        public TestStorage(FileCacheOptions fileCacheOptions, bool ignoreDispose = false) : base(fileCacheOptions, ignoreDispose)
        {
        }

        public override ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            lock (_writtenKeys)
            {
                _writtenKeys.Clear();
            }
            return base.CheckpointAsync(metadata, includeIndex);
        }

        public override Task InitializeAsync()
        {
            lock (_writtenKeys)
            {
                _writtenKeys.Clear();
            }
            return base.InitializeAsync();
        }

        public void AddWrittenKey(long key)
        {
            lock (_writtenKeys)
            {
                if (key == 2)
                {

                }
                if (_writtenKeys.Contains(key))
                {
                    throw new InvalidOperationException($"Key {key} already written");
                }
                _writtenKeys.Add(key);
            }
        }

        public override ValueTask Write(long key, byte[] value)
        {
            AddWrittenKey(key);
            return base.Write(key, value);
        }

        public override IPersistentStorageSession CreateSession()
        {
            return new TestStorageSession(m_fileCache, this);
        }
    }
}

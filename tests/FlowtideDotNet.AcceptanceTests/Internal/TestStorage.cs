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
        private Dictionary<long, byte[]> _writtenValues = new Dictionary<long, byte[]>();
        private Dictionary<long, byte[]> _lastCheckpointValues = new Dictionary<long, byte[]>();
        private readonly List<TestStorageSession> _sessions = new List<TestStorageSession>();
        private readonly bool _ignoreSameDataCheck;
        public TestStorage(FileCacheOptions fileCacheOptions, bool ignoreSameDataCheck, bool ignoreDispose = false) : base(fileCacheOptions, ignoreDispose)
        {
            _ignoreSameDataCheck = ignoreSameDataCheck;
        }

        public override ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            if (!_sessions.TrueForAll(s => s.HasCommitted))
            {
                throw new InvalidOperationException("Not all sessions have committed");
            }

            lock (_writtenKeys)
            {
                _writtenKeys.Clear();
                // Insert data into the last checkpoint values
                foreach (var kvp in _writtenValues)
                {
                    _lastCheckpointValues[kvp.Key] = kvp.Value;
                }
            }
            return base.CheckpointAsync(metadata, includeIndex);
        }

        public override Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            lock (_writtenKeys)
            {
                _writtenKeys.Clear();
                _writtenValues.Clear();
                foreach (var kvp in _lastCheckpointValues)
                {
                    _writtenValues[kvp.Key] = kvp.Value;
                }
            }
            return base.InitializeAsync(metadata);
        }

        public void AddWrittenKey(long key, byte[] data)
        {
            lock (_writtenKeys)
            {
                if (_writtenKeys.Contains(key))
                {
                    throw new InvalidOperationException($"Key {key} already written");
                }
                _writtenKeys.Add(key);
                if (_writtenValues.TryGetValue(key, out var existingData) && data.SequenceEqual(existingData) && !_ignoreSameDataCheck)
                {
                    throw new InvalidOperationException($"Key {key} already written with the same data");
                }
                _writtenValues[key] = data;
            }
        }

        public override ValueTask Write(long key, byte[] value)
        {
            AddWrittenKey(key, value);
            return base.Write(key, value);
        }

        public override IPersistentStorageSession CreateSession()
        {
            var session = new TestStorageSession(m_fileCache, this);
            _sessions.Add(session);
            return session;
        }
    }
}

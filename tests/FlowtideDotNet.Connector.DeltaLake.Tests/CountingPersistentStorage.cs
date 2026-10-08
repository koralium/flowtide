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
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// Tracks which page keys were written and not deleted.
    /// </summary>
    internal sealed class CountingPersistentStorage : IPersistentStorage
    {
        private readonly IPersistentStorage _inner;
        private readonly ConcurrentDictionary<long, byte> _livePages = new ConcurrentDictionary<long, byte>();
        private readonly ConcurrentDictionary<long, int> _pageSession = new ConcurrentDictionary<long, int>();
        private readonly ConcurrentDictionary<int, string> _sessionOrigins = new ConcurrentDictionary<int, string>();
        private int _sessions;

        public CountingPersistentStorage(IPersistentStorage inner)
        {
            _inner = inner;
        }

        public int LivePages => _livePages.Count;

        public bool SupportsDistributedCheckpoints => _inner.SupportsDistributedCheckpoints;

        public long CurrentVersion => _inner.CurrentVersion;

        public ValueTask CheckpointAsync(byte[] metadata, bool includeIndex) => _inner.CheckpointAsync(metadata, includeIndex);

        public void ClearForRestore() => _inner.ClearForRestore();

        public ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount) => _inner.CompactAsync(changesSinceLastCompact, pageCount);

        // Each state client opens its own session, the stack that opened it names the client
        public IPersistentStorageSession CreateSession()
        {
            var id = Interlocked.Increment(ref _sessions);
            _sessionOrigins[id] = Environment.StackTrace;
            return new CountingSession(_inner.CreateSession(), _livePages, _pageSession, id);
        }

        // Live pages written by sessions whose opening stack matches
        public int LivePagesOf(Func<string, bool> origin)
        {
            return _livePages.Keys.Count(k => _pageSession.TryGetValue(k, out var session) && origin(_sessionOrigins[session]));
        }

        public void Dispose() => _inner.Dispose();

        public Task InitializeAsync(StorageInitializationMetadata metadata) => _inner.InitializeAsync(metadata);

        public ValueTask RecoverAsync(long checkpointVersion) => _inner.RecoverAsync(checkpointVersion);

        public ValueTask ResetAsync() => _inner.ResetAsync();

        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value) => _inner.TryGetValue(key, out value);

        public ValueTask Write(long key, byte[] value)
        {
            _livePages[key] = 0;
            return _inner.Write(key, value);
        }

        private sealed class CountingSession : IPersistentStorageSession
        {
            private readonly IPersistentStorageSession _inner;
            private readonly ConcurrentDictionary<long, byte> _livePages;
            private readonly ConcurrentDictionary<long, int> _pageSession;
            private readonly int _id;

            public CountingSession(IPersistentStorageSession inner, ConcurrentDictionary<long, byte> livePages, ConcurrentDictionary<long, int> pageSession, int id)
            {
                _inner = inner;
                _livePages = livePages;
                _pageSession = pageSession;
                _id = id;
            }

            public bool SupportsConcurrentReads => _inner.SupportsConcurrentReads;

            public Task Commit() => _inner.Commit();

            public Task Delete(long key)
            {
                _livePages.TryRemove(key, out _);
                return _inner.Delete(key);
            }

            public void Dispose() => _inner.Dispose();

            public ValueTask<T> Read<T>(long key, IStateSerializer<T> stateSerializer) where T : ICacheObject => _inner.Read(key, stateSerializer);

            public ValueTask<ReadOnlyMemory<byte>> Read(long key) => _inner.Read(key);

            public Task Write(long key, SerializableObject value)
            {
                _livePages[key] = 0;
                _pageSession[key] = _id;
                return _inner.Write(key, value);
            }
        }
    }
}

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
using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Collections.Concurrent;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// Counts the spill writes that returned, per cache name, so a spill test cannot pass without spilling.
    /// </summary>
    internal sealed class WriteCountingFileCacheFactory : IFileCacheFactory
    {
        private readonly IFileCacheFactory _inner;
        private readonly ConcurrentDictionary<string, int> _writes = new ConcurrentDictionary<string, int>();

        public WriteCountingFileCacheFactory(IFileCacheFactory inner)
        {
            _inner = inner;
        }

        public IFileCache Create(string name, IMemoryAllocator memoryAllocator)
        {
            return new CountingFileCache(_inner.Create(name, memoryAllocator), name, _writes);
        }

        public int WritesOf(Func<string, bool> name) => _writes.Where(x => name(x.Key)).Sum(x => x.Value);

        private sealed class CountingFileCache : IFileCache
        {
            private readonly IFileCache _inner;
            private readonly string _name;
            private readonly ConcurrentDictionary<string, int> _writes;

            public CountingFileCache(IFileCache inner, string name, ConcurrentDictionary<string, int> writes)
            {
                _inner = inner;
                _name = name;
                _writes = writes;
            }

            public void Write(long id, SerializableObject serializableObject)
            {
                _inner.Write(id, serializableObject);
                _writes.AddOrUpdate(_name, 1, static (_, count) => count + 1);
            }

            public ValueTask<ReadOnlyMemory<byte>> Read(long pageKey) => _inner.Read(pageKey);

            public ValueTask<T> Read<T>(long pageKey, IStateSerializer<T> serializer) where T : ICacheObject => _inner.Read(pageKey, serializer);

            public void Free(in long pageKey) => _inner.Free(pageKey);

            public void FreeAll(IEnumerable<long> keys) => _inner.FreeAll(keys);

            public void Flush() => _inner.Flush();

            public void ClearTemporaryAllocations() => _inner.ClearTemporaryAllocations();

            public void Dispose() => _inner.Dispose();
        }
    }
}

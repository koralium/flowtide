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

using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    /// <summary>
    /// A state client that is never persisted, so a deleted page leaves no bookkeeping behind.
    /// </summary>
    internal sealed class EphemeralSyncStateClient<V, TMetadata> : SyncStateClient<V, TMetadata>, IStateClient<V, TMetadata>, IEphemeralStateClient
        where V : ICacheObject
        where TMetadata : class, IStorageMetadata
    {
        // Signals when a delete reaches the cache delete, runs under the client lock so it must not block
        internal Action<long>? BeforeCacheDeleteHookForTests;

        public EphemeralSyncStateClient(
            StateManagerSync stateManager,
            string name,
            long metadataId,
            StateClientMetadata<TMetadata> metadata,
            IPersistentStorageSession session,
            StateClientOptions<V> options,
            IFileCacheFactory fileCacheFactory,
            Meter meter,
            bool useReadCache,
            bool backgroundCommit,
            int bplusTreePageSize,
            int bplusTreePageSizeBytes,
            IMemoryAllocator memoryAllocator)
            : base(stateManager, name, metadataId, metadata, session, options, fileCacheFactory, meter, useReadCache, backgroundCommit, bplusTreePageSize, bplusTreePageSizeBytes, memoryAllocator)
        {
        }

        ValueTask IStateClient<V, TMetadata>.Commit() => ValueTask.CompletedTask;

        void IStateClient<V, TMetadata>.Delete(in long key) => ForgetPage(key, BeforeCacheDeleteHookForTests);
    }
}

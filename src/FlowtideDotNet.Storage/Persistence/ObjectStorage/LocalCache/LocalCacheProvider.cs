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

using FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.LocalCache
{
    internal class LocalCacheProvider : IFileStorageProvider
    {
        private readonly IFileStorageProvider _remoteStorage;
        private LocalCacheManager _localCacheManager;

        public LocalCacheProvider(BlobPersistentStorage blobPersistentStorage, IFileStorageProvider localCache, IFileStorageProvider remoteStorage)
        {
            _localCacheManager = new LocalCacheManager(blobPersistentStorage, localCache, remoteStorage, 10L * 1000 * 1000 * 1000);
            _remoteStorage = remoteStorage;
        }

        public long CurrentSize => _localCacheManager.CurrentSize;

        /// <summary>
        /// Initialize the local cache, this includes listing data files from disk and adding them to the cache.
        /// This allows the local cache to be reused even after a crash
        /// </summary>
        /// <returns></returns>
        public Task InitializeAsync(CancellationToken cancellationToken)
        {
            return _localCacheManager.InitializeAsync(cancellationToken);
        }

        public Task DeleteCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            return _remoteStorage.DeleteCheckpointFileAsync(checkpointVersion);
        }

        /// <summary>
        /// Used for testing
        /// </summary>
        /// <param name="fileId"></param>
        /// <returns></returns>
        internal Task EvictDataFileAsync(long fileId, CancellationToken cancellationToken = default)
        {
            return _localCacheManager.EvictDataFileAsync(fileId);
        }

        public async Task DeleteDataFileAsync(long fileId, CancellationToken cancellationToken = default)
        {
            await _localCacheManager.EvictDataFileAsync(fileId);
            await _remoteStorage.DeleteDataFileAsync(fileId);
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(long fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
            return _localCacheManager.ReadMemoryAsync(fileId, offset, length, crc32);
        }

        public ValueTask<T> ReadAsync<T>(long fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default) where T : ICacheObject
        {
            return _localCacheManager.ReadAsync(fileId, offset, length, crc32, stateSerializer);
        }

        public Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            return _remoteStorage.ReadCheckpointFileAsync(checkpointVersion);
        }

        public Task<PipeReader?> ReadCheckpointRegistryFileAsync(CancellationToken cancellationToken = default)
        {
            return _remoteStorage.ReadCheckpointRegistryFileAsync();
        }

        public Task<PipeReader> ReadDataFileAsync(long fileId, CancellationToken cancellationToken = default)
        {
            // Fix later to read from cache also
            // Should probably have a try read from cache, if its not in cache, just skip it and read directly from remote
            // Since this method is only called on compactions
            return _remoteStorage.ReadDataFileAsync(fileId);
        }

        public Task WriteCheckpointFileAsync(CheckpointVersion checkpointVersion, PipeReader data, CancellationToken cancellationToken = default)
        {
            return _remoteStorage.WriteCheckpointFileAsync(checkpointVersion, data);
        }

        public Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default)
        {
            return _remoteStorage.WriteCheckpointRegistryFile(data);
        }

        public async Task WriteDataFileAsync(long fileId, ulong crc64, int size, PipeReader data, CancellationToken cancellationToken = default)
        {
            await _localCacheManager.RegisterNewFileAsync(fileId, crc64, size, data);
            data.CancelPendingRead(); // Cancel pending read is implemented in the file readers to reset to start, this is a special case for cache
            await _remoteStorage.WriteDataFileAsync(fileId, crc64, size, data);
        }

        public Task<IEnumerable<long>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }
    }
}

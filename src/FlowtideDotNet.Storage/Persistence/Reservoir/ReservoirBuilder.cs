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

using System.Buffers;

namespace FlowtideDotNet.Storage.Persistence.Reservoir
{
    public class ReservoirBuilder : IReservoirBuilder
    {
        private IReservoirStorageProvider? _reservoirStorageProvider;
        private IReservoirStorageProvider? _cacheProvider;
        private MemoryPool<byte> _memoryPool = MemoryPool<byte>.Shared;
        private int _snapshotInterval = 20;
        private int _maxFileSize = 64 * 1024 * 1024;
        private long _cacheSize = 10L * 1024 * 1024 * 1024;
        private int _oldVersionsToKeep = -1;



        public void SetStorage(IReservoirStorageProvider reservoirStorageProvider)
        {
            _reservoirStorageProvider = reservoirStorageProvider;
        }

        public void SetCache(IReservoirStorageProvider cache)
        {
            _cacheProvider = cache;
        }

        public void DisableCache()
        {
            _cacheProvider = default;
        }

        public void SetSnapshotCheckpointInterval(int snapshotInterval)
        {
            _snapshotInterval = snapshotInterval;
        }

        public void SetMemoryPool(MemoryPool<byte> memoryPool)
        {
            _memoryPool = memoryPool;
        }

        public void SetMaxDataFileSize(int maxFileSize)
        {
            _maxFileSize = maxFileSize;
        }

        public void SetCacheSize(long cacheSize)
        {
            _cacheSize = cacheSize;
        }

        public void OldStreamVersionsRetention(int versionsToKeep)
        {
            _oldVersionsToKeep = versionsToKeep;
        }

        public ReservoirStorageOptions Build()
        {
            return new ReservoirStorageOptions()
            {
                CacheProvider = _cacheProvider,
                FileProvider = _reservoirStorageProvider,
                MemoryPool = _memoryPool,
                SnapshotCheckpointInterval = _snapshotInterval,
                MaxFileSize = _maxFileSize,
                MaxCacheSizeBytes = _cacheSize,
                KeepLastStreamVersions = _oldVersionsToKeep
            };
        }
    }
}

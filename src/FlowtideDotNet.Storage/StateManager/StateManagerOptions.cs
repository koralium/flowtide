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
using FlowtideDotNet.Storage.Persistence;

namespace FlowtideDotNet.Storage.StateManager
{
    public class StateManagerOptions
    {
        public int CachePageCount { get; set; } = 10000;

        /// <summary>
        /// The minimum cache page count, used to keep pages in memory to reduce latency.
        /// </summary>
        public int MinCachePageCount { get; set; } = 1000;

        /// <summary>
        /// Keeps the cache's small queue at its target share even when the cache is below the
        /// eviction threshold, instead of letting it use the space the cache has free.
        /// On by default. Turning it off keeps every page the cache has room for, which raises
        /// the hit rate at the cost of holding several times as many pages resident.
        /// </summary>
        public bool DrainSmallQueueEarly { get; set; } = true;

        /// <summary>
        /// Sizes the cache's small queue from what the ghost queue observes rather than holding
        /// the fixed 10% share, so a workload that reuses pages gives the main queue more room and
        /// one that does not gives it less.
        /// On by default. Turning it off holds the share the S3-FIFO paper fixes it at, which is
        /// the right one only for a workload that keeps the same shape throughout.
        /// </summary>
        public bool AdaptiveSmallQueueSize { get; set; } = true;

        /// <summary>
        /// Optional: Set a maximum process memory limit for the state manager. If the limit is reached, the state manager will start evicting pages from the cache.
        /// This can help keep the application at a steady memory usage, but can also cause performance issues if the limit is too low.
        /// Setting this value increases cache page count if memory usage is low.
        /// 
        /// -1 disables the limit and only uses cache page count.
        /// </summary>
        public long MaxProcessMemory { get; set; } = -1;

        public IPersistentStorage? PersistentStorage { get; set; }

        /// <summary>
        /// Used if file cache factory is not set, with the default file cache
        /// </summary>
        public FileCacheOptions? TemporaryStorageOptions { get; set; }

        public IFileCacheFactory? FileCacheFactory { get; set; }

        public StateSerializeOptions SerializeOptions { get; set; } = new StateSerializeOptions();

        /// <summary>
        /// Uses temporary cache as a read cache.
        /// This can help increase performance and lower I/O against persistence storage.
        /// Useful if persistent storage is not on disk and instead in a remote location.
        /// </summary>
        public bool UseReadCache { get; set; } = false;

        public int DefaultBPlusTreePageSize { get; set; } = 1024;

        public int DefaultBPlusTreePageSizeBytes { get; set; } = 32 * 1024;
    }
}

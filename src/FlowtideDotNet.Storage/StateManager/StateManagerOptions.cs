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

using FlowtideDotNet.Storage.Persistence;
using FASTER.core;

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
        /// Optional: Set a maximum process memory limit for the state manager. If the limit is reached, the state manager will start evicting pages from the cache.
        /// This can help keep the application at a steady memory usage, but can also cause performance issues if the limit is too low.
        /// Setting this value increases cache page count if memory usage is low.
        /// 
        /// -1 disables the limit and only uses cache page count.
        /// </summary>
        public long MaxProcessMemory { get; set; } = -1;

        [Obsolete]
        public IDevice? LogDevice { get; set; }

        public IPersistentStorage? PersistentStorage { get; set; }

        [Obsolete]
        public string? CheckpointDir { get; set; }

        [Obsolete]
        public ICheckpointManager? CheckpointManager { get; set; }

        [Obsolete]
        public INamedDeviceFactory? TemporaryStorageFactory { get; set; }

        public FileCacheOptions? TemporaryStorageOptions { get; set; }

        [Obsolete]
        public bool RemoveOutdatedCheckpoints { get; set; } = true;

        public StateSerializeOptions SerializeOptions { get; set; } = new StateSerializeOptions();

        /// <summary>
        /// Uses temporary cache as a read cache.
        /// This can help increase performance and lower I/O against persistence storage.
        /// Useful if persistent storage is not on disk and instead in a remote location.
        /// </summary>
        public bool UseReadCache { get; set; } = false;

        public int DefaultBPlusTreePageSize { get; set; } = 1024;
    }
}

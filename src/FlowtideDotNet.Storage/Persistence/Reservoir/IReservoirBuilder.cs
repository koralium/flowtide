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

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir
{
    /// <summary>
    /// Allows configuring the reservoir storage options, such as cache settings, snapshot intervals, and storage providers. 
    /// This interface is used during the setup of the reservoir storage to specify how the reservoir should manage its data storage and caching behavior.
    /// </summary>
    /// <remarks>
    /// The reservoir storage is designed to efficiently manage the state of streams by utilizing a combination of primary storage and optional caching.
    /// The options configured through this interface can have a significant impact on the performance and resource utilization of the reservoir storage. For example, enabling caching can improve read and write performance by reducing the number of interactions with the underlying storage provider.
    /// </remarks>
    public interface IReservoirBuilder
    {
        /// <summary>
        /// Disables the use of a cache for the reservoir storage. 
        /// </summary>
        /// <remarks>
        /// When this method is called, all read and write operations will interact directly with the underlying storage provider without utilizing an intermediate caching layer. 
        /// This may result in increased latency for read and write operations, especially if the underlying storage provider has higher access times. Use this option when you want to ensure that all data is read from and written to the 
        /// primary storage without any caching optimizations.
        /// 
        /// Some storage providers will not work without the use of the local cache.
        /// </remarks>
        void DisableCache();

        /// <summary>
        /// Sets the number of previous stream versions to retain in storage.
        /// Stream versioning is opt-in and requires explicit configuration on the stream builder using
        /// <c>AddVersioningFromPlanHash()</c>, <c>AddVersioningFromString()</c>, or <c>AddVersioningFromAssembly()</c>.
        /// Without versioning, the stream uses a single default version and this setting has no effect.
        /// The current version is always preserved; this setting controls how many <em>previous</em> versions are kept alongside it.
        /// Default is -1 (all versions are kept).
        /// </summary>
        /// <remarks>
        /// A value of -1 retains all versions indefinitely.
        /// A value of 0 deletes all old versions immediately after a checkpoint, keeping only the current version.
        /// A value of 1 keeps one previous version in addition to the current version.
        /// Versions are sorted by their last initialization time; the oldest versions beyond the retention count are deleted first.
        /// This controls cleanup of old stream versions, not old checkpoints within a version.
        /// </remarks>
        /// <param name="versionsToKeep">
        /// The number of previous stream versions to retain. A value of -1 retains all versions without limit.
        /// </param>
        void OldStreamVersionsRetention(int versionsToKeep);

        /// <summary>
        /// Sets the cache provider for the reservoir storage. The cache is used to store frequently accessed data in memory, 
        /// which can improve performance by reducing the number of read and write operations to the underlying storage provider.
        /// </summary>
        /// <param name="cache"></param>
        void SetCache(IReservoirStorageProvider cache);

        /// <summary>
        /// Sets the maximum size of the cache in bytes. When the cache exceeds this size, the least recently used items will be evicted.
        /// </summary>
        /// <param name="cacheSize">
        /// The maximum size of the cache in bytes. When the cache exceeds this size, the least recently used items will be evicted.
        /// </param>
        void SetCacheSize(long cacheSize);

        /// <summary>
        /// Sets the maximum allowed file size for the reservoir storage, in bytes. This limits the size of the uploaded files to the underlying storage provider.
        /// </summary>
        /// <param name="maxFileSize">The maximum allowed file size, in bytes.</param>
        void SetMaxDataFileSize(int maxFileSize);

        /// <summary>
        /// Sets the memory pool to be used for buffer management in the reservoir storage.
        /// </summary>
        /// <param name="memoryPool">
        /// The memory pool to be used for buffer management in the reservoir storage.
        /// </param>
        void SetMemoryPool(MemoryPool<byte> memoryPool);

        /// <summary>
        /// Sets the interval, in checkpoints, at which a snapshot checkpoint is created. A lower value results in more frequent snapshot checkpoints, 
        /// which can improve recovery time at the cost of increased overhead due to more frequent snapshot creation.
        /// </summary>
        /// <param name="snapshotInterval">The interval, in checkpoints, at which a snapshot checkpoint is created.</param>
        void SetSnapshotCheckpointInterval(int snapshotInterval);

        /// <summary>
        /// Sets the storage provider for the reservoir storage. The storage provider is responsible for handling the actual read and write operations to the underlying storage system, such as a file system, cloud storage, or database.
        /// </summary>
        /// <param name="reservoirStorageProvider">The storage provider to be used for the reservoir storage.</param>
        void SetStorage(IReservoirStorageProvider reservoirStorageProvider);
    }
}

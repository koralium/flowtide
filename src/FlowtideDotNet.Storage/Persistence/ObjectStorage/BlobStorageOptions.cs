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

using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    public class BlobStorageOptions
    {
        public IFileStorageProvider? FileProvider { get; set; }

        public IFileStorageProvider? CacheProvider { get; set; }

        public MemoryPool<byte> MemoryPool { get; set; } = MemoryPool<byte>.Shared;

        public IMemoryAllocator MemoryAllocator { get; set; } = GlobalMemoryManager.Instance;

        /// <summary>
        /// Gets or sets the maximum allowed file size, in bytes.
        /// </summary>
        /// <remarks>The default value is 64 MB. Set this property to limit the size of
        /// the uploaded files.</remarks>
        public int MaxFileSize { get; set; } = 64 * 1024 * 1024;

        /// <summary>
        /// Gets or sets the interval, in operations, at which a snapshot checkpoint is created.
        /// </summary>
        /// <remarks>A lower value results in more frequent snapshot checkpoints, which can improve recovery time
        /// but may impact performance due to increased snapshot creation. A higher value reduces the frequency of
        /// snapshot checkpoints, potentially increasing recovery time after a failure.</remarks>
        public int SnapshotCheckpointInterval { get; set; } = 20;

        /// <summary>
        /// Gets or sets the file size ratio threshold used to determine when compaction should occur.
        /// This in relation to the max file size. For example, with a max file size of 64 MB and a compaction ratio threshold of 0.33, 
        /// compaction will be triggered when active pages are below 21 MB.
        /// </summary>
        public float CompactionFileSizeRatioThreshold { get; set; } = 0.33f;

        /// <summary>
        /// If Cache is configured, this sets the maximum size of the cache in bytes. 
        /// When the cache exceeds this size, the least recently used items will be evicted.
        /// 
        /// Default is 10 GB.
        /// </summary>
        public long MaxCacheSizeBytes { get; set; } = 10L * 1000 * 1000 * 1000;
    }
}

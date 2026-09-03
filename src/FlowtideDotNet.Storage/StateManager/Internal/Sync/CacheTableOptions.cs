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
using Microsoft.Extensions.Logging;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class CacheTableOptions
    {
        public CacheTableOptions(string streamName, ILogger logger, Meter meter, IMemoryAllocationStats memoryAllocationStats)
        {
            StreamName = streamName;
            Logger = logger;
            Meter = meter;
            MemoryAllocationStats = memoryAllocationStats;
        }

        public int MaxSize { get; set; } = 10000;

        public long MaxMemoryUsageInBytes { get; set; } = -1;

        public int MinSize { get; set; } = 1000;

        /// <summary>
        /// Hold the small queue at its target share even while the cache is below the eviction
        /// threshold. Off by default, a cache with room to spare keeps what it has rather than
        /// trading resident pages for queue shares.
        /// </summary>
        public bool DrainSmallQueueEarly { get; set; }

        /// <summary>
        /// Let the small queue's share of the cache follow what the ghost queue observes instead
        /// of holding the fixed 10%. A hit on a key the small queue evicted grows its share, a hit
        /// on one the main queue evicted shrinks it.
        /// </summary>
        public bool AdaptiveSmallQueueSize { get; set; }

        public string StreamName { get; }

        public ILogger Logger { get; }

        public Meter Meter { get; }
        public IMemoryAllocationStats MemoryAllocationStats { get; }
    }
}

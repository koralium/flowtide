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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir
{
    /// <summary>
    /// Provides contextual information supplied to an <see cref="IReservoirStorageProvider"/> during initialization.
    /// </summary>
    /// <remarks>
    /// A <see cref="StorageProviderContext"/> is constructed by the storage system and passed to
    /// <see cref="IReservoirStorageProvider.InitializeAsync"/> before the provider is used.
    /// Storage providers use the values it exposes to derive stream-specific storage paths and to
    /// allocate any native or pooled memory buffers they require at runtime.
    /// </remarks>
    public class StorageProviderContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StorageProviderContext"/> class.
        /// </summary>
        /// <param name="streamName">The logical name of the stream that owns the storage provider.</param>
        /// <param name="streamVersion">
        /// The version string used to isolate this stream's data within the storage backend.
        /// This is typically derived from the stream's plan hash or a semantic version identifier.
        /// </param>
        /// <param name="memoryAllocator">
        /// The memory allocator the storage provider should use to allocate native or pooled memory buffers.
        /// </param>
        public StorageProviderContext(string streamName, string streamVersion, IMemoryAllocator memoryAllocator)
        {
            StreamName = streamName;
            StreamVersion = streamVersion;
            MemoryAllocator = memoryAllocator;
        }

        /// <summary>
        /// Gets the logical name of the stream that owns the storage provider.
        /// </summary>
        /// <remarks>
        /// Storage providers use this value to create or resolve stream-specific directories and
        /// file paths within their backing storage system.
        /// </remarks>
        public string StreamName { get; }

        /// <summary>
        /// Gets the version string that identifies the specific version of the stream.
        /// </summary>
        /// <remarks>
        /// This value is typically a plan hash or a semantic version string derived from
        /// <see cref="StreamVersionInformation"/>. Storage providers use it to scope stored data to a
        /// particular stream version, enabling multiple incompatible plan versions to coexist in the
        /// same backend without interfering with each other.
        /// </remarks>
        public string StreamVersion { get; }

        /// <summary>
        /// Gets the memory allocator the storage provider should use for allocating native or pooled memory buffers.
        /// </summary>
        /// <remarks>
        /// Providers that perform direct I/O or manage native memory receive this allocator at
        /// initialization so that all memory operations remain consistent with the broader stream
        /// memory-management strategy.
        /// </remarks>
        public IMemoryAllocator MemoryAllocator { get; }
    }
}

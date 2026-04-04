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
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.Persistence.Reservoir.TemporaryDisk;

namespace FlowtideDotNet.DependencyInjection
{
    public static class FlowtideStorageExtensions
    {
        /// <summary>
        /// Add temporary development storage, use ZLib compression as default
        /// </summary>
        /// <param name="storageBuilder"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public static IFlowtideStorageBuilder AddTemporaryDevelopmentStorage(this IFlowtideStorageBuilder storageBuilder, Action<FileCacheOptions>? options = null)
        {
            FileCacheOptions fileCacheOptions = new FileCacheOptions();
            options?.Invoke(fileCacheOptions);
            storageBuilder.SetPersistentStorage(new FileCachePersistentStorage(fileCacheOptions));
            storageBuilder.ZstdPageCompression();
            return storageBuilder;
        }

        public static IFlowtideStorageBuilder ZstdPageCompression(this IFlowtideStorageBuilder storageBuilder, int compressionLevel = 3)
        {
            return storageBuilder.SetCompression(new StateSerializeOptions()
            {
                CompressionMethod = CompressionMethod.Page,
                CompressionType = CompressionType.Zstd,
                CompressionLevel = compressionLevel
            });
        }

        public static IFlowtideStorageBuilder NoCompression(this IFlowtideStorageBuilder storageBuilder)
        {
            return storageBuilder.SetCompression(new FlowtideDotNet.Storage.StateSerializeOptions());
        }

        public static IReservoirBuilder AddFileStorage(this IFlowtideStorageBuilder storageBuilder, string directory)
        {
            ReservoirBuilder reservoirBuilder = new ReservoirBuilder();
            reservoirBuilder.SetStorage(new LocalDiskProvider(directory));
            storageBuilder.SetPersistentStorage((provider) =>
            {
                return new ReservoirPersistentStorage(reservoirBuilder);
            });

            storageBuilder.ZstdPageCompression();
            return reservoirBuilder;
        }

        /// <summary>
        /// Add temporary storage that uses the local disk for development and testing purposes. 
        /// It will store the data in the system's temporary directory and automatically clean up all files when the application closes. 
        /// This is not recommended for production use.
        /// </summary>
        /// <remarks>
        /// The temporary storage is designed for ease of use during development and testing, providing a simple way to run a state without persisting the data.
        /// </remarks>
        /// <param name="storageBuilder">
        /// </param>
        /// <param name="directory">
        /// Optional directory name for the temporary storage. If not provided, it will default to a "flowtide-state" folder within the system's temporary directory.
        /// </param>
        /// <returns></returns>
        public static IReservoirBuilder AddTemporaryStorage(this IFlowtideStorageBuilder storageBuilder, string? directory = default)
        {
            ReservoirBuilder reservoirBuilder = new ReservoirBuilder();

            if (directory == null)
            {
                directory = Path.Combine(Path.GetTempPath(), "flowtide-state");
            }

            reservoirBuilder.SetStorage(new TemporaryDiskProvider(directory));
            storageBuilder.SetPersistentStorage((provider) =>
            {
                return new ReservoirPersistentStorage(reservoirBuilder);
            });

            storageBuilder.ZstdPageCompression();
            return reservoirBuilder;
        }
    }
}

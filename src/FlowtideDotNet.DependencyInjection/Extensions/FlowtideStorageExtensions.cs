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

using FASTER.core;
using FASTER.devices;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Persistence.FasterStorage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.IO.Compression;

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
            storageBuilder.ZLibCompression();
            return storageBuilder;
        }

        /// <summary>
        /// Use FasterKV local file system storage, uses ZLib compression as default
        /// </summary>
        /// <param name="storageBuilder"></param>
        /// <param name="baseDir"></param>
        /// <returns></returns>
        public static IFlowtideStorageBuilder AddFasterKVFileSystemStorage(this IFlowtideStorageBuilder storageBuilder, string baseDir)
        {
            storageBuilder.SetPersistentStorage(new FasterKvPersistentStorage(new FASTER.core.FasterKVSettings<long, FASTER.core.SpanByte>(baseDir)
            {
                MemorySize = 1024 * 1024 * 32,
                PageSize = 1024 * 1024 * 16
            }));
            storageBuilder.ZLibCompression();
            return storageBuilder;
        }
        
        /// <summary>
        /// Use FasterKV Azure storage, uses ZLib compression as default
        /// </summary>
        /// <param name="storageBuilder"></param>
        /// <param name="azureStorageString">The connection string to the azure storage</param>
        /// <param name="containerName">Which container it should write the state to</param>
        /// <param name="directoryName">Directory name where the data should be stored.</param>
        /// <returns></returns>
        public static IFlowtideStorageBuilder AddFasterKVAzureStorage(
            this IFlowtideStorageBuilder storageBuilder, 
            string azureStorageString, 
            string containerName,
            string directoryName)
        {
            storageBuilder.SetPersistentStorage((provider) =>
            {
                var azureStorageLogger = provider.GetRequiredService<ILogger<AzureStorageDevice>>();
                var log = new AzureStorageDevice(azureStorageString, containerName, directoryName, "hlog.log", logger: azureStorageLogger);

                var checkpointManagerLogger = provider.GetRequiredService<ILogger<DeviceLogCommitCheckpointManager>>();
                // Create azure storage backed checkpoint manager
                var checkpointManager = new DeviceLogCommitCheckpointManager(
                                new AzureStorageNamedDeviceFactory(azureStorageString),
                                new DefaultCheckpointNamingScheme($"{containerName}/{directoryName}/checkpoints/"), logger: checkpointManagerLogger);

                var fasterKvLogger = provider.GetRequiredService<ILogger<FasterKvPersistentStorage>>();
                return new FasterKvPersistentStorage(
                    new FasterKVSettings<long, SpanByte>(null, logger: fasterKvLogger)
                    {
                        MemorySize = 1024 * 1024 * 32,
                        PageSize = 1024 * 1024 * 16,
                        CheckpointManager = checkpointManager,
                        LogDevice = log
                    }
                );
            });
            storageBuilder.ZLibCompression();

            return storageBuilder;
        }

        public static IFlowtideStorageBuilder ZLibCompression(this IFlowtideStorageBuilder storageBuilder)
        {
            return storageBuilder.SetCompressionFunction(new StateSerializeOptions()
            {
                CompressFunc = (stream) =>
                {
                    return new ZLibStream(stream, CompressionMode.Compress);
                },
                DecompressFunc = (stream) =>
                {
                    return new ZLibStream(stream, CompressionMode.Decompress);
                }
            });
        }

        public static IFlowtideStorageBuilder NoCompression(this IFlowtideStorageBuilder storageBuilder)
        {
            return storageBuilder.SetCompressionFunction(new FlowtideDotNet.Storage.StateSerializeOptions());
        }
    }
}

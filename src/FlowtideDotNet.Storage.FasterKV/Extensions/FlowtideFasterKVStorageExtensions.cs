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
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Storage.FasterKV;
using FlowtideDotNet.Storage.Persistence;
using Microsoft.Extensions.Logging;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class FlowtideFasterKVStorageExtensions
    {
        /// <summary>
        /// Use FasterKV local file system storage, uses ZLib compression as default
        /// </summary>
        /// <param name="storageBuilder"></param>
        /// <param name="baseDir"></param>
        /// <returns></returns>
        public static IFlowtideStorageBuilder AddFasterKVFileSystemStorage(this IFlowtideStorageBuilder storageBuilder, string baseDir)
        {
            return storageBuilder.AddFasterKVFileSystemStorage(_ => baseDir);
        }

        /// <summary>
        /// Use FasterKV local file system storage, uses ZLib compression as default
        /// Allows dynamic base directory naming based on stream metadata such as name and version
        /// </summary>
        /// <param name="storageBuilder"></param>
        /// <param name="baseDirFunc"></param>
        /// <returns></returns>
        public static IFlowtideStorageBuilder AddFasterKVFileSystemStorage(this IFlowtideStorageBuilder storageBuilder, Func<StorageInitializationMetadata, string> baseDirFunc)
        {
            storageBuilder.SetPersistentStorage(new FasterKvPersistentStorage(meta => new FASTER.core.FasterKVSettings<long, FASTER.core.SpanByte>(baseDirFunc(meta))
            {
                MemorySize = 1024 * 1024 * 64,
                PageSize = 1024 * 1024 * 32
            }));
            storageBuilder.ZstdPageCompression();
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
            return storageBuilder.AddFasterKVAzureStorage(azureStorageString, containerName, _ => directoryName);
        }

        /// <summary>
        /// Use FasterKV Azure storage, uses ZLib compression as default
        /// Allows dynamic directory naming based on stream metadata such as name and version
        /// </summary>
        /// <param name="storageBuilder"></param>
        /// <param name="azureStorageString"></param>
        /// <param name="containerName"></param>
        /// <param name="directoryNameFunc"></param>
        /// <returns></returns>
        public static IFlowtideStorageBuilder AddFasterKVAzureStorage(
            this IFlowtideStorageBuilder storageBuilder,
            string azureStorageString,
            string containerName,
            Func<StorageInitializationMetadata, string> directoryNameFunc)
        {
            storageBuilder.SetPersistentStorage((provider) =>
            {
                var azureStorageLogger = provider.GetRequiredService<ILogger<AzureStorageDevice>>();

                var checkpointManagerLogger = provider.GetRequiredService<ILogger<DeviceLogCommitCheckpointManager>>();

                var fasterKvLogger = provider.GetRequiredService<ILogger<FasterKvPersistentStorage>>();
                return new FasterKvPersistentStorage(meta =>
                {
                    var directory = directoryNameFunc(meta);
                    var log = new AzureStorageDevice(azureStorageString, containerName, directory, "hlog.log", logger: azureStorageLogger);
                    var checkpointManager = new DeviceLogCommitCheckpointManager(
                        new AzureStorageNamedDeviceFactory(azureStorageString),
                        new DefaultCheckpointNamingScheme($"{containerName}/{directory}/checkpoints/"), logger: checkpointManagerLogger);
                    return new FasterKVSettings<long, SpanByte>(null, logger: fasterKvLogger)
                    {
                        MemorySize = 1024 * 1024 * 64,
                        PageSize = 1024 * 1024 * 32,
                        CheckpointManager = checkpointManager,
                        LogDevice = log
                    };
                });
            });
            storageBuilder.ZstdPageCompression();

            return storageBuilder;
        }
    }
}

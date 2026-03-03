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

using Azure;
using Azure.Core;
using Azure.Storage;
using FlowtideDotNet.Storage.AzureBlobs;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.DependencyInjection
{
    public static class FlowtideAzureBlobStorageExtensions
    {

        /// <summary>
        /// Adds Azure Blob Storage as a storage provider using the provided connection
        /// string, container name, and optional directory path.
        /// </summary>
        /// <param name="storageBuilder">The storage builder to which Azure Blob Storage will be added.</param>
        /// <param name="connectionString">The connection string used to authenticate and connect to the Azure Blob Storage account. Cannot be null or
        /// empty.</param>
        /// <param name="containerName">The name of the Azure Blob Storage container to use. Cannot be null or empty.</param>
        /// <param name="directoryPath">An optional directory path within the container where data will be stored. If not specified, the root of the
        /// container is used.</param>
        /// <param name="localCacheDirectory">An optional local directory path for caching data. If not specified, a default temporary directory will be used for caching.</param>
        /// <returns>The same storage builder instance with Azure Blob Storage configured as a storage provider.</returns>
        public static IFlowtideStorageBuilder AddAzureBlobStorage(
            this IFlowtideStorageBuilder storageBuilder,
            string connectionString,
            string containerName,
            string? directoryPath = default,
            string? localCacheDirectory = default)
        {
            return storageBuilder.AddAzureBlobStorage(opt =>
            {
                opt.ContainerName = containerName;
                opt.ConnectionString = connectionString;
                opt.DirectoryPath = directoryPath;
                opt.LocalCacheDirectory = localCacheDirectory;
            });
        }

        /// <summary>
        /// Adds Azure Blob Storage as a storage provider using the provided service
        /// URI, container name, and SAS credential.
        /// </summary>
        /// <param name="storageBuilder">The storage builder to which Azure Blob Storage will be added.</param>
        /// <param name="storageUri">The URI of the Azure Blob Storage service endpoint.</param>
        /// <param name="containerName">The name of the Azure Blob Storage container to use.</param>
        /// <param name="sasCredential">The SAS credential used to authenticate requests to the Azure Blob Storage service.</param>
        /// <param name="directoryPath">An optional directory path within the container to scope storage operations. If not specified, the root of
        /// the container is used.</param>
        /// <param name="localCacheDirectory">An optional local directory path for caching data. If not specified, a default temporary directory will be used for caching.</param>
        /// <returns>The same storage builder instance, configured to use Azure Blob Storage.</returns>
        public static IFlowtideStorageBuilder AddAzureBlobStorage(
            this IFlowtideStorageBuilder storageBuilder,
            Uri storageUri,
            string containerName,
            AzureSasCredential sasCredential,
            string? directoryPath = default,
            string? localCacheDirectory = default)
        {
            return storageBuilder.AddAzureBlobStorage(opt =>
            {
                opt.ContainerName = containerName;
                opt.ServiceUri = storageUri;
                opt.SasCredential = sasCredential;
                opt.DirectoryPath = directoryPath;
                opt.LocalCacheDirectory = localCacheDirectory;
            });
        }

        /// <summary>
        /// Adds Azure Blob Storage as a storage provider to the builder using the specified service URI, container
        /// name, and token credential.
        /// </summary>
        /// <param name="storageBuilder">The storage builder to which Azure Blob Storage will be added.</param>
        /// <param name="storageUri">The URI of the Azure Blob Storage service endpoint.</param>
        /// <param name="containerName">The name of the Azure Blob Storage container to use.</param>
        /// <param name="tokenCredential">The token credential used to authenticate requests to Azure Blob Storage.</param>
        /// <param name="directoryPath">An optional directory path within the container to scope storage operations. If not specified, the root of
        /// the container is used.</param>
        /// <param name="localCacheDirectory">An optional local directory path for caching data. If not specified, a default temporary directory will be used for caching.</param>
        /// <returns>The same storage builder instance, configured to use Azure Blob Storage for persistent storage.</returns>
        public static IFlowtideStorageBuilder AddAzureBlobStorage(
            this IFlowtideStorageBuilder storageBuilder,
            Uri storageUri,
            string containerName,
            TokenCredential tokenCredential,
            string? directoryPath = default,
            string? localCacheDirectory = default)
        {
            return storageBuilder.AddAzureBlobStorage(opt =>
            {
                opt.ContainerName = containerName;
                opt.ServiceUri = storageUri;
                opt.TokenCredential = tokenCredential;
                opt.DirectoryPath = directoryPath;
                opt.LocalCacheDirectory = localCacheDirectory;
            });
        }

        /// <summary>
        /// Adds Azure Blob Storage as a storage provider  using the provided service
        /// URI, container name, and shared key credentials.
        /// </summary>
        /// <param name="storageBuilder">The storage builder to which Azure Blob Storage will be added.</param>
        /// <param name="storageUri">The URI of the Azure Blob Storage service endpoint.</param>
        /// <param name="containerName">The name of the Azure Blob Storage container to use for storage operations.</param>
        /// <param name="sharedKeyCredential">The shared key credentials used to authenticate requests to Azure Blob Storage.</param>
        /// <param name="directoryPath">An optional directory path within the container to scope storage operations. If not specified, the root of
        /// the container is used.</param>
        /// <param name="localCacheDirectory">An optional local directory path for caching data. If not specified, a default temporary directory will be used for caching.</param>
        /// <returns>The same storage builder instance with Azure Blob Storage configured as a storage provider.</returns>
        public static IFlowtideStorageBuilder AddAzureBlobStorage(
            this IFlowtideStorageBuilder storageBuilder,
            Uri storageUri,
            string containerName,
            StorageSharedKeyCredential sharedKeyCredential,
            string? directoryPath = default,
            string? localCacheDirectory = default)
        {
            return storageBuilder.AddAzureBlobStorage(opt =>
            {
                opt.ContainerName = containerName;
                opt.ServiceUri = storageUri;
                opt.SharedKeyCredential = sharedKeyCredential;
                opt.DirectoryPath = directoryPath;
                opt.LocalCacheDirectory = localCacheDirectory;
            });
        }

        /// <summary>
        /// Configures Azure Blob Storage as the persistent storage provider for Flowtide.
        /// </summary>
        /// <remarks>Use this method to enable Flowtide to store data in Azure Blob Storage, with optional
        /// local disk caching and customizable storage options.</remarks>
        /// <param name="storageBuilder">The storage builder to configure with Azure Blob Storage support.</param>
        /// <param name="options">A delegate to configure the options for Azure Blob Storage integration. Cannot be null.</param>
        /// <returns>The same storage builder instance, configured to use Azure Blob Storage for persistent storage.</returns>
        public static IFlowtideStorageBuilder AddAzureBlobStorage(
            this IFlowtideStorageBuilder storageBuilder,
            Action<FlowtideAzureBlobOptions> options)
        {
            FlowtideAzureBlobOptions blobOptions = new FlowtideAzureBlobOptions();
            options?.Invoke(blobOptions);
            storageBuilder.SetPersistentStorage((provider) =>
            {
                return new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = new AzureFileProvider(blobOptions),
                    CacheProvider = new LocalDiskProvider(blobOptions.LocalCacheDirectory ?? Path.Combine(Path.GetTempPath(), "flowtide_cache"), default),
                    CompactionFileSizeRatioThreshold = blobOptions.CompactionFileSizeRatioThreshold,
                    MaxFileSize = blobOptions.MaxFileSize,
                    SnapshotCheckpointInterval = blobOptions.SnapshotCheckpointInterval,
                    MaxCacheSizeBytes = blobOptions.MaxCacheSizeBytes
                });
            });
            storageBuilder.ZstdPageCompression();
            return storageBuilder;
        }
    }
}

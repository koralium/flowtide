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
using Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.AzureBlobs
{
    public class FlowtideAzureBlobOptions
    {
        public string? ContainerName { get; set; }

        /// <summary>
        /// Connect using connection string
        /// </summary>
        public string? ConnectionString { get; set; }

        /// <summary>
        /// Azure blob service URL, for example https://myaccount.blob.core.windows.net
        /// </summary>
        public Uri? ServiceUri { get; set; }

        /// <summary>
        /// Gets or sets the credential used to authenticate requests to Azure services.
        /// </summary>
        public TokenCredential? TokenCredential { get; set; }

        /// <summary>
        /// Gets or sets the shared access signature (SAS) credential used to authenticate requests.
        /// </summary>
        public AzureSasCredential? SasCredential { get; set; }

        /// <summary>
        /// Gets or sets the shared key credential used to authenticate requests to the storage account.
        /// </summary>
        public StorageSharedKeyCredential? SharedKeyCredential { get; set; }


        /// <summary>
        /// Gets or sets the factory function used to create instances of the BlobContainerClient.
        /// </summary>
        /// <remarks>This function will take priority and allow custom client creation logic.</remarks>
        public Func<BlobContainerClient>? ClientFactory { get; set; }

        /// <summary>
        /// Optional sub-directory path within the container. 
        /// </summary>
        public string? DirectoryPath { get; set; }

        /// <summary>
        /// Gets or sets the path to the local directory used for caching data.
        /// </summary>
        /// <remarks>If not set, a temporary folder will be setup.</remarks>
        public string? LocalCacheDirectory { get; set; }

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
        /// The maximum size of the cache in bytes. 
        /// When the cache exceeds this size, the least recently used items will be evicted.
        /// 
        /// Default is 10 GB.
        /// </summary>
        public long MaxCacheSizeBytes { get; set; } = 10L * 1000 * 1000 * 1000;

        internal BlobContainerClient GetClient()
        {
            if (ClientFactory != null)
            {
                return ClientFactory();
            }

            if (ContainerName == null)
            {
                throw new InvalidOperationException("ContainerName must be used together with either ConnectionString, or ServiceUri with appropriate credentials. Please check your configuration.");
            }

            if (!string.IsNullOrEmpty(ConnectionString))
            {
                return new BlobContainerClient(ConnectionString, ContainerName);
            }

            if (ServiceUri == null)
            {
                throw new InvalidOperationException("ServiceUri must be provided if ConnectionString is not used.");
            }

            var uri = new Uri($"{ServiceUri.ToString()}/{ContainerName}");

            if (TokenCredential != null)
            {
                return new BlobContainerClient(uri, TokenCredential);
            }
            else if (SasCredential != null)
            {
                return new BlobContainerClient(uri, SasCredential);
            }
            else if (SharedKeyCredential != null)
            {
                return new BlobContainerClient(uri, SharedKeyCredential);
            }
            else
            {
                throw new InvalidOperationException("Invalid Azure Blob Storage configuration. Please provide either a connection string with container name, or service URI with appropriate credentials and container name.");
            }
        }
    }
}

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

using Azure.Storage.Blobs;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.AzureBlobs
{
    internal class AzureFileProvider : IReservoirStorageProvider
    {
        private const string checkpointsDirectory = "checkpoints/";
        private const string checkpointRegistryFile = checkpointsDirectory + "checkpoints.registry";
        private const string metadataFile = "streamsMetadata.json";
        private readonly BlobContainerClient _blobContainerClient;

        private readonly string? _optionsDirectoryPath;
        private string _dataDirectory;
        private string _checkpointDirectory;
        private string _checkpointRegistryFile;
        private string _metadataFile;
        private string? _streamVersion;


        public bool SupportsDataFileListing => true;

        public AzureFileProvider(FlowtideAzureBlobOptions options)
        {
            _blobContainerClient = options.GetClient();
            _optionsDirectoryPath = options.DirectoryPath;

            if (options.DirectoryPath != null)
            {
                _dataDirectory = options.DirectoryPath.TrimEnd('/') + "/";
                _checkpointDirectory = options.DirectoryPath.TrimEnd('/') + "/" + checkpointsDirectory;
                _checkpointRegistryFile = options.DirectoryPath.TrimEnd('/') + "/" + checkpointRegistryFile;
                _metadataFile = options.DirectoryPath.TrimEnd('/') + "/" + metadataFile;
            }
            else
            {
                _dataDirectory = string.Empty;
                _checkpointDirectory = checkpointsDirectory;
                _checkpointRegistryFile = checkpointRegistryFile;
                _metadataFile = metadataFile;
            }
        }

        public async Task DeleteCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            await _blobContainerClient.DeleteBlobAsync(GetCheckpointFileName(checkpointVersion), cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public async Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            await _blobContainerClient.DeleteBlobIfExistsAsync(GetDataFileName(fileId), cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("GetMemoryy is not supported, Azure Blobs should be used with local cache which supports this operation.");
        }

        public Task<IEnumerable<ulong>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("GetStoredDataFileIdsAsync is not supported, Azure Blobs should be used with local cache which supports this operation.");
        }

        public async Task<IEnumerable<ulong>> ListDataFilesAboveVersionAsync(ulong minVersion, CancellationToken cancellationToken = default)
        {
            var list = _blobContainerClient.GetBlobsAsync(new Azure.Storage.Blobs.Models.GetBlobsOptions()
            {
                Prefix = "dataFile_",
                StartFrom = GetDataFileName(minVersion)
            }, cancellationToken);
            List<ulong> result = new List<ulong>();
            await foreach(var file in list.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var name = file.Name;
                if (name.Length == 34 && name.StartsWith("dataFile_") && name.EndsWith(".data"))
                {
                    if (ulong.TryParse(name.AsSpan(9, 20), out var fileId))
                    {
                        if (fileId > minVersion)
                        {
                            result.Add(fileId);
                        }
                    }
                }
            }
            return result;
        }

        public ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default) where T : ICacheObject
        {
            throw new NotSupportedException();
        }

        public async Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            var blobClient = _blobContainerClient.GetBlobClient(GetCheckpointFileName(checkpointVersion));
            var result = await blobClient.DownloadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            return PipeReader.Create(result.Value.Content, new StreamPipeReaderOptions(leaveOpen: false));
        }

        public async Task<PipeReader?> ReadCheckpointRegistryFileAsync(CancellationToken cancellationToken = default)
        {
            var blobClient = _blobContainerClient.GetBlobClient(_checkpointRegistryFile);
            if (await blobClient.ExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                var result = await blobClient.DownloadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                return PipeReader.Create(result.Value.Content, new StreamPipeReaderOptions(leaveOpen: false));
            }
            return default;
        }

        public async Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
        {
            var blobClient = _blobContainerClient.GetBlobClient(GetDataFileName(fileId));
            var result = await blobClient.DownloadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            return PipeReader.Create(result.Value.Content, new StreamPipeReaderOptions(leaveOpen: false, pool: MemoryPool<byte>.Shared));
        }

        private string GetCheckpointFileName(CheckpointVersion checkpointVersion)
        {
            return $"{_checkpointDirectory}{checkpointVersion.Version.ToString("D20")}.checkpoint";
        }

        public async Task WriteCheckpointFileAsync(CheckpointVersion checkpointVersion, PipeReader data, CancellationToken cancellationToken = default)
        {
            await _blobContainerClient.UploadBlobAsync(GetCheckpointFileName(checkpointVersion), data.AsStream(), cancellationToken).ConfigureAwait(false);
        }

        public async Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default)
        {
            var client = _blobContainerClient.GetBlobClient(_checkpointRegistryFile);
            await client.UploadAsync(data.AsStream(), overwrite: true, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        private string GetDataFileName(ulong fileId)
        {
            return $"{_dataDirectory}dataFile_{fileId.ToString("D20")}.data";
        }

        public async Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundled, PipeReader data, CancellationToken cancellationToken = default)
        {
            await _blobContainerClient.UploadBlobAsync(GetDataFileName(fileId), data.AsStream(), cancellationToken).ConfigureAwait(false);
        }

        public async Task InitializeAsync(string streamVersion, CancellationToken cancellationToken = default)
        {
            _streamVersion = streamVersion;

            if (_optionsDirectoryPath != null)
            {
                _dataDirectory = _optionsDirectoryPath.Trim('/') + "/" + _streamVersion + "/";
                _checkpointDirectory = _dataDirectory + checkpointsDirectory;
                _checkpointRegistryFile = _dataDirectory  + checkpointRegistryFile;
            }
            else
            {
                _dataDirectory = _streamVersion + "/";
                _checkpointDirectory = _streamVersion + "/" + checkpointsDirectory;
                _checkpointRegistryFile = _streamVersion + "/" + checkpointRegistryFile;
            }

            if (!await _blobContainerClient.ExistsAsync(cancellationToken).ConfigureAwait(false))
            {
                await _blobContainerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task<PipeReader?> ReadStreamsMetadataFileAsync(CancellationToken cancellationToken = default)
        {
            var blobClient = _blobContainerClient.GetBlobClient(_metadataFile);
            if (await blobClient.ExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                var result = await blobClient.DownloadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                return PipeReader.Create(result.Value.Content, new StreamPipeReaderOptions(leaveOpen: false));
            }
            return default;
        }

        public async Task WriteStreamsMetadataFileAsync(PipeReader data, CancellationToken cancellationToken = default)
        {
            var client = _blobContainerClient.GetBlobClient(_metadataFile);
            await client.UploadAsync(data.AsStream(), overwrite: true, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }
}

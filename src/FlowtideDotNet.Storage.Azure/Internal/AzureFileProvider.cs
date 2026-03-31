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
using Microsoft.VisualBasic;
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


        public bool SupportsFileListing => true;

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

        public async Task DeleteCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
        {
            await _blobContainerClient.DeleteBlobIfExistsAsync(GetCheckpointFileName(checkpointVersion), cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public async Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            await _blobContainerClient.DeleteBlobIfExistsAsync(GetDataFileName(fileId), cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("GetMemory is not supported, Azure Blobs should be used with local cache which supports this operation.");
        }

        public Task<IEnumerable<ulong>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("GetStoredDataFileIdsAsync is not supported, Azure Blobs should be used with local cache which supports this operation.");
        }

        public async Task<IEnumerable<ulong>> ListDataFilesAboveVersionAsync(ulong minVersion, CancellationToken cancellationToken = default)
        {
            var list = _blobContainerClient.GetBlobsAsync(new Azure.Storage.Blobs.Models.GetBlobsOptions()
            {
                Prefix = $"{_dataDirectory}dataFile_"
            }, cancellationToken);
            List<ulong> result = new List<ulong>();
            await foreach(var file in list.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var name = Path.GetFileName(file.Name);
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

        public async Task<PipeReader> ReadCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
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

        private string GetCheckpointFileName(CheckpointId checkpointVersion)
        {
            return $"{_checkpointDirectory}{checkpointVersion.Version.ToString("D20")}.checkpoint";
        }

        public Task WriteCheckpointFileAsync(CheckpointId checkpointVersion, PipeReader data, CancellationToken cancellationToken = default)
        {
            return UploadBlob(GetCheckpointFileName(checkpointVersion), data, cancellationToken);
        }

        private async Task UploadBlob(string fileName, PipeReader data, CancellationToken cancellationToken)
        {
            Stream stream;
            if (data is IFileWithSequence fileWithSequence)
            {
                stream = new ReadOnlySequenceStream(fileWithSequence.WrittenData);
            }
            else
            {
                stream = data.AsStream();
            }
            try
            {
                var client = _blobContainerClient.GetBlobClient(fileName);
                await client.UploadAsync(stream, overwrite: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await stream.DisposeAsync();
            }
        }

        public async Task<IEnumerable<CheckpointId>> ListCheckpointFilesAsync(CancellationToken cancellationToken = default)
        {
            var list = _blobContainerClient.GetBlobsAsync(new Azure.Storage.Blobs.Models.GetBlobsOptions()
            {
                Prefix = _checkpointDirectory
            }, cancellationToken);
            string pattern = @"^(?<fileId>\d+)(?<isSnapshot>\.snapshot)?\.checkpoint$";
            List<CheckpointId> result = new List<CheckpointId>();
            await foreach (var file in list.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var name = file.Name;
                
                var fileName = Path.GetFileName(name);
                Match match = Regex.Match(fileName, pattern);
                if (match.Success)
                {
                    string fileIdString = match.Groups["fileId"].Value;
                    bool isSnapshot = match.Groups["isSnapshot"].Success;
                    if (ulong.TryParse(fileIdString, out var fileId))
                    {
                        result.Add(new CheckpointId(fileId, isSnapshot));
                    }
                }
            }
            return result;
        }

        public Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default)
        {
            return UploadBlob(_checkpointRegistryFile, data, cancellationToken);
        }

        private string GetDataFileName(ulong fileId)
        {
            return $"{_dataDirectory}dataFile_{fileId.ToString("D20")}.data";
        }

        public Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundled, PipeReader data, CancellationToken cancellationToken = default)
        {
            return UploadBlob(GetDataFileName(fileId), data, cancellationToken);
        }

        public async Task InitializeAsync(StorageProviderContext providerContext, CancellationToken cancellationToken = default)
        {
            _streamVersion = providerContext.StreamVersion;

            if (_optionsDirectoryPath != null)
            {
                _dataDirectory = $"{_optionsDirectoryPath.Trim('/')}/{providerContext.StreamName}/{_streamVersion}/";
                _checkpointDirectory = _dataDirectory + checkpointsDirectory;
                _checkpointRegistryFile = _dataDirectory  + checkpointRegistryFile;
            }
            else
            {
                _dataDirectory = $"{providerContext.StreamName}/{_streamVersion}/";
                _checkpointDirectory = _dataDirectory + checkpointsDirectory;
                _checkpointRegistryFile = _dataDirectory + checkpointRegistryFile;
            }

            if (!await _blobContainerClient.ExistsAsync(cancellationToken).ConfigureAwait(false))
            {
                await _blobContainerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }

        private string GetMetadataPath(string streamName)
        {
            if (_optionsDirectoryPath != null)
            {
                return $"{_optionsDirectoryPath.Trim('/')}/{streamName}/{metadataFile}";
            }
            else
            {
                return $"{streamName}/{metadataFile}";
            }
        }

        public async Task<PipeReader?> ReadStreamsMetadataFileAsync(string streamName, CancellationToken cancellationToken = default)
        {
            var blobClient = _blobContainerClient.GetBlobClient(GetMetadataPath(streamName));
            if (await blobClient.ExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                var result = await blobClient.DownloadStreamingAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                return PipeReader.Create(result.Value.Content, new StreamPipeReaderOptions(leaveOpen: false));
            }
            return default;
        }

        public async Task WriteStreamsMetadataFileAsync(string streamName, PipeReader data, CancellationToken cancellationToken = default)
        {
            Stream stream;
            if (data is IFileWithSequence fileWithSequence)
            {
                stream = new ReadOnlySequenceStream(fileWithSequence.WrittenData);
            }
            else
            {
                stream = data.AsStream();
            }
            var client = _blobContainerClient.GetBlobClient(GetMetadataPath(streamName));
            await client.UploadAsync(stream, overwrite: true, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        private string GetStreamVersionDirectory(string streamName, string streamVersion)
        {
            if (_optionsDirectoryPath != null)
            {
                return $"{_optionsDirectoryPath.Trim('/')}/{streamName}/{streamVersion}/";
            }
            else
            {
                return $"{streamName}/{streamVersion}/";
            }
        }

        public async Task DeleteStreamVersionAsync(string streamName, string streamVersion, CancellationToken cancellationToken = default)
        {
            var versionPath = GetStreamVersionDirectory(streamName, streamVersion);

            var blobs = _blobContainerClient.GetBlobsAsync(new Azure.Storage.Blobs.Models.GetBlobsOptions()
            {
                Prefix = versionPath,
            }, cancellationToken);

            await foreach (var blob in blobs.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await _blobContainerClient.DeleteBlobIfExistsAsync(blob.Name, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }
    }
}

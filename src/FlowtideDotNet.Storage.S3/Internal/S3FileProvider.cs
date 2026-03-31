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

using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
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
using static System.Net.WebRequestMethods;

namespace FlowtideDotNet.Storage.S3.Internal
{
    internal class S3FileProvider : IReservoirStorageProvider
    {
        private const string checkpointsDirectory = "checkpoints/";
        private const string checkpointRegistryFile = checkpointsDirectory + "checkpoints.registry";
        private const string metadataFile = "streamsMetadata.json";

        private readonly IAmazonS3 _s3Client;
        private readonly string _bucketName;

        private readonly string? _optionsDirectoryPath;
        private string _dataDirectory;
        private string _checkpointDirectory;
        private string _checkpointRegistryFile;
        private string _metadataFile;
        private string? _streamVersion;
        private TransferUtility _transferUtility;

        public bool SupportsFileListing => true;

        public S3FileProvider(FlowtideS3Options options)
        {
            if (options.BucketName == null)
            {
                throw new InvalidOperationException("BucketName must be provided in options for S3 Storage");
            }

            _s3Client = options.GetClient();
            _bucketName = options.BucketName;
            _optionsDirectoryPath = options.DirectoryPath;
            _transferUtility = new TransferUtility(_s3Client);

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
            await _s3Client.DeleteObjectAsync(_bucketName, GetCheckpointFileName(checkpointVersion), cancellationToken).ConfigureAwait(false);
        }

        public async Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            await _s3Client.DeleteObjectAsync(_bucketName, GetDataFileName(fileId), cancellationToken).ConfigureAwait(false);
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("GetMemory is not supported, S3 should be used with local cache which supports this operation.");
        }

        public Task<IEnumerable<ulong>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("GetStoredDataFileIdsAsync is not supported, S3 should be used with local cache which supports this operation.");
        }

        public async Task<IEnumerable<ulong>> ListDataFilesAboveVersionAsync(ulong minVersion, CancellationToken cancellationToken = default)
        {
            var result = new List<ulong>();
            var request = new ListObjectsV2Request
            {
                BucketName = _bucketName,
                Prefix = $"{_dataDirectory}dataFile_"
            };

            ListObjectsV2Response response;
            do
            {
                response = await _s3Client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

                if (response.S3Objects != null)
                {
                    foreach (var file in response.S3Objects)
                    {
                        var name = Path.GetFileName(file.Key);
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
                }
                
                request.ContinuationToken = response.NextContinuationToken;
            } while (response.IsTruncated.HasValue && response.IsTruncated.Value);

            return result;
        }

        public ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default) where T : ICacheObject
        {
            throw new NotSupportedException();
        }

        public async Task<PipeReader> ReadCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
        {
            var response = await _s3Client.GetObjectAsync(_bucketName, GetCheckpointFileName(checkpointVersion), cancellationToken).ConfigureAwait(false);
            return PipeReader.Create(response.ResponseStream, new StreamPipeReaderOptions(leaveOpen: false));
        }

        public async Task<PipeReader?> ReadCheckpointRegistryFileAsync(CancellationToken cancellationToken = default)
        {
            if (await ObjectExistsAsync(_checkpointRegistryFile, cancellationToken).ConfigureAwait(false))
            {
                var response = await _s3Client.GetObjectAsync(_bucketName, _checkpointRegistryFile, cancellationToken).ConfigureAwait(false);
                return PipeReader.Create(response.ResponseStream, new StreamPipeReaderOptions(leaveOpen: false));
            }
            return default;
        }

        public async Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
        {
            var response = await _s3Client.GetObjectAsync(_bucketName, GetDataFileName(fileId), cancellationToken).ConfigureAwait(false);
            return PipeReader.Create(response.ResponseStream, new StreamPipeReaderOptions(leaveOpen: false, pool: MemoryPool<byte>.Shared));
        }

        private string GetCheckpointFileName(CheckpointId checkpointVersion)
        {
            return $"{_checkpointDirectory}{checkpointVersion.Version.ToString("D20")}.checkpoint";
        }

        public async Task WriteCheckpointFileAsync(CheckpointId checkpointVersion, PipeReader data, CancellationToken cancellationToken = default)
        {
            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = GetCheckpointFileName(checkpointVersion),
                InputStream = data.AsStream()
            };

            await _transferUtility.UploadAsync(uploadRequest, cancellationToken).ConfigureAwait(false);
            data.Complete();
        }

        public async Task<IEnumerable<CheckpointId>> ListCheckpointFilesAsync(CancellationToken cancellationToken = default)
        {
            var result = new List<CheckpointId>();
            string pattern = @"^(?<fileId>\d+)(?<isSnapshot>\.snapshot)?\.checkpoint$";

            var request = new ListObjectsV2Request
            {
                BucketName = _bucketName,
                Prefix = _checkpointDirectory
            };

            ListObjectsV2Response response;
            do
            {
                response = await _s3Client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);

                if (response.S3Objects != null)
                {
                    foreach (var file in response.S3Objects)
                    {
                        var fileName = Path.GetFileName(file.Key);
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
                }
                
                request.ContinuationToken = response.NextContinuationToken;
            } while (response.IsTruncated.HasValue && response.IsTruncated.Value);

            return result;
        }

        public async Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default)
        {
            using var memoryStream = new MemoryStream();
            await data.AsStream().CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);

            memoryStream.Position = 0;

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = _checkpointRegistryFile,
                InputStream = memoryStream
            };
            await _s3Client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
            data.Complete();
        }

        private string GetDataFileName(ulong fileId)
        {
            return $"{_dataDirectory}dataFile_{fileId.ToString("D20")}.data";
        }

        public async Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundled, PipeReader data, CancellationToken cancellationToken = default)
        {
            var uploadRequest = new TransferUtilityUploadRequest
            {
                BucketName = _bucketName,
                Key = GetDataFileName(fileId),
                InputStream = data.AsStream()
            };

            await _transferUtility.UploadAsync(uploadRequest, cancellationToken).ConfigureAwait(false);
            data.Complete();
        }

        public async Task InitializeAsync(StorageProviderContext providerContext, CancellationToken cancellationToken = default)
        {
            _streamVersion = providerContext.StreamVersion;

            if (_optionsDirectoryPath != null)
            {
                _dataDirectory = $"{_optionsDirectoryPath.Trim('/')}/{providerContext.StreamName}/{_streamVersion}/";
                _checkpointDirectory = _dataDirectory + checkpointsDirectory;
                _checkpointRegistryFile = _dataDirectory + checkpointRegistryFile;
            }
            else
            {
                _dataDirectory = $"{providerContext.StreamName}/{_streamVersion}/";
                _checkpointDirectory = _dataDirectory + checkpointsDirectory;
                _checkpointRegistryFile = _dataDirectory + checkpointRegistryFile;
            }

            // Kontrollera om bucket existerar, skapa annars
            var bucketExists = await Amazon.S3.Util.AmazonS3Util.DoesS3BucketExistV2Async(_s3Client, _bucketName).ConfigureAwait(false);
            if (!bucketExists)
            {
                var putBucketRequest = new PutBucketRequest
                {
                    BucketName = _bucketName,
                    UseClientRegion = true
                };
                await _s3Client.PutBucketAsync(putBucketRequest, cancellationToken).ConfigureAwait(false);
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
            var key = GetMetadataPath(streamName);
            if (await ObjectExistsAsync(key, cancellationToken).ConfigureAwait(false))
            {
                var response = await _s3Client.GetObjectAsync(_bucketName, key, cancellationToken).ConfigureAwait(false);
                return PipeReader.Create(response.ResponseStream, new StreamPipeReaderOptions(leaveOpen: false));
            }
            return default;
        }

        public async Task WriteStreamsMetadataFileAsync(string streamName, PipeReader data, CancellationToken cancellationToken = default)
        {
            // Metadata file is small, so copying here will not add much overhead
            using var memoryStream = new MemoryStream();
            await data.AsStream().CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);

            memoryStream.Position = 0;

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = GetMetadataPath(streamName),
                InputStream = memoryStream
            };
            await _s3Client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
            data.Complete();
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

            var listRequest = new ListObjectsV2Request
            {
                BucketName = _bucketName,
                Prefix = versionPath
            };

            ListObjectsV2Response listResponse;
            do
            {
                listResponse = await _s3Client.ListObjectsV2Async(listRequest, cancellationToken).ConfigureAwait(false);

                if (listResponse.S3Objects.Count > 0)
                {
                    // Optimering: S3 kan radera upp till 1000 objekt per batch
                    var deleteRequest = new DeleteObjectsRequest
                    {
                        BucketName = _bucketName,
                        Objects = listResponse.S3Objects.Select(o => new KeyVersion { Key = o.Key }).ToList()
                    };

                    await _s3Client.DeleteObjectsAsync(deleteRequest, cancellationToken).ConfigureAwait(false);
                }

                listRequest.ContinuationToken = listResponse.NextContinuationToken;
            } while (listResponse.IsTruncated.HasValue && listResponse.IsTruncated.Value);
        }

        // --- S3 Hjälpmetod för Existens-check ---
        private async Task<bool> ObjectExistsAsync(string key, CancellationToken cancellationToken)
        {
            try
            {
                await _s3Client.GetObjectMetadataAsync(_bucketName, key, cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
        }
    }
}

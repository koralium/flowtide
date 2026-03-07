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

using FlowtideDotNet.Storage.Persistence.Reservoir.Internal.DiskReader;
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk
{
    public class LocalDiskProvider : IReservoirStorageProvider
    {
        private const string CheckpointRegistryFileName = "checkpoints.registry";
        private readonly string optionDataDirectory;
        private string _dataFileDirectory;
        private string _checkpointFileDirectory;
        private LocalDiskReadManager? localDiskReadManager;
        private string? _streamVersion;

        public bool SupportsDataFileListing => true;

        public LocalDiskProvider(string dataDirectory)
        {
            dataDirectory = dataDirectory.TrimEnd('/').TrimEnd('\\');
            optionDataDirectory = dataDirectory;

            SetDirectories("default", "default");
        }

        [MemberNotNull(nameof(_dataFileDirectory), nameof(_checkpointFileDirectory))]
        private void SetDirectories(string streamName, string streamVersion)
        {
            _dataFileDirectory = $"{optionDataDirectory}/{streamName}/{streamVersion}/";
            _checkpointFileDirectory = $"{optionDataDirectory}/{streamName}/{streamVersion}/checkpoints/";
        }

        public Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            Debug.Assert(_streamVersion != null, "Stream version must be initialized before reading checkpoint files.");
            if (_checkpointFileDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            string fileName = GetCheckpointFileName(checkpointVersion);
            var filePath = Path.Combine(_checkpointFileDirectory, fileName);

            return Task.FromResult(PipeReader.Create(File.OpenRead(filePath)));
        }

        private string GetCheckpointFileName(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            if (checkpointVersion.IsSnapshot)
            {
                return $"{checkpointVersion.Version.ToString("D20")}.snapshot.checkpoint";
            }
            else
            {
                return $"{checkpointVersion.Version.ToString("D20")}.checkpoint";
            }
        }

        public async Task WriteCheckpointFileAsync(CheckpointVersion checkpointVersion, PipeReader data, CancellationToken cancellationToken = default)
        {
            Debug.Assert(_streamVersion != null, "Stream version must be initialized before writing checkpoint files.");
            if (_checkpointFileDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            string fileName = GetCheckpointFileName(checkpointVersion);

            var filePath = Path.Combine(_checkpointFileDirectory, fileName);
            if (_checkpointFileDirectory != null && !Directory.Exists(_checkpointFileDirectory))
            {
                Directory.CreateDirectory(_checkpointFileDirectory);
            }

            var filewrite = File.OpenWrite(filePath);
            await data.CopyToAsync(filewrite);
            await filewrite.FlushAsync();
            await filewrite.DisposeAsync();
        }

        public Task DeleteCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            Debug.Assert(_streamVersion != null, "Stream version must be initialized before deleting checkpoint files.");
            if (_checkpointFileDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            string fileName = GetCheckpointFileName(checkpointVersion);
            var filePath = Path.Combine(_checkpointFileDirectory, fileName);
            File.Delete(filePath);
            return Task.CompletedTask;
        }

        public async Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundle, PipeReader data, CancellationToken cancellationToken = default)
        {
            Debug.Assert(localDiskReadManager != null, "LocalDiskReadManager must be initialized before writing data files.");
            var filePath = GetDataFileName(fileId);

            if (!Directory.Exists(_dataFileDirectory))
            {
                Directory.CreateDirectory(_dataFileDirectory);
            }

            await localDiskReadManager.Write(filePath, data);
        }

        private string GetDataFileName(ulong fileId)
        {
            return $"{_dataFileDirectory}dataFile_{fileId}.data";
        }

        public Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            Debug.Assert(localDiskReadManager != null);
            var filePath = GetDataFileName(fileId);
            localDiskReadManager.DropFile(filePath);
            File.Delete(filePath);
            return Task.CompletedTask;
        }

        public ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default) where T : ICacheObject
        {
            Debug.Assert(localDiskReadManager != null);
            var path = $"{_dataFileDirectory}dataFile_{fileId}.data";
            return localDiskReadManager.Read(path, offset, length, crc32, stateSerializer);
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
            Debug.Assert(localDiskReadManager != null);
            var path = GetDataFileName(fileId);
            return localDiskReadManager.Read(path, offset, length, crc32);
        }

        public Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
        {
            var path = GetDataFileName(fileId);
            return Task.FromResult(PipeReader.Create(File.OpenRead(path)));
        }

        public Task<PipeReader?> ReadCheckpointRegistryFileAsync(CancellationToken cancellationToken = default)
        {
            Debug.Assert(_streamVersion != null);
            if (_checkpointFileDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            var filePath = Path.Combine(_checkpointFileDirectory, CheckpointRegistryFileName);
            if (!File.Exists(filePath))
            {
                return Task.FromResult<PipeReader?>(null);
            }
            return Task.FromResult<PipeReader?>(PipeReader.Create(File.OpenRead(filePath)));
        }

        public async Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default)
        {
            if (_checkpointFileDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            var filePath = Path.Combine(_checkpointFileDirectory, CheckpointRegistryFileName);
            if (_checkpointFileDirectory != null && !Directory.Exists(_checkpointFileDirectory))
            {
                Directory.CreateDirectory(_checkpointFileDirectory);
            }

            var filewrite = File.OpenWrite(filePath);
            await data.CopyToAsync(filewrite);
            await filewrite.FlushAsync();
            await filewrite.DisposeAsync();
            data.Complete();
        }

        public Task<IEnumerable<ulong>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default)
        {
            List<ulong> storedFileIds = new List<ulong>();
            if (Directory.Exists(_dataFileDirectory))
            {
                string pattern = @"^dataFile_(?<fileId>.+)\.data$";
                foreach (var file in Directory.EnumerateFiles(_dataFileDirectory))
                {
                    var fileName = Path.GetFileName(file);
                    Match match = Regex.Match(fileName, pattern);
                    if (match.Success)
                    {
                        string fileIdString = match.Groups["fileId"].Value;
                        if (ulong.TryParse(fileIdString, out var fileId))
                        {
                            storedFileIds.Add(fileId);
                        }
                    }
                }
            }
            return Task.FromResult<IEnumerable<ulong>>(storedFileIds);
        }

        public Task<IEnumerable<ulong>> ListDataFilesAboveVersionAsync(ulong minVersion, CancellationToken cancellationToken = default)
        {
            if (!Directory.Exists(_dataFileDirectory))
            {
                return Task.FromResult(Enumerable.Empty<ulong>());
            }
            var files = Directory.EnumerateFiles(_dataFileDirectory);
            var result = new List<ulong>();
            string pattern = @"^dataFile_(?<fileId>.+)\.data$";
            foreach (var file in files)
            {
                var fileName = Path.GetFileName(file);
                Match match = Regex.Match(fileName, pattern);
                if (match.Success)
                {
                    string fileIdString = match.Groups["fileId"].Value;
                    if (ulong.TryParse(fileIdString, out var fileId))
                    {
                        if (fileId > minVersion)
                        {
                            result.Add(fileId);
                        }
                    }
                }
            }
            return Task.FromResult<IEnumerable<ulong>>(result);
        }

        public Task InitializeAsync(StorageProviderContext context, CancellationToken cancellationToken = default)
        {
            localDiskReadManager = new LocalDiskReadManager(context.MemoryAllocator);
            _streamVersion = context.StreamVersion;
            SetDirectories(context.StreamName, context.StreamVersion);
            return Task.CompletedTask;
        }

        public Task<PipeReader?> ReadStreamsMetadataFileAsync(string streamName, CancellationToken cancellationToken = default)
        {
            if (!Directory.Exists($"{optionDataDirectory}/{streamName}"))
            {
                return Task.FromResult<PipeReader?>(null);
            }
            var path = $"{optionDataDirectory}/{streamName}/streamVersions.json";

            if (!File.Exists(path))
            {
                return Task.FromResult<PipeReader?>(null);
            }
            return Task.FromResult<PipeReader?>(PipeReader.Create(File.OpenRead(path)));
        }

        public async Task WriteStreamsMetadataFileAsync(string streamName, PipeReader data, CancellationToken cancellationToken = default)
        {
            var dir = $"{optionDataDirectory}/{streamName}";
            var path = $"{dir}/streamVersions.json";
            if (!Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }
            var filewrite = File.Open(path, FileMode.Create, FileAccess.Write);
            await data.CopyToAsync(filewrite);
            await filewrite.FlushAsync();
            await filewrite.DisposeAsync();
            await data.CompleteAsync();
        }

        public Task DeleteStreamVersionAsync(string streamName, string streamVersion, CancellationToken cancellationToken = default)
        {
            var path = $"{optionDataDirectory}/{streamName}/{streamVersion}";
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
            return Task.CompletedTask;
        }
    }
}

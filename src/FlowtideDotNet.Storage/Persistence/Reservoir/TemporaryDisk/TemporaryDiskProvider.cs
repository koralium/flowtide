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

using FlowtideDotNet.Storage.StateManager.Internal;
using Microsoft.VisualBasic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Text.RegularExpressions;
using System.Threading;
using static System.Net.WebRequestMethods;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.TemporaryDisk
{
    internal class TemporaryDiskProvider : IReservoirStorageProvider
    {
        private const string CheckpointRegistryFileName = "checkpoints.registry";
        private readonly Dictionary<string, FileStream> _openFiles = new Dictionary<string, FileStream>();
        private readonly string optionDataDirectory;
        private string _dataFileDirectory;
        private string _checkpointFileDirectory;
        private string? _streamVersion;
        private SemaphoreSlim _semaphore;

        public TemporaryDiskProvider(string path)
        {
            optionDataDirectory = path;
            _semaphore = new SemaphoreSlim(1, 1);
            SetDirectories("default", "default");
        }

        [MemberNotNull(nameof(_dataFileDirectory), nameof(_checkpointFileDirectory))]
        private void SetDirectories(string streamName, string streamVersion)
        {
            _dataFileDirectory = $"{optionDataDirectory}/{streamName}/{streamVersion}/";
            _checkpointFileDirectory = $"{optionDataDirectory}/{streamName}/{streamVersion}/checkpoints/";
        }

        public bool SupportsDataFileListing => true;

        private string GetDataFileName(ulong fileId)
        {
            return $"{_dataFileDirectory}dataFile_{fileId}.data";
        }

        private string GetCheckpointFileName(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
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

        public async Task DeleteCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
        {
            string fileName = GetCheckpointFileName(checkpointVersion);
            var filePath = Path.Combine(_checkpointFileDirectory, fileName);
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(filePath, out var fileStream))
                {
                    fileStream.Dispose();
                    _openFiles.Remove(filePath);
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(GetDataFileName(fileId), out var fileStream))
                {
                    fileStream.Dispose();
                    _openFiles.Remove(GetDataFileName(fileId));
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
           await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(GetDataFileName(fileId), out var fileStream))
                {
                    fileStream.Seek(offset, SeekOrigin.Begin);
                    var buffer = new byte[length];
                    fileStream.ReadExactly(buffer, 0, length);
                    return buffer;
                }
                else
                {
                    throw new InvalidOperationException("Tried to read a data file that has not been written, in temporary storage this is not supported.");
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public Task<IEnumerable<ulong>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public Task InitializeAsync(StorageProviderContext providerContext, CancellationToken cancellationToken = default)
        {
            _streamVersion = providerContext.StreamVersion;
            SetDirectories(providerContext.StreamName, providerContext.StreamVersion);
            return Task.CompletedTask;
        }

        public async Task<IEnumerable<ulong>> ListDataFilesAboveVersionAsync(ulong minVersion, CancellationToken cancellationToken = default)
        {
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                return _openFiles.Keys
                    .Where(fileName => fileName.StartsWith(_dataFileDirectory))
                    .Select(fileName => ulong.Parse(fileName.Substring(fileName.LastIndexOf("dataFile_") + "dataFile_".Length, fileName.Length - fileName.LastIndexOf("dataFile_") - "dataFile_".Length - 5))) // Extract the fileId from the file name
                    .Where(fileId => fileId > minVersion);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default) where T : ICacheObject
        {
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(GetDataFileName(fileId), out var fileStream))
                {
                    fileStream.Seek(offset, SeekOrigin.Begin);
                    var buffer = new byte[length];
                    fileStream.ReadExactly(buffer, 0, length);
                    return stateSerializer.Deserialize(new System.Buffers.ReadOnlySequence<byte>(buffer), length);
                }
                else
                {
                    throw new InvalidOperationException("Tried to read a data file that has not been written, in temporary storage this is not supported.");
                }

            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task<PipeReader> ReadCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
        {
            if (_checkpointFileDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            string fileName = GetCheckpointFileName(checkpointVersion);
            var filePath = Path.Combine(_checkpointFileDirectory, fileName);
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(filePath, out var fileStream))
                {
                    // Copy the file content into a new pipe to ensure the lock
                    fileStream.Seek(0, SeekOrigin.Begin);
                    var pipe = new Pipe();
                    await fileStream.CopyToAsync(pipe.Writer, cancellationToken).ContinueWith(_ => pipe.Writer.Complete(), cancellationToken);
                    return pipe.Reader;
                }
            }
            finally
            {
                _semaphore.Release();
            }
            throw new InvalidOperationException("Tried to read a checkpoint file that has not been written, in temporary storage this is not supported.");
        }

        public async Task<IEnumerable<CheckpointId>> ListCheckpointFilesAsync(CancellationToken cancellationToken = default)
        {
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                var keys = _openFiles.Keys
                    .Where(fileName => fileName.StartsWith(_checkpointFileDirectory));

                var result = new List<CheckpointId>();
                string pattern = @"^(?<fileId>\d+)(?<isSnapshot>\.snapshot)?\.checkpoint$";
                foreach (var file in keys)
                {
                    var fileName = Path.GetFileName(file);
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
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task<PipeReader?> ReadCheckpointRegistryFileAsync(CancellationToken cancellationToken = default)
        {
            Debug.Assert(_streamVersion != null);
            if (_checkpointFileDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            var filePath = Path.Combine(_checkpointFileDirectory, CheckpointRegistryFileName);
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(filePath, out var fileStream))
                {
                    fileStream.Seek(0, SeekOrigin.Begin);
                    var pipe = new Pipe();
                    await fileStream.CopyToAsync(pipe.Writer, cancellationToken).ContinueWith(_ => pipe.Writer.Complete(), cancellationToken);
                    return pipe.Reader;
                }
                return default;
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
        {
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(GetDataFileName(fileId), out var fileStream))
                {
                    // Copy the file content into a new pipe to ensure the lock
                    fileStream.Seek(0, SeekOrigin.Begin);
                    var pipe = new Pipe();
                    await fileStream.CopyToAsync(pipe.Writer, cancellationToken).ContinueWith(_ => pipe.Writer.Complete(), cancellationToken);
                    return pipe.Reader;
                }
            }
            finally
            {
                _semaphore.Release();
            }

            throw new InvalidOperationException("Tried to read a data file that has not been written, in temporary storage this is not supported.");
        }

        public async Task WriteCheckpointFileAsync(CheckpointId checkpointVersion, PipeReader data, CancellationToken cancellationToken = default)
        {
            if (_checkpointFileDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            string fileName = GetCheckpointFileName(checkpointVersion);
            var filePath = Path.Combine(_checkpointFileDirectory, fileName);
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(filePath, out var fileStream))
                {
                    fileStream.Seek(0, SeekOrigin.Begin);
                    await data.CopyToAsync(fileStream, cancellationToken);
                    return;
                }
                fileStream = new FileStream(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.None, 512, FileOptions.DeleteOnClose | FileOptions.RandomAccess);
                _openFiles[filePath] = fileStream;
                await data.CopyToAsync(fileStream, cancellationToken);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default)
        {
            var filePath = Path.Combine(_checkpointFileDirectory, CheckpointRegistryFileName);
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(filePath, out var fileStream))
                {
                    fileStream.Seek(0, SeekOrigin.Begin);
                    await data.CopyToAsync(fileStream, cancellationToken);
                    return;
                }
                fileStream = new FileStream(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.None, 512, FileOptions.DeleteOnClose | FileOptions.RandomAccess);
                _openFiles[filePath] = fileStream;
                await data.CopyToAsync(fileStream, cancellationToken);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundled, PipeReader data, CancellationToken cancellationToken = default)
        {
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (!Directory.Exists(_dataFileDirectory))
                {
                    Directory.CreateDirectory(_dataFileDirectory);
                }

                if (!_openFiles.TryGetValue(GetDataFileName(fileId), out var fileStream))
                {
                    fileStream = new FileStream(GetDataFileName(fileId), FileMode.Create, FileAccess.ReadWrite, FileShare.None, 512, FileOptions.DeleteOnClose | FileOptions.RandomAccess);
                    _openFiles[GetDataFileName(fileId)] = fileStream;
                }
                fileStream.Seek(0, SeekOrigin.Begin);
                await data.CopyToAsync(fileStream, cancellationToken);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task<PipeReader?> ReadStreamsMetadataFileAsync(string streamName, CancellationToken cancellationToken = default)
        {
            var dir = $"{optionDataDirectory}/{streamName}";
            var path = $"{dir}/streamVersions.json";

            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(path, out var fileStream))
                {
                    fileStream.Seek(0, SeekOrigin.Begin);
                    var pipe = new Pipe();
                    _ = fileStream.CopyToAsync(pipe.Writer, cancellationToken).ContinueWith(_ => pipe.Writer.Complete(), cancellationToken);
                    return pipe.Reader;
                }
                return default;
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task WriteStreamsMetadataFileAsync(string streamName, PipeReader data, CancellationToken cancellationToken = default)
        {
            var dir = $"{optionDataDirectory}/{streamName}";
            var path = $"{dir}/streamVersions.json";
            if (!Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(path, out var fileStream))
                {
                    fileStream.Seek(0, SeekOrigin.Begin);
                    await data.CopyToAsync(fileStream, cancellationToken);
                    return;
                }
                fileStream = new FileStream(path, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, 512, FileOptions.DeleteOnClose | FileOptions.RandomAccess);
                _openFiles[path] = fileStream;
                await data.CopyToAsync(fileStream, cancellationToken);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task DeleteStreamVersionAsync(string streamName, string streamVersion, CancellationToken cancellationToken = default)
        {
            var dir = $"{optionDataDirectory}/{streamName}";
            var path = $"{dir}/streamVersions.json";
            await _semaphore.WaitAsync(cancellationToken);
            try
            {
                if (_openFiles.TryGetValue(path, out var fileStream))
                {
                    fileStream.Dispose();
                    _openFiles.Remove(path);
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }
}

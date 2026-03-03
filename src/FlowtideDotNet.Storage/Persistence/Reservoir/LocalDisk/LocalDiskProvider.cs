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

using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal.DiskReader;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk
{
    public class LocalDiskProvider : IReservoirStorageProvider
    {
        private const string CheckpointRegistryFileName = "checkpoints.registry";
        private readonly string dataDirectory;
        private readonly string? checkpointDirectory;
        private LocalDiskReadManager localDiskReadManager;

        public bool SupportsDataFileListing => true;

        public LocalDiskProvider(string dataDirectory, string? checkpointDirectory)
        {
            dataDirectory = dataDirectory.TrimEnd('/').TrimEnd('\\');
            this.dataDirectory = dataDirectory;
            this.checkpointDirectory = checkpointDirectory;
            localDiskReadManager = new LocalDiskReadManager();
        }

        public Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            if (checkpointDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            string fileName = GetCheckpointFileName(checkpointVersion);
            var filePath = Path.Combine(checkpointDirectory, fileName);

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
            if (checkpointDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            string fileName = GetCheckpointFileName(checkpointVersion);

            var filePath = Path.Combine(checkpointDirectory, fileName);
            if (checkpointDirectory != null && !Directory.Exists(checkpointDirectory))
            {
                Directory.CreateDirectory(checkpointDirectory);
            }

            var filewrite = File.OpenWrite(filePath);
            await data.CopyToAsync(filewrite);
            await filewrite.FlushAsync();
            await filewrite.DisposeAsync();
        }

        public Task DeleteCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            if (checkpointDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            string fileName = GetCheckpointFileName(checkpointVersion);
            var filePath = Path.Combine(checkpointDirectory, fileName);
            File.Delete(filePath);
            return Task.CompletedTask;
        }

        public async Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundle, PipeReader data, CancellationToken cancellationToken = default)
        {
            var filePath = GetDataFileName(fileId);

            if (!Directory.Exists(dataDirectory))
            {
                Directory.CreateDirectory(dataDirectory);
            }

            var filewrite = File.OpenWrite(filePath);
            await data.CopyToAsync(filewrite);
            await filewrite.FlushAsync();
            await filewrite.DisposeAsync();
        }

        private string GetDataFileName(ulong fileId)
        {
            return $"{dataDirectory}/dataFile_{fileId}.data";
        }

        public Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            var filePath = GetDataFileName(fileId);
            localDiskReadManager.DropFile(filePath);
            File.Delete(filePath);
            return Task.CompletedTask;
        }

        public ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default) where T : ICacheObject
        {
            var path = $"{dataDirectory}/dataFile_{fileId}.data";
            return localDiskReadManager.Read(path, offset, length, crc32, stateSerializer);
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
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
            if (checkpointDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            var filePath = Path.Combine(checkpointDirectory, CheckpointRegistryFileName);
            if (!File.Exists(filePath))
            {
                return Task.FromResult<PipeReader?>(null);
            }
            return Task.FromResult<PipeReader?>(PipeReader.Create(File.OpenRead(filePath)));
        }

        public async Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default)
        {
            if (checkpointDirectory == null)
            {
                throw new InvalidOperationException("Checkpoint directory is not configured.");
            }
            var filePath = Path.Combine(checkpointDirectory, CheckpointRegistryFileName);
            if (checkpointDirectory != null && !Directory.Exists(checkpointDirectory))
            {
                Directory.CreateDirectory(checkpointDirectory);
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
            if (Directory.Exists(dataDirectory))
            {
                string pattern = @"^dataFile_(?<fileId>.+)\.data$";
                foreach (var file in Directory.EnumerateFiles(dataDirectory))
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
            if (!Directory.Exists(dataDirectory))
            {
                return Task.FromResult(Enumerable.Empty<ulong>());
            }
            var files = Directory.EnumerateFiles(dataDirectory);
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

        public Task InitializeAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}

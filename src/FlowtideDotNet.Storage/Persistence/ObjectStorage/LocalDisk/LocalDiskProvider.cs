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
using FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.LocalDisk
{
    public class LocalDiskProvider : IFileStorageProvider
    {
        private readonly string dataDirectory;
        private readonly string checkpointDirectory;

        public LocalDiskProvider(string dataDirectory, string checkpointDirectory)
        {
            this.dataDirectory = dataDirectory;
            this.checkpointDirectory = checkpointDirectory;
        }

        public Task<IEnumerable<CheckpointVersion>> ListCheckpointVersionsAsync()
        {
            if (Directory.Exists(checkpointDirectory))
            {
                var checkpointVersions = Directory.EnumerateFiles(checkpointDirectory)
                    .Select(x => new CheckpointFileInfo(x))
                    .Where(x => x.IsCheckpoint)
                    .Select(x => new CheckpointVersion(x.Version, x.IsSnapshot))
                    .ToList();
                return Task.FromResult<IEnumerable<CheckpointVersion>>(checkpointVersions);
            }

            return Task.FromResult(Enumerable.Empty<CheckpointVersion>());
        }

        public Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion)
        {
            string fileName = GetCheckpointFileName(checkpointVersion);
            var filePath = Path.Combine(checkpointDirectory, fileName);

            return Task.FromResult(PipeReader.Create(File.OpenRead(filePath)));
        }

        private string GetCheckpointFileName(CheckpointVersion checkpointVersion)
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

        public async Task WriteCheckpointFileAsync(CheckpointVersion checkpointVersion, PipeReader data)
        {
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

        public Task DeleteCheckpointFileAsync(CheckpointVersion checkpointVersion)
        {
            string fileName = GetCheckpointFileName(checkpointVersion);
            var filePath = Path.Combine(checkpointDirectory, fileName);
            File.Delete(filePath);
            return Task.CompletedTask;
        }

        public async Task WriteDataFileAsync(long fileId, PipeReader data)
        {
            var fileName = GetDataFileName(fileId);
            var filePath = Path.Combine(dataDirectory, fileName);

            if (!Directory.Exists(dataDirectory))
            {
                Directory.CreateDirectory(dataDirectory);
            }

            var filewrite = File.OpenWrite(filePath);
            await data.CopyToAsync(filewrite);
            await filewrite.FlushAsync();
            await filewrite.DisposeAsync();
        }

        private string GetDataFileName(long fileId)
        {
            return $"dataFile_{fileId}.data";
        }

        public Task DeleteDataFileAsync(long fileId)
        {
            var fileName = GetDataFileName(fileId);
            var filePath = Path.Combine(dataDirectory, fileName);
            File.Delete(filePath);
            return Task.CompletedTask;
        }

        public async ValueTask<T> ReadAsync<T>(long fileId, int offset, int length, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            try
            {
                var fileName = GetDataFileName(fileId);
                var path = Path.Combine(dataDirectory, fileName);
                using var stream = File.OpenRead(path);
                stream.Seek(offset, SeekOrigin.Begin);

                var buffer = new byte[length];
                await stream.ReadExactlyAsync(buffer, 0, length);

                return stateSerializer.Deserialize(new ReadOnlySequence<byte>(buffer), length);
            }
            catch(Exception e)
            {
                throw;
            }
            
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(long fileId, int offset, int length)
        {
            var fileName = GetDataFileName(fileId);
            var path = Path.Combine(dataDirectory, fileName);
            using var stream = System.IO.File.OpenRead(path);
            stream.Position = offset;
            var buffer = new byte[length];
            stream.ReadExactly(buffer, 0, length);
            return ValueTask.FromResult<ReadOnlyMemory<byte>>(buffer);
        }
    }
}

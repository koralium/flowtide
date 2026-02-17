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
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.MemoryDisk
{
    /// <summary>
    /// An implementation of IFileStorageProvider that uses in-memory storage for checkpoint and data files. 
    /// This is primarily intended for testing purposes, allowing for fast and efficient storage operations without the overhead of disk I/O. 
    /// The MemoryFileProvider can be used to simulate file storage behavior in unit tests or other scenarios where a lightweight, in-memory storage solution is sufficient.
    /// </summary>
    internal class MemoryFileProvider : IFileStorageProvider
    {
        private object _lock = new object();
        private Dictionary<long, byte[]> _dataFiles = new Dictionary<long, byte[]>();
        private Dictionary<CheckpointVersion, byte[]> _checkpointFiles = new Dictionary<CheckpointVersion, byte[]>();

        public Task DeleteDataFileAsync(long fileId)
        {
            lock (_lock)
            {
                _dataFiles.Remove(fileId);
                return Task.CompletedTask;
            }
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(long fileId, int offset, int length)
        {
            lock (_lock)
            {
                if (_dataFiles.TryGetValue(fileId, out var data))
                {
                    return new ValueTask<ReadOnlyMemory<byte>>(data.AsMemory(offset, length));
                }
            }

            throw new InvalidOperationException("File not found");
        }

        public Task<IEnumerable<CheckpointVersion>> ListCheckpointVersionsAsync()
        {
            lock (_lock) 
            {
                return Task.FromResult(_checkpointFiles.Keys.AsEnumerable());
            }
        }

        public ValueTask<T> ReadAsync<T>(long fileId, int offset, int length, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            lock (_lock)
            {
                if (_dataFiles.TryGetValue(fileId, out var data))
                {
                    var bytes = new ReadOnlySequence<byte>(data, offset, length);
                    return new ValueTask<T>(stateSerializer.Deserialize(bytes, length));
                }
            }
            throw new InvalidOperationException("File not found");
        }

        public Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion)
        {
            lock (_lock)
            {
                if (_checkpointFiles.TryGetValue(checkpointVersion, out var data))
                {
                    var pipe = new Pipe();
                    pipe.Writer.Write(data);
                    pipe.Writer.Complete();
                    return Task.FromResult(pipe.Reader);
                }
            }
            throw new InvalidOperationException("Checkpoint file not found");
        }

        public async Task WriteCheckpointFileAsync(CheckpointVersion checkpointVersion, PipeReader data)
        {
            using MemoryStream stream = new MemoryStream();
            await data.CopyToAsync(stream);
            var bytes = stream.ToArray();
            lock (_lock)
            {
                _checkpointFiles[checkpointVersion] = bytes;
            }
        }

        public async Task WriteDataFileAsync(long fileId, PipeReader data)
        {
            using MemoryStream stream = new MemoryStream();
            await data.CopyToAsync(stream);
            var bytes = stream.ToArray();

            lock (_lock)
            {
                _dataFiles[fileId] = bytes;
            }
        }
    }
}

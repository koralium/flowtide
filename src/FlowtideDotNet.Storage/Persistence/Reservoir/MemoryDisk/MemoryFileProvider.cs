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

using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk
{
    /// <summary>
    /// An implementation of IFileStorageProvider that uses in-memory storage for checkpoint and data files. 
    /// This is primarily intended for testing purposes, allowing for fast and efficient storage operations without the overhead of disk I/O. 
    /// The MemoryFileProvider can be used to simulate file storage behavior in unit tests or other scenarios where a lightweight, in-memory storage solution is sufficient.
    /// </summary>
    internal class MemoryFileProvider : IReservoirStorageProvider
    {
        private object _lock = new object();
        private  Dictionary<ulong, byte[]> _dataFiles = new Dictionary<ulong, byte[]>();
        private Dictionary<CheckpointVersion, byte[]> _checkpointFiles = new Dictionary<CheckpointVersion, byte[]>();
        private byte[]? _registryBytes;
        private byte[]? _metadataBytes;

        public virtual bool SupportsDataFileListing => true;

        internal bool TryGetFileData(ulong fileId, [NotNullWhen(true)] out byte[]? bytes)
        {
            return _dataFiles.TryGetValue(fileId, out bytes);
        }

        internal void SetFileData(ulong fileId, byte[] bytes)
        {
            _dataFiles[fileId] = bytes;
        }

        public Task DeleteCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                _checkpointFiles.Remove(checkpointVersion);
                return Task.CompletedTask;
            }
        }

        public Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                _dataFiles.Remove(fileId);
                return Task.CompletedTask;
            }
        }

        public bool DataFileExists(ulong fileId, CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                return _dataFiles.ContainsKey(fileId);
            }
        }

        public virtual ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                if (_dataFiles.TryGetValue(fileId, out var data))
                {
                    var memory = data.AsMemory(offset, length);
                    CrcUtils.CheckCrc32(memory.Span, crc32);
                    return new ValueTask<ReadOnlyMemory<byte>>(memory);
                }
            }

            throw new InvalidOperationException("File not found");
        }

        public virtual ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default) where T : ICacheObject
        {
            lock (_lock)
            {
                if (_dataFiles.TryGetValue(fileId, out var data))
                {
                    var bytes = new ReadOnlySequence<byte>(data, offset, length);
                    CrcUtils.CheckCrc32(bytes, crc32);
                    return new ValueTask<T>(stateSerializer.Deserialize(bytes, length));
                }
            }
            throw new InvalidOperationException("File not found");
        }

        public Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
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

        public Task<PipeReader?> ReadCheckpointRegistryFileAsync(CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                if (_registryBytes != null)
                {
                    var pipe = new Pipe();
                    pipe.Writer.Write(_registryBytes);
                    pipe.Writer.Complete();
                    return Task.FromResult<PipeReader?>(pipe.Reader);
                }
                return Task.FromResult<PipeReader?>(null);
            }
        }

        public virtual Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                if (_dataFiles.TryGetValue(fileId, out var data))
                {
                    var pipe = new Pipe();
                    pipe.Writer.Write(data);
                    pipe.Writer.Complete();
                    return Task.FromResult(pipe.Reader);
                }
            }
            throw new InvalidOperationException("Data file not found");
        }

        public async Task WriteCheckpointFileAsync(CheckpointVersion checkpointVersion, PipeReader data, CancellationToken cancellationToken = default)
        {
            using MemoryStream stream = new MemoryStream();
            await data.CopyToAsync(stream);
            var bytes = stream.ToArray();
            lock (_lock)
            {
                _checkpointFiles[checkpointVersion] = bytes;
            }
        }

        public Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                using MemoryStream stream = new MemoryStream();
                data.CopyToAsync(stream).GetAwaiter().GetResult();
                _registryBytes = stream.ToArray();
                return Task.CompletedTask;
            }
        }

        public virtual async Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundle, PipeReader data, CancellationToken cancellationToken = default)
        {
            using MemoryStream stream = new MemoryStream();
            await data.CopyToAsync(stream);
            var bytes = stream.ToArray();

            lock (_lock)
            {
                _dataFiles[fileId] = bytes;
            }
        }

        public Task<IEnumerable<ulong>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult<IEnumerable<ulong>>(_dataFiles.Keys.ToList());
        }

        public Task<IEnumerable<ulong>> ListDataFilesAboveVersionAsync(ulong minVersion, CancellationToken cancellationToken = default)
        {
            return Task.FromResult<IEnumerable<ulong>>(_dataFiles.Keys.Where(x => x > minVersion));
        }

        public Task InitializeAsync(string streamName, string streamVersion, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task<PipeReader?> ReadStreamsMetadataFileAsync(CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                if (_registryBytes != null)
                {
                    var pipe = new Pipe();
                    pipe.Writer.Write(_metadataBytes);
                    pipe.Writer.Complete();
                    return Task.FromResult<PipeReader?>(pipe.Reader);
                }
                return Task.FromResult<PipeReader?>(null);
            }
        }

        public Task WriteStreamsMetadataFileAsync(PipeReader data, CancellationToken cancellationToken = default)
        {
            lock (_lock)
            {
                using MemoryStream stream = new MemoryStream();
                data.CopyToAsync(stream).GetAwaiter().GetResult();
                _metadataBytes = stream.ToArray();
                return Task.CompletedTask;
            }
        }
    }
}

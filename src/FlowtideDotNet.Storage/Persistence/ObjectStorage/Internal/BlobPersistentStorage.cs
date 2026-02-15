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

using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    internal class BlobPersistentStorage : IPersistentStorage
    {
        private const int MaxFileSize = 10 * 1024 * 1024;
        private readonly MemoryPool<byte> memoryPool;
        private readonly IMemoryAllocator memoryAllocator;
        private MergedBlobFileWriter _mergedBlobFileWriter;
        private CheckpointHandler _checkpointHandler;
        private BlobPersistentSession _adminSession;

        public BlobPersistentStorage(MemoryPool<byte> memoryPool, IMemoryAllocator memoryAllocator)
        {
            this.memoryPool = memoryPool;
            this.memoryAllocator = memoryAllocator;
            _mergedBlobFileWriter = new MergedBlobFileWriter(memoryPool, memoryAllocator);
            _checkpointHandler = new CheckpointHandler(memoryPool, memoryAllocator);
            _adminSession = new BlobPersistentSession(this, memoryAllocator);
        }

        public long CurrentVersion => throw new NotImplementedException();

        public async Task AddNonCompletedBlobFile(BlobFileWriter blobFileWriter)
        {
            _mergedBlobFileWriter.AddBlobFile(blobFileWriter);

            if (_mergedBlobFileWriter.WrittenData.Length >= MaxFileSize)
            {
                // Finish the file writer, this adds the page ids and offsets
                _mergedBlobFileWriter.Finish();

                // Send the file to checkpoint handler
                await _checkpointHandler.EnqueueFileAsync(_mergedBlobFileWriter);
                _mergedBlobFileWriter = new MergedBlobFileWriter(memoryPool, memoryAllocator);
            }
        }

        public async ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            // If there is any data in the merged blob file writer, we need to finish it and send it to the checkpoint handler
            if (_mergedBlobFileWriter.PageIds.Count > 0)
            {
                _mergedBlobFileWriter.Finish();
                // Send the file to checkpoint handler
                await _checkpointHandler.EnqueueFileAsync(_mergedBlobFileWriter);
                _mergedBlobFileWriter = new MergedBlobFileWriter(memoryPool, memoryAllocator);
            }
            await _checkpointHandler.FinishCheckpoint();
        }

        public async ValueTask<ReadOnlySequence<byte>> ReadAsync(long key)
        {
            if (_checkpointHandler.TryGetPageFileLocation(key, out var location))
            {
                var stream = System.IO.File.OpenRead(GetBlobFileName(location.FileId));
                stream.Position = location.Offset;
                var buffer = new byte[location.Size];
                await stream.ReadExactlyAsync(buffer, 0, location.Size);
                return new ReadOnlySequence<byte>(buffer);
            }
            throw new NotImplementedException();
        }

        public void DeletePages(IReadOnlySet<long> keys)
        {
            _checkpointHandler.AddDeletedPages(keys);
        }

        private string GetBlobFileName(long fileId)
        {
            return $"blob_{fileId}.blob";
        }

        public ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount)
        {
            throw new NotImplementedException();
        }

        public IPersistentStorageSession CreateSession()
        {
            return new BlobPersistentSession(this, memoryAllocator);
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            _checkpointHandler = new CheckpointHandler(memoryPool, memoryAllocator);

            return Task.CompletedTask;
        }

        public ValueTask RecoverAsync(long checkpointVersion)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask ResetAsync()
        {
            _checkpointHandler = new CheckpointHandler(memoryPool, memoryAllocator);
            return ValueTask.CompletedTask;
        }
            
        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value)
        {
            if (_checkpointHandler.TryGetPageFileLocation(key, out var location))
            {
                var stream = System.IO.File.OpenRead(GetBlobFileName(location.FileId));
                stream.Position = location.Offset;
                var buffer = new byte[location.Size];
                stream.ReadExactly(buffer, 0, location.Size);
                value = buffer;
                return true;
            }
            value = null;
            return false;
        }

        public ValueTask Write(long key, byte[] value)
        {
            _adminSession.Write(key, new SerializableObject(value));
            return ValueTask.CompletedTask;
        }
    }
}

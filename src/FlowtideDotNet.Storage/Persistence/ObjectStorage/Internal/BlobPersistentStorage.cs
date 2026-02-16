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

using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    public class BlobPersistentStorage : IPersistentStorage
    {
        private const int MaxFileSize = 10 * 1024 * 1024;
        private readonly IFileStorageProvider fileProvider;
        private readonly MemoryPool<byte> memoryPool;
        private readonly IMemoryAllocator memoryAllocator;
        private MergedBlobFileWriter _mergedBlobFileWriter;
        private CheckpointHandler _checkpointHandler;
        private BlobPersistentSession _adminSession;
        private SemaphoreSlim _mergedBlobLock = new SemaphoreSlim(1);
        private List<BlobPersistentSession> _sessions = new List<BlobPersistentSession>();
        private object _sessionsLock = new object();

        public BlobPersistentStorage(IFileStorageProvider fileProvider, MemoryPool<byte> memoryPool, IMemoryAllocator memoryAllocator)
        {
            this.fileProvider = fileProvider;
            this.memoryPool = memoryPool;
            this.memoryAllocator = memoryAllocator;
            _mergedBlobFileWriter = new MergedBlobFileWriter(memoryPool, memoryAllocator);
            _checkpointHandler = new CheckpointHandler(fileProvider, memoryPool, memoryAllocator);
            _adminSession = new BlobPersistentSession(this, memoryAllocator);
        }

        public long CurrentVersion => _checkpointHandler.CheckpointVersion;

        internal async Task AddNonCompletedBlobFile(BlobFileWriter blobFileWriter)
        {
            await _mergedBlobLock.WaitAsync();
            _mergedBlobFileWriter.AddBlobFile(blobFileWriter);

            if (_mergedBlobFileWriter.WrittenData.Length >= MaxFileSize)
            {
                // Finish the file writer, this adds the page ids and offsets
                _mergedBlobFileWriter.Finish();

                // Send the file to checkpoint handler
                await _checkpointHandler.EnqueueFileAsync(_mergedBlobFileWriter);
                _mergedBlobFileWriter = new MergedBlobFileWriter(memoryPool, memoryAllocator);
            }
            _mergedBlobLock.Release();
        }

        internal async Task AddCompleteBlobFile(BlobFileWriter blobFileWriter)
        {
            await _checkpointHandler.EnqueueFileAsync(blobFileWriter);
        }

        public async ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            await _adminSession.Write(1, new SerializableObject(metadata));
            await _adminSession.Commit();
            // If there is any data in the merged blob file writer, we need to finish it and send it to the checkpoint handler
            await _mergedBlobLock.WaitAsync();
            if (_mergedBlobFileWriter.PageIds.Count > 0)
            {
                _mergedBlobFileWriter.Finish();
                // Send the file to checkpoint handler
                await _checkpointHandler.EnqueueFileAsync(_mergedBlobFileWriter);
                _mergedBlobFileWriter = new MergedBlobFileWriter(memoryPool, memoryAllocator);
            }
            _mergedBlobLock.Release();
            await _checkpointHandler.FinishCheckpoint();
        }

        internal async ValueTask<ReadOnlySequence<byte>> ReadAsync(long key)
        {
            if (_checkpointHandler.TryGetPageFileLocation(key, out var location))
            {
                var memory = fileProvider.GetMemory(GetBlobFileName(location.FileId), location.Offset, location.Size);
                return new ReadOnlySequence<byte>(memory);
            }
            throw new NotImplementedException();
        }

        internal ValueTask<T> ReadAsync<T>(long key, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            if (_checkpointHandler.TryGetPageFileLocation(key, out var location))
            {
                return fileProvider.Read<T>(GetBlobFileName(location.FileId), location.Offset, location.Size, stateSerializer);
            }
            throw new FlowtidePersistentStorageException($"Key {key} not found in persistent storage.");
        }

        internal void DeletePages(IReadOnlySet<long> keys)
        {
            _checkpointHandler.AddDeletedPages(keys);
        }

        private string GetBlobFileName(long fileId)
        {
            return $"blob_{fileId}.blob";
        }

        public async ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount)
        {
            await _checkpointHandler.Compact();
        }

        public IPersistentStorageSession CreateSession()
        {
            var session = new BlobPersistentSession(this, memoryAllocator);
            lock (_sessionsLock)
            {
                _sessions.Add(session);
            }
            return session;
        }

        public void Dispose()
        {
            // TODO: Implement
            //throw new NotImplementedException();
        }

        public async Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            _checkpointHandler = new CheckpointHandler(fileProvider, memoryPool, memoryAllocator);
            await _checkpointHandler.RecoverToLatest();
        }

        public async ValueTask RecoverAsync(long checkpointVersion)
        {
            await _checkpointHandler.RecoverTo(checkpointVersion);
        }

        public ValueTask ResetAsync()
        {
            _checkpointHandler = new CheckpointHandler(fileProvider, memoryPool, memoryAllocator);
            
            lock (_sessionsLock)
            {
                foreach(var session in _sessions)
                {
                    session.Reset();
                }
            }

            return ValueTask.CompletedTask;
        }
            
        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value)
        {
            if (_checkpointHandler.TryGetPageFileLocation(key, out var location))
            {
                value = fileProvider.GetMemory(GetBlobFileName(location.FileId), location.Offset, location.Size);
                return true;
            }
            value = null;
            return false;
        }

        public async ValueTask Write(long key, byte[] value)
        {
            await _adminSession.Write(key, new SerializableObject(value));
        }

        public void ClearForRestore()
        {
            lock (_sessionsLock)
            {
                foreach (var session in _sessions)
                {
                    session.Reset();
                }
            }
        }
    }
}

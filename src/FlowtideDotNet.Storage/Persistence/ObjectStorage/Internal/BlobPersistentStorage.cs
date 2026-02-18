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
using System.IO.Pipelines;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    public class BlobPersistentStorage : IPersistentStorage
    {
        private readonly int _maxFileSize;
        private readonly IFileStorageProvider _fileProvider;
        private readonly MemoryPool<byte> _memoryPool;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly BlobStorageOptions _blobStorageOptions;
        private MergedBlobFileWriter _mergedBlobFileWriter;
        private CheckpointHandler _checkpointHandler;
        private BlobPersistentSession _adminSession;
        private SemaphoreSlim _mergedBlobLock = new SemaphoreSlim(1);
        private List<BlobPersistentSession> _sessions = new List<BlobPersistentSession>();
        private object _sessionsLock = new object();

        public BlobPersistentStorage(BlobStorageOptions blobStorageOptions)
        {
            if (blobStorageOptions.FileProvider == null)
            {
                throw new ArgumentNullException(nameof(blobStorageOptions.FileProvider), "FileProvider must be provided in BlobStorageOptions.");
            }
            this._fileProvider = blobStorageOptions.FileProvider;
            this._memoryPool = blobStorageOptions.MemoryPool;
            this._memoryAllocator = blobStorageOptions.MemoryAllocator;
            this._maxFileSize = blobStorageOptions.MaxFileSize;
            _mergedBlobFileWriter = new MergedBlobFileWriter(_memoryPool, _memoryAllocator);
            _checkpointHandler = new CheckpointHandler(blobStorageOptions);
            _adminSession = new BlobPersistentSession(this, _memoryAllocator, _maxFileSize);
            this._blobStorageOptions = blobStorageOptions;
        }

        public long CurrentVersion => _checkpointHandler.CheckpointVersion;

        internal async Task AddNonCompletedBlobFile(BlobFileWriter blobFileWriter)
        {
            await _mergedBlobLock.WaitAsync();
            _mergedBlobFileWriter.AddBlobFile(blobFileWriter);

            if (_mergedBlobFileWriter.WrittenData.Length >= _maxFileSize)
            {
                // Finish the file writer, this adds the page ids and offsets
                _mergedBlobFileWriter.Finish();

                // Send the file to checkpoint handler
                await _checkpointHandler.EnqueueFileAsync(_mergedBlobFileWriter);
                _mergedBlobFileWriter = new MergedBlobFileWriter(_memoryPool, _memoryAllocator);
            }
            _mergedBlobLock.Release();
        }

        internal async Task AddCompleteBlobFile(BlobFileWriter blobFileWriter)
        {
            await _checkpointHandler.EnqueueFileAsync(blobFileWriter);
        }

        private async Task CompactFiles()
        {
            var files = _checkpointHandler.GetAllFileInformation().ToList();

            foreach(var file in files)
            {
                var actualSize = file.FileSize - file.DeletedSize;
                var sizeRatio = (double)actualSize / _maxFileSize;

                // If the actual size of the file is less than 33% of the max file size, we consider it for compaction.
                // This threshold can be tuned based on the workload and performance requirements.
                if (sizeRatio < _blobStorageOptions.CompactionFileSizeRatioThreshold)
                {
                    await CompactFile(file.FileId);
                }
            }
        }

        internal async Task CompactFile(long fileId)
        {
            var reader = await _fileProvider.ReadDataFileAsync(fileId);

            // Read all content to sequence
            ReadResult readResult;
            do
            {
                readResult = await reader.ReadAsync();
                reader.AdvanceTo(readResult.Buffer.Start, readResult.Buffer.End);
            } while (!readResult.IsCompleted);
            CopyDataFileContent(fileId, readResult);
            reader.Complete();
        }

        private void CopyDataFileContent(long fileId, ReadResult readResult)
        {
            BlobFileWriter blobFileWriter = new BlobFileWriter((file) => { }, _memoryPool, _memoryAllocator);
            var reader = new DataFileReader(readResult.Buffer);

            while(reader.TryGetNextPageInfo(out var pageInfo))
            {
                // Check that the current location of the page is in the same file, if not, it means the page has been updated and we should skip it
                if (_checkpointHandler.TryGetPageFileLocation(pageInfo.PageId, out var location) &&
                    location.FileId == fileId)
                {
                    _mergedBlobFileWriter.AddSequence(pageInfo.PageId, readResult.Buffer.Slice(pageInfo.Offset, pageInfo.Length));
                }
            }
        }

        public async ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            await CompactFiles();
            await _adminSession.Write(1, new SerializableObject(metadata));
            await _adminSession.Commit();
            // If there is any data in the merged blob file writer, we need to finish it and send it to the checkpoint handler
            await _mergedBlobLock.WaitAsync();
            if (_mergedBlobFileWriter.PageIds.Count > 0)
            {
                _mergedBlobFileWriter.Finish();
                // Send the file to checkpoint handler
                await _checkpointHandler.EnqueueFileAsync(_mergedBlobFileWriter);
                _mergedBlobFileWriter = new MergedBlobFileWriter(_memoryPool, _memoryAllocator);
            }
            _mergedBlobLock.Release();
            await _checkpointHandler.FinishCheckpoint();
        }

        internal async ValueTask<ReadOnlySequence<byte>> ReadAsync(long key)
        {
            if (_checkpointHandler.TryGetPageFileLocation(key, out var location))
            {
                var memory = await _fileProvider.GetMemoryAsync(location.FileId, location.Offset, location.Size);
                return new ReadOnlySequence<byte>(memory);
            }
            throw new FlowtidePersistentStorageException($"Key {key} not found in persistent storage.");
        }

        internal ValueTask<T> ReadAsync<T>(long key, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            if (_checkpointHandler.TryGetPageFileLocation(key, out var location))
            {
                return _fileProvider.ReadAsync<T>(location.FileId, location.Offset, location.Size, stateSerializer);
            }
            throw new FlowtidePersistentStorageException($"Key {key} not found in persistent storage.");
        }

        internal void DeletePages(IReadOnlySet<long> keys)
        {
            _checkpointHandler.AddDeletedPages(keys);
        }

        public async ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount)
        {
            await _checkpointHandler.Compact();
        }

        public IPersistentStorageSession CreateSession()
        {
            var session = new BlobPersistentSession(this, _memoryAllocator, _maxFileSize);
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
            _checkpointHandler = new CheckpointHandler(_blobStorageOptions);
            await _checkpointHandler.RecoverToLatest();
        }

        public async ValueTask RecoverAsync(long checkpointVersion)
        {
            await _checkpointHandler.RecoverTo(checkpointVersion);
        }

        public ValueTask ResetAsync()
        {
            _checkpointHandler = new CheckpointHandler(_blobStorageOptions);
            
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
                value = _fileProvider.GetMemoryAsync(location.FileId, location.Offset, location.Size).GetAwaiter().GetResult();
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

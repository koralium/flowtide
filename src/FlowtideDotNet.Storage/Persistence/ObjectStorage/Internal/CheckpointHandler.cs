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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using static System.Net.WebRequestMethods;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    internal class CheckpointHandler
    {
        private readonly int VersionBetweenSnapshot = 5;

        private readonly IFileStorageProvider _fileProvider;
        private readonly MemoryPool<byte> _memoryPool;
        private readonly IMemoryAllocator _memoryAllocator;
        private Channel<PagesFile> _channel;
        private BlobNewCheckpoint _newCheckpoint;
        private long _nextFileId = 0;
        private object _lock = new object();
        private object _checkpointFileLock = new object();
        private bool _writeSnapshotCheckpoint;
        private int _countSinceLastSnapshot;


        private object _taskLock = new object();
        private Task[]? _writeTasks;
        private CancellationTokenSource? _cancellationTokenSource;

        private ConcurrentDictionary<long, PageFileLocation> _pageFileLocations = new ConcurrentDictionary<long, PageFileLocation>();
        private ConcurrentDictionary<long, FileInformation> _fileInformations = new ConcurrentDictionary<long, FileInformation>();

        private HashSet<long> _deletedPages = new HashSet<long>();
        private object _deletedPagesLock = new object();

        private HashSet<long> _modifiedFileIds = new HashSet<long>();
        private HashSet<long> _deletedFileIds = new HashSet<long>();
        private object _modifiedFileIdsLock = new object();
        private List<DeletedFileInfo> deletedFilesList = new List<DeletedFileInfo>();
        private List<CheckpointVersion> _activeVersions = new List<CheckpointVersion>();

        private long _currentCheckpointVersion = 0;
        private long _checkpointVersion;
        private bool _modifiedSinceLastCheckpoint = false;

        public long CheckpointVersion => _checkpointVersion;


        public CheckpointHandler(BlobStorageOptions blobStorageOptions)
        {
            if (blobStorageOptions.FileProvider == null)
            {
                throw new ArgumentNullException(nameof(blobStorageOptions.FileProvider), "FileProvider must be provided in BlobStorageOptions.");
            }

            _channel = Channel.CreateBounded<PagesFile>(1000);
            this._fileProvider = blobStorageOptions.FileProvider;
            _memoryPool = blobStorageOptions.MemoryPool;
            _newCheckpoint = new BlobNewCheckpoint(_memoryPool, blobStorageOptions.MemoryAllocator);
            VersionBetweenSnapshot = blobStorageOptions.SnapshotCheckpointInterval;

            this._memoryAllocator = blobStorageOptions.MemoryAllocator;
            _currentCheckpointVersion = 0;
            _checkpointVersion = 1;
        }

        public async Task RecoverToLatest()
        {
            var checkpointVersions = (await _fileProvider.ListCheckpointVersionsAsync())
                .OrderBy(x => x.Version)
                .ToList();

            if (checkpointVersions.Count == 0)
            {
                return;
            }

            _activeVersions.Clear();
            _activeVersions.AddRange(checkpointVersions);

            await ReadCheckpointFiles(checkpointVersions);
            var lastVersion = checkpointVersions[checkpointVersions.Count - 1];

            _currentCheckpointVersion = lastVersion.Version;
            _checkpointVersion = lastVersion.Version + 1;
        }

        public async Task RecoverTo(long version)
        {
            if (_currentCheckpointVersion == version && !Volatile.Read(ref _modifiedSinceLastCheckpoint))
            {
                // If we are already at the checkpoint version and there is no modification since last checkpoint, we can skip recovery
                return;
            }

            // List all checkpoint files and order them by version
            var checkpointVersions = (await _fileProvider.ListCheckpointVersionsAsync())
                .Where(x => x.Version <= version)
                .OrderBy(x => x.Version)
                .ToList();

            _activeVersions.Clear();
            _activeVersions.AddRange(checkpointVersions);

            if (checkpointVersions.Count == 0)
            {
                throw new InvalidOperationException($"No checkpoint versions found for recovery.");
            }

            if (!checkpointVersions.Any(x => x.Version == version))
            {
                throw new InvalidOperationException($"Checkpoint file with version {version} not found for recovery.");
            }

            await ReadCheckpointFiles(checkpointVersions);
            _currentCheckpointVersion = version;
            _checkpointVersion = version + 1;
        }

        private async Task ReadCheckpointFiles(List<CheckpointVersion> checkpointFiles)
        {
            // Check if there are snapshots in the list
            int lastSnapshot = -1;
            for (int i = 0; i < checkpointFiles.Count; i++)
            {
                if (checkpointFiles[i].IsSnapshot)
                {
                    lastSnapshot = i;
                }
            }

            // If there is a snapshot we start reading from that one
            if (lastSnapshot > 0)
            {
                // Remove files before this index
                checkpointFiles.RemoveRange(0, lastSnapshot);
            }

            foreach (var checkpointFile in checkpointFiles)
            {
                if (checkpointFile.IsSnapshot)
                {
                    _countSinceLastSnapshot = 0;
                }
                else
                {
                    // Increase the count since last snapshot to correctly take snapshots after X incremental even after a crash
                    _countSinceLastSnapshot++;
                }
                var fileReader = await _fileProvider.ReadCheckpointFileAsync(checkpointFile);

                // Read all content of the file
                ReadResult readResult;
                do
                {
                    readResult = await fileReader.ReadAsync();
                    fileReader.AdvanceTo(readResult.Buffer.Start, readResult.Buffer.End);
                } while (!readResult.IsCompleted);
                ReadCheckpointFile(checkpointFile, readResult.Buffer);
                fileReader.Complete();
            }
        }

        private void ReadCheckpointFile(CheckpointVersion checkpointFileInfo, ReadOnlySequence<byte> buffer)
        {
            var reader = new CheckpointDataReader(buffer);
            
            while (reader.TryGetNextUpsertPageInfo(out var upsertPageInfo))
            {
                if (upsertPageInfo.size < 0)
                {
                    throw new InvalidOperationException("Page size cannot be less than 0");
                }
                _pageFileLocations[upsertPageInfo.pageId] = new PageFileLocation()
                {
                    FileId = upsertPageInfo.fileId,
                    Offset = upsertPageInfo.offset,
                    Size = upsertPageInfo.size
                };
            }

            while (reader.TryGetNextDeletedPageId(out var deletedFileInfo))
            {
                deletedFilesList.Add(deletedFileInfo);
                _pageFileLocations.TryRemove(deletedFileInfo.fileId, out _);
            }

            while (reader.TryGetFileInformation(out var fileInfo))
            {
                _fileInformations[fileInfo.FileId] = fileInfo;
            }

            while (reader.TryGetNextDeletedFileId(out var deletedFileId))
            {
                deletedFilesList.Add(new DeletedFileInfo()
                {
                    fileId = deletedFileId,
                    deletedAtVersion = checkpointFileInfo.Version
                });
                _fileInformations.TryRemove(deletedFileId, out _);
            }

            _nextFileId = reader.NextFileId;
        }

        public async Task EnqueueFileAsync(PagesFile fileWriter)
        {
            Volatile.Write(ref _modifiedSinceLastCheckpoint, true);
            if (_writeTasks == null)
            {
                lock (_taskLock)
                {
                    if (_writeTasks == null)
                    {
                        _cancellationTokenSource = new CancellationTokenSource();
                        _writeTasks = new Task[1];
                        _writeTasks[0] = Task.Run(WriteLoop);
                    }
                }
            }
            await _channel.Writer.WriteAsync(fileWriter);
        }

        public bool TryGetPageFileLocation(long pageId, out PageFileLocation pageFileLocation)
        {
            return _pageFileLocations.TryGetValue(pageId, out pageFileLocation);
        }

        public void AddDeletedPages(IReadOnlySet<long> pageIds)
        {
            Volatile.Write(ref _modifiedSinceLastCheckpoint, true);

            // Deleted pages are stored locally until checkpoint
            // This is to solve race condition so all pages have been assigned a fileId
            // before deleting a page so the file statistics are updated correctly.
            lock (_deletedPagesLock)
            {
                _deletedPages.UnionWith(pageIds);
            }
        }

        /// <summary>
        /// Writes a snapshot checkpoint, persisting the current state of file and page information to durable storage.
        /// </summary>
        /// <remarks>This method finalizes any pending write operations, updates file and deleted file
        /// information, and writes a consistent snapshot of the checkpoint to storage. After completion, the checkpoint
        /// state is reset for subsequent operations. This method is not thread-safe and should not be called
        /// concurrently with other checkpoint operations.</remarks>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        /// <exception cref="InvalidOperationException">Thrown if the checkpoint has not been started before calling this method.</exception>
        public async Task WriteSnapshotCheckpoint()
        {
            if (_writeTasks == null)
            {
                throw new InvalidOperationException("Checkpoint has not been started.");
            }
            _channel.Writer.Complete();
            await Task.WhenAll(_writeTasks);

            lock (_checkpointFileLock)
            {
                _newCheckpoint.AddUpsertPages(_pageFileLocations);

                lock (_modifiedFileIdsLock)
                {
                    // Start by reading deleted files and clear up file information
                    // This ensures that the written file information only contains active files
                    foreach (var deletedFileId in _deletedFileIds)
                    {
                        deletedFilesList.Add(new DeletedFileInfo()
                        {
                            fileId = deletedFileId,
                            deletedAtVersion = _checkpointVersion
                        });
                        // Remove the deleted file from file informations
                        _fileInformations.TryRemove(deletedFileId, out _);
                    }
                    _deletedFileIds.Clear();

                    // Write the file information of all active files
                    foreach (var file in _fileInformations)
                    {
                        _newCheckpoint.AddFileInformation(file.Value);
                    }

                    // Write a copy of the deleted files list
                    foreach(var deletedFile in deletedFilesList)
                    {
                        _newCheckpoint.AddDeletedFileId(deletedFile);
                    }
                }

                // Set the next file id
                _newCheckpoint.SetNextFileId(_nextFileId);

                // Finish the checkpoint for writing, adds header information
                _newCheckpoint.FinishForWriting();
            }

            // Write the checkpoint as a snapshot
            await _fileProvider.WriteCheckpointFileAsync(new CheckpointVersion(_checkpointVersion, true), _newCheckpoint);

            _newCheckpoint = new BlobNewCheckpoint(_memoryPool, _memoryAllocator);
            // Create a new channel
            _channel = Channel.CreateBounded<PagesFile>(1000);
            lock (_writeTasks)
            {
                _writeTasks = null;
            }

            _activeVersions.Add(new CheckpointVersion(_checkpointVersion, true));

            _currentCheckpointVersion = _checkpointVersion;
            _checkpointVersion++;


            lock (_checkpointFileLock)
            {
                _countSinceLastSnapshot = 0;
                if (_countSinceLastSnapshot >= VersionBetweenSnapshot)
                {
                    _writeSnapshotCheckpoint = true;
                }
                else
                {
                    _writeSnapshotCheckpoint = false;
                }
            }
        }

        public IEnumerable<FileInformation> GetAllFileInformation()
        {
            return _fileInformations.Values;
        }
        
        public async Task FinishCheckpoint()
        {
            if (_writeSnapshotCheckpoint)
            {
                await WriteSnapshotCheckpoint();
                return;
            }

            if (_writeTasks == null)
            {
                throw new InvalidOperationException("Checkpoint has not been started.");
            }

            _channel.Writer.Complete();

            await Task.WhenAll(_writeTasks);

            // Go through deleted pages add them to checkpoint and update file statistics
            lock (_deletedPagesLock)
            {
                if (_deletedPages.Count > 0)
                {
                    lock (_modifiedFileIdsLock)
                    {
                        foreach (var page in _deletedPages)
                        {
                            _newCheckpoint.AddDeletedPageId(page);

                            if (_pageFileLocations.TryGetValue(page, out var location))
                            {
                                if (_fileInformations.TryGetValue(location.FileId, out var fileInfo))
                                {
                                    fileInfo.AddNonActivePage();
                                    fileInfo.AddDeletedSize(location.Size);
                                    _modifiedFileIds.Add(location.FileId);

                                    if (fileInfo.PageCount == fileInfo.NonActivePageCount)
                                    {
                                        // If no of the pages are active in the file, add it to deleted file ids
                                        _deletedFileIds.Add(location.FileId);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Add all modified files to the checkpoint
            lock (_checkpointFileLock)
            {
                lock (_modifiedFileIdsLock)
                {
                    // Add all modified files first, this also adds deleted file info
                    foreach (var modifiedFileId in _modifiedFileIds)
                    {
                        if (_fileInformations.TryGetValue(modifiedFileId, out var fileInfo))
                        {
                            _newCheckpoint.AddFileInformation(fileInfo);
                        }
                    }

                    // Add all deleted file ids
                    foreach(var deletedFileId in _deletedFileIds)
                    {
                        deletedFilesList.Add(new DeletedFileInfo()
                        {
                            fileId = deletedFileId,
                            deletedAtVersion = _checkpointVersion
                        });

                        _newCheckpoint.AddDeletedFileId(new DeletedFileInfo()
                        {
                            fileId = deletedFileId,
                            deletedAtVersion = _checkpointVersion
                        });
                        // Remove the deleted file from file informations
                        _fileInformations.TryRemove(deletedFileId, out _);
                    }
                    _deletedFileIds.Clear();
                }
            }

            _newCheckpoint.SetNextFileId(_nextFileId);

            // All data has now been written to the checkpoint file
            _newCheckpoint.FinishForWriting();


            await _fileProvider.WriteCheckpointFileAsync(new CheckpointVersion(_checkpointVersion, false), _newCheckpoint);

            _newCheckpoint = new BlobNewCheckpoint(_memoryPool, _memoryAllocator);
            // Create a new channel
            _channel = Channel.CreateBounded<PagesFile>(1000);
            lock (_writeTasks)
            {
                _writeTasks = null;
            }
            _activeVersions.Add(new CheckpointVersion(_checkpointVersion, false));

            _currentCheckpointVersion = _checkpointVersion;
            _checkpointVersion++;

            lock (_checkpointFileLock)
            {
                _countSinceLastSnapshot++;
                if (_countSinceLastSnapshot >= VersionBetweenSnapshot)
                {
                    _writeSnapshotCheckpoint = true;
                }
            }
        }

        private long GetNextFileId()
        {
            lock (_lock)
            {
                return _nextFileId++;
            }
        }

        internal async Task Compact()
        {
            var minVersion = _currentCheckpointVersion - 2;

            List<DeletedFileInfo>? filesToRemove = default;
            lock (_modifiedFileIdsLock)
            {
                if (deletedFilesList.Count > 0)
                {
                    
                    filesToRemove = new List<DeletedFileInfo>();
                    for (int i = 0; i < deletedFilesList.Count; i++)
                    {
                        if (deletedFilesList[i].deletedAtVersion < minVersion)
                        {
                            filesToRemove.Add(deletedFilesList[i]);
                            deletedFilesList.RemoveAt(i);
                            i--;
                        }
                    }
                }
            }

            List<CheckpointVersion>? checkpointsToRemove = default;
            lock (_checkpointFileLock)
            {
                
                for (int i = 0; i < _activeVersions.Count; i++)
                {
                    var version = _activeVersions[i];
                    
                    if (version.IsSnapshot && version.Version <= minVersion)
                    {
                        for (int k = 0; k < i; k++)
                        {
                            if (checkpointsToRemove == null)
                            {
                                checkpointsToRemove = new List<CheckpointVersion>();
                            }
                            checkpointsToRemove.Add(_activeVersions[k]);
                            _activeVersions.RemoveAt(k);
                            k--;
                            i--;
                        }
                    }
                }
            }

            if (filesToRemove != null)
            {
                foreach (var fileToRemove in filesToRemove)
                {
                    await _fileProvider.DeleteDataFileAsync(fileToRemove.fileId);
                }
            }

            if (checkpointsToRemove != null)
            {
                foreach (var checkpointToRemove in checkpointsToRemove)
                {
                    await _fileProvider.DeleteCheckpointFileAsync(checkpointToRemove);
                }
            }
            

            
        }

        private async Task WriteLoop()
        {
            Debug.Assert(_cancellationTokenSource != null);
            while (!_cancellationTokenSource.Token.IsCancellationRequested)
            {
                var wait = await _channel.Reader.WaitToReadAsync();
                if (!wait)
                {
                    break;
                }
                if (!_channel.Reader.TryRead(out var file))
                {
                    continue;
                }

                long fileId = GetNextFileId();

                var fileIds = new PrimitiveList<long>(_memoryAllocator);
                fileIds.InsertStaticRange(0, fileId, file.PageIds.Count);

                var pageSizes = new PrimitiveList<int>(_memoryAllocator);
                for (int i = 0; i < file.PageIds.Count; i++)
                {
                    var offset = file.PageOffsets[i];
                    var next = file.PageOffsets[i + 1];
                    pageSizes.Add(next - offset);
                }

                lock (_checkpointFileLock)
                {
                    // If we are writing a snapshot checkpoint, it is not required to add upsert data since all data will be added to the checkpoint
                    // from the page file locations
                    if (!_writeSnapshotCheckpoint)
                    {
                        _newCheckpoint.AddUpsertPages(file.PageIds, fileIds, file.PageOffsets, pageSizes);
                    }
                }

                await _fileProvider.WriteDataFileAsync(fileId, file);

                for (int i = 0; i < file.PageIds.Count; i++)
                {
                    // Check if the page already exists in another file, if it does, we need to update the file information of the existing file and add it to modified file ids
                    if (_pageFileLocations.TryGetValue(file.PageIds[i], out var existingLocation))
                    {
                        if (_fileInformations.TryGetValue(existingLocation.FileId, out var existingFileInfo))
                        {
                            existingFileInfo.AddNonActivePage();
                            existingFileInfo.AddDeletedSize(existingLocation.Size);
                            lock (_modifiedFileIdsLock)
                            {
                                _modifiedFileIds.Add(existingLocation.FileId);
                                if (existingFileInfo.PageCount == existingFileInfo.NonActivePageCount)
                                {
                                    _deletedFileIds.Add(existingFileInfo.FileId);
                                }
                            }
                        }
                    }
                    var pageInfo = new PageFileLocation()
                    {
                        FileId = fileId,
                        Offset = file.PageOffsets[i],
                        Size = file.PageOffsets[i + 1] - file.PageOffsets[i]
                    };

                    if (pageInfo.Size < 0)
                    {
                        throw new InvalidOperationException("Negative page size");
                    }

                    _pageFileLocations[file.PageIds[i]] = pageInfo;
                }
                var fileInfo = new FileInformation(fileId, file.PageIds.Count, 0, file.FileSize, 0, CheckpointVersion);
                _fileInformations.AddOrUpdate(fileId, fileInfo, static (key, old) => old);
                lock (_modifiedFileIdsLock)
                {
                    _modifiedFileIds.Add(fileId);
                }

                file.DoneWriting();
                file.Dispose();
                fileIds.Dispose();
            }
        }
    }
}

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
        private readonly IFileStorageProvider _fileProvider;
        private readonly MemoryPool<byte> _memoryPool;
        private readonly IMemoryAllocator _memoryAllocator;
        private Channel<PagesFile> _channel;
        private BlobNewCheckpoint _newCheckpoint;
        private long _nextFileId = 0;
        private object _lock = new object();
        private object _checkpointFileLock = new object();


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

        private long _currentCheckpointVersion = 0;
        private long _checkpointVersion;
        private bool _modifiedSinceLastCheckpoint = false;

        public long CheckpointVersion => _checkpointVersion;


        public CheckpointHandler(IFileStorageProvider fileProvider, MemoryPool<byte> memoryPool, IMemoryAllocator memoryAllocator)
        {
            _channel = Channel.CreateBounded<PagesFile>(1000);
            this._fileProvider = fileProvider;
            _memoryPool = memoryPool;
            _newCheckpoint = new BlobNewCheckpoint(memoryPool, memoryAllocator);
            
            this._memoryAllocator = memoryAllocator;
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
            foreach (var checkpointFile in checkpointFiles)
            {
                var fileReader = await _fileProvider.ReadCheckpointFileAsync(checkpointFile);

                // Read all content of the file
                ReadResult readResult;
                do
                {
                    readResult = await fileReader.ReadAsync();
                    fileReader.AdvanceTo(readResult.Buffer.Start, readResult.Buffer.End);
                } while (!readResult.IsCompleted);
                ReadCheckpointFile(checkpointFile, readResult.Buffer);
            }
        }

        private void ReadCheckpointFile(CheckpointVersion checkpointFileInfo, ReadOnlySequence<byte> buffer)
        {
            var reader = new CheckpointDataReader(buffer);
            
            while (reader.TryGetNextUpsertPageInfo(out var upsertPageInfo))
            {
                _pageFileLocations[upsertPageInfo.pageId] = new PageFileLocation()
                {
                    FileId = upsertPageInfo.fileId,
                    Offset = upsertPageInfo.offset,
                    Size = upsertPageInfo.size
                };
            }

            while (reader.TryGetNextDeletedPageId(out var deletedPageId))
            {
                _pageFileLocations.TryRemove(deletedPageId, out _);
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

        public async Task FinishCheckpoint()
        {
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

                        _newCheckpoint.AddDeletedFileId(deletedFileId);
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
            _currentCheckpointVersion = _checkpointVersion;
            _checkpointVersion++;
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
            //foreach(var file in _fileInformations)
            //{
            //    var ratio = file.Value.NonActivePageCount / (float)file.Value.PageCount;
            //    if (ratio > 0.3f)
            //    {

            //    }
            //}

            List<DeletedFileInfo>? filesToRemove = default;
            lock (_modifiedFileIdsLock)
            {
                if (deletedFilesList.Count > 0)
                {
                    var minVersion = _currentCheckpointVersion - 2;
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

            if (filesToRemove != null)
            {
                foreach (var fileToRemove in filesToRemove)
                {
                    await _fileProvider.DeleteDataFileAsync(fileToRemove.fileId);
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
                    _newCheckpoint.AddUpsertPages(file.PageIds, fileIds, file.PageOffsets, pageSizes);
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
                    _pageFileLocations[file.PageIds[i]] = new PageFileLocation()
                    {
                        FileId = fileId,
                        Offset = file.PageOffsets[i],
                        Size = file.PageOffsets[i + 1] - file.PageOffsets[i]
                    };
                }
                var fileInfo = new FileInformation(fileId, file.PageIds.Count, 0);
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

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
using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalCache;
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    internal class CheckpointHandler : IDisposable, IAsyncDisposable
    {
        private readonly int VersionBetweenSnapshot = 5;
        private readonly ILogger _logger;
        private readonly LocalCacheProvider? cacheProvider;
        private readonly IReservoirStorageProvider _fileProvider;
        private readonly MemoryPool<byte> _memoryPool;
        private readonly IMemoryAllocator _memoryAllocator;
        private Channel<PagesFile> _channel;
        private BlobNewCheckpoint _newCheckpoint;
        private ulong _nextFileId = 0;
        private object _lock = new object();
        private object _checkpointFileLock = new object();
        private bool _writeSnapshotCheckpoint;
        private int _countSinceLastSnapshot;


        private object _taskLock = new object();
        private Task[]? _writeTasks;
        private CancellationTokenSource? _cancellationTokenSource;

        private ConcurrentDictionary<long, PageFileLocation> _pageFileLocations = new ConcurrentDictionary<long, PageFileLocation>();
        private ConcurrentDictionary<ulong, FileInformation> _fileInformations = new ConcurrentDictionary<ulong, FileInformation>();

        private HashSet<long> _deletedPages = new HashSet<long>();
        private object _deletedPagesLock = new object();

        private HashSet<ulong> _modifiedFileIds = new HashSet<ulong>();
        private HashSet<ulong> _deletedFileIds = new HashSet<ulong>();
        private object _modifiedFileIdsLock = new object();
        private List<DeletedFileInfo> deletedFilesList = new List<DeletedFileInfo>();
        private List<CheckpointVersion> _activeVersions = new List<CheckpointVersion>();

        private long _currentCheckpointVersion = 0;
        private long _checkpointVersion;
        private long _lastSnapshotVersion;
        private bool _modifiedSinceLastCheckpoint = false;

        private CheckpointRegistryFile _checkpointRegistryFile;
        private bool disposedValue;

        public long CheckpointVersion => _checkpointVersion;

        public long LastSnapshotVersion => _lastSnapshotVersion;


        public CheckpointHandler(
            IReservoirStorageProvider fileProvider, 
            MemoryPool<byte> pool,
            IMemoryAllocator memoryAllocator,
            int snapshotCheckpointInterval,
            ILogger logger,
            LocalCacheProvider? cacheProvider)
        {
            _channel = Channel.CreateBounded<PagesFile>(new BoundedChannelOptions(4)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false
            });
            this._fileProvider = fileProvider;
            _memoryPool = pool;
            _newCheckpoint = new BlobNewCheckpoint(_memoryPool, memoryAllocator);
            VersionBetweenSnapshot = snapshotCheckpointInterval;
            _logger = logger;
            this.cacheProvider = cacheProvider;
            this._memoryAllocator = memoryAllocator;
            _currentCheckpointVersion = 0;
            _checkpointVersion = 1;
            _checkpointRegistryFile = new CheckpointRegistryFile(_memoryAllocator);
        }

        private async Task ReadCheckpointRegistryFile(CancellationToken cancellationToken)
        {
            
            if (_fileProvider.SupportsFileListing)
            {
                // List checkpoint files
                var checkpointIds = await _fileProvider.ListCheckpointFilesAsync(cancellationToken);
                var checkpoints = checkpointIds.OrderByDescending(x => x.Version).ToList();

                bool foundRegistry = false;
                
                for (int i = 0; i < checkpoints.Count; i++)
                {
                    var checkpoint = checkpoints[i];
                    try
                    {
                        var checkpointBundlePipeReader = await _fileProvider.ReadCheckpointFileAsync(checkpoint, cancellationToken).ConfigureAwait(false);
                        if (checkpointBundlePipeReader != null)
                        {
                            if (_checkpointRegistryFile != null)
                            {
                                _checkpointRegistryFile.Dispose();
                            }
                            _checkpointRegistryFile = await CheckpointRegistryFile.DeserializeFromBundle(checkpointBundlePipeReader, _memoryAllocator, cancellationToken).ConfigureAwait(false);
                            foundRegistry = true;
                            break;
                        }
                    }
                    catch (InvalidOperationException e)
                    {
                         _logger.LogWarning(e, "Failed to read checkpoint registry from bundle file with id {CheckpointId}", checkpoint);
                    }
                    catch (InvalidDataException e)
                    {
                        _logger.LogWarning(e, "Invalid data in checkpoint registry from bundle file with id {CheckpointId}", checkpoint);
                    }
                }
                if (!foundRegistry)
                {
                    if (_checkpointRegistryFile != null)
                    {
                        _checkpointRegistryFile.Dispose();
                    }
                    _checkpointRegistryFile = new CheckpointRegistryFile(_memoryAllocator);
                }
            }
            else
            {
                var checkpointRegistryFileReader = await _fileProvider.ReadCheckpointRegistryFileAsync(cancellationToken).ConfigureAwait(false);

                if (_checkpointRegistryFile != null)
                {
                    _checkpointRegistryFile.Dispose();
                }
                if (checkpointRegistryFileReader == null)
                {
                    _checkpointRegistryFile = new CheckpointRegistryFile(_memoryAllocator);
                }
                else
                {
                    _checkpointRegistryFile = await CheckpointRegistryFile.Deserialize(checkpointRegistryFileReader, _memoryAllocator, cancellationToken).ConfigureAwait(false);
                }
            }
            

            // Try and read bundle files if the file provider supports it, this is to make sure we have the latest checkpoint registry file
            // in case there are bundled checkpoint files which contains later versions than the registry file
            if (_fileProvider.SupportsFileListing)
            {
                var lastVersion = _checkpointRegistryFile.LastOrDefault()?.Version ?? 0;
                var fileId = (1UL << 63) | (ulong)lastVersion;
                var bundledDataFileIds = (await _fileProvider.ListDataFilesAboveVersionAsync(fileId)).ToList();

                if (bundledDataFileIds.Count > 0)
                {
                    //ulong maxFileId = fileId;
                    // Validate that there are no version below the asked version
                    // This is a safe guard for implementation errors in file providers
                    // we also take out the max fileId here
                    for (int i = 0; i < bundledDataFileIds.Count; i++)
                    {
                        if (bundledDataFileIds[i] < fileId)
                        {
                            bundledDataFileIds.RemoveAt(i);
                            i--;
                        }
                    }

                    bundledDataFileIds = bundledDataFileIds.OrderByDescending(x => x).ToList();

                    for (int i = 0; i < bundledDataFileIds.Count; i++)
                    {
                        if (bundledDataFileIds[i] > fileId)
                        {
                            try
                            {
                                var dataFilePipe = await _fileProvider.ReadDataFileAsync(bundledDataFileIds[i], 0, cancellationToken);
                                _checkpointRegistryFile = await BundleFileRegistryReader.ReadRegistryAsync(dataFilePipe, _memoryAllocator, cancellationToken);
                                return;
                            }
                            catch(InvalidOperationException e)
                            {
                                _logger.LogWarning(e, "Failed to read checkpoint registry from bundle file with id {FileId}", bundledDataFileIds[i]);
                            }
                            catch(InvalidDataException e)
                            {
                                _logger.LogWarning(e, "Invalid data in checkpoint registry from bundle file with id {FileId}", bundledDataFileIds[i]);
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
        }

        internal void AddDeletedFile(ulong fileId)
        {
            lock (_modifiedFileIdsLock)
            {
                _deletedFileIds.Add(fileId);
            }
        }

        public async Task RecoverToLatest(CancellationToken cancellationToken)
        {
            await ReadCheckpointRegistryFile(cancellationToken);
            Debug.Assert(_checkpointRegistryFile != null);
            var checkpointVersions = _checkpointRegistryFile
                .OrderBy(x => x.Version)
                .ToList();

            if (checkpointVersions.Count == 0)
            {
                return;
            }

            _activeVersions.Clear();
            _activeVersions.AddRange(checkpointVersions);
            _checkpointRegistryFile.Clear();
            _checkpointRegistryFile.AddCheckpointVersions(checkpointVersions);

            // Clear all in-memory state before replaying checkpoints
            _pageFileLocations.Clear();
            _fileInformations.Clear();
            deletedFilesList.Clear();
            lock (_deletedPagesLock) { _deletedPages.Clear(); }
            lock (_modifiedFileIdsLock) { _modifiedFileIds.Clear(); _deletedFileIds.Clear(); }

            await ReadCheckpointFiles(checkpointVersions);
            var lastVersion = checkpointVersions[checkpointVersions.Count - 1];

            _currentCheckpointVersion = lastVersion.Version;
            _checkpointVersion = lastVersion.Version + 1;
            Volatile.Write(ref _modifiedSinceLastCheckpoint, false);
        }

        public async Task RecoverTo(long version, CancellationToken cancellationToken)
        {
            if (_currentCheckpointVersion == version && !Volatile.Read(ref _modifiedSinceLastCheckpoint))
            {
                // If we are already at the checkpoint version and there is no modification since last checkpoint, we can skip recovery
                return;
            }

            await ReadCheckpointRegistryFile(cancellationToken);
            Debug.Assert(_checkpointRegistryFile != null);

            // List all checkpoint files and order them by version
            var checkpointVersions = _checkpointRegistryFile
                .Where(x => x.Version <= version)
                .OrderBy(x => x.Version)
                .ToList();

            _activeVersions.Clear();
            _activeVersions.AddRange(checkpointVersions);
            _checkpointRegistryFile.Clear();
            _checkpointRegistryFile.AddCheckpointVersions(checkpointVersions);

            if (checkpointVersions.Count == 0)
            {
                throw new InvalidOperationException($"No checkpoint versions found for recovery.");
            }

            if (!checkpointVersions.Any(x => x.Version == version))
            {
                throw new InvalidOperationException($"Checkpoint file with version {version} not found for recovery.");
            }

            // Clear all in-memory state before replaying checkpoints
            _pageFileLocations.Clear();
            _fileInformations.Clear();
            deletedFilesList.Clear();
            lock (_deletedPagesLock) { _deletedPages.Clear(); }
            lock (_modifiedFileIdsLock) { _modifiedFileIds.Clear(); _deletedFileIds.Clear(); }

            await ReadCheckpointFiles(checkpointVersions);
            _currentCheckpointVersion = version;
            _checkpointVersion = version + 1;
            Volatile.Write(ref _modifiedSinceLastCheckpoint, false);
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
                    _lastSnapshotVersion = checkpointFile.Version;
                    _countSinceLastSnapshot = 0;
                }
                else
                {
                    // Increase the count since last snapshot to correctly take snapshots after X incremental even after a crash
                    _countSinceLastSnapshot++;
                }
                if (checkpointFile.IsBundled)
                {
                    var fileId = (1UL << 63) | (ulong)checkpointFile.Version;
                    try
                    {
                        var dataFileReader = await _fileProvider.ReadDataFileAsync(fileId, 0, CancellationToken.None);
                        try
                        {
                            var checkpointData = await BundleFileRegistryReader.ReadCheckpointDataAsync(dataFileReader, default);
                            ReadCheckpointFile(checkpointFile, checkpointData);
                        }
                        finally
                        {
                            dataFileReader.Complete();
                        }
                    }
                    catch (Exception e) when ((e is FlowtideChecksumMismatchException or InvalidOperationException) && cacheProvider != null)
                    {
                        // If we got a checksum error, and we have a cache, try and evict it from cache and try again
                        // If this fails again, we fail the stream
                        await cacheProvider.EvictDataFileAsync(fileId);
                        var dataFileReader = await _fileProvider.ReadDataFileAsync(fileId, 0, CancellationToken.None);
                        try
                        {
                            var checkpointData = await BundleFileRegistryReader.ReadCheckpointDataAsync(dataFileReader, default);
                            ReadCheckpointFile(checkpointFile, checkpointData);
                        }
                        finally
                        {
                            dataFileReader.Complete();
                        }
                    }
                    
                }
                else
                {
                    var fileReader = await _fileProvider.ReadCheckpointFileAsync(new CheckpointId((ulong)checkpointFile.Version, checkpointFile.IsSnapshot));

                    try
                    {
                        // Read all content of the file
                        ReadResult readResult;
                        do
                        {
                            readResult = await fileReader.ReadAsync();
                            fileReader.AdvanceTo(readResult.Buffer.Start, readResult.Buffer.End);
                        } while (!readResult.IsCompleted);
                        ReadCheckpointFile(checkpointFile, readResult.Buffer);
                    }
                    finally
                    {
                        fileReader.Complete();
                    }
                }
                
            }
        }

        private void ReadCheckpointFile(CheckpointVersion checkpointFileInfo, ReadOnlySequence<byte> buffer)
        {
            buffer = CheckpointFileDataExtractor.GetCheckpointData(buffer);
            CrcUtils.CheckCrc64(checkpointFileInfo.Crc64, buffer);

            IMemoryOwner<byte>? memoryOwner = default;

            SequenceReader<byte> seqReader = new SequenceReader<byte>(buffer);
            if (!seqReader.TryReadLittleEndian(out int magicNumber))
            {
                throw new InvalidOperationException("Could not read magic number on checkpoint file");
            }
            if(magicNumber == MagicNumbers.CompressedZstdCheckpointFileMagicNumber)
            {
                if (seqReader.TryReadLittleEndian(out int decompressedSize))
                {
                    memoryOwner = _memoryAllocator.Allocate(decompressedSize, 64);
                    using var decompressor = new ZstdDecompression(_memoryAllocator);
                    decompressor.Read(buffer.Slice(8), memoryOwner.Memory.Span);
                    buffer = new ReadOnlySequence<byte>(memoryOwner.Memory.Slice(0, decompressedSize));
                }
            }

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
                    Size = upsertPageInfo.size,
                    Crc32 = upsertPageInfo.crc32
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

            if (memoryOwner != null)
            {
                memoryOwner.Dispose();
            }
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
            if (_writeTasks != null)
            {
                _channel.Writer.Complete();
                await Task.WhenAll(_writeTasks);
            }

            // Process deleted pages to ensure file statistics are correctly updated
            // and deleted pages are removed from `_pageFileLocations` before a snapshot.
            lock (_deletedPagesLock)
            {
                if (_deletedPages.Count > 0)
                {
                    lock (_modifiedFileIdsLock)
                    {
                        foreach (var page in _deletedPages)
                        {
                            // In a snapshot, we don't output deleted pages to the checkpoint file
                            // as they are inherently not in the active UpsertPages dict.
                            if (_pageFileLocations.TryRemove(page, out var location) &&
                                _fileInformations.TryGetValue(location.FileId, out var fileInfo))
                            {
                                fileInfo.AddNonActivePage();
                                fileInfo.AddDeletedSize(location.Size);
                                _modifiedFileIds.Add(location.FileId);

                                // If the file has all pages deleted and it is not a bundled file delete id
                                if (fileInfo.PageCount == fileInfo.NonActivePageCount &&
                                    !IsBundleFile(fileInfo.FileId))
                                {
                                    _deletedFileIds.Add(location.FileId);
                                }
                            }
                        }
                    }
                    _deletedPages.Clear();
                }
            }

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
                    _modifiedFileIds.Clear();

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
                _newCheckpoint.CompressData();
                _newCheckpoint.RecalculateCrc64();
            }

            var checkpointVersion = new CheckpointVersion(_checkpointVersion, true, _newCheckpoint.Crc64, false);

            _activeVersions.Add(checkpointVersion);
            _checkpointRegistryFile.AddCheckpointVersion(checkpointVersion);

            _checkpointRegistryFile.FinishForWriting();

            if (_fileProvider.SupportsFileListing)
            {
                // Create a bundle with checkpoint data and registry to reduce number of writes
                var bundle = new CheckpointRegistryBundleFile(_newCheckpoint, _checkpointRegistryFile);
                await _fileProvider.WriteCheckpointFileAsync(new CheckpointId((ulong)checkpointVersion.Version, checkpointVersion.IsSnapshot), bundle);
            }
            else
            {
                // Write the checkpoint file
                await _fileProvider.WriteCheckpointFileAsync(new CheckpointId((ulong)checkpointVersion.Version, checkpointVersion.IsSnapshot), _newCheckpoint);
                // Write the registry separately since provider does not support listing
                await _fileProvider.WriteCheckpointRegistryFile(_checkpointRegistryFile);
            }
            
            _newCheckpoint.Dispose();

            _newCheckpoint = new BlobNewCheckpoint(_memoryPool, _memoryAllocator);
            // Create a new channel
            _channel = Channel.CreateBounded<PagesFile>(new BoundedChannelOptions(4)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false
            });
            lock (_taskLock)
            {
                _writeTasks = null;
                _cancellationTokenSource?.Dispose();
                _cancellationTokenSource = null;
            }

            _lastSnapshotVersion = _checkpointVersion;
            _currentCheckpointVersion = _checkpointVersion;
            _checkpointVersion++;
            Volatile.Write(ref _modifiedSinceLastCheckpoint, false);

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

        public bool TryGetFileInformation(ulong fileId, [NotNullWhen(true)] out FileInformation? fileInfo)
        {
            return _fileInformations.TryGetValue(fileId, out fileInfo);
        }

        private bool IsBundleFile(ulong fileId)
        {
            return (fileId & (1UL << 63)) > 0;
        }
        
        public async Task FinishCheckpoint(MergedBlobFileWriter? mergedFile)
        {
            if (_writeSnapshotCheckpoint)
            {
                if (mergedFile != null)
                {
                    // If we are taking a snapshot, we will write the data as a normal data file
                    // since snapshots do not contain bundles.
                    await EnqueueFileAsync(mergedFile);
                }
                await WriteSnapshotCheckpoint();
                return;
            }

            if (_writeTasks != null)
            {
                _channel.Writer.Complete();
                await Task.WhenAll(_writeTasks);
            }

            

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

                            if (_pageFileLocations.TryRemove(page, out var location))
                            {
                                if (_fileInformations.TryGetValue(location.FileId, out var fileInfo))
                                {
                                    fileInfo.AddNonActivePage();
                                    fileInfo.AddDeletedSize(location.Size);
                                    _modifiedFileIds.Add(location.FileId);

                                    // If the file has all pages deleted and it is not a bundled file delete id
                                    if (fileInfo.PageCount == fileInfo.NonActivePageCount &&
                                        !IsBundleFile(fileInfo.FileId))
                                    {
                                        // If no of the pages are active in the file, add it to deleted file ids
                                        _deletedFileIds.Add(location.FileId);
                                    }
                                }
                            }
                        }
                    }
                    _deletedPages.Clear();
                }
            }

            _newCheckpoint.SetNextFileId(_nextFileId);

            // All data has now been written to the checkpoint file

            if (mergedFile != null)
            {
                await HandleBundleCheckpoint(mergedFile);
            }
            else
            {
                // Add all modified files to the checkpoint
                // This is done seperately in bundle checkpoint since no file info has been updated yet from the write loop.
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
                        foreach (var deletedFileId in _deletedFileIds)
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
                        _modifiedFileIds.Clear();
                    }
                }

                _newCheckpoint.FinishForWriting();
                _newCheckpoint.CompressData();
                _newCheckpoint.RecalculateCrc64();

                var checkpointVersion = new CheckpointVersion(_checkpointVersion, false, _newCheckpoint.Crc64, false);

                _checkpointRegistryFile.AddCheckpointVersion(checkpointVersion);
                _checkpointRegistryFile.FinishForWriting();

                if (_fileProvider.SupportsFileListing)
                {
                    var bundle = new CheckpointRegistryBundleFile(_newCheckpoint, _checkpointRegistryFile);
                    await _fileProvider.WriteCheckpointFileAsync(new CheckpointId((ulong)checkpointVersion.Version, checkpointVersion.IsSnapshot), bundle);
                }
                else
                {
                    await _fileProvider.WriteCheckpointFileAsync(new CheckpointId((ulong)checkpointVersion.Version, checkpointVersion.IsSnapshot), _newCheckpoint);
                    await _fileProvider.WriteCheckpointRegistryFile(_checkpointRegistryFile);
                }
                _activeVersions.Add(checkpointVersion);
            }

            _newCheckpoint.Dispose();
            _newCheckpoint = new BlobNewCheckpoint(_memoryPool, _memoryAllocator);
            // Create a new channel
            _channel = Channel.CreateBounded<PagesFile>(new BoundedChannelOptions(4)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false
            });
            if (_writeTasks != null)
            {
                lock (_taskLock)
                {
                    _writeTasks = null;
                    _cancellationTokenSource?.Dispose();
                    _cancellationTokenSource = null;
                }
            }
            

            _currentCheckpointVersion = _checkpointVersion;
            _checkpointVersion++;
            Volatile.Write(ref _modifiedSinceLastCheckpoint, false);

            lock (_checkpointFileLock)
            {
                _countSinceLastSnapshot++;
                if (_countSinceLastSnapshot >= VersionBetweenSnapshot)
                {
                    _writeSnapshotCheckpoint = true;
                }
            }
        }

        /// <summary>
        /// Special method to handle bundled data and checkpoint info.
        /// This is done to reduce the number of write operations being done which can help
        /// reduce cloud costs.
        /// </summary>
        /// <param name="mergedFile"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        private async Task HandleBundleCheckpoint(MergedBlobFileWriter mergedFile)
        {
            // Go through all pages and update file information as the first pass
            // we will go through it again after write to update the actual page locations
            for (int i = 0; i < mergedFile.PageIds.Count; i++)
            {
                // Check if the page already exists in another file, if it does, we need to update the file information of the existing file and add it to modified file ids
                if (_pageFileLocations.TryGetValue(mergedFile.PageIds[i], out var existingLocation))
                {
                    if (_fileInformations.TryGetValue(existingLocation.FileId, out var existingFileInfo))
                    {
                        existingFileInfo.AddNonActivePage();
                        existingFileInfo.AddDeletedSize(existingLocation.Size);
                        lock (_modifiedFileIdsLock)
                        {
                            _modifiedFileIds.Add(existingLocation.FileId);
                            if (existingFileInfo.PageCount == existingFileInfo.NonActivePageCount &&
                                !IsBundleFile(existingFileInfo.FileId))
                            {
                                _deletedFileIds.Add(existingFileInfo.FileId);
                            }
                        }
                    }
                }
            }

            // Update checkpoint file with the modified files and the deleted files
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
                    foreach (var deletedFileId in _deletedFileIds)
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
                    _modifiedFileIds.Clear();
                }
            }

            ulong fileId = 1UL << 63 | (ulong)_checkpointVersion;

            // Update fileIds for all pages
            _newCheckpoint.UpdateAllFileIds(fileId);

            // Create the file id array
            var fileIds = new PrimitiveList<ulong>(_memoryAllocator);
            fileIds.InsertStaticRange(0, fileId, mergedFile.PageIds.Count);

            // Create the page sizes
            var pageSizes = new PrimitiveList<int>(_memoryAllocator);
            for (int i = 0; i < mergedFile.PageIds.Count; i++)
            {
                var offset = mergedFile.PageOffsets[i];
                var next = mergedFile.PageOffsets[i + 1];
                pageSizes.Add(next - offset);
            }

            // add the pages info to the checkpoint file
            _newCheckpoint.AddUpsertPages(mergedFile.PageIds, fileIds, mergedFile.PageOffsets, pageSizes, mergedFile.Crc32s);

            // Add a temporary file info, the crc64 will be incorrect but is updated later
            var temporaryFileInfo = new FileInformation(fileId, mergedFile.PageIds.Count, 0, mergedFile.FileSize, 0, CheckpointVersion, mergedFile.Crc64);
            _newCheckpoint.AddFileInformation(temporaryFileInfo);
            _newCheckpoint.FinishForWriting();
            // No compression in bundle files, since crc64 is recalculated and updated
            // Bundle files are usually small so compression only adds complexity
            _newCheckpoint.RecalculateCrc64();

            var checkpointVersion = new CheckpointVersion(_checkpointVersion, false, _newCheckpoint.Crc64, true);

            _checkpointRegistryFile.AddCheckpointVersion(checkpointVersion);
            _checkpointRegistryFile.FinishForWriting();

            // Create a bundle, this recalculates crc64 of the files
            var bundle = new DataCheckpointBundleFile(mergedFile, _newCheckpoint, _checkpointRegistryFile);

            // when the bundle is created we got a new crc64 for the merged file, so we create a new file info to insert the correct info
            var realFileInfo = new FileInformation(fileId, mergedFile.PageIds.Count, 0, mergedFile.FileSize, 0, CheckpointVersion, mergedFile.Crc64);
            _fileInformations.AddOrUpdate(fileId, realFileInfo, static (key, old) => old);

            await _fileProvider.WriteDataFileAsync(fileId, bundle.Crc64, bundle.FileSize, true, bundle);
            // Add the active version, we also fetch the crc64 again since it will be different after writing the bundle file
            _activeVersions.Add(new Reservoir.CheckpointVersion(_checkpointVersion, false, _newCheckpoint.Crc64, true));

            // Update local page file location for pages so they can be found
            for (int i = 0; i < mergedFile.PageIds.Count; i++)
            {
                var pageInfo = new PageFileLocation()
                {
                    FileId = fileId,
                    Offset = mergedFile.PageOffsets[i],
                    Size = mergedFile.PageOffsets[i + 1] - mergedFile.PageOffsets[i],
                    Crc32 = mergedFile.Crc32s[i],
                };

                if (pageInfo.Size < 0)
                {
                    throw new InvalidOperationException("Negative page size");
                }

                _pageFileLocations[mergedFile.PageIds[i]] = pageInfo;
            }

            // tell done writing to in flight buffers can be cleared
            mergedFile.DoneWriting();
            bundle.Return(); // Return the file when we are done
            fileIds.Dispose();
            pageSizes.Dispose();
        }

        private ulong GetNextFileId()
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
                            _checkpointRegistryFile.RemoveCheckpointVersion(_activeVersions[k]);
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

            if (checkpointsToRemove != null && checkpointsToRemove.Count > 0)
            {
                bool removedCheckpoint = false;
                foreach (var checkpointToRemove in checkpointsToRemove)
                {
                    // Only delete non bundled checkpoint files, bundled checkpoint files are deleted when the bundle file is deleted
                    if (!checkpointToRemove.IsBundled)
                    {
                        await _fileProvider.DeleteCheckpointFileAsync(new CheckpointId((ulong)checkpointToRemove.Version, checkpointToRemove.IsSnapshot));
                        removedCheckpoint = true;
                    }
                }

                // Write the registry since we removed checkpoint versions
                // We only write if we removed checkpoints that are not bundles
                if (removedCheckpoint && !_fileProvider.SupportsFileListing)
                {
                    _checkpointRegistryFile.FinishForWriting();
                    await _fileProvider.WriteCheckpointRegistryFile(_checkpointRegistryFile);
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

                ulong fileId = GetNextFileId();

                var fileIds = new PrimitiveList<ulong>(_memoryAllocator);
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
                        _newCheckpoint.AddUpsertPages(file.PageIds, fileIds, file.PageOffsets, pageSizes, file.Crc32s);
                    }
                }

                await _fileProvider.WriteDataFileAsync(fileId, file.Crc64, file.FileSize, false, file);

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
                                if (existingFileInfo.PageCount == existingFileInfo.NonActivePageCount && 
                                    !IsBundleFile(existingFileInfo.FileId))
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
                        Size = file.PageOffsets[i + 1] - file.PageOffsets[i],
                        Crc32 = file.Crc32s[i],
                    };

                    if (pageInfo.Size < 0)
                    {
                        throw new InvalidOperationException("Negative page size");
                    }

                    _pageFileLocations[file.PageIds[i]] = pageInfo;
                }
                var fileInfo = new FileInformation(fileId, file.PageIds.Count, 0, file.FileSize, 0, CheckpointVersion, file.Crc64);
                _fileInformations.AddOrUpdate(fileId, fileInfo, static (key, old) => old);
                lock (_modifiedFileIdsLock)
                {
                    _modifiedFileIds.Add(fileId);
                }

                file.DoneWriting();
                file.Return();
                fileIds.Dispose();
                pageSizes.Dispose();
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    DisposeAsync(disposing).GetAwaiter().GetResult();
                }


                disposedValue = true;
            }
        }

        ~CheckpointHandler()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        protected virtual async ValueTask DisposeAsync(bool disposing)
        {
            if (!disposedValue)
            {
                if (_cancellationTokenSource != null)
                {
                    _cancellationTokenSource.Cancel();
                }

                _channel.Writer.Complete();

                if (_writeTasks != null)
                {
                    await Task.WhenAll(_writeTasks);
                }

                _newCheckpoint.Dispose();
                _checkpointRegistryFile.Dispose();

                disposedValue = true;
            }
        }

        public async ValueTask DisposeAsync()
        {
            await DisposeAsync(true);
            GC.SuppressFinalize(this);
        }

        #region Testing Properties
        // For Testing Only
        internal HashSet<long> DeletedPages_Test => _deletedPages;
        internal ConcurrentDictionary<long, PageFileLocation> PageFileLocations_Test => _pageFileLocations;
        internal bool IsBundleFile_Test(ulong fileId) => IsBundleFile(fileId);
        internal void ForceSnapshotNext_Test() => _writeSnapshotCheckpoint = true;
        internal BlobNewCheckpoint NewCheckpoint_Test => _newCheckpoint;
        internal CheckpointRegistryFile CheckpointRegistryFile_Test => _checkpointRegistryFile;
        internal bool ModifiedSinceLastCheckpoint_Test => _modifiedSinceLastCheckpoint;
        internal HashSet<ulong> ModifiedFileIds_Test => _modifiedFileIds;
        #endregion
    }
}

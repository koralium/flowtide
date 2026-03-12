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
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalCache;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.IO.Hashing;
using System.IO.Pipelines;
using System.Text.Json;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    public class ReservoirPersistentStorage : IPersistentStorage
    {
        private readonly int _maxFileSize;
        private IReservoirStorageProvider _fileProvider;
        private readonly MemoryPool<byte> _memoryPool;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly ReservoirStorageOptions _blobStorageOptions;
        private MergedBlobFileWriter _mergedBlobFileWriter;
        private CheckpointHandler? _checkpointHandler;
        private ReservoirPersistentSession? _adminSession;
        private SemaphoreSlim _mergedBlobLock = new SemaphoreSlim(1);
        private List<ReservoirPersistentSession> _sessions = new List<ReservoirPersistentSession>();
        private object _sessionsLock = new object();
        private Meter? _meter;


        private int _numberOfWrittenFiles;
        private bool _takingCheckpoint = false;
        private string? _streamVersion;
        private string? _streamName;
        private ReservoirStreamsMetadata? _storageMetadata;
        /// <summary>
        /// Temporary location of written pages from sessions
        /// This must be on this level and not on the individual sessions to allow compaction
        /// to check if a page has already been written (in combination with actual physical location lookup).
        /// </summary>
        private ConcurrentDictionary<long, PageWriteLocation> _temporaryPageLocations = new ConcurrentDictionary<long, PageWriteLocation>();

        /// <summary>
        /// Used for testing
        /// </summary>
        internal LocalCacheProvider? CacheProvider { get; }

        public ReservoirPersistentStorage(ReservoirBuilder reservoirBuilder)
            : this(reservoirBuilder.Build())
        {
            
        }

        public ReservoirPersistentStorage(ReservoirStorageOptions blobStorageOptions)
        {
            if (blobStorageOptions.FileProvider == null)
            {
                throw new ArgumentNullException(nameof(blobStorageOptions.FileProvider), "FileProvider must be provided in BlobStorageOptions.");
            }
            this._fileProvider = blobStorageOptions.FileProvider;

            // If the user provided a cache provider, we add the local cache provider
            if (blobStorageOptions.CacheProvider != null)
            {
                var cacheProvider = new LocalCacheProvider(
                    this, 
                    blobStorageOptions.CacheProvider, 
                    blobStorageOptions.FileProvider, 
                    blobStorageOptions.MaxCacheSizeBytes);

                _fileProvider = cacheProvider;
                CacheProvider = cacheProvider;
            }

            this._memoryPool = blobStorageOptions.MemoryPool;
            this._memoryAllocator = blobStorageOptions.MemoryAllocator;
            this._maxFileSize = blobStorageOptions.MaxFileSize;
            _mergedBlobFileWriter = new MergedBlobFileWriter(_memoryPool, _memoryAllocator);
            this._blobStorageOptions = blobStorageOptions;
        }

        public long CurrentVersion
        {
            get
            {
                Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before fetching current version");
                return _checkpointHandler.CheckpointVersion;
            }
        }

        internal bool TryGetTemporaryLocation(long pageId, out PageWriteLocation location)
        {
            return _temporaryPageLocations.TryGetValue(pageId, out location);
        }

        internal void RemoveTemporaryLocation(long pageId)
        {
            _temporaryPageLocations.TryRemove(pageId, out _);
        }

        internal bool TemporaryLocationExists(long pageId)
        {
            return _temporaryPageLocations.ContainsKey(pageId);
        }

        internal void AddTemporaryLocation(long pageId, PageWriteLocation pageWriteLocation)
        {
            _temporaryPageLocations[pageId] = pageWriteLocation;
        }

        internal async Task AddNonCompletedBlobFile(BlobFileWriter blobFileWriter)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before adding blob files");

            if (Volatile.Read(ref _takingCheckpoint))
            {
                throw new InvalidOperationException("Cannot add blob file while taking checkpoint. This is to ensure that all data that is added during checkpointing is included in the checkpoint");
            }

            await _mergedBlobLock.WaitAsync();
            _mergedBlobFileWriter.AddBlobFile(blobFileWriter);

            if (_mergedBlobFileWriter.WrittenData.Length >= _maxFileSize)
            {
                // Finish the file writer, this adds the page ids and offsets
                _mergedBlobFileWriter.Finish();

                // Send the file to checkpoint handler
                Interlocked.Increment(ref _numberOfWrittenFiles);
                await _checkpointHandler.EnqueueFileAsync(_mergedBlobFileWriter);
                _mergedBlobFileWriter = new MergedBlobFileWriter(_memoryPool, _memoryAllocator);
            }
            _mergedBlobLock.Release();
        }

        internal async Task AddCompleteBlobFile(BlobFileWriter blobFileWriter)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before adding blob files");
            Interlocked.Increment(ref _numberOfWrittenFiles);
            await _checkpointHandler.EnqueueFileAsync(blobFileWriter);
        }

        internal bool TryGetFileInformation(ulong fileId, [NotNullWhen(true)] out FileInformation? fileInformation)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before fetching file information");
            return _checkpointHandler.TryGetFileInformation(fileId, out fileInformation);
        }

        private async Task CompactFiles()
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before compacting files");
            // Fetch all files that where added in previous versions
            var files = _checkpointHandler.GetAllFileInformation()
                .Where(x => x.AddedAtVersion < _checkpointHandler.CheckpointVersion)
                .ToList();

            var currentSize = _mergedBlobFileWriter.FileSize;
            List<FileInformation> filesToCompact = new List<FileInformation>();
            // This list contains possible files to compact
            List<FileInformation> maybeFilesToCompact = new List<FileInformation>();

            bool mergedAtleastOnce = false;
            foreach(var file in files)
            {
                if ((file.FileId & 1UL << 63) != 0)
                {
                    var version = (long)(file.FileId & ~(1UL << 63));
                    if (version >= _checkpointHandler.LastSnapshotVersion)
                    {
                        // This is a bundle file with checkpoint info, we cannot compact it before a snapshot has been taken above this version
                        continue;
                    }
                }

                if (file.PageCount == file.NonActivePageCount)
                {
                    // We add the file to the delete list
                    _checkpointHandler.AddDeletedFile(file.FileId);
                    continue;
                }

                var actualSize = file.FileSize - file.DeletedSize;
                var sizeRatio = (double)actualSize / _maxFileSize;

                // If the actual size of the file is less than 33% of the max file size, we consider it for compaction.
                // This threshold can be tuned based on the workload and performance requirements.
                if (sizeRatio < _blobStorageOptions.CompactionFileSizeRatioThreshold)
                {
                    maybeFilesToCompact.Add(file);
                    currentSize += actualSize;

                    if (currentSize >= _maxFileSize)
                    {
                        filesToCompact.AddRange(maybeFilesToCompact);
                        maybeFilesToCompact.Clear();
                        currentSize = 0;
                        mergedAtleastOnce = true;
                    }
                }
            }

            // Either atleast five files to merge, or the new size must be atleast half of the max size
            // and either it should not have been merged before in this code (so we atleast remove one file)
            // otherwise at least two files must be in the list to make sure we actually reduce number of files
            // and dont copy the same file to a new file
            if (maybeFilesToCompact.Count > 5 ||
                (
                    (currentSize >= _maxFileSize / 2) && 
                    (
                        mergedAtleastOnce == false || 
                        (
                            mergedAtleastOnce && 
                            maybeFilesToCompact.Count > 1)
                    )
                )
            )
            {
                filesToCompact.AddRange(maybeFilesToCompact);
                maybeFilesToCompact.Clear();
                currentSize = 0;
            }

            if (filesToCompact.Count > 0)
            {
                foreach(var fileToCompact in filesToCompact)
                {
                    await CompactFile(fileToCompact.FileId, fileToCompact.FileSize, fileToCompact.Crc64);
                }
            }
        }

        internal async Task CompactFile(ulong fileId, int fileSize, ulong crc64)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before compacting files");

            var reader = await _fileProvider.ReadDataFileAsync(fileId, fileSize);

            try
            {
                Crc32 crc32 = new Crc32();
                DataFileReader dataFileReader = new DataFileReader(reader);
                await dataFileReader.Initialize();
                var pageIdsData = await dataFileReader.ReadPageIds();
                var pageLocations = new List<PageDataInfo>();
                ReadDataFilePageIds(fileId, pageIdsData, pageLocations);
                // Skip the offsets since they where taken from the lookup table
                await dataFileReader.SkipPageOffsets();

                _mergedBlobFileWriter.StartAddingSequences(pageLocations.Count);
                for (int i = 0; i < pageLocations.Count; i++)
                {
                    var location = pageLocations[i];
                    var pageData = await dataFileReader.ReadDataPage(location.Offset, location.Size);
                    // Check the checksum so there is no corruption before we add the data
                    CrcUtils.CheckPageCrc32(crc32, location.PageId, pageData, location.Crc32);
                    _mergedBlobFileWriter.AddSequence(location.PageId, location.Crc32, pageData);
                }
                _mergedBlobFileWriter.FinishAddingSequences();
                // Read to the end of the file, this must be done to get correct crc64
                await dataFileReader.ReadToEnd();

                // Check crc64 for the entire file, this helps if there was any bit errors in pageId list
                // Which could cause the wrong data to be loaded or data to be missed.
                var actualCrc64 = dataFileReader.GetCrc64();
                if (actualCrc64 != crc64)
                {
                    throw new FlowtideChecksumMismatchException($"Missmatching file crc64 for fileId: '{fileId}'.");
                }

                if (_mergedBlobFileWriter.FileSize >= _maxFileSize)
                {
                    _mergedBlobFileWriter.Finish();

                    // Send the file to checkpoint handler
                    Interlocked.Increment(ref _numberOfWrittenFiles);
                    await _checkpointHandler.EnqueueFileAsync(_mergedBlobFileWriter);
                    _mergedBlobFileWriter = new MergedBlobFileWriter(_memoryPool, _memoryAllocator);
                }
            }
            finally
            {
                await reader.CompleteAsync();
            }
        }

        private void ReadDataFilePageIds(ulong fileId, ReadOnlySequence<byte> data, List<PageDataInfo> pageFileLocations)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before reading data file page ids");

            var reader = new DataFilePageIdsReader(data);

            while(reader.TryGetNextPageId(out var pageId))
            {
                if ((!_temporaryPageLocations.ContainsKey(pageId)) &&
                    _checkpointHandler.TryGetPageFileLocation(pageId, out var location) &&
                    location.FileId == fileId)
                {
                    pageFileLocations.Add(new PageDataInfo(pageId, location.Offset, location.Size, location.Crc32));
                }
            }
        }

        public async ValueTask CheckpointAsync(byte[] metadata, bool includeIndex)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before checkpointing");
            Debug.Assert(_adminSession != null, "Persistent storage must be initialized before checkpointing");

            await _adminSession.Write(1, new SerializableObject(metadata));
            await _adminSession.Commit();

            Volatile.Write(ref _takingCheckpoint, true);
            // If there is any data in the merged blob file writer, we need to finish it and send it to the checkpoint handler
            await _mergedBlobLock.WaitAsync();
            // Add compaction data
            await CompactFiles();
            if (_mergedBlobFileWriter.PageIds.Count > 0)
            {
                _mergedBlobFileWriter.Finish();
                // Send the file to checkpoint handler
                if (Volatile.Read(ref _numberOfWrittenFiles) > 0)
                {
                    // If we already sent files, send another one since we will do a "big" checkpoint
                    // If not we will do a bundle checkpoint to do a single write to the storage
                    await _checkpointHandler.EnqueueFileAsync(_mergedBlobFileWriter);
                    _mergedBlobFileWriter = new MergedBlobFileWriter(_memoryPool, _memoryAllocator);
                }
            }
            _mergedBlobLock.Release();
            if (Volatile.Read(ref _numberOfWrittenFiles) == 0)
            {
                bool finishedCheckpoint = false;
                await _mergedBlobLock.WaitAsync();
                if (_mergedBlobFileWriter.PageIds.Count > 0)
                {
                    var file = _mergedBlobFileWriter;
                    _mergedBlobFileWriter = new MergedBlobFileWriter(_memoryPool, _memoryAllocator);
                    await _checkpointHandler.FinishCheckpoint(file);
                    finishedCheckpoint = true;
                }
                _mergedBlobLock.Release();

                if (!finishedCheckpoint)
                {
                    // If we did not write any data at all, just finish with a normal checkpoint
                    await _checkpointHandler.FinishCheckpoint(default);
                }
            }
            else
            {
                await _checkpointHandler.FinishCheckpoint(default);
            }

            Volatile.Write(ref _numberOfWrittenFiles, 0);
            Volatile.Write(ref _takingCheckpoint, false);
            await TryDeleteOldStreamVersions(default);
        }

        private Task TryDeleteOldStreamVersions(CancellationToken cancellationToken)
        {
            if (_storageMetadata == null ||
                _streamVersion == null ||
                _streamName == null ||
                _blobStorageOptions.KeepLastStreamVersions == -1 ||
                (_blobStorageOptions.KeepLastStreamVersions + 1) == _storageMetadata.Versions.Count)
            {
                return Task.CompletedTask;
            }

            return TryDeleteOldStreamVersions_Slow(cancellationToken);
        }

        private async Task TryDeleteOldStreamVersions_Slow(CancellationToken cancellationToken)
        {
            Debug.Assert(_storageMetadata != null);
            Debug.Assert(_streamVersion != null);
            Debug.Assert(_streamName != null);
            var sorted = _storageMetadata.Versions.OrderBy(x => x.LastInitializeTime).ToList();

            var index = sorted.FindIndex(x => x.Version == _streamVersion);
            if (index == -1)
            {
                sorted.Add(new ReservoirStreamVersion(_streamVersion, DateTime.UtcNow, DateTime.UtcNow));
            }

            for (int i = 0; i < sorted.Count - (_blobStorageOptions.KeepLastStreamVersions + 1); i++)
            {
                var versionToDelete = sorted[i];
                await _fileProvider.DeleteStreamVersionAsync(_streamName, versionToDelete.Version);
                sorted.RemoveAt(i);
                i--;
            }
            var metadataBytes = JsonSerializer.SerializeToUtf8Bytes(_storageMetadata.UpdateVersions(sorted), new JsonSerializerOptions()
            {
                WriteIndented = true
            });
            await _fileProvider.WriteStreamsMetadataFileAsync(_streamName, PipeReader.Create(new ReadOnlySequence<byte>(metadataBytes)), cancellationToken);
        }

        internal async ValueTask<ReadOnlySequence<byte>> ReadAsync(long key)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before reading data");

            if (_checkpointHandler.TryGetPageFileLocation(key, out var location))
            {
                var memory = await _fileProvider.GetMemoryAsync(location.FileId, location.Offset, location.Size, location.Crc32);
                return new ReadOnlySequence<byte>(memory);
            }
            throw new FlowtidePersistentStorageException($"Key {key} not found in persistent storage.");
        }

        internal ValueTask<T> ReadAsync<T>(long key, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before reading data");

            if (_checkpointHandler.TryGetPageFileLocation(key, out var location))
            {
                return _fileProvider.ReadAsync<T>(location.FileId, location.Offset, location.Size, location.Crc32, stateSerializer);
            }
            throw new FlowtidePersistentStorageException($"Key {key} not found in persistent storage.");
        }

        internal void DeletePages(IReadOnlySet<long> keys)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before deleting pages");

            _checkpointHandler.AddDeletedPages(keys);
        }

        public async ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before compacting");

            await _checkpointHandler.Compact();
        }

        public IPersistentStorageSession CreateSession()
        {
            if (_checkpointHandler == null)
            {
                throw new InvalidOperationException("Persistent storage must be initialized before creating sessions");
            }
            var session = new ReservoirPersistentSession(this, _memoryAllocator, _maxFileSize);
            lock (_sessionsLock)
            {
                _sessions.Add(session);
            }
            return session;
        }

        public void Dispose()
        {
            _mergedBlobFileWriter.Return(); // Return file, will dispose when counter is 0, might be others dependent on the file still
            
            if (_checkpointHandler != null)
            {
                _checkpointHandler.Dispose();
            }
            if (_adminSession != null)
            {
                _adminSession.Dispose();
            }
            
            foreach(var session in _sessions)
            {
                session.Dispose();
            }

            _temporaryPageLocations.Clear();
        }

        private async Task<ReservoirStreamsMetadata> ReadMetadata(string streamName, CancellationToken cancellationToken)
        {
            var pipe = await _fileProvider.ReadStreamsMetadataFileAsync(streamName, cancellationToken);

            if (pipe != null)
            {
                ReadResult readResult;
                do
                {
                    readResult = await pipe.ReadAsync();
                    pipe.AdvanceTo(readResult.Buffer.Start, readResult.Buffer.End);
                } while (!readResult.IsCompleted);
                
                var parsedMetadata = ParseMetadata(readResult.Buffer);
                await pipe.CompleteAsync();
                if (parsedMetadata != null)
                {
                    return parsedMetadata;
                }
            }
            return new ReservoirStreamsMetadata(streamName, new List<ReservoirStreamVersion>());
        }

        private ReservoirStreamsMetadata? ParseMetadata(ReadOnlySequence<byte> data)
        {
            var reader = new Utf8JsonReader(data);
            return JsonSerializer.Deserialize<ReservoirStreamsMetadata>(ref reader);
        }

        private async Task FetchAndUpdateMetadata(string streamName, string streamVersion, CancellationToken cancellationToken)
        {
            var storageMetadata = await ReadMetadata(streamName, cancellationToken);
            var existingIndex = storageMetadata.Versions.FindIndex(x => x.Version == streamVersion);
            if (existingIndex >= 0)
            {
                var existing = storageMetadata.Versions[existingIndex];
                storageMetadata.Versions.Remove(existing);
                storageMetadata.Versions.Add(existing.UpdateLastInitializeTime(DateTime.UtcNow));
            }
            else
            {
                storageMetadata.Versions.Add(new ReservoirStreamVersion(streamVersion, DateTime.UtcNow, DateTime.UtcNow));
            }
            _storageMetadata = storageMetadata;
            var metadataBytes = JsonSerializer.SerializeToUtf8Bytes(storageMetadata, new JsonSerializerOptions()
            {
                WriteIndented = true
            });
            await _fileProvider.WriteStreamsMetadataFileAsync(streamName, PipeReader.Create(new ReadOnlySequence<byte>(metadataBytes)), cancellationToken);
        }

        public async Task InitializeAsync(StorageInitializationMetadata metadata)
        {
            _meter = new Meter($"flowtide.{metadata.StreamName}.storage");

            if (_checkpointHandler != null)
            {
                await _checkpointHandler.DisposeAsync();
            }

            _checkpointHandler = new CheckpointHandler(_fileProvider, _memoryPool, _memoryAllocator, _blobStorageOptions.SnapshotCheckpointInterval);
            _adminSession = new ReservoirPersistentSession(this, _memoryAllocator, _maxFileSize);
            _temporaryPageLocations.Clear();

            string streamVersion = "default";
            if (metadata.StreamVersion != null)
            {
                if (string.IsNullOrEmpty(metadata.StreamVersion.Version))
                {
                    streamVersion = metadata.StreamVersion.Hash;
                }
                else
                {
                    streamVersion = metadata.StreamVersion.Version;
                }
            }
            _streamName = metadata.StreamName;
            _streamVersion = streamVersion;
            await _fileProvider.InitializeAsync(new StorageProviderContext(metadata.StreamName, streamVersion, _memoryAllocator), default);
            await FetchAndUpdateMetadata(metadata.StreamName, streamVersion, default);

            // CancellationToken needs to be added upstream
            await _checkpointHandler.RecoverToLatest(default);
            if (CacheProvider != null)
            {
                await CacheProvider.InitializeAsync(metadata, _meter, default);
            }
            else if (!(_fileProvider is MetricsFileStorageProvider))
            {
                // Add the metrics proxy here so we get metrics
                _fileProvider = new MetricsFileStorageProvider(_meter, _fileProvider);
            }
            
        }

        public async ValueTask RecoverAsync(long checkpointVersion)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before recovering");

            // CancellationToken needs to be added upstream
            await _checkpointHandler.RecoverTo(checkpointVersion, default);
        }

        public async ValueTask ResetAsync()
        {
            if (_checkpointHandler != null)
            {
                await _checkpointHandler.DisposeAsync();
            }
            _checkpointHandler = new CheckpointHandler(_fileProvider, _memoryPool, _memoryAllocator, _blobStorageOptions.SnapshotCheckpointInterval);
            _temporaryPageLocations.Clear();
            lock (_sessionsLock)
            {
                foreach(var session in _sessions)
                {
                    session.Reset();
                }
            }
        }
            
        public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value)
        {
            Debug.Assert(_checkpointHandler != null, "Persistent storage must be initialized before fetching values");

            if (_checkpointHandler.TryGetPageFileLocation(key, out var location))
            {
                value = _fileProvider.GetMemoryAsync(location.FileId, location.Offset, location.Size, location.Crc32).GetAwaiter().GetResult();
                return true;
            }
            value = null;
            return false;
        }

        public async ValueTask Write(long key, byte[] value)
        {
            Debug.Assert(_adminSession != null, "Persistent storage must be initialized before writing values");

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

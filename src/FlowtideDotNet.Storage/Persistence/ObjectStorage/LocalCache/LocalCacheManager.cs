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
using FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal;
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.IO.Hashing;
using System.IO.Pipelines;
using System.Threading.Channels;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.LocalCache
{
    internal class LocalCacheManager : IDisposable
    {
        private readonly BlobPersistentStorage storage;
        private readonly IFileStorageProvider _localCache;
        private readonly IFileStorageProvider _remoteStorage;
        private Meter? _meter;

        private readonly Dictionary<long, LinkedListNode<LocalCacheEntry>> _index = new();
        private readonly LinkedList<LocalCacheEntry> _lruList = new();
        private readonly object _syncRoot = new();

        private readonly ConcurrentDictionary<long, Task<LocalCacheEntry>> _pendingDownloads = new();
        private readonly Channel<DownloadJob> _downloadChannel;

        private readonly long _maxSizeBytes;
        private readonly LocalCacheMetricValues _metricValues;
        private long _currentSize;

        private long _cacheHits;
        private long _totalCacheTries;

        private readonly Queue<TaskCompletionSource> _spaceWaiters = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly Task[] _workers;
        private readonly Task _evictionTask;

        public long CurrentSize => _currentSize;

        private record DownloadJob(long FileId, TaskCompletionSource<LocalCacheEntry> Tcs);

        public LocalCacheManager(
            BlobPersistentStorage storage,
            IFileStorageProvider localCache,
            IFileStorageProvider remoteStorage,
            long maxSizeBytes,
            LocalCacheMetricValues metricValues,
            int workerCount = 2)
        {
            this.storage = storage;
            _localCache = localCache;
            _remoteStorage = remoteStorage;
            _maxSizeBytes = maxSizeBytes;
            this._metricValues = metricValues;
            _downloadChannel = Channel.CreateBounded<DownloadJob>(new BoundedChannelOptions(4)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

            _workers = Enumerable.Range(0, workerCount)
                .Select(_ => Task.Run(ProcessDownloadQueueAsync))
                .ToArray();

            _evictionTask = Task.Run(ProactiveEvictionLoopAsync);
        }

        public async Task InitializeAsync(StorageInitializationMetadata metadata, Meter meter, CancellationToken cancellationToken)
        {
            if (_meter == null)
            {
                _meter = meter;
                _meter.CreateObservableGauge<long>(
                    MetricNames.LocalCacheSizeBytes, 
                    () => new Measurement<long>(Interlocked.Read(ref _currentSize), _metricValues.TagList), 
                    "bytes", 
                    "Current size of the local cache in bytes");
                _meter.CreateObservableCounter<long>(
                    MetricNames.LocalCacheHits, 
                    () => new Measurement<long>(Interlocked.Read(ref _cacheHits), _metricValues.TagList), 
                    "hits", 
                    "Number of cache hits");
                _meter.CreateObservableCounter<long>(
                    MetricNames.LocalCacheTotalTries, 
                    () => new Measurement<long>(Interlocked.Read(ref _totalCacheTries), _metricValues.TagList), 
                    "tries", 
                    "Total number of cache tries");
            }
            lock (_syncRoot)
            {
                // We always clear local cache and refetch info to make sure it is updated with relevant info
                _index.Clear();
                _lruList.Clear();
            }

            // Fetch already existing data file ids
            var dataFileIds = await _localCache.GetStoredDataFileIdsAsync();
            
            // Go through them and seee if they should exist, if they should register them to the cache
            foreach(var dataFileId in dataFileIds)
            {
                if (storage.TryGetFileInformation(dataFileId, out var fileInfo))
                {
                    lock (_syncRoot)
                    {
                        var entry = new LocalCacheEntry(fileInfo.FileId, fileInfo.FileSize, fileInfo.Crc64, 0);

                        var node = new LinkedListNode<LocalCacheEntry>(entry);
                        _index[fileInfo.FileId] = node;
                        _lruList.AddLast(node);

                        Interlocked.Add(ref _currentSize, fileInfo.FileSize);
                    }
                }
                else
                {
                    // If the data file is not in the file information, remove it to clear up the cache
                    await _localCache.DeleteDataFileAsync(dataFileId);
                }
            }
        }

        public async ValueTask<ReadOnlyMemory<byte>> ReadMemoryAsync(long fileId, int offset, int length, uint crc32)
        {
            Interlocked.Increment(ref _totalCacheTries);
            if (TryGetCacheEntry(fileId, out var entry))
            {
                try
                {
                    entry.RecordAccess();
                    Interlocked.Increment(ref _cacheHits);
                    return await _localCache.GetMemoryAsync(fileId, offset, length, crc32);
                }
                catch (FlowtideChecksumMismatchException)
                {
                    // If there is a CRC missmatch, we delete the file once and try and redownload it, if that does not work, it will cause a crash
                    await EvictDataFileAsync(fileId);
                    return await ReadMemory_FetchSlow(fileId, offset, length, crc32);
                }
                finally
                {
                    if (entry.Return()) await HandlePhysicalDeletion(entry);
                }
            }

            return await ReadMemory_FetchSlow(fileId, offset, length, crc32);
        }

        private async ValueTask<ReadOnlyMemory<byte>> ReadMemory_FetchSlow(long fileId, int offset, int length, uint crc32)
        {
            var entry = await GetOrDownloadAsync(fileId);

            // Try and rent, if we cant, we need to try again, file was already evicted
            if (!entry.TryRent())
            {
                return await ReadMemory_FetchSlow(fileId, offset, length, crc32);
            }

            try
            {
                return await _localCache.GetMemoryAsync(fileId, offset, length, crc32);
            }
            finally
            {
                if (entry.Return()) await HandlePhysicalDeletion(entry);
            }
        }

        public async ValueTask<T> ReadAsync<T>(long fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            Interlocked.Increment(ref _totalCacheTries);
            if (TryGetCacheEntry(fileId, out var entry))
            {
                try
                {
                    entry.RecordAccess();
                    Interlocked.Increment(ref _cacheHits);
                    return await _localCache.ReadAsync(fileId, offset, length, crc32, stateSerializer);
                }
                catch (FlowtideChecksumMismatchException)
                {
                    // If there is a CRC missmatch, we delete the file once and try and redownload it, if that does not work, it will cause a crash
                    await EvictDataFileAsync(fileId);
                    return await ReadAsync_FetchSlow(fileId, offset, length, crc32, stateSerializer);
                }
                finally
                {
                    if (entry.Return()) await HandlePhysicalDeletion(entry);
                }
            }

            return await ReadAsync_FetchSlow(fileId, offset, length, crc32, stateSerializer);
        }

        private async ValueTask<T> ReadAsync_FetchSlow<T>(long fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            var entry = await GetOrDownloadAsync(fileId);

            // Try and rent, if we cant, we need to try again, file was already evicted
            if (!entry.TryRent())
            {
                return await ReadAsync_FetchSlow<T>(fileId, offset, length, crc32, stateSerializer);
            }

            try
            {
                return await _localCache.ReadAsync(fileId, offset, length, crc32, stateSerializer);
            }
            finally
            {
                if (entry.Return()) await HandlePhysicalDeletion(entry);
            }
        }

        private Task<LocalCacheEntry> GetOrDownloadAsync(long fileId)
        {
            return _pendingDownloads.GetOrAdd(fileId, id =>
            {
                var tcs = new TaskCompletionSource<LocalCacheEntry>(TaskCreationOptions.RunContinuationsAsynchronously);
                if (!_downloadChannel.Writer.TryWrite(new DownloadJob(id, tcs)))
                {
                    // If write failed, we add it asynchronously
                    _ = Task.Run(async () => await _downloadChannel.Writer.WriteAsync(new DownloadJob(id, tcs)));
                }
                return tcs.Task;
            });
        }

        public async Task EvictDataFileAsync(long fileId)
        {
            LocalCacheEntry? entry = null;
            lock (_syncRoot)
            {
                if (_index.Remove(fileId, out var node))
                {
                    _lruList.Remove(node);
                    entry = node.Value;
                }
            }

            if (entry != null)
            {
                if (entry.TryFinalizeForEviction())
                {
                    await HandlePhysicalDeletion(entry);
                }
            }
        }


        internal bool TryGetCacheEntry(long fileId, [NotNullWhen(true)] out LocalCacheEntry? entry)
        {
            lock (_syncRoot)
            {
                if (_index.TryGetValue(fileId, out var node))
                {
                    if (node.Value.TryRent())
                    {
                        _lruList.Remove(node);
                        _lruList.AddFirst(node);
                        entry = node.Value;
                        return true;
                    }
                    _index.Remove(fileId);
                    _lruList.Remove(node);
                }
            }
            entry = null;
            return false;
        }

        private async Task ProcessDownloadQueueAsync()
        {
            try
            {
                await foreach (var job in _downloadChannel.Reader.ReadAllAsync(_cts.Token))
                {
                    try
                    {
                        if (!storage.TryGetFileInformation(job.FileId, out var fileInfo))
                        {
                            throw new InvalidOperationException($"Tried to fetch file with id {job.FileId} that does not exist");
                        }

                        await EnsureSpaceAsync(fileInfo.FileSize);

                        _metricValues.AddPersistentRead();
                        _metricValues.AddPersistentBytesRead(fileInfo.FileSize);
                        var reader = await _remoteStorage.ReadDataFileAsync(job.FileId, fileInfo.FileSize, _cts.Token);
                        await _localCache.WriteDataFileAsync(job.FileId, fileInfo.Crc64, fileInfo.FileSize, reader, _cts.Token);

                        lock (_syncRoot)
                        {
                            var entry = new LocalCacheEntry(job.FileId, fileInfo.FileSize, fileInfo.Crc64, 1);
                            var node = new LinkedListNode<LocalCacheEntry>(entry);
                            _index[job.FileId] = node;
                            _lruList.AddFirst(node);
                            Interlocked.Add(ref _currentSize, fileInfo.FileSize);
                            job.Tcs.SetResult(entry);
                        }
                    }
                    catch (Exception ex)
                    {
                        job.Tcs.SetException(ex);
                    }
                    finally
                    {
                        _pendingDownloads.TryRemove(job.FileId, out _);
                    }
                }
            }
            catch (OperationCanceledException) { }
        }

        private async Task EnsureSpaceAsync(long requiredBytes)
        {
            while (true)
            {
                TaskCompletionSource? tcs = null;
                lock (_syncRoot)
                {
                    while (_currentSize + requiredBytes > _maxSizeBytes)
                    {
                        if (!TryEvictOneInternal())
                        {
                            tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                            _spaceWaiters.Enqueue(tcs);
                            break;
                        }
                    }
                    if (tcs == null) return;
                }
                await tcs.Task;
            }
        }

        private bool TryEvictOneInternal()
        {
            var current = _lruList.Last;
            while (current != null)
            {
                if (current.Value.CurrentWeight == 0 && current.Value.TryFinalizeForEviction())
                {
                    var entry = current.Value;
                    _index.Remove(entry.FileId);
                    _lruList.Remove(current);
                    _ = HandlePhysicalDeletion(entry);
                    return true;
                }
                current.Value.DecrementWeight();
                current = current.Previous;
            }
            return false;
        }

        private async Task HandlePhysicalDeletion(LocalCacheEntry entry)
        {
            await _localCache.DeleteDataFileAsync(entry.FileId);
            Interlocked.Add(ref _currentSize, -entry.Size);

            lock (_syncRoot)
            {
                if (_spaceWaiters.TryDequeue(out var tcs)) tcs.SetResult();
            }
        }

        private async Task ProactiveEvictionLoopAsync()
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                await Task.Delay(5000);
                if (Volatile.Read(ref _currentSize) > _maxSizeBytes * 0.8)
                {
                    lock (_syncRoot) { TryEvictOneInternal(); }
                }
            }
        }

        public async Task RegisterNewFileAsync(long fileId, ulong crc64, int fileSize, PipeReader reader)
        {
            await EnsureSpaceAsync(fileSize);
            await _localCache.WriteDataFileAsync(fileId, crc64, fileSize, reader);

            lock (_syncRoot)
            {
                var entry = new LocalCacheEntry(fileId, fileSize, crc64, 0);

                var node = new LinkedListNode<LocalCacheEntry>(entry);
                _index[fileId] = node;
                _lruList.AddFirst(node);

                Interlocked.Add(ref _currentSize, fileSize);
            }
        }


        public void Dispose()
        {
            _cts.Cancel();
            _downloadChannel.Writer.Complete();
            _evictionTask.Wait();
            Task.WaitAll(_workers);
        }
    }
}
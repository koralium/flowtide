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
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.StateManager.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.LocalCache
{
    internal class ConcurrentLocalCacheManager : IAsyncDisposable, IDisposable
    {
        private class CacheFileState
        {
            public ulong FileId { get; }
            public int Size { get; }
            public ulong ExpectedCrc64 { get; }

            private int _rentCount;
            private int _isEvicted;
            private int _isDeleted;
            private int _usageWeight;
            private int _downloadStarted;
            private int _sizeAllocated;

            public TaskCompletionSource DownloadTcs { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public CacheFileState(ulong fileId, int size, ulong expectedCrc64, int initialWeight = 0, int initialRent = 0)
            {
                FileId = fileId;
                Size = size;
                ExpectedCrc64 = expectedCrc64;
                _rentCount = initialRent;
                _isEvicted = 0;
                _isDeleted = 0;
                _usageWeight = initialWeight;
                _downloadStarted = 0;
                _sizeAllocated = 0;
            }

            public bool IsMarkedForEviction => Volatile.Read(ref _isEvicted) == 1;
            public int RentCount => Volatile.Read(ref _rentCount);
            public int CurrentWeight => Volatile.Read(ref _usageWeight);

            public void Rent() => Interlocked.Increment(ref _rentCount);

            private readonly TaskCompletionSource _deletionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public Task DeletionTask => _deletionTcs.Task;
            public void MarkDeletionCompleted() => _deletionTcs.TrySetResult();

            public bool Return()
            {
                int after = Interlocked.Decrement(ref _rentCount);
                return IsMarkedForEviction && after == 0;
            }

            public void RecordAccess()
            {
                int current = Volatile.Read(ref _usageWeight);
                while (current < 5)
                {
                    int old = Interlocked.CompareExchange(ref _usageWeight, current + 1, current);
                    if (old == current) break;
                    current = old;
                }
            }

            public bool DecrementWeight()
            {
                int current;
                do
                {
                    current = Volatile.Read(ref _usageWeight);
                    if (current <= 0) return true;
                } while (Interlocked.CompareExchange(ref _usageWeight, current - 1, current) != current);
                return false;
            }

            public bool TryMarkEvicted()
            {
                return Interlocked.CompareExchange(ref _isEvicted, 1, 0) == 0;
            }

            public bool TrySetDeleted()
            {
                return Interlocked.CompareExchange(ref _isDeleted, 1, 0) == 0;
            }

            public bool TryStartDownload()
            {
                return Interlocked.CompareExchange(ref _downloadStarted, 1, 0) == 0;
            }

            public bool TryAllocateSize()
            {
                return Interlocked.CompareExchange(ref _sizeAllocated, 1, 0) == 0;
            }

            public bool TryDeallocateSize()
            {
                return Interlocked.CompareExchange(ref _sizeAllocated, 0, 1) == 1;
            }
        }

        private readonly ReservoirPersistentStorage storage;
        private readonly IReservoirStorageProvider _localCache;
        private readonly IReservoirStorageProvider _remoteStorage;
        private Meter? _meter;
        private ILogger _logger;

        private readonly ConcurrentDictionary<ulong, CacheFileState> _fileStates = new();
        private readonly ConcurrentQueue<CacheFileState> _lruQueue = new();
        private readonly ConcurrentDictionary<ulong, Task> _backgroundTasks = new();

        private readonly long _maxSizeBytes;
        private readonly LocalCacheMetricValues _metricValues;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan _evictionInterval;
        private long _currentSize;

        private long _cacheHits;
        private long _totalCacheTries;

        private readonly object _spaceWaitRoot = new();
        private readonly Queue<TaskCompletionSource> _spaceWaiters = new();
        private readonly SemaphoreSlim _downloadSemaphore;
        
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _evictionTask;

        public long CurrentSize => Interlocked.Read(ref _currentSize);

        public ConcurrentLocalCacheManager(
            ReservoirPersistentStorage storage,
            IReservoirStorageProvider localCache,
            IReservoirStorageProvider remoteStorage,
            long maxSizeBytes,
            LocalCacheMetricValues metricValues,
            int workerCount = 2,
            TimeSpan? evictionInterval = default,
            TimeProvider? timeProvider = default)
        {
            _logger = NullLogger.Instance;
            this.storage = storage;
            _localCache = localCache;
            _remoteStorage = remoteStorage;
            _maxSizeBytes = maxSizeBytes;
            this._metricValues = metricValues;
            _evictionInterval = evictionInterval ?? TimeSpan.FromSeconds(5);
            _timeProvider = timeProvider ?? TimeProvider.System;
            
            _downloadSemaphore = new SemaphoreSlim(workerCount, workerCount);

            _evictionTask = Task.Run(ProactiveEvictionLoopAsync);
        }

        public async Task InitializeAsync(StorageInitializationMetadata metadata, Meter meter, StorageProviderContext storageProviderContext, CancellationToken cancellationToken)
        {
            _logger = metadata.LoggerFactory.CreateLogger<ConcurrentLocalCacheManager>();
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

            Interlocked.Exchange(ref _currentSize, 0);
            _fileStates.Clear();
            while (_lruQueue.TryDequeue(out _)) { }

            await _localCache.InitializeAsync(storageProviderContext, cancellationToken);
            
            var dataFileIds = await _localCache.GetStoredDataFileIdsAsync();

            foreach (var dataFileId in dataFileIds)
            {
                if (storage.TryGetFileInformation(dataFileId, out var fileInfo))
                {
                    var state = new CacheFileState(fileInfo.FileId, fileInfo.FileSize, fileInfo.Crc64, 0, 0);
                    state.TryStartDownload();
                    state.DownloadTcs.TrySetResult();

                    if (_fileStates.TryAdd(dataFileId, state))
                    {
                        if (state.TryAllocateSize())
                        {
                            Interlocked.Add(ref _currentSize, fileInfo.FileSize);
                        }
                        _lruQueue.Enqueue(state);
                    }
                }
                else
                {
                    await _localCache.DeleteDataFileAsync(dataFileId);
                }
            }
        }

        public async ValueTask<ReadOnlyMemory<byte>> ReadMemoryAsync(ulong fileId, int offset, int length, uint crc32)
        {
            Interlocked.Increment(ref _totalCacheTries);

            if (_fileStates.TryGetValue(fileId, out var quickState))
            {
                quickState.Rent();
                if (!quickState.IsMarkedForEviction && quickState.DownloadTcs.Task.IsCompletedSuccessfully)
                {
                    quickState.RecordAccess();
                    Interlocked.Increment(ref _cacheHits);
                    bool rentHeld = true;
                    try
                    {
                        return await _localCache.GetMemoryAsync(fileId, offset, length, crc32);
                    }
                    catch (FlowtideChecksumMismatchException)
                    {
                        rentHeld = false;
                        quickState.TryMarkEvicted();
                        if (quickState.Return() && quickState.TrySetDeleted())
                        {
                            await HandlePhysicalDeletion(quickState);
                        }
                        await quickState.DeletionTask;
                        _fileStates.TryRemove(new KeyValuePair<ulong, CacheFileState>(fileId, quickState));
                    }
                    finally
                    {
                        if (rentHeld && quickState.Return())
                        {
                            if (quickState.TrySetDeleted())
                            {
                                await HandlePhysicalDeletion(quickState);
                            }
                        }
                    }
                }
                else
                {
                    if (quickState.Return())
                    {
                        if (quickState.TrySetDeleted())
                        {
                            await HandlePhysicalDeletion(quickState);
                        }
                    }
                }
            }

            return await ReadMemory_FetchSlow(fileId, offset, length, crc32);
        }

        private async ValueTask<ReadOnlyMemory<byte>> ReadMemory_FetchSlow(ulong fileId, int offset, int length, uint crc32)
        {
            var state = await RentAndDownloadAsync(fileId);
            try
            {
                return await _localCache.GetMemoryAsync(fileId, offset, length, crc32);
            }
            finally
            {
                if (state.Return())
                {
                    if (state.TrySetDeleted())
                    {
                        await HandlePhysicalDeletion(state);
                    }
                }
            }
        }

        public async ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            Interlocked.Increment(ref _totalCacheTries);
            if (_fileStates.TryGetValue(fileId, out var quickState))
            {
                quickState.Rent();
                if (!quickState.IsMarkedForEviction && quickState.DownloadTcs.Task.IsCompletedSuccessfully)
                {
                    quickState.RecordAccess();
                    Interlocked.Increment(ref _cacheHits);
                    bool rentHeld = true;
                    try
                    {
                        return await _localCache.ReadAsync(fileId, offset, length, crc32, stateSerializer);
                    }
                    catch (FlowtideChecksumMismatchException)
                    {
                        rentHeld = false;
                        quickState.TryMarkEvicted();
                        if (quickState.Return())
                        {
                            if (quickState.TrySetDeleted())
                            {
                                await HandlePhysicalDeletion(quickState);
                            }
                        }
                        await quickState.DeletionTask;
                        _fileStates.TryRemove(new KeyValuePair<ulong, CacheFileState>(fileId, quickState));
                    }
                    finally
                    {
                        if (rentHeld && quickState.Return() && quickState.TrySetDeleted())
                        {
                            await HandlePhysicalDeletion(quickState);
                        }
                    }
                }
                else
                {
                    if (quickState.Return())
                    {
                        if (quickState.TrySetDeleted())
                        {
                            await HandlePhysicalDeletion(quickState);
                        }
                    }
                }
            }

            return await ReadAsync_FetchSlow(fileId, offset, length, crc32, stateSerializer);
        }

        private async ValueTask<T> ReadAsync_FetchSlow<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            var state = await RentAndDownloadAsync(fileId);
            try
            {
                return await _localCache.ReadAsync(fileId, offset, length, crc32, stateSerializer);
            }
            finally
            {
                if (state.Return())
                {
                    if (state.TrySetDeleted())
                    {
                        await HandlePhysicalDeletion(state);
                    }
                }
            }
        }

        public async ValueTask<PipeReader?> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _totalCacheTries);
            if (_fileStates.TryGetValue(fileId, out var state))
            {
                state.Rent();
                if (!state.IsMarkedForEviction && state.DownloadTcs.Task.IsCompletedSuccessfully)
                {
                    state.RecordAccess();
                    Interlocked.Increment(ref _cacheHits);
                    bool rentHeld = true;
                    try
                    {
                        return await _localCache.ReadDataFileAsync(fileId, fileSize, cancellationToken).ConfigureAwait(false);
                    }
                    catch (FlowtideChecksumMismatchException)
                    {
                        rentHeld = false;
                        state.TryMarkEvicted();
                        if (state.Return())
                        {
                            if (state.TrySetDeleted())
                            {
                                await HandlePhysicalDeletion(state);
                            }
                        }
                        await state.DeletionTask;
                        _fileStates.TryRemove(new KeyValuePair<ulong, CacheFileState>(fileId, state));
                        return default;
                    }
                    finally
                    {
                        if (rentHeld && state.Return())
                        {
                            if (state.TrySetDeleted())
                            {
                                await HandlePhysicalDeletion(state);
                            }
                        }
                    }
                }
                else
                {
                    if (state.Return() && state.TrySetDeleted())
                    {
                        await HandlePhysicalDeletion(state);
                    }
                }
            }
            return default;
        }

        private async ValueTask<CacheFileState> RentAndDownloadAsync(ulong fileId)
        {
            while (true)
            {
                if (storage.TryGetFileInformation(fileId, out var fileInfo))
                {
                    var newState = new CacheFileState(fileId, fileInfo.FileSize, fileInfo.Crc64, 1, 0);
                    var state = _fileStates.GetOrAdd(fileId, newState);
                    
                    if (ReferenceEquals(state, newState))
                    {
                        _lruQueue.Enqueue(state);
                    }

                    state.Rent();

                    if (state.IsMarkedForEviction)
                    {
                        if (state.Return())
                        {
                            if (state.TrySetDeleted())
                            {
                                await HandlePhysicalDeletion(state);
                            }
                        }
                        
                        await state.DeletionTask;
                        continue;
                    }

                    if (state.DownloadTcs.Task.IsCompleted)
                    {
                        if (state.DownloadTcs.Task.IsFaulted || state.DownloadTcs.Task.IsCanceled)
                        {
                            state.TryMarkEvicted();
                            if (state.Return())
                            {
                                if (state.TrySetDeleted())
                                {
                                    await HandlePhysicalDeletion(state);
                                }
                            }
   
                            await state.DeletionTask;
                            _fileStates.TryRemove(new KeyValuePair<ulong, CacheFileState>(fileId, state));
                            await state.DownloadTcs.Task; // always throws — exits the loop
                        }
                        else
                        {
                            return state;
                        }
                    }

                    bool isNewDownload = state.TryStartDownload();
                    if (isNewDownload)
                    {
                        var downloadTask = Task.Run(async () =>
                        {
                            try
                            {
                                await _downloadSemaphore.WaitAsync(_cts.Token);
                                try
                                {
                                    await EnsureSpaceAsync(state.Size);
                                    if (state.TryAllocateSize())
                                    {
                                        Interlocked.Add(ref _currentSize, state.Size);
                                    }

                                    _metricValues.AddPersistentRead();
                                    _metricValues.AddPersistentBytesRead(state.Size);

                                    var reader = await _remoteStorage.ReadDataFileAsync(state.FileId, state.Size, _cts.Token);
                                    try
                                    {
                                        await _localCache.WriteDataFileAsync(state.FileId, state.ExpectedCrc64, state.Size, false, reader, _cts.Token);
                                    }
                                    finally
                                    {
                                        await reader.CompleteAsync();
                                    }

                                    state.DownloadTcs.TrySetResult();
                                }
                                finally
                                {
                                    _downloadSemaphore.Release();
                                }
                            }
                            catch (Exception ex)
                            {
                                state.DownloadTcs.TrySetException(ex);

                                if (state.TryDeallocateSize())
                                {
                                    Interlocked.Add(ref _currentSize, -state.Size);
                                    WakeUpOneSpaceWaiter();
                                }
                                state.TryMarkEvicted();
                                if (state.RentCount == 0 && state.TrySetDeleted())
                                {
                                    await HandlePhysicalDeletion(state);
                                }
                                await state.DeletionTask;
                                _fileStates.TryRemove(new KeyValuePair<ulong, CacheFileState>(state.FileId, state));
                            }
                        });

                        _backgroundTasks[state.FileId] = downloadTask;
                        _ = downloadTask.ContinueWith(_ =>
                            _backgroundTasks.TryRemove(new KeyValuePair<ulong, Task>(state.FileId, downloadTask)));
                    }

                    try
                    {
                        await state.DownloadTcs.Task;

                        if (state.IsMarkedForEviction)
                        {
                            if (state.Return())
                            {
                                if (state.TrySetDeleted())
                                {
                                    await HandlePhysicalDeletion(state);
                                }
                            }
                            await state.DeletionTask;
                            continue;
                        }

                        return state;
                    }
                    catch
                    {
                        if (state.Return())
                        {
                            if (state.TrySetDeleted())
                            {
                                await HandlePhysicalDeletion(state);
                            }
                        }
                        throw;
                    }
                }
                else
                {
                    throw new InvalidOperationException($"Tried to fetch file with id {fileId} that does not exist");
                }
            }
        }

        public async Task EvictDataFileAsync(ulong fileId)
        {
            if (_fileStates.TryGetValue(fileId, out var state))
            {
                if (state.TryMarkEvicted())
                {
                    if (state.RentCount == 0)
                    {
                        if (state.TrySetDeleted())
                        {
                            await HandlePhysicalDeletion(state);
                            return;
                        }
                    }
                }
                await state.DeletionTask;
            }
        }

        private async Task EnsureSpaceAsync(long requiredBytes)
        {
            while (true)
            {
                if (Interlocked.Read(ref _currentSize) + requiredBytes <= _maxSizeBytes)
                {
                    return;
                }

                if (!await TryEvictOneAsync())
                {
                    // Overcommit check to avoid deadlocks
                    bool parked = false;
                    var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    lock (_spaceWaitRoot)
                    {
                        if (Interlocked.Read(ref _currentSize) + requiredBytes > _maxSizeBytes)
                        {
                            if (_lruQueue.Any(s => !s.IsMarkedForEviction && s.RentCount == 0))
                            {
                                _spaceWaiters.Enqueue(tcs);
                                parked = true;
                            }
                        }
                        else
                        {
                            tcs.TrySetResult();
                        }
                    }

                    if (!parked)
                    {
                        return;
                    }

                    var waitTask = tcs.Task;
                    try
                    {
                        await waitTask.WaitAsync(_cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        WakeUpOneSpaceWaiter();
                        throw;
                    }
                }
            }
        }

        private async ValueTask<bool> TryEvictOneAsync()
        {
            int attempts = _lruQueue.Count;
            for (int i = 0; i < attempts; i++)
            {
                if (_lruQueue.TryDequeue(out var state))
                {
                    if (state.IsMarkedForEviction) continue;

                    if (state.CurrentWeight > 0)
                    {
                        state.DecrementWeight();
                        _lruQueue.Enqueue(state);
                        continue;
                    }

                    if (state.RentCount == 0)
                    {
                        if (state.TryMarkEvicted())
                        {
                            if (state.RentCount == 0 && state.TrySetDeleted())
                            {
                                await HandlePhysicalDeletion(state);
                                return true;
                            }
                        }
                    }
                    else
                    {
                        _lruQueue.Enqueue(state);
                    }
                }
            }
            return false;
        }

        private async Task HandlePhysicalDeletion(CacheFileState state)
        {
            if (state.TryDeallocateSize())
            {
                Interlocked.Add(ref _currentSize, -state.Size);
                WakeUpOneSpaceWaiter();
            }

            try
            {
                await _localCache.DeleteDataFileAsync(state.FileId);
            }
            catch(Exception e)
            {
                _logger.LogError(e, $"Failed to delete local cache file with id {state.FileId}, continuing since its not critical to delete immediately but should be cleaned up eventually");
            }

            _fileStates.TryRemove(new KeyValuePair<ulong, CacheFileState>(state.FileId, state));

            state.MarkDeletionCompleted();
        }

        private void WakeUpOneSpaceWaiter()
        {
            lock (_spaceWaitRoot)
            {
                if (_spaceWaiters.TryDequeue(out var tcs))
                {
                    tcs.TrySetResult();
                }
            }
        }

        private async Task ProactiveEvictionLoopAsync()
        {
            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    await Task.Delay(_evictionInterval, _timeProvider, _cts.Token);
                    
                    while (Interlocked.Read(ref _currentSize) > _maxSizeBytes * 0.8)
                    {
                        if (!await TryEvictOneAsync())
                        {
                            break;
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
        }

        public async Task RegisterNewFileAsync(ulong fileId, ulong crc64, int fileSize, PipeReader reader)
        {
            var state = new CacheFileState(fileId, fileSize, crc64, 0, 0);
            state.TryStartDownload();

            await EnsureSpaceAsync(fileSize);
            if (state.TryAllocateSize())
            {
                Interlocked.Add(ref _currentSize, fileSize);
            }

            try
            {
                await _localCache.WriteDataFileAsync(fileId, crc64, fileSize, false, reader);
            }
            catch
            {
                if (state.TryDeallocateSize())
                {
                    Interlocked.Add(ref _currentSize, -state.Size);
                    WakeUpOneSpaceWaiter();
                }
                try { await _localCache.DeleteDataFileAsync(fileId); } catch { }
                throw;
            }

            state.DownloadTcs.TrySetResult();

            // Use TryAdd so that a concurrent registration for the same fileId doesn't silently overwrite an existing entry,
            // which would double-count _currentSize.
            if (!_fileStates.TryAdd(fileId, state))
            {
                // Another thread already registered this file; undo the size we just claimed.
                if (state.TryDeallocateSize())
                {
                    Interlocked.Add(ref _currentSize, -state.Size);
                    WakeUpOneSpaceWaiter();
                }
                return;
            }
            _lruQueue.Enqueue(state);
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            try { await _evictionTask.ConfigureAwait(false); } catch { }
            try { await Task.WhenAll(_backgroundTasks.Values).ConfigureAwait(false); } catch { }
            _cts.Dispose();
            _downloadSemaphore.Dispose();
        }

        public void Dispose()
        {
            _cts.Cancel();
            try { _evictionTask.Wait(); } catch { }
            try { Task.WaitAll(_backgroundTasks.Values.ToArray()); } catch { }

            _cts.Dispose();
            _downloadSemaphore.Dispose();
        }
    }
}

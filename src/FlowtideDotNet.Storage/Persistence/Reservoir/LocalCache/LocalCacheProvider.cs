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
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.LocalCache
{
    internal class LocalCacheProvider : IReservoirStorageProvider
    {
        private readonly IReservoirStorageProvider _remoteStorage;
        private LocalCacheManager _localCacheManager;
        private LocalCacheMetricValues _metricValues;
        private Meter? _meter;
        private TagList _metricTagList;
        private ObservableCounter<long>? _persistentReadCounter;
        private ObservableCounter<long>? _persistentWriteCounter;
        private ObservableCounter<long>? _persistentDeleteCounter;
        private Histogram<long>? _persistentBytesRead;
        private Histogram<long>? _persistentBytesWritten;

        public LocalCacheProvider(
            ReservoirPersistentStorage blobPersistentStorage, 
            IReservoirStorageProvider localCache, 
            IReservoirStorageProvider remoteStorage,
            long maxCacheSizeBytes)
        {
            _metricValues = new LocalCacheMetricValues();
            _localCacheManager = new LocalCacheManager(blobPersistentStorage, localCache, remoteStorage, maxCacheSizeBytes, _metricValues);
            _remoteStorage = remoteStorage;
        }

        public long CurrentSize => _localCacheManager.CurrentSize;

        public bool SupportsDataFileListing => _remoteStorage.SupportsDataFileListing;

        /// <summary>
        /// Initialize the local cache, this includes listing data files from disk and adding them to the cache.
        /// This allows the local cache to be reused even after a crash
        /// </summary>
        /// <returns></returns>
        public Task InitializeAsync(StorageInitializationMetadata metadata, Meter meter, CancellationToken cancellationToken)
        {
            if (_meter == null)
            {
                _meter = meter;
                _metricTagList.Add("stream", metadata.StreamName);
                _persistentReadCounter = _meter.CreateObservableCounter<long>(
                    MetricNames.PersistentStorageNumberOfReads, 
                    () => new Measurement<long>(_metricValues.PersistentReadCount, _metricTagList), 
                    description: "Number of reads to persistent storage");
                _persistentWriteCounter = _meter.CreateObservableCounter<long>(
                    MetricNames.PersistentStorageNumberOfWrites, 
                    () => new Measurement<long>(_metricValues.PersistentWriteCount, _metricTagList), 
                    description: "Number of writes to persistent storage");
                _persistentDeleteCounter = _meter.CreateObservableCounter<long>(
                    MetricNames.PersistentStorageNumberOfDeletes, 
                    () => new Measurement<long>(_metricValues.PersistentDeleteCount, _metricTagList), 
                    description: "Number of deletes to persistent storage");
                _metricValues.SetTagList(_metricTagList);
                _persistentBytesRead = _meter.CreateHistogram<long>(MetricNames.PersistentStorageDataBytesRead, "bytes");
                _metricValues.SetPersistentBytesReadHistogram(_persistentBytesRead);
                _persistentBytesWritten = _meter.CreateHistogram<long>(MetricNames.PersistentStorageDataBytesWritten, "bytes");
                _metricValues.SetPersistentBytesWrittenHistogram(_persistentBytesWritten);
            }
            return _localCacheManager.InitializeAsync(metadata, meter, cancellationToken);
        }

        public Task DeleteCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            _metricValues.AddPersistentDelete();
            return _remoteStorage.DeleteCheckpointFileAsync(checkpointVersion);
        }

        /// <summary>
        /// Used for testing
        /// </summary>
        /// <param name="fileId"></param>
        /// <returns></returns>
        internal Task EvictDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            return _localCacheManager.EvictDataFileAsync(fileId);
        }

        public async Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            _metricValues.AddPersistentDelete();
            await _localCacheManager.EvictDataFileAsync(fileId);
            await _remoteStorage.DeleteDataFileAsync(fileId);
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
            return _localCacheManager.ReadMemoryAsync(fileId, offset, length, crc32);
        }

        public ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default) where T : ICacheObject
        {
            return _localCacheManager.ReadAsync(fileId, offset, length, crc32, stateSerializer);
        }

        public Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default)
        {
            _metricValues.AddPersistentRead();
            return _remoteStorage.ReadCheckpointFileAsync(checkpointVersion);
        }

        public Task<PipeReader?> ReadCheckpointRegistryFileAsync(CancellationToken cancellationToken = default)
        {
            _metricValues.AddPersistentRead();
            return _remoteStorage.ReadCheckpointRegistryFileAsync();
        }

        public Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
        {
            _metricValues.AddPersistentRead();
            _metricValues.AddPersistentBytesRead(fileSize);
            // Fix later to read from cache also
            // Should probably have a try read from cache, if its not in cache, just skip it and read directly from remote
            // Since this method is only called on compactions
            return _remoteStorage.ReadDataFileAsync(fileId, fileSize);
        }

        public Task WriteCheckpointFileAsync(CheckpointVersion checkpointVersion, PipeReader data, CancellationToken cancellationToken = default)
        {
            _metricValues.AddPersistentWrite();
            return _remoteStorage.WriteCheckpointFileAsync(checkpointVersion, data);
        }

        public Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default)
        {
            _metricValues.AddPersistentWrite();
            return _remoteStorage.WriteCheckpointRegistryFile(data);
        }

        public async Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundle, PipeReader data, CancellationToken cancellationToken = default)
        {
            _metricValues.AddPersistentWrite();
            _metricValues.AddPersistentBytesWritten(size);
            await _localCacheManager.RegisterNewFileAsync(fileId, crc64, size, data);
            data.CancelPendingRead(); // Cancel pending read is implemented in the file readers to reset to start, this is a special case for cache
            await _remoteStorage.WriteDataFileAsync(fileId, crc64, size, isBundle, data);
        }

        public Task<IEnumerable<ulong>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public Task<IEnumerable<ulong>> ListDataFilesAboveVersionAsync(ulong minVersion, CancellationToken cancellationToken = default)
        {
            return _remoteStorage.ListDataFilesAboveVersionAsync(minVersion);
        }

        public Task InitializeAsync(string streamVersion, CancellationToken cancellationToken = default)
        {
            return _remoteStorage.InitializeAsync(streamVersion, cancellationToken);
        }

        public Task<PipeReader?> ReadStreamsMetadataFileAsync(CancellationToken cancellationToken = default)
        {
            return _remoteStorage.ReadStreamsMetadataFileAsync(cancellationToken);
        }

        public Task WriteStreamsMetadataFileAsync(PipeReader data, CancellationToken cancellationToken = default)
        {
            return _remoteStorage.WriteStreamsMetadataFileAsync(data, cancellationToken);
        }
    }
}

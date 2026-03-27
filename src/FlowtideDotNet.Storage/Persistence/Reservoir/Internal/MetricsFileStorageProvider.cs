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

using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    internal class MetricsFileStorageProvider : IReservoirStorageProvider
    {
        private readonly IReservoirStorageProvider _internalProvider;
        private long _numberOfReads;
        private long _numberOfWrites;
        private long _numberOfDeletes;
        private Histogram<long> _numberOfDataBytesRead;
        private Histogram<long> _numberOfDataBytesWritten;

        public bool SupportsFileListing => _internalProvider.SupportsFileListing;

        private void AddRead()
        {
            Interlocked.Increment(ref _numberOfReads);
        }

        private void AddWrite()
        {
            Interlocked.Increment(ref _numberOfWrites);
        }

        private void AddDelete()
        {
            Interlocked.Increment(ref _numberOfDeletes);
        }

        public MetricsFileStorageProvider(Meter meter, IReservoirStorageProvider internalProvider)
        {
            _internalProvider = internalProvider;
            meter.CreateObservableCounter<long>(MetricNames.PersistentStorageNumberOfReads, () => Interlocked.Read(ref _numberOfReads));
            meter.CreateObservableCounter<long>(MetricNames.PersistentStorageNumberOfWrites, () => Interlocked.Read(ref _numberOfWrites));
            meter.CreateObservableCounter<long>(MetricNames.PersistentStorageNumberOfDeletes, () => Interlocked.Read(ref _numberOfDeletes));
            _numberOfDataBytesRead = meter.CreateHistogram<long>(MetricNames.PersistentStorageDataBytesRead, "bytes");
            _numberOfDataBytesWritten = meter.CreateHistogram<long>(MetricNames.PersistentStorageDataBytesWritten, "bytes");
        }

        public Task DeleteCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
        {
            AddDelete();
            return _internalProvider.DeleteCheckpointFileAsync(checkpointVersion, cancellationToken);
        }

        public Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
        {
            AddDelete();
            return _internalProvider.DeleteDataFileAsync(fileId, cancellationToken);
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default)
        {
            AddRead();
            return _internalProvider.GetMemoryAsync(fileId, offset, length, crc32, cancellationToken);
        }

        public Task<IEnumerable<ulong>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default)
        {
            // List data files is only used by local cache, so no metrics required here
            return _internalProvider.GetStoredDataFileIdsAsync(cancellationToken);
        }

        public ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default) where T : ICacheObject
        {
            AddRead();
            return _internalProvider.ReadAsync<T>(fileId, offset, length, crc32, stateSerializer, cancellationToken);
        }

        public Task<PipeReader> ReadCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
        {
            AddRead();
            return _internalProvider.ReadCheckpointFileAsync(checkpointVersion, cancellationToken);
        }

        public Task<PipeReader?> ReadCheckpointRegistryFileAsync(CancellationToken cancellationToken = default)
        {
            AddRead();
            return _internalProvider.ReadCheckpointRegistryFileAsync(cancellationToken);
        }

        public Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
        {
            AddRead();
            _numberOfDataBytesRead.Record(fileSize);
            return _internalProvider.ReadDataFileAsync(fileId, fileSize, cancellationToken);
        }

        public Task WriteCheckpointFileAsync(CheckpointId checkpointVersion, PipeReader data, CancellationToken cancellationToken = default)
        {
            AddWrite();
            return _internalProvider.WriteCheckpointFileAsync(checkpointVersion, data, cancellationToken);
        }

        public Task<IEnumerable<CheckpointId>> ListCheckpointFilesAsync(CancellationToken cancellationToken = default)
        {
            return _internalProvider.ListCheckpointFilesAsync(cancellationToken);
        }

        public Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default)
        {
            AddWrite();
            return _internalProvider.WriteCheckpointRegistryFile(data, cancellationToken);
        }

        public Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundled, PipeReader data, CancellationToken cancellationToken = default)
        {
            AddWrite();
            _numberOfDataBytesWritten.Record(size);
            return _internalProvider.WriteDataFileAsync(fileId, crc64, size, isBundled, data, cancellationToken);
        }

        public Task<IEnumerable<ulong>> ListDataFilesAboveVersionAsync(ulong minVersion, CancellationToken cancellationToken = default)
        {
            return _internalProvider.ListDataFilesAboveVersionAsync(minVersion);
        }

        public Task InitializeAsync(StorageProviderContext providerContext, CancellationToken cancellationToken = default)
        {
            return _internalProvider.InitializeAsync(providerContext, cancellationToken);
        }

        public Task<PipeReader?> ReadStreamsMetadataFileAsync(string streamName, CancellationToken cancellationToken = default)
        {
            return _internalProvider.ReadStreamsMetadataFileAsync(streamName, cancellationToken);
        }

        public Task WriteStreamsMetadataFileAsync(string streamName, PipeReader data, CancellationToken cancellationToken = default)
        {
            return _internalProvider.WriteStreamsMetadataFileAsync(streamName, data);
        }

        public Task DeleteStreamVersionAsync(string streamName, string streamVersion, CancellationToken cancellationToken = default)
        {
            return _internalProvider.DeleteStreamVersionAsync(streamName, streamVersion, cancellationToken);
        }
    }
}

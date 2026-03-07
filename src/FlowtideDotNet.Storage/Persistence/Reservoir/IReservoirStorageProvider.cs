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
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir
{
    public interface IReservoirStorageProvider
    {
        bool SupportsDataFileListing { get; }

        /// <summary>
        /// Fetches streams metadata file that contains information of all streams and their versions.
        /// Usually its only one stream and its versions (such as different plan hashes etc).
        /// This is used to determine of older versions of the stream exist and if so what are their versions, so they can be deleted if needed.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>A pipe reader to read the metadata file, or null if it does not exist</returns>
        Task<PipeReader?> ReadStreamsMetadataFileAsync(string streamName, CancellationToken cancellationToken = default);

        /// <summary>
        /// Writes a streams metadata file that contains information of all streams and their versions.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        Task WriteStreamsMetadataFileAsync(string streamName, PipeReader data, CancellationToken cancellationToken = default);

        Task DeleteStreamVersionAsync(string streamName, string streamVersion, CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously performs any necessary initialization.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the initialization operation.</param>
        /// <returns>A task that represents the asynchronous initialization operation.</returns>
        Task InitializeAsync(StorageProviderContext providerContext, CancellationToken cancellationToken = default);

        Task<IEnumerable<ulong>> ListDataFilesAboveVersionAsync(ulong minVersion, CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously reads the checkpoint registry file and returns a PipeReader for its contents.
        /// </summary>
        /// <returns>A PipeReader representing the contents of the checkpoint registry file, or null if the file does not exist
        /// </returns>
        Task<PipeReader?> ReadCheckpointRegistryFileAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Writes a checkpoint registry file using the provided data stream.
        /// </summary>
        /// <param name="data">A <see cref="PipeReader"/> containing the data to be written to the checkpoint registry file. The reader
        /// must be positioned at the start of the data to be written.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        Task WriteCheckpointRegistryFile(PipeReader data, CancellationToken cancellationToken = default);

        /// <summary>
        /// Used in local cache, returns the data file ids currently on disk.
        /// It is used to populate the cache list after a restart so the files do not need to be redownloaded if they already exist.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<IEnumerable<ulong>> GetStoredDataFileIdsAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously retrieves a list of checkpoint versions available in the storage.
        /// </summary>
        /// <returns></returns>
        //Task<IEnumerable<CheckpointVersion>> ListCheckpointVersionsAsync();

        /// <summary>
        /// Asynchronously reads the checkpoint file corresponding to the specified checkpoint version and returns a PipeReader 
        /// that can be used to read the contents of the checkpoint file.
        /// </summary>
        /// <param name="checkpointVersion">The checkpoint version to read</param>
        /// <returns>A PipeReader that can be used to read the contents of the checkpoint file.</returns>
        Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously writes the contents of the specified PipeReader to a checkpoint file corresponding to the given checkpoint version.
        /// </summary>
        /// <param name="checkpointVersion">Checkpoint version</param>
        /// <param name="data">The PipeReader that provides the data to write to the checkpoint file.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        Task WriteCheckpointFileAsync(CheckpointVersion checkpointVersion, PipeReader data, CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously deletes the checkpoint file corresponding to the specified checkpoint version from the storage.
        /// </summary>
        /// <param name="checkpointVersion">The checkpoint version to delete.</param>
        /// <returns></returns>
        Task DeleteCheckpointFileAsync(CheckpointVersion checkpointVersion, CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously writes the contents of the specified PipeReader to a file corresponding to the given file ID.
        /// </summary>
        /// <param name="fileId"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        Task WriteDataFileAsync(ulong fileId, ulong crc64, int size, bool isBundled, PipeReader data, CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously deletes the file corresponding to the given file ID from the storage. 
        /// </summary>
        /// <param name="fileId">The fileId to delete</param>
        /// <returns>A task that completes whwen the file has been deleted.</returns>
        Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously reads the file corresponding to the given file ID and returns a PipeReader that can be used to read the contents of the file.
        /// </summary>
        /// <param name="fileId"></param>
        /// <returns></returns>
        Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default);

        /// <summary>
        /// Reads a serialized object of type T from the specified file segment using the provided state serializer.
        /// </summary>
        /// <typeparam name="T">The type of object to read. Must implement ICacheObject.</typeparam>
        /// <param name="fileId">The unique identifier of the file from which to read the object.</param>
        /// <param name="offset">The zero-based byte offset in the file at which to begin reading.</param>
        /// <param name="length">The number of bytes to read from the file, starting at the specified offset. Must be non-negative.</param>
        /// <param name="stateSerializer">The serializer used to deserialize the object from the file segment. Cannot be null.</param>
        /// <returns>A ValueTask that represents the asynchronous read operation. The result contains the deserialized object of
        /// type T.</returns>
        ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default) where T : ICacheObject;

        /// <summary>
        /// Asynchronously retrieves a read-only block of memory containing a specified range of bytes from the file
        /// identified by the given file ID.
        /// </summary>
        /// <param name="fileId">The unique identifier of the file from which to read the memory segment.</param>
        /// <param name="offset">The zero-based byte offset in the file at which to begin reading. Must be greater than or equal to 0.</param>
        /// <param name="length">The number of bytes to read from the file. Must be greater than or equal to 0.</param>
        /// <returns>A value task that represents the asynchronous operation. The result contains a read-only memory region with
        /// the requested bytes.</returns>
        ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(ulong fileId, int offset, int length, uint crc32, CancellationToken cancellationToken = default);
    }
}

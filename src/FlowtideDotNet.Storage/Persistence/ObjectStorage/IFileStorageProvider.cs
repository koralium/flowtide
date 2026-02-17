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

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    public interface IFileStorageProvider
    {
        /// <summary>
        /// Asynchronously retrieves a list of checkpoint versions available in the storage.
        /// </summary>
        /// <returns></returns>
        Task<IEnumerable<CheckpointVersion>> ListCheckpointVersionsAsync();

        /// <summary>
        /// Asynchronously reads the checkpoint file corresponding to the specified checkpoint version and returns a PipeReader 
        /// that can be used to read the contents of the checkpoint file.
        /// </summary>
        /// <param name="checkpointVersion">The checkpoint version to read</param>
        /// <returns>A PipeReader that can be used to read the contents of the checkpoint file.</returns>
        Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion);

        /// <summary>
        /// Asynchronously writes the contents of the specified PipeReader to a checkpoint file corresponding to the given checkpoint version.
        /// </summary>
        /// <param name="checkpointVersion">Checkpoint version</param>
        /// <param name="data">The PipeReader that provides the data to write to the checkpoint file.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        Task WriteCheckpointFileAsync(CheckpointVersion checkpointVersion, PipeReader data);

        /// <summary>
        /// Asynchronously deletes the checkpoint file corresponding to the specified checkpoint version from the storage.
        /// </summary>
        /// <param name="checkpointVersion">The checkpoint version to delete.</param>
        /// <returns></returns>
        Task DeleteCheckpointFileAsync(CheckpointVersion checkpointVersion);

        /// <summary>
        /// Asynchronously writes the contents of the specified PipeReader to a file corresponding to the given file ID.
        /// </summary>
        /// <param name="fileId"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        Task WriteDataFileAsync(long fileId, PipeReader data);

        /// <summary>
        /// Asynchronously deletes the file corresponding to the given file ID from the storage. 
        /// </summary>
        /// <param name="fileId">The fileId to delete</param>
        /// <returns>A task that completes whwen the file has been deleted.</returns>
        Task DeleteDataFileAsync(long fileId);

        /// <summary>
        /// Asynchronously reads the file corresponding to the given file ID and returns a PipeReader that can be used to read the contents of the file.
        /// </summary>
        /// <param name="fileId"></param>
        /// <returns></returns>
        Task<PipeReader> ReadDataFileAsync(long fileId);

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
        ValueTask<T> ReadAsync<T>(long fileId, int offset, int length, IStateSerializer<T> stateSerializer) where T : ICacheObject;

        /// <summary>
        /// Asynchronously retrieves a read-only block of memory containing a specified range of bytes from the file
        /// identified by the given file ID.
        /// </summary>
        /// <param name="fileId">The unique identifier of the file from which to read the memory segment.</param>
        /// <param name="offset">The zero-based byte offset in the file at which to begin reading. Must be greater than or equal to 0.</param>
        /// <param name="length">The number of bytes to read from the file. Must be greater than or equal to 0.</param>
        /// <returns>A value task that represents the asynchronous operation. The result contains a read-only memory region with
        /// the requested bytes.</returns>
        ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(long fileId, int offset, int length);
    }
}

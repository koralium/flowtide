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
        /// Asynchronously retrieves the names of all files in the specified directory.
        /// </summary>
        /// <param name="path">The path to the directory whose file names are to be listed.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains an enumerable collection of file
        /// names in the specified directory. The collection is empty if the directory contains no files.</returns>
        public Task<IEnumerable<string>> ListFilesAsync(string path);

        /// <summary>
        /// Opens a file at the specified path for reading and returns a PipeReader that can be used to read the contents of the file.
        /// </summary>
        /// <param name="path">Path to read at</param>
        /// <returns>A pipe reader to read the contents of the file</returns>
        public PipeReader OpenReadFile(string path);

        /// <summary>
        /// Asynchronously writes the contents of the specified PipeReader to a file at the given path.
        /// </summary>
        /// <param name="data">The PipeReader that provides the data to write to the file. The reader is read until completion.</param>
        /// <param name="path">The path of the file to which the data will be written. Cannot be null or empty.</param>
        /// <returns>A ValueTask that represents the asynchronous write operation.</returns>
        public ValueTask WriteFile(PipeReader data, string path);

        /// <summary>
        /// Reads a serialized object of type T from the specified path, starting at the given offset and reading the
        /// specified number of bytes.
        /// </summary>
        /// <remarks>
        /// This method allows a provider to utilize internal buffers and do not return its internal memory only the deserialized object.
        /// This can allow better utilization of memory and reduce the need for copying data when deserializing objects from storage.
        /// </remarks>
        /// <typeparam name="T">The type of the object to read. Must implement ICacheObject.</typeparam>
        /// <param name="path">The path from which to read the serialized data. Cannot be null or empty.</param>
        /// <param name="offset">The zero-based byte offset at which to begin reading from the path. Must be non-negative.</param>
        /// <param name="length">The number of bytes to read from the specified offset. Must be greater than zero.</param>
        /// <param name="stateSerializer">The serializer used to deserialize the object of type T. Cannot be null.</param>
        /// <returns>A ValueTask that represents the asynchronous read operation. The result contains the deserialized object of
        /// type T.</returns>
        ValueTask<T> Read<T>(string path, int offset, int length, IStateSerializer<T> stateSerializer) where T : ICacheObject;

        ReadOnlyMemory<byte> GetMemory(string path, int offset, int length);

        Task DeleteFile(string path);
    }
}

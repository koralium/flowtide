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

using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Storage.Persistence
{
    public interface IPersistentStorage : IDisposable
    {
        /// <summary>
        /// Returns the current version of the persistent storage
        /// </summary>
        long CurrentVersion { get; }

        /// <summary>
        /// Initializes the persistent store
        /// </summary>
        /// <returns></returns>
        Task InitializeAsync();

        /// <summary>
        /// Create a session for the persitent storage
        /// </summary>
        /// <returns></returns>
        IPersistentStorageSession CreateSession();

        /// <summary>
        /// Checkpoint the storage
        /// </summary>
        /// <returns></returns>
        ValueTask CheckpointAsync(byte[] metadata);

        /// <summary>
        /// Compacts the data, removes old versions of data that are no longer in use
        /// </summary>
        /// <returns></returns>
        ValueTask CompactAsync();

        /// <summary>
        /// Reset the store to an empty state
        /// </summary>
        /// <returns></returns>
        ValueTask ResetAsync();

        /// <summary>
        /// Recover the store to a specific checkpoint version
        /// </summary>
        /// <param name="checkpointVersion">Version to recover to.</param>
        /// <returns></returns>
        ValueTask RecoverAsync(long checkpointVersion);

        /// <summary>
        /// Used by administrative operations to look up metadata.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool TryGetValue(long key, [NotNullWhen(true)] out byte[]? value);

        /// <summary>
        /// Used by administrative operations to write metadata.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        ValueTask Write(long key, byte[] value);
    }
}

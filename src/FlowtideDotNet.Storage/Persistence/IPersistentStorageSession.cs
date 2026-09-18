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

namespace FlowtideDotNet.Storage.Persistence
{
    public interface IPersistentStorageSession : IDisposable
    {
        /// <summary>
        /// True when reads may overlap a write, delete or commit on this session. The caller
        /// still serializes writes, deletes and commits with each other. When false, the
        /// state client serializes all session calls, including reads.
        /// </summary>
        bool SupportsConcurrentReads => false;

        ValueTask<T> Read<T>(long key, IStateSerializer<T> stateSerializer)
            where T : ICacheObject;

        ValueTask<ReadOnlyMemory<byte>> Read(long key);

        Task Write(long key, SerializableObject value);

        Task Delete(long key);

        /// <summary>
        /// Publishes this session's writes to the storage's next checkpoint. Durability is
        /// established by <see cref="IPersistentStorage.CheckpointAsync"/>.
        /// </summary>
        Task Commit();
    }
}

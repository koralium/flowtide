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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.FileCache
{
    public interface IFileCache : IDisposable
    {
        /// <summary>
        /// Called by eviction threads to write a page to the file cache.
        /// This is sync because it is called from a thread pool thread and requires locking of the object.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="serializableObject"></param>
        void Write(long id, SerializableObject serializableObject);

        ValueTask<ReadOnlyMemory<byte>> Read(long pageKey);

        ValueTask<T> Read<T>(long pageKey, IStateSerializer<T> serializer)
            where T : ICacheObject;

        /// <summary>
        /// Called when a state client wants to free/remove a page from the file cache.
        /// </summary>
        /// <param name="pageKey"></param>
        void Free(in long pageKey);

        /// <summary>
        /// Called when a state client wants to clear all its keys.
        /// If one cache exist per client, then the keys does not need to be handled and
        /// the entire cache can be cleared.
        /// </summary>
        /// <param name="keys"></param>
        void FreeAll(IEnumerable<long> keys);

        /// <summary>
        /// Called at the end of an eviction of a state client
        /// </summary>
        void Flush();

        /// <summary>
        /// Called when the stream has not received any data for a while.
        /// Allows a file cache to remove any temporary allocations.
        /// </summary>
        void ClearTemporaryAllocations();
    }
}

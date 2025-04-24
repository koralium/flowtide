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
        void Write(long id, SerializableObject serializableObject);

        ReadOnlyMemory<byte> Read(long pageKey);

        ValueTask<T> Read<T>(long pageKey, IStateSerializer<T> serializer)
            where T : ICacheObject;

        void Free(in long pageKey);

        /// <summary>
        /// Called when a state client wants to clear all its keys.
        /// If one cache exist per client, then the keys does not need to be handled and
        /// the entire cache can be cleared.
        /// </summary>
        /// <param name="keys"></param>
        void FreeAll(IEnumerable<long> keys);

        void Flush();

        void ClearTemporaryAllocations();
    }
}

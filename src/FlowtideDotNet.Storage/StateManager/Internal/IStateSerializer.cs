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

using System.Buffers;

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    public interface IStateSerializer : IDisposable
    {
        void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value);

        /// <summary>
        /// Called when there has been no activity in a while on the stream.
        /// Allows clearing of temporary memory structures to reduce fragmentation.
        /// </summary>
        void ClearTemporaryAllocations();

        ICacheObject DeserializeCacheObject(ReadOnlyMemory<byte> bytes, int length);

        Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata)
            where TMetadata : IStorageMetadata;

        Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata)
            where TMetadata : IStorageMetadata;
    }
    public interface IStateSerializer<T> : IStateSerializer
        where T: ICacheObject
    {
        void Serialize(in IBufferWriter<byte> bufferWriter, in T value);

        T Deserialize(ReadOnlyMemory<byte> bytes, int length);
    }
}

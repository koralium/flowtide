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
    internal interface IStateSerializer
    {
        void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value, in StateSerializeOptions stateSerializeOptions);

        ICacheObject DeserializeCacheObject(IMemoryOwner<byte> bytes, int length, StateSerializeOptions stateSerializeOptions);

        Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata)
            where TMetadata : IStorageMetadata;

        Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata)
            where TMetadata : IStorageMetadata;
    }
    internal interface IStateSerializer<T> : IStateSerializer
        where T: ICacheObject
    {
        void Serialize(in IBufferWriter<byte> bufferWriter, in T value, in StateSerializeOptions stateSerializeOptions);

        T Deserialize(IMemoryOwner<byte> bytes, int length, StateSerializeOptions stateSerializeOptions);
    }
}

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
using System.Diagnostics;
using System.Text.Json;

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    internal class StateManagerMetadataSerializer<T> : IStateSerializer<StateManagerMetadata>
    {
        public Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
        {
            return Task.CompletedTask;
        }

        public StateManagerMetadata Deserialize(IMemoryOwner<byte> bytes, int length, StateSerializeOptions stateSerializeOptions)
        {
            var slice = bytes.Memory.Span.Slice(0, length);
            var reader = new Utf8JsonReader(slice);
            var deserializedValue = JsonSerializer.Deserialize<StateManagerMetadata<T>>(ref reader);
            bytes.Dispose();
            Debug.Assert(deserializedValue != null);
            return deserializedValue;
        }

        public ICacheObject DeserializeCacheObject(IMemoryOwner<byte> bytes, int length, StateSerializeOptions stateSerializeOptions)
        {
            return Deserialize(bytes, length, stateSerializeOptions);
        }

        public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
        {
            return Task.CompletedTask;
        }

        public byte[] Serialize(in StateManagerMetadata value, in StateSerializeOptions stateSerializeOptions)
        {
            if (value is StateManagerMetadata<T> metadata)
            {
                using MemoryStream memoryStream = new MemoryStream();
                JsonSerializer.Serialize(memoryStream, metadata);
                return memoryStream.ToArray();
            }
            throw new NotSupportedException();
        }

        public byte[] Serialize(in ICacheObject value, in StateSerializeOptions stateSerializeOptions)
        {
            if (value is StateManagerMetadata<T> metadata)
            {
                return Serialize(metadata, stateSerializeOptions);
            }
            throw new NotSupportedException();
        }
    }
}

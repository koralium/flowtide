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
using System.Text.Json;

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    internal class StateManagerMetadataSerializer<T> : IStateSerializer<StateManagerMetadata>
    {
        public StateManagerMetadata Deserialize(IMemoryOwner<byte> bytes, int length)
        {
            var slice = bytes.Memory.Span.Slice(0, length);
            var reader = new Utf8JsonReader(slice);
            var deserializedValue = JsonSerializer.Deserialize<StateManagerMetadata<T>>(ref reader);
            bytes.Dispose();
            return deserializedValue;
        }

        public ICacheObject DeserializeCacheObject(IMemoryOwner<byte> bytes, int length)
        {
            return Deserialize(bytes, length);
        }

        public byte[] Serialize(in StateManagerMetadata value)
        {
            if (value is StateManagerMetadata<T> metadata)
            {
                using MemoryStream memoryStream = new MemoryStream();
                JsonSerializer.Serialize(memoryStream, metadata);
                return memoryStream.ToArray();
            }
            throw new NotSupportedException();
        }

        public byte[] Serialize(in ICacheObject value)
        {
            if (value is StateManagerMetadata<T> metadata)
            {
                return Serialize(metadata);
            }
            throw new NotSupportedException();
        }
    }

    internal class StateManagerMetadataSerializer : IStateSerializer<StateManagerMetadata>
    {
        public StateManagerMetadata Deserialize(IMemoryOwner<byte> bytes, int length)
        {
            var slice = bytes.Memory.Span.Slice(0, length);
            var reader = new Utf8JsonReader(slice);
            var deserializedValue = JsonSerializer.Deserialize<StateManagerMetadata>(ref reader);
            bytes.Dispose();
            return deserializedValue;
        }

        public ICacheObject DeserializeCacheObject(IMemoryOwner<byte> bytes, int length)
        {
            return Deserialize(bytes, length);
        }

        public byte[] Serialize(in StateManagerMetadata value)
        {
            using MemoryStream memoryStream = new MemoryStream();
            JsonSerializer.Serialize(memoryStream, value);
            return memoryStream.ToArray();
        }

        public byte[] Serialize(in ICacheObject value)
        {
            if (value is StateManagerMetadata metadata)
            {
                return Serialize(metadata);
            }
            throw new NotSupportedException();
        }
    }
}

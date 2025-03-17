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

using FlowtideDotNet.Storage.Tree;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Storage.Serializers
{
    public class KeyListSerializer<K> : IBPlusTreeKeySerializer<K, ListKeyContainer<K>>
    {
        private readonly IBplusTreeSerializer<K> serializer;

        public KeyListSerializer(IBplusTreeSerializer<K> serializer)
        {
            this.serializer = serializer;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public ListKeyContainer<K> CreateEmpty()
        {
            return new ListKeyContainer<K>();
        }

        public ListKeyContainer<K> Deserialize(ref SequenceReader<byte> reader)
        {
            var container = new ListKeyContainer<K>();

            if (!reader.TryReadLittleEndian(out int length))
            {
                throw new InvalidOperationException("Failed to read length");
            }

            var bytes = new byte[length];
            if (!reader.TryCopyTo(bytes))
            {
                throw new InvalidOperationException("Failed to read bytes");
            }
            reader.Advance(length);
            // Legacy format
            using var memoryStream = new System.IO.MemoryStream(bytes);
            using var binaryReader = new System.IO.BinaryReader(memoryStream);

            serializer.Deserialize(binaryReader, container._list);
            return container;
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in ListKeyContainer<K> values)
        {
            using MemoryStream memoryStream = new MemoryStream();
            using BinaryWriter binaryWriter = new BinaryWriter(memoryStream);
            serializer.Serialize(binaryWriter, values._list);
            var bytes = memoryStream.ToArray();
            var lengthSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(lengthSpan, bytes.Length);
            writer.Advance(4);
            writer.Write(bytes);
        }
    }
}

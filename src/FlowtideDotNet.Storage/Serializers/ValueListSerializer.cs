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
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Serializers
{
    public class ValueListSerializer<V> : IBplusTreeValueSerializer<V, ListValueContainer<V>>
    {
        private readonly IBplusTreeSerializer<V> serializer;

        public ValueListSerializer(IBplusTreeSerializer<V> serializer)
        {
            this.serializer = serializer;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public ListValueContainer<V> CreateEmpty()
        {
            return new ListValueContainer<V>();
        }

        public ListValueContainer<V> Deserialize(ref SequenceReader<byte> reader)
        {
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

            using var memoryStream = new System.IO.MemoryStream(bytes);
            using var binaryReader = new System.IO.BinaryReader(memoryStream);

            var container = new ListValueContainer<V>();
            serializer.Deserialize(binaryReader, container._values);
            return container;
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in ListValueContainer<V> values)
        {
            using MemoryStream memoryStream = new MemoryStream();
            using BinaryWriter binaryWriter = new BinaryWriter(memoryStream);
            serializer.Serialize(binaryWriter, values._values);
            var bytes = memoryStream.ToArray();

            var lengthSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(lengthSpan, bytes.Length);
            writer.Advance(4);

            writer.Write(bytes);
        }
    }
}

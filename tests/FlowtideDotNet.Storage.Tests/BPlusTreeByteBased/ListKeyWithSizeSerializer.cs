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

namespace FlowtideDotNet.Storage.Tests.BPlusTreeByteBased
{
    internal class ListKeyWithSizeSerializer : IBPlusTreeKeySerializer<KeyValuePair<long, long>, ListKeyContainerWithSize>
    {
        private readonly int sizePerElement;

        public ListKeyWithSizeSerializer(int sizePerElement)
        {
            this.sizePerElement = sizePerElement;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public ListKeyContainerWithSize CreateEmpty()
        {
            return new ListKeyContainerWithSize(1);
        }

        public ListKeyContainerWithSize Deserialize(ref SequenceReader<byte> reader)
        {
            var container = new ListKeyContainerWithSize(sizePerElement);
            reader.TryReadLittleEndian(out int count);
            for (int i = 0; i < count; i++)
            {
                reader.TryReadLittleEndian(out long key);
                reader.TryReadLittleEndian(out long size);
                container.Add(new KeyValuePair<long, long>(key, size));
            }
            return container;
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in ListKeyContainerWithSize values)
        {
            var span = writer.GetSpan(4 + values.Count * 16);
            BinaryPrimitives.WriteInt32LittleEndian(span, values.Count);
            for (int i = 0; i < values.Count; i++)
            {
                var key = values.Get(i);
                BinaryPrimitives.WriteInt64LittleEndian(span.Slice(4 + i * 16), key.Key);
                BinaryPrimitives.WriteInt64LittleEndian(span.Slice(12 + i * 16), key.Value);
            }
            writer.Advance(4 + values.Count * 16);
        }
    }
}

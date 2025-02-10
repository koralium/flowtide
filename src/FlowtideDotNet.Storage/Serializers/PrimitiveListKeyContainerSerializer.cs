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

using FlowtideDotNet.Storage.Memory;
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
    public class PrimitiveListKeyContainerSerializer<T> : IBPlusTreeKeySerializer<T, PrimitiveListKeyContainer<T>>
        where T : unmanaged
    {
        private readonly IMemoryAllocator memoryAllocator;

        public PrimitiveListKeyContainerSerializer(IMemoryAllocator memoryAllocator)
        {
            this.memoryAllocator = memoryAllocator;
        }
        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public PrimitiveListKeyContainer<T> CreateEmpty()
        {
            return new PrimitiveListKeyContainer<T>(memoryAllocator);
        }

        public PrimitiveListKeyContainer<T> Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int count))
            {
                throw new InvalidOperationException("Failed to read count");
            }
            if (!reader.TryReadLittleEndian(out int length))
            {
                throw new InvalidOperationException("Failed to read length");
            }
            var memory = memoryAllocator.Allocate(length, 64);

            if (!reader.TryCopyTo(memory.Memory.Span.Slice(0, length)))
            {
                throw new InvalidOperationException("Failed to read bytes");
            }
            reader.Advance(length);
            return new PrimitiveListKeyContainer<T>(memory, count, memoryAllocator);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in PrimitiveListKeyContainer<T> values)
        {
            var span = writer.GetSpan(values.Memory.Length + 8);
            BinaryPrimitives.WriteInt32LittleEndian(span, values.Count);
            BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4), values.Memory.Length);
            values.Memory.Span.CopyTo(span.Slice(8));
            writer.Advance(values.Memory.Length + 8);
        }
    }
}

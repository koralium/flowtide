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

using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class TimestampKeySerializer : IBPlusTreeKeySerializer<long, TimestampKeyContainer>
    {
        private readonly IMemoryAllocator memoryAllocator;

        public TimestampKeySerializer(IMemoryAllocator memoryAllocator)
        {
            this.memoryAllocator = memoryAllocator;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public TimestampKeyContainer CreateEmpty()
        {
            return new TimestampKeyContainer(memoryAllocator);
        }

        public TimestampKeyContainer Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int count))
            {
                throw new InvalidOperationException("Failed to read count");
            }
            var nativeMemory = memoryAllocator.Allocate(count, 64);

            if (!reader.TryCopyTo(nativeMemory.Memory.Span.Slice(0, count)))
            {
                throw new InvalidOperationException("Failed to read bytes");
            }
            reader.Advance(count);
            return new TimestampKeyContainer(new PrimitiveList<long>(nativeMemory, count / 8, memoryAllocator));
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in TimestampKeyContainer values)
        {
            var mem = values._list.SlicedMemory;
            var headerSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(headerSpan, mem.Length);
            writer.Advance(4);
            //writer.Write(mem.Length);
            writer.Write(mem.Span);
        }
    }
}

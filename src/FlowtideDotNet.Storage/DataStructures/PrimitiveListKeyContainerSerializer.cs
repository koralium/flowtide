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

namespace FlowtideDotNet.Storage.DataStructures
{
    internal class PrimitiveListKeyContainerSerializer<K> : IBPlusTreeKeySerializer<K, PrimitiveListKeyContainer<K>>
        where K : unmanaged
    {
        private readonly IMemoryAllocator _memoryAllocator;

        public PrimitiveListKeyContainerSerializer(IMemoryAllocator memoryAllocator)
        {
            this._memoryAllocator = memoryAllocator;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public PrimitiveListKeyContainer<K> CreateEmpty()
        {
            return new PrimitiveListKeyContainer<K>(_memoryAllocator);
        }

        public unsafe PrimitiveListKeyContainer<K> Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int count))
            {
                throw new InvalidOperationException("Failed to read count");
            }
            var nativeMemory = _memoryAllocator.Allocate(count, 64);

            if (!reader.TryCopyTo(nativeMemory.Memory.Span.Slice(0, count)))
            {
                throw new InvalidOperationException("Failed to read bytes");
            }
            reader.Advance(count);
            return new PrimitiveListKeyContainer<K>(new PrimitiveList<K>(nativeMemory, count / sizeof(K), _memoryAllocator));
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in PrimitiveListKeyContainer<K> values)
        {
            var mem = values._list.SlicedMemory;
            var headerSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(headerSpan, mem.Length);
            writer.Advance(4);
            writer.Write(mem.Span);
        }
    }
}

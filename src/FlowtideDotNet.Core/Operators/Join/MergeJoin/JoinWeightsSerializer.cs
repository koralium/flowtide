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

using Apache.Arrow.Memory;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class JoinWeightsSerializer : IBplusTreeValueSerializer<JoinWeights, JoinWeightsValueContainer>
    {
        private readonly IMemoryAllocator _memoryAllocator;

        public JoinWeightsSerializer(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public JoinWeightsValueContainer CreateEmpty()
        {
            return new JoinWeightsValueContainer(_memoryAllocator);
        }

        public JoinWeightsValueContainer Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int count))
            {
                throw new InvalidOperationException("Failed to read count");
            }
            if (!reader.TryReadLittleEndian(out int length))
            {
                throw new InvalidOperationException("Failed to read length");
            }
            var nativeMemory = _memoryAllocator.Allocate(length, 64);

            if (!reader.TryCopyTo(nativeMemory.Memory.Span.Slice(0, length)))
            {
                throw new InvalidOperationException("Failed to read bytes");
            }
            reader.Advance(length);

            return new JoinWeightsValueContainer(nativeMemory, count, _memoryAllocator);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in JoinWeightsValueContainer values)
        {
            var memory = values.Memory;

            var headerSpan = writer.GetSpan(8);
            BinaryPrimitives.WriteInt32LittleEndian(headerSpan, values.Count);
            BinaryPrimitives.WriteInt32LittleEndian(headerSpan.Slice(4), memory.Length);
            writer.Advance(8);

            writer.Write(memory.Span);
        }
    }
}

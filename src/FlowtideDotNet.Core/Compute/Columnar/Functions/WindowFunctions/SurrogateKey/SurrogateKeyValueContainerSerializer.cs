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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.SurrogateKey
{
    internal class SurrogateKeyValueContainerSerializer : IBplusTreeValueSerializer<SurrogateKeyValue, SurrogateKeyValueContainer>
    {
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly EventBatchBPlusTreeSerializer _batchSerializer;

        public SurrogateKeyValueContainerSerializer(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _batchSerializer = new EventBatchBPlusTreeSerializer();
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public SurrogateKeyValueContainer CreateEmpty()
        {
            return new SurrogateKeyValueContainer(_memoryAllocator);
        }

        public SurrogateKeyValueContainer Deserialize(ref SequenceReader<byte> reader)
        {
            var result = _batchSerializer.Deserialize(ref reader, _memoryAllocator);

            if (!reader.TryReadLittleEndian(out int weightsLength))
            {
                throw new InvalidOperationException("Failed to read weights length");
            }

            var weightsMemory = _memoryAllocator.Allocate(weightsLength, 64);

            if (!reader.TryCopyTo(weightsMemory.Memory.Span.Slice(0, weightsLength)))
            {
                throw new InvalidOperationException("Failed to copy weights memory");
            }
            reader.Advance(weightsLength);

            
            var column = result.EventBatch.Columns[0];
            if (column is Column c)
            {
                return new SurrogateKeyValueContainer(c, new PrimitiveList<int>(weightsMemory, weightsLength / sizeof(int), _memoryAllocator));
            }
            throw new NotImplementedException();
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in SurrogateKeyValueContainer values)
        {
            _batchSerializer.Serialize(writer, new EventBatchData([values._column]), values.Count);

            var weightsMemory = values._weights.SlicedMemory;
            var weightsSpan = writer.GetSpan(4 + weightsMemory.Length);

            BinaryPrimitives.WriteInt32LittleEndian(weightsSpan, weightsMemory.Length);
            weightsMemory.Span.CopyTo(weightsSpan.Slice(4));
            writer.Advance(weightsMemory.Length + 4);
        }
    }
}

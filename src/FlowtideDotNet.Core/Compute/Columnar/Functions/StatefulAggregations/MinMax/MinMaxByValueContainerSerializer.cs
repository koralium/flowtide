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

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax
{
    internal class MinMaxByValueContainerSerializer : IBplusTreeValueSerializer<MinMaxByValue, MinMaxByValueContainer>
    {
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly EventBatchSerializer _batchSerializer;

        public MinMaxByValueContainerSerializer(IMemoryAllocator memoryAllocator)
        {
            this._memoryAllocator = memoryAllocator;
            _batchSerializer = new EventBatchSerializer();
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public MinMaxByValueContainer CreateEmpty()
        {
            return new MinMaxByValueContainer(_memoryAllocator);
        }

        public MinMaxByValueContainer Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int weightsMemoryLength))
            {
                throw new InvalidOperationException("Failed to read weights memory length");
            }

            var weightsNativeMemory = _memoryAllocator.Allocate(weightsMemoryLength, 64);
            var slice = weightsNativeMemory.Memory.Span.Slice(0, weightsMemoryLength);

            if (!reader.TryCopyTo(slice))
            {
                throw new InvalidOperationException("Failed to read weights memory");
            }
            reader.Advance(weightsMemoryLength);

            var weights = new PrimitiveList<int>(weightsNativeMemory, weightsMemoryLength / sizeof(int), _memoryAllocator);

            var deserializer = new EventBatchDeserializer(_memoryAllocator);
            var batch = deserializer.DeserializeBatch(ref reader);

            if (batch.EventBatch.Columns[0] is not Column column)
            {
                throw new InvalidOperationException("Failed to deserialize column");
            }

            return new MinMaxByValueContainer(column, weights);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in MinMaxByValueContainer values)
        {
            var weightsmemory = values._weights.SlicedMemory.Span;

            var writeSpan = writer.GetSpan(weightsmemory.Length + 4);

            BinaryPrimitives.WriteInt32LittleEndian(writeSpan, weightsmemory.Length);
            weightsmemory.CopyTo(writeSpan.Slice(4));
            writer.Advance(weightsmemory.Length + 4);

            _batchSerializer.SerializeEventBatch(writer, new ColumnStore.EventBatchData([values._data]), values._weights.Count);
        }
    }
}

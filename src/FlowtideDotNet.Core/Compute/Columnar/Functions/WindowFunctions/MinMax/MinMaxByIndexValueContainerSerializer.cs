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
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.MinMax
{
    internal class MinMaxByIndexValueContainerSerializer : IBplusTreeValueSerializer<MinMaxByIndexValue, MinMaxByIndexValueContainer>
    {
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly EventBatchSerializer _batchSerializer;

        public MinMaxByIndexValueContainerSerializer(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _batchSerializer = new EventBatchSerializer();
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public MinMaxByIndexValueContainer CreateEmpty()
        {
            return new MinMaxByIndexValueContainer(_memoryAllocator);
        }

        public MinMaxByIndexValueContainer Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int indicesMemoryLength))
            {
                throw new InvalidOperationException("Failed to read weights memory length");
            }

            var indicesNativeMemory = _memoryAllocator.Allocate(indicesMemoryLength, 64);
            var slice = indicesNativeMemory.Memory.Span.Slice(0, indicesMemoryLength);

            if (!reader.TryCopyTo(slice))
            {
                throw new InvalidOperationException("Failed to read weights memory");
            }
            reader.Advance(indicesMemoryLength);

            var indices = new PrimitiveList<long>(indicesNativeMemory, indicesMemoryLength / sizeof(long), _memoryAllocator);

            var deserializer = new EventBatchDeserializer(_memoryAllocator);
            var batch = deserializer.DeserializeBatch(ref reader);

            if (batch.EventBatch.Columns[0] is not Column column)
            {
                throw new InvalidOperationException("Failed to deserialize column");
            }

            if (batch.EventBatch.Columns[1] is not Column compareColumn)
            {
                throw new InvalidOperationException("Failed to deserialize column");
            }

            return new MinMaxByIndexValueContainer(column, compareColumn, indices);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in MinMaxByIndexValueContainer values)
        {
            var indicesmemory = values._indices.SlicedMemory.Span;

            var writeSpan = writer.GetSpan(indicesmemory.Length + 4);

            BinaryPrimitives.WriteInt32LittleEndian(writeSpan, indicesmemory.Length);
            indicesmemory.CopyTo(writeSpan.Slice(4));
            writer.Advance(indicesmemory.Length + 4);

            _batchSerializer.SerializeEventBatch(writer, new ColumnStore.EventBatchData([values._data, values._compareData]), values._indices.Count);
        }
    }
}

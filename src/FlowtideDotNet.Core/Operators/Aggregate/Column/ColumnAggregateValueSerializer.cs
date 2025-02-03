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

using Apache.Arrow.Ipc;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Normalization;
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

namespace FlowtideDotNet.Core.Operators.Aggregate.Column
{
    internal class ColumnAggregateValueSerializer : IBplusTreeValueSerializer<ColumnAggregateStateReference, ColumnAggregateValueContainer>
    {
        private readonly int measureCount;
        private readonly IMemoryAllocator memoryAllocator;
        private readonly EventBatchBPlusTreeSerializer _batchSerializer;

        public ColumnAggregateValueSerializer(int measureCount, IMemoryAllocator memoryAllocator)
        {
            this.measureCount = measureCount;
            this.memoryAllocator = memoryAllocator;
            _batchSerializer = new EventBatchBPlusTreeSerializer();
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public ColumnAggregateValueContainer CreateEmpty()
        {
            return new ColumnAggregateValueContainer(measureCount, memoryAllocator);
        }

        public ColumnAggregateValueContainer Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int previousValueLength))
            {
                throw new InvalidOperationException("Failed to read previous value length");
            }
            var previousValueNativeMemory = memoryAllocator.Allocate(previousValueLength, 64);
            var slice = previousValueNativeMemory.Memory.Span.Slice(0, previousValueLength);
            if (!reader.TryCopyTo(slice))
            {
                throw new InvalidOperationException("Failed to read previous value");
            }
            reader.Advance(previousValueLength);

            if (!reader.TryReadLittleEndian(out int weightLength))
            {
                throw new InvalidOperationException("Failed to read weight length");
            }
            var weightNativeMemory = memoryAllocator.Allocate(weightLength, 64);
            if (!reader.TryCopyTo(weightNativeMemory.Memory.Span.Slice(0, weightLength)))
            {
                throw new InvalidOperationException("Failed to read weight");
            }
            reader.Advance(weightLength);

            var eventBatchResult = _batchSerializer.Deserialize(ref reader, memoryAllocator);
            var previousValueList = new PrimitiveList<bool>(previousValueNativeMemory, eventBatchResult.Count, memoryAllocator);
            var weightsList = new PrimitiveList<int>(weightNativeMemory, eventBatchResult.Count, memoryAllocator);

            return new ColumnAggregateValueContainer(measureCount, eventBatchResult.EventBatch, weightsList, previousValueList);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in ColumnAggregateValueContainer values)
        {
            var previousValueMemory = values._previousValueSent.SlicedMemory;
            var lengthSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(lengthSpan, previousValueMemory.Length);
            writer.Advance(4);

            writer.Write(previousValueMemory.Span);
            var weightMemory = values._weights.SlicedMemory;
            lengthSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(lengthSpan, weightMemory.Length);
            writer.Advance(4);

            writer.Write(weightMemory.Span);

            _batchSerializer.Serialize(writer, values._eventBatch, values._weights.Count);
        }
    }
}

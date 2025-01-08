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

        public ColumnAggregateValueSerializer(int measureCount, IMemoryAllocator memoryAllocator)
        {
            this.measureCount = measureCount;
            this.memoryAllocator = memoryAllocator;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public ColumnAggregateValueContainer CreateEmpty()
        {
            return new ColumnAggregateValueContainer(measureCount, memoryAllocator);
        }

        public ColumnAggregateValueContainer Deserialize(in BinaryReader reader)
        {
            var previousValueLength = reader.ReadInt32();
            var previousValueMemory = reader.ReadBytes(previousValueLength);
            var previousValueNativeMemory = memoryAllocator.Allocate(previousValueLength, 64);

            previousValueMemory.CopyTo(previousValueNativeMemory.Memory.Span);

            var weightLength = reader.ReadInt32();
            var weightMemory = reader.ReadBytes(weightLength);
            var weightNativeMemory = memoryAllocator.Allocate(weightLength, 64);
            weightMemory.CopyTo(weightNativeMemory.Memory.Span);

            using var arrowReader = new ArrowStreamReader(reader.BaseStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var recordBatch = arrowReader.ReadNextRecordBatch();

            var eventBatch = EventArrowSerializer.ArrowToBatch(recordBatch, memoryAllocator);
            var previousValueList = new PrimitiveList<bool>(previousValueNativeMemory, recordBatch.Length, memoryAllocator);
            var weightsList = new PrimitiveList<int>(weightNativeMemory, recordBatch.Length, memoryAllocator);

            return new ColumnAggregateValueContainer(measureCount, eventBatch, weightsList, previousValueList);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in BinaryWriter writer, in ColumnAggregateValueContainer values)
        {
            var previousValueMemory = values._previousValueSent.SlicedMemory;
            writer.Write(previousValueMemory.Length);
            writer.Write(previousValueMemory.Span);
            var weightMemory = values._weights.SlicedMemory;
            writer.Write(weightMemory.Length);
            writer.Write(weightMemory.Span);

            var recordBatch = EventArrowSerializer.BatchToArrow(values._eventBatch, values._weights.Count);
            var batchWriter = new ArrowStreamWriter(writer.BaseStream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);
        }
    }
}

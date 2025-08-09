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

using Apache.Arrow;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;
using System.Reflection;

namespace FlowtideDotNet.Core.Operators.Aggregate.Column
{
    internal class AggregateKeySerializer : IBPlusTreeKeySerializer<ColumnRowReference, AggregateKeyStorageContainer>
    {
        private readonly int columnCount;
        private readonly IMemoryAllocator memoryAllocator;
        private readonly EventBatchBPlusTreeSerializer _batchSerializer;

        public AggregateKeySerializer(int columnCount, IMemoryAllocator memoryAllocator)
        {
            this.columnCount = columnCount;
            this.memoryAllocator = memoryAllocator;
            _batchSerializer = new EventBatchBPlusTreeSerializer();
        }
        public AggregateKeyStorageContainer CreateEmpty()
        {
            return new AggregateKeyStorageContainer(columnCount, memoryAllocator);
        }

        private static readonly FieldInfo _memoryOwnerField = GetMethodArrowBufferMemoryOwner();
        private static FieldInfo GetMethodArrowBufferMemoryOwner()
        {
            var fieldInfo = typeof(RecordBatch).GetField("_memoryOwner", BindingFlags.NonPublic | BindingFlags.Instance);
            return fieldInfo!;
        }

        public AggregateKeyStorageContainer Deserialize(ref SequenceReader<byte> reader)
        {
            var eventBatch = _batchSerializer.Deserialize(ref reader, memoryAllocator);
            return new AggregateKeyStorageContainer(eventBatch.EventBatch.Columns.Count, eventBatch.EventBatch, eventBatch.Count);
        }

        public void Serialize(in IBufferWriter<byte> writer, in AggregateKeyStorageContainer values)
        {
            _batchSerializer.Serialize(writer, values._data, values.Count);
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }
    }
}

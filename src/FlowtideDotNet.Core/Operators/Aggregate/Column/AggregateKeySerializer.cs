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
using Apache.Arrow;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;

namespace FlowtideDotNet.Core.Operators.Aggregate.Column
{
    internal class AggregateKeySerializer : IBPlusTreeKeySerializer<ColumnRowReference, AggregateKeyStorageContainer>
    {
        private readonly int columnCount;
        private readonly IMemoryAllocator memoryAllocator;

        public AggregateKeySerializer(int columnCount, IMemoryAllocator memoryAllocator)
        {
            this.columnCount = columnCount;
            this.memoryAllocator = memoryAllocator;
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

        public AggregateKeyStorageContainer Deserialize(in BinaryReader reader)
        {
            using var arrowReader = new ArrowStreamReader(reader.BaseStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var recordBatch = arrowReader.ReadNextRecordBatch();

            var eventBatch = EventArrowSerializer.ArrowToBatch(recordBatch, memoryAllocator);
            
            return new AggregateKeyStorageContainer(recordBatch.ColumnCount, eventBatch, recordBatch.Length);
        }

        public void Serialize(in IBufferWriter<byte> writer, in AggregateKeyStorageContainer values)
        {
            var recordBatch = EventArrowSerializer.BatchToArrow(values._data, values.Count);
            var batchWriter = new ArrowStreamWriter(writer.BaseStream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);
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

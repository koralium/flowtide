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
using Apache.Arrow.Ipc;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    internal class ColumnStoreSerializer : IBPlusTreeKeySerializer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private readonly int columnCount;
        private readonly IMemoryAllocator memoryAllocator;
        private readonly EventBatchBPlusTreeSerializer _batchSerializer;
        

        public ColumnStoreSerializer(int columnCount, IMemoryAllocator memoryAllocator)
        {
            this.columnCount = columnCount;
            this.memoryAllocator = memoryAllocator;
            _batchSerializer = new EventBatchBPlusTreeSerializer();
        }

        public ColumnKeyStorageContainer CreateEmpty()
        {
            return new ColumnKeyStorageContainer(columnCount, memoryAllocator);
        }

        private static readonly FieldInfo _memoryOwnerField = GetMethodArrowBufferMemoryOwner();
        private static FieldInfo GetMethodArrowBufferMemoryOwner()
        {
            var fieldInfo = typeof(RecordBatch).GetField("_memoryOwner", BindingFlags.NonPublic | BindingFlags.Instance);
            return fieldInfo!;
        }

        public ColumnKeyStorageContainer Deserialize(ref SequenceReader<byte> reader)
        {
            var batchResult = _batchSerializer.Deserialize(ref reader, memoryAllocator);
            return new ColumnKeyStorageContainer(batchResult.EventBatch.Columns.Count, batchResult.EventBatch, batchResult.Count);
        }

        public void Serialize(in IBufferWriter<byte> writer, in ColumnKeyStorageContainer values)
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

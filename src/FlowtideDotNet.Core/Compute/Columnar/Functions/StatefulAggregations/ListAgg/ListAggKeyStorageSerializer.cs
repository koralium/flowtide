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

using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations
{
    internal class ListAggKeyStorageSerializer : IBPlusTreeKeySerializer<ListAggColumnRowReference, ListAggKeyStorageContainer>
    {
        private readonly int _groupingKeyLength;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly EventBatchBPlusTreeSerializer _batchSerializer;

        public ListAggKeyStorageSerializer(int groupingKeyLength, IMemoryAllocator memoryAllocator)
        {
            _groupingKeyLength = groupingKeyLength;
            _memoryAllocator = memoryAllocator;
            _batchSerializer = new EventBatchBPlusTreeSerializer();
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public ListAggKeyStorageContainer CreateEmpty()
        {
            return new ListAggKeyStorageContainer(_groupingKeyLength, _memoryAllocator);
        }

        public ListAggKeyStorageContainer Deserialize(ref SequenceReader<byte> reader)
        {
            var batchResult = _batchSerializer.Deserialize(ref reader, _memoryAllocator);
            return new ListAggKeyStorageContainer(_groupingKeyLength, batchResult.EventBatch);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in ListAggKeyStorageContainer values)
        {
            _batchSerializer.Serialize(writer, values._data, values.Count);
        }
    }
}

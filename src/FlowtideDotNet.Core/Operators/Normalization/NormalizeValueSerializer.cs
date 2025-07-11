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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;

namespace FlowtideDotNet.Core.Operators.Normalization
{
    internal class NormalizeValueSerializer : IBplusTreeValueSerializer<ColumnRowReference, NormalizeValueStorage>
    {
        private readonly List<int> _columnsToStore;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly EventBatchBPlusTreeSerializer _batchSerializer;

        public NormalizeValueSerializer(List<int> columnsToStore, IMemoryAllocator memoryAllocator)
        {
            _columnsToStore = columnsToStore;
            _memoryAllocator = memoryAllocator;
            _batchSerializer = new EventBatchBPlusTreeSerializer();
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public NormalizeValueStorage CreateEmpty()
        {
            return new NormalizeValueStorage(_columnsToStore, _memoryAllocator);
        }

        public NormalizeValueStorage Deserialize(ref SequenceReader<byte> reader)
        {
            var batchResult = _batchSerializer.Deserialize(ref reader, _memoryAllocator);
            return new NormalizeValueStorage(_columnsToStore, batchResult.EventBatch, batchResult.Count);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in NormalizeValueStorage values)
        {
            _batchSerializer.Serialize(writer, values._data, values.Count);
        }
    }
}

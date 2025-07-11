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
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    internal class DataValueValueContainerSerializer : IBplusTreeValueSerializer<IDataValue, DataValueValueContainer>
    {
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly EventBatchBPlusTreeSerializer _batchSerializer;

        public DataValueValueContainerSerializer(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _batchSerializer = new EventBatchBPlusTreeSerializer();
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public DataValueValueContainer CreateEmpty()
        {
            return new DataValueValueContainer(_memoryAllocator);
        }

        public DataValueValueContainer Deserialize(ref SequenceReader<byte> reader)
        {
            var result = _batchSerializer.Deserialize(ref reader, _memoryAllocator);
            var column = result.EventBatch.Columns[0];
            if (column is Column c)
            {
                return new DataValueValueContainer(c);
            }
            throw new InvalidOperationException("Invalid column type when deserializing");
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in DataValueValueContainer keys)
        {
            _batchSerializer.Serialize(writer, new EventBatchData([keys._column]), keys.Count);
        }
    }
}

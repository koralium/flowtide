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
    internal class ColumnValueSerializer : IBplusTreeValueSerializer<ColumnRowReference, ColumnValueStorageContainer>
    {
        private readonly int _columnCount;
        private readonly IMemoryAllocator _memoryAllocator;

        public ColumnValueSerializer(int columnCount, IMemoryAllocator memoryAllocator)
        {
            _columnCount = columnCount;
            _memoryAllocator = memoryAllocator;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public ColumnValueStorageContainer CreateEmpty()
        {
            return new ColumnValueStorageContainer(_columnCount, _memoryAllocator);
        }

        public ColumnValueStorageContainer Deserialize(ref SequenceReader<byte> reader)
        {
            throw new NotImplementedException();
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in ColumnValueStorageContainer values)
        {
            throw new NotImplementedException();
        }
    }
}

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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Serializers
{
    public class PrimitiveListKeyContainerSerializer<T> : IBPlusTreeKeySerializer<T, PrimitiveListKeyContainer<T>>
        where T : unmanaged
    {
        private readonly IMemoryAllocator memoryAllocator;

        public PrimitiveListKeyContainerSerializer(IMemoryAllocator memoryAllocator)
        {
            this.memoryAllocator = memoryAllocator;
        }
        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public PrimitiveListKeyContainer<T> CreateEmpty()
        {
            return new PrimitiveListKeyContainer<T>(memoryAllocator);
        }

        public PrimitiveListKeyContainer<T> Deserialize(in BinaryReader reader)
        {
            var count = reader.ReadInt32();
            var length = reader.ReadInt32();
            
            var bytes = reader.ReadBytes(length);

            var memory = memoryAllocator.Allocate(length, 64);
            bytes.CopyTo(memory.Memory.Span);

            return new PrimitiveListKeyContainer<T>(memory, count, memoryAllocator);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in BinaryWriter writer, in PrimitiveListKeyContainer<T> values)
        {
            writer.Write(values.Count);
            var mem = values.Memory;
            writer.Write(mem.Length);
            writer.Write(mem.Span);
        }
    }
}

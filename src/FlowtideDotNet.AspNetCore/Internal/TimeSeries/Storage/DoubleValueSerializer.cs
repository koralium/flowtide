﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using Apache.Arrow.Memory;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class DoubleValueSerializer : IBplusTreeValueSerializer<double, DoubleValueContainer>
    {
        private readonly IMemoryAllocator memoryAllocator;

        public DoubleValueSerializer(IMemoryAllocator memoryAllocator)
        {
            this.memoryAllocator = memoryAllocator;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public DoubleValueContainer CreateEmpty()
        {
            return new DoubleValueContainer(memoryAllocator);
        }

        public DoubleValueContainer Deserialize(in BinaryReader reader)
        {
            var count = reader.ReadInt32();
            var memory = reader.ReadBytes(count);

            var nativeMemory = memoryAllocator.Allocate(count, 64);

            memory.CopyTo(nativeMemory.Memory.Span);
            return new DoubleValueContainer(new PrimitiveList<double>(nativeMemory, count / 8, memoryAllocator));   
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in BinaryWriter writer, in DoubleValueContainer values)
        {
            var mem = values._list.SlicedMemory;
            writer.Write(mem.Length);
            writer.Write(mem.Span);
        }
    }
}

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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Serializers
{
    public class PrimitiveListValueContainerSerializer<T> : IBplusTreeValueSerializer<T, PrimitiveListValueContainer<T>>
        where T : unmanaged
    {
        private readonly IMemoryAllocator _memoryAllocator;

        public PrimitiveListValueContainerSerializer(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public PrimitiveListValueContainer<T> CreateEmpty()
        {
            return new PrimitiveListValueContainer<T>(_memoryAllocator);
        }

        public PrimitiveListValueContainer<T> Deserialize(in BinaryReader reader)
        {
            var count = reader.ReadInt32();
            var length = reader.ReadInt32();
            var memory = reader.ReadBytes(length);

            var memoryAllocator = _memoryAllocator;
            var nativeMemory = memoryAllocator.Allocate(length, 64);

            memory.CopyTo(nativeMemory.Memory.Span);

            return new PrimitiveListValueContainer<T>(nativeMemory, count, memoryAllocator);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in BinaryWriter writer, in PrimitiveListValueContainer<T> values)
        {
            var memory = values.Memory;

            writer.Write(values.Count);
            writer.Write(memory.Length);
            writer.Write(memory.Span);
        }
    }
}

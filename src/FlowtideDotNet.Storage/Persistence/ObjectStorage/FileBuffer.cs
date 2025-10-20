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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    /// <summary>
    /// Represents a file which data can be written into
    /// </summary>
    internal class FileBuffer : IBufferWriter<byte>
    {
        private readonly IMemoryAllocator memoryAllocator;
        private PrimitiveList<PageInfo> pageInfo;

        public FileBuffer(IMemoryAllocator memoryAllocator)
        {
            this.memoryAllocator = memoryAllocator;
            pageInfo = new PrimitiveList<PageInfo>(memoryAllocator);
        }

        public void Write(long key, ReadOnlySpan<byte> value)
        {
           // using var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            if (value.PreSerializedData.HasValue)
            {
                pageInfo.Add(new PageInfo() { PageKey = key, PageValue = 0 });
                EnsureCapacity(value.PreSerializedData.Value.Length);
                var handle = value.PreSerializedData.Value.Pin();
                var span = _end.AvailableMemory.Span.Slice(endIndex);
                value.PreSerializedData.Value.Span.CopyTo(span);
                handle.Dispose();
            }
            else
            {
                value.Serialize(this);
            }
        }

        public void Advance(int count)
        {
            throw new NotImplementedException();
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            throw new NotImplementedException();
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            throw new NotImplementedException();
        }
    }
}

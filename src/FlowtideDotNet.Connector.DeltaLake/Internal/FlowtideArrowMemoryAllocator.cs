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

using Apache.Arrow.Memory;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class FlowtideArrowMemoryAllocator : MemoryAllocator
    {
        private readonly IMemoryAllocator memoryAllocator;

        public FlowtideArrowMemoryAllocator(IMemoryAllocator memoryAllocator)
        {
            this.memoryAllocator = memoryAllocator;
        }
        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            var memoryOwner = memoryAllocator.Allocate(length, 64);
            bytesAllocated = memoryOwner.Memory.Length;
            return memoryOwner;
        }
    }
}

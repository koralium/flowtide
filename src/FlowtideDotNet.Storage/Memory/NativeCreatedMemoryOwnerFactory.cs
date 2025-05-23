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

using Microsoft.Extensions.ObjectPool;

namespace FlowtideDotNet.Storage.Memory
{
    internal unsafe static class NativeCreatedMemoryOwnerFactory
    {
        private class ObjectPoolProvider : DefaultPooledObjectPolicy<NativeCreatedMemoryOwner>
        {
            public override NativeCreatedMemoryOwner Create()
            {
                return new NativeCreatedMemoryOwner();
            }
        }
        private static ObjectPool<NativeCreatedMemoryOwner> _pool = new DefaultObjectPool<NativeCreatedMemoryOwner>(new ObjectPoolProvider(), 10000);

        public static NativeCreatedMemoryOwner Get(void* ptr, int length, IMemoryAllocator operatorMemoryManager)
        {
            var mem = new NativeCreatedMemoryOwner();
            mem.Assign(ptr, length, operatorMemoryManager);
            return mem;
            //var memory = _pool.Get();
            //memory.Assign(ptr, length);
            //return memory;
        }

        public static void Return(NativeCreatedMemoryOwner memory)
        {
            //_pool.Return(memory);
        }
    }
}

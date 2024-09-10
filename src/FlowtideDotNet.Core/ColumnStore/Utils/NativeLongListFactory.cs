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

using FlowtideDotNet.Core.ColumnStore.Memory;
using Microsoft.Extensions.ObjectPool;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    internal static class NativeLongListFactory
    {
        private class ObjectPoolProvider : DefaultPooledObjectPolicy<NativeLongList>
        {
            public override NativeLongList Create()
            {
                return new NativeLongList();
            }
        }
        private static ObjectPool<NativeLongList> _pool = new DefaultObjectPool<NativeLongList>(new ObjectPoolProvider(), 10000);

        public static NativeLongList Get(IMemoryAllocator memoryAllocator)
        {   
            var list = _pool.Get();
            list.Assign(memoryAllocator);
            return list;
        }

        public static NativeLongList Get(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            var list = _pool.Get();
            list.Assign(memory, length, memoryAllocator);
            return list;
        }

        public static void Return(NativeLongList list)
        {
            _pool.Return(list);
        }
    }
}

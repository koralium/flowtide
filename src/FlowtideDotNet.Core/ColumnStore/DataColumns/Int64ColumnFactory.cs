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
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using Microsoft.Extensions.ObjectPool;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    internal static class Int64ColumnFactory
    {
        private class ObjectPoolProvider : DefaultPooledObjectPolicy<Int64Column>
        {
            public override Int64Column Create()
            {
                return new Int64Column();
            }
        }
        private static ObjectPool<Int64Column> _pool = new DefaultObjectPool<Int64Column>(new ObjectPoolProvider(), 10000);

        public static Int64Column Get(IMemoryAllocator memoryAllocator)
        {
            //var list = _pool.Get();
            //list.Assign(memoryAllocator);
            //return list;
            return new Int64Column(memoryAllocator);
        }

        public static Int64Column Get(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            return new Int64Column(memory, length, memoryAllocator);
            //var list = _pool.Get();
            //list.Assign(memory, length, memoryAllocator);
            //return list;
        }

        public static void Return(Int64Column list)
        {
            //_pool.Return(list);
        }
    }
}

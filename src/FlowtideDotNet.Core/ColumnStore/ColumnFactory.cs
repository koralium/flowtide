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
using Microsoft.Extensions.ObjectPool;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal static class ColumnFactory
    {
        private class ObjectPoolProvider : DefaultPooledObjectPolicy<Column>
        {
            public override Column Create()
            {
                return new Column();
            }
        }
        private static ObjectPool<Column> _pool = new DefaultObjectPool<Column>(new ObjectPoolProvider(), 10000);

        public static Column Get(IMemoryAllocator memoryAllocator)
        {
            var list = _pool.Get();
            list.Assign(memoryAllocator);
            return list;
        }

        public static Column Get(int nullCounter, IDataColumn? dataColumn, BitmapList validityList, ArrowTypeId type, IMemoryAllocator memoryAllocator)
        {
            var list = _pool.Get();
            list.Assign(nullCounter, dataColumn, validityList, type, memoryAllocator);
            return list;
        }

        public static void Return(Column list)
        {
            _pool.Return(list);
        }
    }
}

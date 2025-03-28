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

namespace FlowtideDotNet.Storage.DataStructures
{
    internal class PrimitiveListKeyContainer<K> : IKeyContainer<K>
        where K : unmanaged
    {
        internal PrimitiveList<K> _list;

        internal PrimitiveListKeyContainer(IMemoryAllocator memoryAllocator)
        {
            _list = new PrimitiveList<K>(memoryAllocator);
        }

        internal PrimitiveListKeyContainer(PrimitiveList<K> list)
        {
            _list = list;
        }


        public int Count => _list.Count;

        public void Add(K key)
        {
            _list.Add(key);
        }

        public void AddRangeFrom(IKeyContainer<K> container, int start, int count)
        {
            if (container is PrimitiveListKeyContainer<K> other)
            {
                _list.AddRangeFrom(other._list, start, count);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public int BinarySearch(K key, IComparer<K> comparer)
        {
            int lo = 0;
            int hi = _list.Count - 1;

            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = comparer.Compare(_list.Get(i), key);
                if (c == 0)
                {
                    return lo;
                }
                else if (c < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }
            return ~lo;
        }

        public void Dispose()
        {
            _list.Dispose();
        }

        public K Get(in int index)
        {
            return _list[index];
        }

        public int GetByteSize()
        {
            return _list.SlicedMemory.Length;
        }

        public unsafe int GetByteSize(int start, int end)
        {
            return (end - start + 1) * sizeof(K);
        }

        public void Insert(int index, K key)
        {
            _list.InsertAt(index, key);
        }

        public void Insert_Internal(int index, K key)
        {
            _list.InsertAt(index, key);
        }

        public void RemoveAt(int index)
        {
            _list.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _list.RemoveRange(start, count);
        }

        public void Update(int index, K key)
        {
            _list.Update(index, key);
        }
    }
}

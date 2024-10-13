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

using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class TimestampKeyContainer : IKeyContainer<long>
    {
        internal PrimitiveList<long> _list;
        public TimestampKeyContainer(IMemoryAllocator memoryAllocator)
        {
            _list = new PrimitiveList<long>(memoryAllocator);
        }

        public TimestampKeyContainer(PrimitiveList<long> list)
        {
            _list = list;
        }

        public int Count => _list.Count;

        public void Add(long key)
        {
            _list.Add(key);
        }

        public void AddRangeFrom(IKeyContainer<long> container, int start, int count)
        {
            if (container is TimestampKeyContainer columnKeyStorageContainer)
            {
                for (int i = start; i < start + count; i++)
                {
                    Add(columnKeyStorageContainer.Get(i));
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public int BinarySearch(long key, IComparer<long> comparer)
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

        public long Get(in int index)
        {
            return _list[index];
        }

        public int GetByteSize()
        {
            return _list.SlicedMemory.Length;
        }

        public int GetByteSize(int start, int end)
        {
            return (end - start + 1) * sizeof(long);
        }

        public void Insert(int index, long key)
        {
            _list.InsertAt(index, key);
        }

        public void Insert_Internal(int index, long key)
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

        public void Update(int index, long key)
        {
            _list.Update(index, key);
        }
    }
}

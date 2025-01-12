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
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree
{
    public class PrimitiveListKeyContainer<T> : IKeyContainer<T>
        where T : unmanaged
    {
        internal readonly PrimitiveList<T> _values;

        public Memory<byte> Memory => _values.SlicedMemory;

        public PrimitiveListKeyContainer(IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<T>(memoryAllocator);
        }

        public PrimitiveListKeyContainer(IMemoryOwner<byte> memory, int count, IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<T>(memory, count, memoryAllocator);
        }

        public int Count => _values.Count;

        public void Add(T key)
        {
            _values.Add(key);
        }

        public void AddRangeFrom(IKeyContainer<T> container, int start, int count)
        {
            if (container is PrimitiveListKeyContainer<T> primitiveListKeyContainer)
            {
                _values.AddRangeFrom(primitiveListKeyContainer._values, start, count);
                return;
            }
            throw new NotImplementedException();
        }

        public int BinarySearch(T key, IComparer<T> comparer)
        {
            int lo = 0;
            int hi = _values.Count - 1;
            int maxNotFound = hi;

            bool found = false;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = comparer.Compare(_values.Get(i), key);
                if (c == 0)
                {
                    found = true;
                    hi = i - 1;
                }
                else if (c < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                    maxNotFound = hi;
                }
            }
            int lowerbound = lo;
            if (!found)
            {
                lowerbound = ~lo;
                // We did not find the value so this is the the bounds.
                return lowerbound;
            }
            return lowerbound;
        }

        public void Dispose()
        {
            _values.Dispose();
        }

        public T Get(in int index)
        {
            return _values.Get(in index);
        }

        public unsafe int GetByteSize()
        {
            return _values.Count * sizeof(T);
        }

        public unsafe int GetByteSize(int start, int end)
        {
            return (end - start) * sizeof(T);
        }

        public void Insert(int index, T key)
        {
            _values.InsertAt(index, key);
        }

        public void Insert_Internal(int index, T key)
        {
            _values.InsertAt(index, key);
        }

        public void RemoveAt(int index)
        {
            _values.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _values.RemoveRange(start, count);
        }

        public void Update(int index, T key)
        {
            _values.Update(index, key);
        }
    }
}

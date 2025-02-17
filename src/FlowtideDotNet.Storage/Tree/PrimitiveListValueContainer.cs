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

namespace FlowtideDotNet.Storage.Tree
{
    public class PrimitiveListValueContainer<T> : IValueContainer<T>
        where T : unmanaged
    {
        private readonly PrimitiveList<T> _values;

        public Memory<byte> Memory => _values.SlicedMemory;

        public PrimitiveList<T> Data => _values;

        public PrimitiveListValueContainer(IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<T>(memoryAllocator);
        }

        public PrimitiveListValueContainer(IMemoryOwner<byte> memory, int count, IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<T>(memory, count, memoryAllocator);
        }

        public int Count => _values.Count;

        public void AddRangeFrom(IValueContainer<T> container, int start, int count)
        {
            for (int i = start; i < start + count; i++)
            {
                _values.Add(container.Get(i));
            }
        }

        public void Dispose()
        {
            _values.Dispose();
        }

        public T Get(int index)
        {
            return _values.Get(in index);
        }

        public ref T GetRef(int index)
        {
            return ref _values.GetRef(in index);
        }

        public void Insert(int index, T value)
        {
            _values.InsertAt(index, value);
        }

        public void RemoveAt(int index)
        {
            _values.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _values.RemoveRange(start, count);
        }

        public void Update(int index, T value)
        {
            _values.Update(index, value);
        }

        public unsafe int GetByteSize()
        {
            return _values.Count * sizeof(T);
        }

        public unsafe int GetByteSize(int start, int end)
        {
            return (end - start + 1) * sizeof(T);
        }

        public PrimitiveList<T> GetPrimitiveListCopy(IMemoryAllocator memoryAllocator)
        {
            return _values.Copy(memoryAllocator);
        } 
    }
}

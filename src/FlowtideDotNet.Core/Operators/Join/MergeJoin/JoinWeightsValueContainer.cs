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
using FlowtideDotNet.Storage.Tree;
using System.Buffers;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class JoinWeightsValueContainer : IValueContainer<JoinWeights>
    {
        private PrimitiveList<JoinWeights> _values;

        public Memory<byte> Memory => _values.SlicedMemory;

        public JoinWeightsValueContainer(IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<JoinWeights>(memoryAllocator);
        }

        public JoinWeightsValueContainer(IMemoryOwner<byte> memory, int count, IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<JoinWeights>(memory, count, memoryAllocator);
        }

        public int Count => _values.Count;

        public void AddRangeFrom(IValueContainer<JoinWeights> container, int start, int count)
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

        public JoinWeights Get(int index)
        {
            return _values.Get(in index);
        }

        public ref JoinWeights GetRef(int index)
        {
            return ref _values.GetRef(in index);
        }

        public void Insert(int index, JoinWeights value)
        {
            _values.InsertAt(index, value);
        }

        public void RemoveAt(int index)
        {
            _values.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            var end = start + count;
            for (int i = end - 1; i >= start; i--)
            {
                RemoveAt(i);
            }
        }

        public void Update(int index, JoinWeights value)
        {
            _values.Update(index, value);
        }

        public int GetByteSize()
        {
            return _values.Count * 8;
        }

        public int GetByteSize(int start, int end)
        {
            return (end - start + 1) * 8;
        }
    }
}

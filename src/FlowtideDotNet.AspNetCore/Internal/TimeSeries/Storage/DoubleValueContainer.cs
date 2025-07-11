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

namespace FlowtideDotNet.AspNetCore.TimeSeries
{
    internal class DoubleValueContainer : IValueContainer<double>
    {
        internal PrimitiveList<double> _list;

        public DoubleValueContainer(IMemoryAllocator memoryAllocator)
        {
            _list = new PrimitiveList<double>(memoryAllocator);
        }

        public DoubleValueContainer(PrimitiveList<double> list)
        {
            _list = list;
        }


        public int Count => _list.Count;

        public void AddRangeFrom(IValueContainer<double> container, int start, int count)
        {
            if (container is DoubleValueContainer other)
            {
                for (int i = start; i < start + count; i++)
                {
                    _list.Add(other.Get(i));
                }
            }
        }

        public void Dispose()
        {
            _list.Dispose();
        }

        public double Get(int index)
        {
            return _list.Get(index);
        }

        public int GetByteSize()
        {
            return _list.SlicedMemory.Length;
        }

        public int GetByteSize(int start, int end)
        {
            return (end - start + 1) * sizeof(double);
        }

        public ref double GetRef(int index)
        {
            return ref _list.GetRef(index);
        }

        public void Insert(int index, double value)
        {
            _list.InsertAt(index, value);
        }

        public void RemoveAt(int index)
        {
            _list.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _list.RemoveRange(start, count);
        }

        public void Update(int index, double value)
        {
            _list.Update(index, value);
        }
    }
}

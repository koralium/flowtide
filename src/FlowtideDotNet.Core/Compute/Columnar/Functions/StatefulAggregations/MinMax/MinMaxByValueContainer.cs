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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax
{
    internal struct MinMaxByValue
    {
        public IDataValue Value;
        public int Weight;
    }
    internal class MinMaxByValueContainer : IValueContainer<MinMaxByValue>
    {
        internal Column _data;
        internal PrimitiveList<int> _weights;

        public MinMaxByValueContainer(IMemoryAllocator memoryAllocator)
        {
            _data = Column.Create(memoryAllocator);
            _weights = new PrimitiveList<int>(memoryAllocator);
        }

        public MinMaxByValueContainer(Column data, PrimitiveList<int> weights)
        {
            _data = data;
            _weights = weights;
        }

        public int Count => _weights.Count;

        public void AddRangeFrom(IValueContainer<MinMaxByValue> container, int start, int count)
        {
            if (container is MinMaxByValueContainer other)
            {
                _data.InsertRangeFrom(_data.Count, other._data, start, count);
                _weights.AddRangeFrom(other._weights, start, count);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void Dispose()
        {
            _data.Dispose();
            _weights.Dispose();
        }

        public MinMaxByValue Get(int index)
        {
            return new MinMaxByValue()
            {
                Value = _data.GetValueAt(index, default),
                Weight = _weights.Get(index)
            };
        }

        public int GetByteSize()
        {
            return _data.GetByteSize() + (_weights.Count * sizeof(int));
        }

        public int GetByteSize(int start, int end)
        {
            return _data.GetByteSize(start, end) + ((end - start + 1) * sizeof(int));
        }

        public ref MinMaxByValue GetRef(int index)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, MinMaxByValue value)
        {
            _data.InsertAt(index, value.Value);
            _weights.InsertAt(index, value.Weight);
        }

        public void RemoveAt(int index)
        {
            _data.RemoveAt(index);
            _weights.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _data.RemoveRange(in start, in count);
            _weights.RemoveRange(start, count);
        }

        public void Update(int index, MinMaxByValue value)
        {
            _data.UpdateAt(index, value.Value);
            _weights.Update(index, value.Weight);
        }
    }
}

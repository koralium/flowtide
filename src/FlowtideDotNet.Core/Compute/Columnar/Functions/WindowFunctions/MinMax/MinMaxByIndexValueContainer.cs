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
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.MinMax
{
    internal struct MinMaxByIndexValue
    {
        public IDataValue Value;
        public IDataValue CompareValue;
        public long Index;
    }

    internal class MinMaxByIndexValueContainer : IValueContainer<MinMaxByIndexValue>
    {
        internal Column _data;
        internal Column _compareData;
        internal PrimitiveList<long> _indices;
        private DataValueContainer _valueContainer;
        private DataValueContainer _compareContainer;

        public MinMaxByIndexValueContainer(IMemoryAllocator memoryAllocator)
        {
            _data = Column.Create(memoryAllocator);
            _compareData = Column.Create(memoryAllocator);
            _indices = new PrimitiveList<long>(memoryAllocator);
            _valueContainer = new DataValueContainer();
            _compareContainer = new DataValueContainer();
        }

        public MinMaxByIndexValueContainer(Column data, Column compareData, PrimitiveList<long> indices)
        {
            _data = data;
            _compareData = compareData;
            _indices = indices;
            _valueContainer = new DataValueContainer();
            _compareContainer = new DataValueContainer();
        }
        public int Count => _indices.Count;

        public void AddRangeFrom(IValueContainer<MinMaxByIndexValue> container, int start, int count)
        {
            if (container is MinMaxByIndexValueContainer other)
            {
                _data.InsertRangeFrom(_data.Count, other._data, start, count);
                _compareData.InsertRangeFrom(_data.Count, other._compareData, start, count);
                _indices.AddRangeFrom(other._indices, start, count);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void Dispose()
        {
            _data.Dispose();
            _compareData.Dispose();
            _indices.Dispose();
        }

        public MinMaxByIndexValue Get(int index)
        {
            _data.GetValueAt(index, _valueContainer, default);
            _compareData.GetValueAt(index, _compareContainer, default);
            return new MinMaxByIndexValue()
            {
                Value = _valueContainer,
                CompareValue = _compareContainer,
                Index = _indices.Get(index)
            };
        }

        public int GetByteSize()
        {
            return _data.GetByteSize() + _compareData.GetByteSize() + (_indices.Count * sizeof(long));
        }

        public int GetByteSize(int start, int end)
        {
            return _data.GetByteSize(start, end) + _compareData.GetByteSize(start, end) + ((end - start + 1) * sizeof(long));
        }

        public ref MinMaxByIndexValue GetRef(int index)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, MinMaxByIndexValue value)
        {
            _data.InsertAt(index, value.Value);
            _compareData.InsertAt(index, value.CompareValue);
            _indices.InsertAt(index, value.Index);
        }

        public void RemoveAt(int index)
        {
            _data.RemoveAt(index);
            _compareData.RemoveAt(index);
            _indices.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _data.RemoveRange(in start, in count);
            _compareData.RemoveRange(in start, in count);
            _indices.RemoveRange(start, count);
        }

        public void Update(int index, MinMaxByIndexValue value)
        {
            _data.UpdateAt(index, value.Value);
            _compareData.UpdateAt(index, value.CompareValue);
            _indices.Update(index, value.Index);
        }
    }
}

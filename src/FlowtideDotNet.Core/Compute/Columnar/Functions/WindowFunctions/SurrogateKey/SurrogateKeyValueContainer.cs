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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.SurrogateKey
{
    internal struct SurrogateKeyValue
    {
        public IDataValue Value;
        public int Weight;
    }
    internal class SurrogateKeyValueContainer : IValueContainer<SurrogateKeyValue>
    {
        internal readonly Column _column;
        internal readonly PrimitiveList<int> _weights;

        internal SurrogateKeyValueContainer(IMemoryAllocator memoryAllocator)
        {
            _column = new Column(memoryAllocator);
            _weights = new PrimitiveList<int>(memoryAllocator);
        }

        internal SurrogateKeyValueContainer(Column column, PrimitiveList<int> weights)
        {
            _column = column;
            _weights = weights;
        }

        public int Count => _column.Count;

        public void AddRangeFrom(IValueContainer<SurrogateKeyValue> container, int start, int count)
        {
            if (container is SurrogateKeyValueContainer valueContainer)
            {
                _column.InsertRangeFrom(_column.Count, valueContainer._column, start, count);
                _weights.InsertRangeFrom(_weights.Count, valueContainer._weights, start, count);
            }
            else
            {
                throw new InvalidOperationException("Invalid container type");
            }
        }

        public void Dispose()
        {
            _column.Dispose();
            _weights.Dispose();
        }

        public SurrogateKeyValue Get(int index)
        {
            return new SurrogateKeyValue()
            {
                Value = _column.GetValueAt(index, default),
                Weight = _weights.Get(index)
            };
        }

        public int GetByteSize()
        {
            return _column.GetByteSize() + _weights.Count * sizeof(int);
        }

        public int GetByteSize(int start, int end)
        {
            return _column.GetByteSize(start, end) + ((end - start + 1) * sizeof(int));
        }

        public ref SurrogateKeyValue GetRef(int index)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, SurrogateKeyValue value)
        {
            _column.InsertAt(index, value.Value);
            _weights.InsertAt(index, value.Weight);
        }

        public void RemoveAt(int index)
        {
            _column.RemoveAt(index);
            _weights.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _column.RemoveRange(start, count);
            _weights.RemoveRange(start, count);
        }

        public void Update(int index, SurrogateKeyValue value)
        {
            _column.UpdateAt(index, value.Value);
            _weights.Update(index, value.Weight);
        }
    }
}

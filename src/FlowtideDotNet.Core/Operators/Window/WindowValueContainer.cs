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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Window
{
    internal struct WindowValue
    {
        internal WindowValueContainer valueContainer;
        internal int index;
        public int weight;

        /// <summary>
        /// Updates the state for a specific index and specific weight.
        /// This allows different states for duplicate rows.
        /// 
        /// Returns true, if a new value was added to state or existing one was updated.
        /// Returns false if the same value already exists in the state.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="functionIndex"></param>
        /// <param name="weightIndex"></param>
        /// <param name="value"></param>
        public bool UpdateStateValue<T>(int functionIndex, int weightIndex, T value, ColumnRowReference columnRowReference, IWindowAddOutputRow addOutputRow)
            where T : IDataValue
        {
            var listCount = valueContainer._functionStates[functionIndex].GetListLength(index);
            valueContainer._previousValueSent.Set(index);
            if (listCount <= weightIndex)
            {
                valueContainer._functionStates[functionIndex].AppendToList(index, value);
                addOutputRow.AddOutputRow(columnRowReference, value, 1);
                return true;
            }
            else
            {
                var oldValue = valueContainer._functionStates[functionIndex].GetListElementValue(index, weightIndex);
                if (DataValueComparer.Instance.Compare(value, oldValue) != 0)
                {
                    addOutputRow.AddOutputRow(columnRowReference, oldValue, -1);
                    valueContainer._functionStates[functionIndex].UpdateListElement(index, weightIndex, value);
                    addOutputRow.AddOutputRow(columnRowReference, value, 1);
                    return true;
                }
                return false;
            }
        }
    }

    internal class WindowValueContainer : IValueContainer<WindowValue>
    {
        internal PrimitiveList<int> _weights;
        internal ListColumn[] _functionStates;
        internal BitmapList _previousValueSent;

        public WindowValueContainer(int numberOfFunctions, IMemoryAllocator memoryAllocator)
        {
            _weights = new PrimitiveList<int>(memoryAllocator);
            _functionStates = new ListColumn[numberOfFunctions];
            _previousValueSent = new BitmapList(memoryAllocator);
            for (int i = 0; i < numberOfFunctions; i++)
            {
                _functionStates[i] = new ListColumn(memoryAllocator);
            }
        }

        internal WindowValueContainer(PrimitiveList<int> weights, ListColumn[] functionStates, BitmapList previousValueSent)
        {
            _weights = weights;
            _functionStates = functionStates;
            _previousValueSent = previousValueSent;
        }

        public int Count => _weights.Count;

        public void AddRangeFrom(IValueContainer<WindowValue> container, int start, int count)
        {
            if (container is WindowValueContainer windowValueContainer)
            {
                _weights.InsertRangeFrom(_weights.Count, windowValueContainer._weights, start, count);
                for (int i = 0; i < _functionStates.Length; i++)
                {
                    _functionStates[i].InsertRangeFrom(_functionStates[i].Count, windowValueContainer._functionStates[i], start, count, default);
                }
                _previousValueSent.InsertRangeFrom(_previousValueSent.Count, windowValueContainer._previousValueSent, start, count);
            }
            else
            {
                throw new InvalidOperationException("Invalid container type");
            }
        }

        public void Dispose()
        {
            _weights.Dispose();
            for (int i = 0; i < _functionStates.Length; i++)
            {
                _functionStates[i].Dispose();
            }
            _previousValueSent.Dispose();
        }

        public WindowValue Get(int index)
        {
            return new WindowValue()
            {
                index = index,
                valueContainer = this,
                weight = _weights.Get(index)
            };
        }

        public int GetByteSize()
        {
            return _weights.SlicedMemory.Length + _functionStates.Sum(x => x.GetByteSize()) + _previousValueSent.MemorySlice.Length;
        }

        public int GetByteSize(int start, int end)
        {
            var count = end - start + 1;
            return (count * sizeof(int)) + _functionStates.Sum(x => x.GetByteSize(start, end)) + _previousValueSent.GetByteSize(start, end);
        }

        public ref WindowValue GetRef(int index)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, WindowValue value)
        {
            _weights.InsertAt(index, value.weight);
            for (int i = 0; i < _functionStates.Length; i++)
            {
                _functionStates[i].InsertAt(index, NullValue.Instance);
            }
            _previousValueSent.InsertAt(index, false);
        }

        public void RemoveAt(int index)
        {
            _weights.RemoveAt(index);
            for (int i = 0; i < _functionStates.Length; i++)
            {
                _functionStates[i].RemoveAt(index);
            }
            _previousValueSent.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _weights.RemoveRange(start, count);
            for (int i = 0; i < _functionStates.Length; i++)
            {
                _functionStates[i].RemoveRange(start, count);
            }
            _previousValueSent.RemoveRange(start, count);
        }

        public void Update(int index, WindowValue value)
        {
            _weights.Update(index, value.weight);
        }
    }
}

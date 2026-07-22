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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    internal struct BulkWindowValue
    {
        internal BulkWindowValueContainer valueContainer;
        internal int index;
        public int weight;
    }

    /// <summary>
    /// Per row: weight, sent flag, one output list per function then auxiliary lists.
    /// List elements are one per weight duplicate.
    /// </summary>
    internal class BulkWindowValueContainer : IValueContainer<BulkWindowValue>
    {
        internal PrimitiveList<int> _weights;
        internal ListColumn[] _functionStates;
        internal BitmapList _previousValueSent;

        public BulkWindowValueContainer(int numberOfColumns, IMemoryAllocator memoryAllocator)
        {
            _weights = new PrimitiveList<int>(memoryAllocator);
            _functionStates = new ListColumn[numberOfColumns];
            _previousValueSent = new BitmapList(memoryAllocator);
            for (int i = 0; i < numberOfColumns; i++)
            {
                _functionStates[i] = new ListColumn(memoryAllocator);
            }
        }

        internal BulkWindowValueContainer(PrimitiveList<int> weights, ListColumn[] functionStates, BitmapList previousValueSent)
        {
            _weights = weights;
            _functionStates = functionStates;
            _previousValueSent = previousValueSent;
        }

        public int Count => _weights.Count;

        public void AddRangeFrom(IValueContainer<BulkWindowValue> container, int start, int count)
        {
            if (container is BulkWindowValueContainer other)
            {
                _weights.InsertRangeFrom(_weights.Count, other._weights, start, count);
                for (int i = 0; i < _functionStates.Length; i++)
                {
                    _functionStates[i].InsertRangeFrom(_functionStates[i].Count, other._functionStates[i], start, count, default);
                }
                _previousValueSent.InsertRangeFrom(_previousValueSent.Count, other._previousValueSent, start, count);
            }
            else
            {
                throw new InvalidOperationException("Invalid container type");
            }
        }

        public void DeleteBatch(ReadOnlySpan<int> positions)
        {
            _weights.DeleteBatch(positions);
            for (int i = 0; i < _functionStates.Length; i++)
            {
                _functionStates[i].DeleteBatch(positions);
            }
            _previousValueSent.DeleteBatch(positions);
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

        public BulkWindowValue Get(int index)
        {
            return new BulkWindowValue()
            {
                index = index,
                valueContainer = this,
                weight = _weights.Get(index)
            };
        }

        public int GetByteSize()
        {
            var size = _weights.SlicedMemory.Length + _previousValueSent.MemorySlice.Length;
            for (int i = 0; i < _functionStates.Length; i++)
            {
                size += _functionStates[i].GetByteSize();
            }
            return size;
        }

        public int GetByteSize(int start, int end)
        {
            var count = end - start + 1;
            var size = (count * sizeof(int)) + _previousValueSent.GetByteSize(start, end);
            for (int i = 0; i < _functionStates.Length; i++)
            {
                size += _functionStates[i].GetByteSize(start, end);
            }
            return size;
        }

        public ref BulkWindowValue GetRef(int index)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, BulkWindowValue value)
        {
            _weights.InsertAt(index, value.weight);
            for (int i = 0; i < _functionStates.Length; i++)
            {
                _functionStates[i].InsertAt(index, NullValue.Instance);
            }
            _previousValueSent.InsertAt(index, false);
        }

        public void InsertFrom(BulkWindowValue[] values, ReadOnlySpan<int> sortedLookup, ReadOnlySpan<int> targetPositions)
        {
            // Incoming values carry only a weight, lists start empty.
            for (int i = 0; i < sortedLookup.Length; i++)
            {
                var position = targetPositions[i] + i;
                _weights.InsertAt(position, values[sortedLookup[i]].weight);
                for (int f = 0; f < _functionStates.Length; f++)
                {
                    _functionStates[f].InsertAt(position, NullValue.Instance);
                }
                _previousValueSent.InsertAt(position, false);
            }
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

        public void Update(int index, BulkWindowValue value)
        {
            _weights.Update(index, value.weight);
        }
    }
}

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

using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <summary>
    /// Values held as objects with a byte estimate, one per file, so pages split by size.
    /// </summary>
    internal sealed class ManagedValueContainer<T> : IValueContainer<T>
    {
        private readonly List<T> _values;
        private readonly Func<T, int> _sizeOf;
        private int _byteSize;

        public ManagedValueContainer(Func<T, int> sizeOf)
        {
            _values = new List<T>();
            _sizeOf = sizeOf;
        }

        internal List<T> Values => _values;

        public int Count => _values.Count;

        internal void Add(T value)
        {
            _byteSize += _sizeOf(value);
            _values.Add(value);
        }

        public void Insert(int index, T value)
        {
            _byteSize += _sizeOf(value);
            _values.Insert(index, value);
        }

        public void Update(int index, T value)
        {
            _byteSize += _sizeOf(value) - _sizeOf(_values[index]);
            _values[index] = value;
        }

        public void RemoveAt(int index)
        {
            _byteSize -= _sizeOf(_values[index]);
            _values.RemoveAt(index);
        }

        public T Get(int index)
        {
            return _values[index];
        }

        public ref T GetRef(int index)
        {
            throw new NotSupportedException();
        }

        public void AddRangeFrom(IValueContainer<T> container, int start, int count)
        {
            var other = (ManagedValueContainer<T>)container;
            for (int i = start; i < start + count; i++)
            {
                Add(other._values[i]);
            }
        }

        public void RemoveRange(int start, int count)
        {
            for (int i = start; i < start + count; i++)
            {
                _byteSize -= _sizeOf(_values[i]);
            }
            _values.RemoveRange(start, count);
        }

        public int GetByteSize()
        {
            return _byteSize;
        }

        // The tree passes an inclusive end
        public int GetByteSize(int start, int end)
        {
            var size = 0;
            for (int i = start; i <= end && i < _values.Count; i++)
            {
                size += _sizeOf(_values[i]);
            }
            return size;
        }

        public void InsertFrom(T[] values, ReadOnlySpan<int> sortedLookup, ReadOnlySpan<int> targetPositions)
        {
            throw new NotSupportedException();
        }

        public void DeleteBatch(ReadOnlySpan<int> positions)
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        {
        }
    }
}

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
using FlowtideDotNet.Storage.Memory;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    /// <summary>
    /// Column backed value ring for bounded frames, pushes copy so they outlive the page.
    /// Grows on demand up to capacity so a huge declared frame only allocates what it uses.
    /// </summary>
    internal sealed class BulkWindowValueRing : IDisposable
    {
        private const int InitialAllocation = 16;

        private readonly IMemoryAllocator _memoryAllocator;
        private readonly long _capacity;
        private Column _storage;
        private int _allocated;
        private int _head;
        private int _count;

        public BulkWindowValueRing(long capacity, IMemoryAllocator memoryAllocator)
        {
            Debug.Assert(capacity > 0);
            _capacity = capacity;
            _memoryAllocator = memoryAllocator;
            _allocated = (int)Math.Min(capacity, InitialAllocation);
            _storage = Column.Create(memoryAllocator);
            for (int i = 0; i < _allocated; i++)
            {
                _storage.Add(NullValue.Instance);
            }
        }

        public int Count => _count;

        public void Push(IDataValue value)
        {
            Debug.Assert(_count < _capacity, "Ring is full");
            if (_count == _allocated)
            {
                Grow();
            }
            var slot = (_head + _count) % _allocated;
            _storage.UpdateAt(slot, value);
            _count++;
        }

        private void Grow()
        {
            var newAllocated = (int)Math.Min(Math.Max((long)_allocated * 2, InitialAllocation), Math.Min(_capacity, int.MaxValue));
            var newStorage = Column.Create(_memoryAllocator);
            for (int i = 0; i < _count; i++)
            {
                newStorage.Add(_storage.GetValueAt((_head + i) % _allocated, default));
            }
            for (int i = _count; i < newAllocated; i++)
            {
                newStorage.Add(NullValue.Instance);
            }
            _storage.Dispose();
            _storage = newStorage;
            _allocated = newAllocated;
            _head = 0;
        }

        /// <summary>
        /// Pops the oldest value, valid until the slot is overwritten.
        /// </summary>
        public IDataValue PopOldest()
        {
            Debug.Assert(_count > 0, "Ring is empty");
            var value = _storage.GetValueAt(_head, default);
            _head = (_head + 1) % _allocated;
            _count--;
            return value;
        }

        /// <summary>
        /// Value at the index, 0 is oldest, valid until the slot is overwritten.
        /// </summary>
        public IDataValue GetAt(int index)
        {
            Debug.Assert(index >= 0 && index < _count);
            return _storage.GetValueAt((_head + index) % _allocated, default);
        }

        public void Clear()
        {
            _head = 0;
            _count = 0;
        }

        public void Dispose()
        {
            _storage.Dispose();
        }
    }
}

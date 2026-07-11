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
    /// A fixed capacity ring of data values backed by a column, used by bounded frame window functions to
    /// keep the values currently inside the frame. Pushing copies the value into the ring's own memory, so
    /// pushed values stay valid when the source page is released.
    /// </summary>
    internal sealed class BulkWindowValueRing : IDisposable
    {
        private readonly Column _storage;
        private readonly int _capacity;
        private int _head;
        private int _count;

        public BulkWindowValueRing(int capacity, IMemoryAllocator memoryAllocator)
        {
            Debug.Assert(capacity > 0);
            _capacity = capacity;
            _storage = Column.Create(memoryAllocator);
            for (int i = 0; i < capacity; i++)
            {
                _storage.Add(NullValue.Instance);
            }
        }

        public int Count => _count;

        public void Push(IDataValue value)
        {
            Debug.Assert(_count < _capacity, "Ring is full");
            var slot = (_head + _count) % _capacity;
            _storage.UpdateAt(slot, value);
            _count++;
        }

        /// <summary>
        /// Removes and returns the oldest value. The returned value references the ring's storage and is
        /// only valid until the slot is overwritten by a later push, so it must be consumed immediately.
        /// </summary>
        public IDataValue PopOldest()
        {
            Debug.Assert(_count > 0, "Ring is empty");
            var value = _storage.GetValueAt(_head, default);
            _head = (_head + 1) % _capacity;
            _count--;
            return value;
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

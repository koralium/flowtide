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
using FlowtideDotNet.Storage.StateManager.Internal;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    internal sealed class RowHeapPage : ICacheObject
    {
        private readonly EventBatchData _data;
        private readonly int _count;
        private readonly long _sequence;
        private readonly RowHeapSlotMap? _map;
        private int _rentCount;

        // An appended page, the row of a slot is the slot
        public RowHeapPage(EventBatchData data, int count, long sequence)
        {
            _data = data;
            _count = count;
            _sequence = sequence;
            // The first rent is the creator's, later the cache's
            _rentCount = 1;
        }

        // A page that took over the rows of other pages
        public RowHeapPage(EventBatchData data, int count, RowHeapSlotMap map)
        {
            _data = data;
            _count = count;
            _map = map;
            _rentCount = 1;
        }

        public EventBatchData Data => _data;

        public int Count => _count;

        // Zero on a merged page
        public long Sequence => _sequence;

        // Null on a page as it was appended
        public RowHeapSlotMap? Map => _map;

        public bool RemovedFromCache { get; set; }

        public int RentCount => Volatile.Read(ref _rentCount);

        public bool TryRent()
        {
            int current = Volatile.Read(ref _rentCount);
            while (current > 0)
            {
                int seen = Interlocked.CompareExchange(ref _rentCount, current + 1, current);
                if (seen == current)
                {
                    return true;
                }
                current = seen;
            }
            return false;
        }

        public void Return()
        {
            if (Interlocked.Decrement(ref _rentCount) == 0)
            {
                _data.Dispose();
            }
        }

        public bool TryReclaimForEviction()
        {
            if (Interlocked.CompareExchange(ref _rentCount, 0, 1) == 1)
            {
                _data.Dispose();
                return true;
            }
            return false;
        }

        public void EnterWriteLock()
        {
            Monitor.Enter(this);
        }

        public void ExitWriteLock()
        {
            Monitor.Exit(this);
        }
    }
}

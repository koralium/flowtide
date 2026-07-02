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

namespace FlowtideDotNet.Storage.Persistence.Reservoir.LocalCache
{
    internal class LocalCacheEntry
    {
        /// <summary>
        /// 1 = cache owns it, 2+ is rented, and 0 is deleted
        /// </summary>
        private int _rentCount;
        private int _usageWeight;

        public ulong FileId { get; }
        public long Size { get; }
        public ulong ExpectedCrc64 { get; }

        public LocalCacheEntry(ulong fileId, long size, ulong expectedCrc64, int initialWeight = 0)
        {
            FileId = fileId;
            Size = size;
            ExpectedCrc64 = expectedCrc64;
            _rentCount = 1;
            _usageWeight = initialWeight;
        }

        public int ReferenceCount => Volatile.Read(ref _rentCount);
        public int CurrentWeight => Volatile.Read(ref _usageWeight);

        public void RecordAccess()
        {
            int current = Volatile.Read(ref _usageWeight);
            while (current < 5)
            {
                int old = Interlocked.CompareExchange(ref _usageWeight, current + 1, current);
                if (old == current) break;
                current = old;
            }
        }

        public bool DecrementWeight()
        {
            int current = Volatile.Read(ref _usageWeight);
            if (current > 0)
            {
                Interlocked.Decrement(ref _usageWeight);
                return false;
            }
            return true;
        }

        public bool TryRent()
        {
            int current = Volatile.Read(ref _rentCount);
            while (true)
            {
                if (current == 0) return false;
                int old = Interlocked.CompareExchange(ref _rentCount, current + 1, current);
                if (old == current) return true;
                current = old;
            }
        }

        public bool Return() => Interlocked.Decrement(ref _rentCount) == 0;

        public bool TryFinalizeForEviction() => Interlocked.CompareExchange(ref _rentCount, 0, 1) == 1;
    }

}

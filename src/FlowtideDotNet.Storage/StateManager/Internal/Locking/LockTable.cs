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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.StateManager.Internal.Locking
{
    internal class LockTable : ILockTable
    {
        internal struct MemLockInfo
        {
            public int lockCount;
            public SemaphoreSlim? semaphore;
        }

        internal class LockTableFunctions : IInMemKVUserFunctions<long, long, MemLockInfo>
        {
            public long CreateHeapKey(ref long key)
            {
                return key;
            }

            public void Dispose(ref long key, ref MemLockInfo value)
            {
            }

            public bool Equals(ref long key, long heapKey)
            {
                return key == heapKey;
            }

            public long GetHashCode64(ref long key)
            {
                return key;
            }

            public ref long GetHeapKeyRef(long heapKey)
            {
                throw new NotImplementedException();
            }

            public bool IsActive(ref MemLockInfo value)
            {
                return value.lockCount > 0;
            }
        }

        InMemKV<long, long, MemLockInfo, LockTableFunctions> InMemKV = new InMemKV<long, long, MemLockInfo, LockTableFunctions>(1000, 16 >> 4, new LockTableFunctions());

        public LockTable()
        {
            
        }

        public ValueTask LockAsync(long id)
        {
            var findentry = new FindEntryFunc();
            InMemKV.FindOrAddEntry(ref id, id, ref findentry);
            if (findentry.semaphore != null)
            {
                var task = findentry.semaphore.WaitAsync();
                if (task.IsCompletedSuccessfully)
                {
                    return ValueTask.CompletedTask;
                }
                return new ValueTask(task);
            }
            return ValueTask.CompletedTask;
        }

        public void Unlock(long id)
        {
            var findentry = new RemoveEntryFunc();
            InMemKV.FindEntry(ref id, id, ref findentry);
            if (findentry.semaphore != null)
            {
                findentry.semaphore.Release();
            }
        }

        private struct RemoveEntryFunc : InMemKV<long, long, MemLockInfo, LockTableFunctions>.IFindEntryFunctions
        {
            public SemaphoreSlim? semaphore;
            public void FoundEntry(ref long key, ref MemLockInfo value)
            {
                value.lockCount--;
                if (value.lockCount > 0)
                {
                    this.semaphore = value.semaphore;
                }
            }

            public void NotFound(ref long key)
            {
                throw new Exception("Unlock without lock");
            }
        }

        private struct FindEntryFunc : InMemKV<long, long, MemLockInfo, LockTableFunctions>.IFindOrAddEntryFunctions
        {
            public SemaphoreSlim? semaphore;
            public void AddedEntry(ref long key, ref MemLockInfo value)
            {
                value.lockCount = 1;
            }

            public void FoundEntry(ref long key, ref MemLockInfo value)
            {
                value.lockCount++;
                if (value.lockCount > 1)
                {
                    if (value.semaphore == null)
                    {
                        value.semaphore = new SemaphoreSlim(0, 1);
                    }
                    this.semaphore = value.semaphore;
                }
            }
        }

    }
}

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
using FlowtideDotNet.Storage.Tree.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Queue.Internal
{
    internal class QueueNode<V, TValueContainer> : IBPlusTreeNode, IDisposable
        where TValueContainer : IValueContainer<V>
    {
        public TValueContainer values;

        public long next;
        public long previous;

        private int _rentCount;
        private bool disposedValue;

        public long Id { get; }

        public int RentCount => Thread.VolatileRead(ref _rentCount);

        public bool RemovedFromCache { get; set; }

        public QueueNode(long id, TValueContainer valueContainer)
        {
            Id = id;
            values = valueContainer;
            // Rent counter always starts at 1.
            _rentCount = 1;
        }

        public int GetByteSize()
        {
            return values.GetByteSize();
        }

        public int GetByteSize(int start, int end)
        {
            return values.GetByteSize(start, end);
        }

        public bool TryRent()
        {
            var localRentCount = Thread.VolatileRead(ref _rentCount);
            if (localRentCount == 0)
            {
                return false;
            }
            int incrementedValue;
            // Run a loop to try and add the new value, if compare exchange fails, add to the new returned value + 1, if 0 return false
            do
            {
                incrementedValue = localRentCount;
                localRentCount = Interlocked.CompareExchange(ref _rentCount, incrementedValue + 1, localRentCount);
                if (localRentCount == 0)
                {
                    return false;
                }
            } while (localRentCount != incrementedValue);

            return true;
        }

        public void Return()
        {
            var val = Interlocked.Decrement(ref _rentCount);
            if (val == 0)
            {
                Dispose();
            }
        }

        public void EnterWriteLock()
        {
            Monitor.Enter(this);
        }

        public void ExitWriteLock()
        {
            Monitor.Exit(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    values.Dispose();
                }
                disposedValue = true;
            }
        }

        ~QueueNode()
        {
            Dispose(true);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

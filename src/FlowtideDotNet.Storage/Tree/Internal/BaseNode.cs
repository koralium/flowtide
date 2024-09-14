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

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal abstract class BaseNode<K, TKeyContainer> : IBPlusTreeNode, IDisposable
        where TKeyContainer: IKeyContainer<K>
    {
        public TKeyContainer keys;
        private int _rentCount;
        private bool disposedValue;

        public BaseNode(long id, TKeyContainer container)
        {
            keys = container;
            Id = id;
            // Rent counter always starts at 1.
            _rentCount = 1;
        }

        public long Id { get; }

        public int RentCount => Thread.VolatileRead(ref _rentCount);

        public bool RemovedFromCache { get; set; }

        public void EnterWriteLock()
        {
            Monitor.Enter(this);
        }

        public void ExitWriteLock()
        {
            Monitor.Exit(this);
        }

        public abstract Task Print(StringBuilder stringBuilder, Func<long, ValueTask<BaseNode<K, TKeyContainer>>> lookupFunc);

        public abstract Task PrintNextPointers(StringBuilder stringBuilder);

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

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    keys.Dispose();
                }
                disposedValue = true;
            }
        }

        ~BaseNode()
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

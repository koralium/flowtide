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

using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Storage.Memory
{
    public class StreamMemoryManager : IStreamMemoryManager
    {
        private readonly string _streamName;
        private readonly Meter _meter;
        private readonly Dictionary<string, OperatorMemoryManager> _managers;
        private MemoryHeap _memoryHeap;
        private bool disposedValue;
        private long _allocatedMemory;

        internal MemoryHeap MemoryHeap => _memoryHeap;

        public StreamMemoryManager(string streamName)
        {
            this._streamName = streamName;
            _meter = new Meter($"flowtide.{streamName}.memory");
            _managers = new Dictionary<string, OperatorMemoryManager>();
            _memoryHeap = FlowtideMemoryAllocation.CreateMemoryHeap();
        }

        public IOperatorMemoryManager CreateOperatorMemoryManager(string operatorName)
        {
            if (_managers.TryGetValue(operatorName, out var manager))
            {
                return manager;
            }
            manager = new OperatorMemoryManager(_streamName, operatorName, _meter, this);
            _managers.Add(operatorName, manager);
            return manager;
        }

        public long GetAllocatedMemory()
        {
            return Interlocked.Read(ref _allocatedMemory);
        }

        internal void AddMemoryDelta(int delta)
        {
            Interlocked.Add(ref _allocatedMemory, delta);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                FlowtideMemoryAllocation.FreeMemoryHeap(_memoryHeap);
                disposedValue = true;
            }
        }

        ~StreamMemoryManager()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}

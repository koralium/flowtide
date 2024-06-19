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
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Memory
{
    internal unsafe class BatchMemoryManager : IMemoryAllocator
    {
        private class MemoryContainer
        {
            public IMemoryOwner<byte>? _owner;
            public int _usageCount;
        }
        private readonly object _lock = new object();
        private Dictionary<IntPtr, MemoryContainer> _usage = new Dictionary<IntPtr, MemoryContainer>();

        private int usageCounter = 0;

        public BatchMemoryManager(int startUsage)
        {
            usageCounter = startUsage;
        }

        public void IncreaseUsage()
        {
            lock (_lock)
            {
                usageCounter++;
            }
        }

        public void DecreaseUsage()
        {
            lock (_lock)
            {
                usageCounter--;
                if (usageCounter == 0)
                {
                    foreach (var ptr in _usage.Keys)
                    {
                        NativeMemory.AlignedFree(ptr.ToPointer());
                    }
                    _usage.Clear();
                }
            }
        }

        public IMemoryOwner<byte> Allocate(int size, int alignment)
        {
            var ptr = NativeMemory.AlignedAlloc((nuint)size, (nuint)alignment); 

            lock (_lock)
            {
                _usage.Add(new IntPtr(ptr), new MemoryContainer()
                {
                    _owner = new NativeMemoryOwner(ptr, size, alignment),
                    _usageCount = 1
                });
            }
            
            return new NativeMemoryOwner(ptr, size, alignment);
        }

        internal void Free(void* ptr)
        {
            IMemoryOwner<byte>? memory = null;
            lock (_lock)
            {
                if (_usage.TryGetValue(new IntPtr(ptr), out var container))
                {
                    
                    if (container._usageCount == 1)
                    {
                        memory = container._owner;
                        _usage.Remove(new IntPtr(ptr));
                    }
                    else
                    {
                        container._usageCount--;
                    }
                }
            }
            if (memory != null)
            {
                memory.Dispose();
            }
        }

        /// <summary>
        /// Add already allocated memory to the manager with a usage count.
        /// This is used when data has been read in from apache arrow and we want to manage the memory.
        /// Since arrow only allocates a single array of memory for all columns, we need to manage the memory ourselves.
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="memory"></param>
        /// <param name="usageCount"></param>
        internal void AddUsedMemory(IntPtr ptr, IMemoryOwner<byte> memory, int usageCount)
        {
            lock (_lock)
            {
                _usage.Add(ptr, new MemoryContainer()
                {
                    _owner = memory,
                    _usageCount = usageCount
                });
            }
        }
    }
}

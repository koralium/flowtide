//// Licensed under the Apache License, Version 2.0 (the "License")
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////     http://www.apache.org/licenses/LICENSE-2.0
////  
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.

//using System;
//using System.Buffers;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Linq;
//using System.Runtime.InteropServices;
//using System.Text;
//using System.Threading.Tasks;

//namespace FlowtideDotNet.Core.ColumnStore.Memory
//{
//    internal unsafe class BatchMemoryManager : IMemoryAllocator
//    {
//#if DEBUG_MEMORY
//        internal class MemoryDebugInfo
//        {
//            public IMemoryOwner<byte> memory;
//            public string stackTrace;

//            public MemoryDebugInfo(IMemoryOwner<byte> memory, string stackTrace)
//            {
//                this.memory = memory;
//                this.stackTrace = stackTrace;
//            }
//        }
//        private static readonly ConcurrentDictionary<IntPtr, MemoryDebugInfo> _allocatedMemory = new ConcurrentDictionary<nint, MemoryDebugInfo>();

//        public static IReadOnlyDictionary<IntPtr, MemoryDebugInfo> AllocatedMemory => _allocatedMemory;

//        private static void AddAllocatedMemoryToDebug(IntPtr ptr, IMemoryOwner<byte> memory)
//        {
//            var trace = Environment.StackTrace;
//            _allocatedMemory.TryAdd(ptr, new MemoryDebugInfo(memory, trace));
//        }

//        private static void RemoveAllocatedMemoryFromDebug(IntPtr ptr)
//        {
//            _allocatedMemory.TryRemove(ptr, out _);
//        }

//#endif
//        private unsafe class MemoryContainer
//        {
//            public IMemoryOwner<byte>? _owner;
//            public void* _ptr;
//            public int _usageCount;
//        }
//        private readonly object _lock = new object();
//        private Dictionary<IntPtr, MemoryContainer> _usage = new Dictionary<IntPtr, MemoryContainer>();

//        private int usageCounter = 0;

//        public BatchMemoryManager(int startUsage)
//        {
//            usageCounter = startUsage;
//        }

//        public void IncreaseUsage(int count)
//        {
//            lock (_lock)
//            {
//                usageCounter += count;
//            }
//        }

//        public void DecreaseUsage(int count)
//        {
//            lock (_lock)
//            {
//                usageCounter -= count;
//                if (usageCounter == 0)
//                {
//                    foreach (var ptr in _usage.Keys)
//                    {
//                        NativeMemory.AlignedFree(ptr.ToPointer());
//                    }
//                    _usage.Clear();
//                }
//            }
//        }

//        public IMemoryOwner<byte> Allocate(int size, int alignment)
//        {
//            var ptr = NativeMemory.AlignedAlloc((nuint)size, (nuint)alignment);

//            var memoryOwner = new BatchMemoryOwner(this, ptr, size);
//            lock (_lock)
//            {
//                _usage.Add(new IntPtr(ptr), new MemoryContainer()
//                {
//                    _owner = memoryOwner,
//                    _ptr = ptr,
//                    _usageCount = 1
//                });
//            }

//#if DEBUG_MEMORY
//            AddAllocatedMemoryToDebug(new IntPtr(ptr), memoryOwner);
//#endif

//            return memoryOwner;
//        }

//        internal void Free(void* ptr)
//        {
//            IMemoryOwner<byte>? memory = null;
//            void* aligned_ptr = default;
//            lock (_lock)
//            {
//                if (_usage.TryGetValue(new IntPtr(ptr), out var container))
//                {
                    
//                    if (container._usageCount == 1)
//                    {
//                        memory = container._owner;
//                        aligned_ptr = container._ptr;
//                        _usage.Remove(new IntPtr(ptr));
//                    }
//                    else
//                    {
//                        container._usageCount--;
//                    }
//                }
//            }
//            if (memory != null)
//            {
//#if DEBUG_MEMORY
//                RemoveAllocatedMemoryFromDebug(new IntPtr(ptr));
//#endif
//                // If the pointer is set, then we need to free the memory since the memory came from this instance
//                if (aligned_ptr != null)
//                {
//                    NativeMemory.AlignedFree(aligned_ptr);
//                }
//                else
//                {
//                    // Someone else created the memory, free it here
//                    memory.Dispose();
//                }
//            }
//        }

//        /// <summary>
//        /// Add already allocated memory to the manager with a usage count.
//        /// This is used when data has been read in from apache arrow and we want to manage the memory.
//        /// Since arrow only allocates a single array of memory for all columns, we need to manage the memory ourselves.
//        /// </summary>
//        /// <param name="ptr"></param>
//        /// <param name="memory"></param>
//        /// <param name="usageCount"></param>
//        internal void AddUsedMemory(IntPtr ptr, IMemoryOwner<byte> memory, int usageCount)
//        {
//#if DEBUG_MEMORY
//            AddAllocatedMemoryToDebug(ptr, memory);
//#endif
//            lock (_lock)
//            {
//                _usage.Add(ptr, new MemoryContainer()
//                {
//                    _owner = memory,
//                    _usageCount = usageCount
//                });
//            }
//        }
//    }
//}

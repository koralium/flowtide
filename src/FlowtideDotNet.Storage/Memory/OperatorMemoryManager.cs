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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Memory
{
    internal unsafe class OperatorMemoryManager : IOperatorMemoryManager
    {
        private readonly string m_streamName;
        private readonly string m_operatorName;
        private TagList tagList;
        private long _allocatedMemory;
        private long _freedMemory;
        private long _allocationCount;
        private long _freeCount;

        public OperatorMemoryManager(string streamName, string operatorName, Meter meter)
        {
            this.m_streamName = streamName;
            this.m_operatorName = operatorName;

            tagList = new TagList
            {
                { "stream", m_streamName },
                { "operator", m_operatorName }
            };
            meter.CreateObservableGauge("flowtide.memory.allocated_bytes", () =>
            {
                return new Measurement<long>(_allocatedMemory, new KeyValuePair<string, object?>("stream", m_streamName), new KeyValuePair<string, object?>("operator", m_operatorName));
            }, "bytes");
            meter.CreateObservableGauge("flowtide.memory.allocation_count", () =>
            {
                return new Measurement<long>(_allocationCount, new KeyValuePair<string, object?>("stream", m_streamName), new KeyValuePair<string, object?>("operator", m_operatorName));
            });
            meter.CreateObservableCounter("flowtide.memory.freed_bytes", () =>
            {
                return new Measurement<long>(_freedMemory, new KeyValuePair<string, object?>("stream", m_streamName), new KeyValuePair<string, object?>("operator", m_operatorName));
            }, "bytes");
            meter.CreateObservableGauge("flowtide.memory.free_count", () =>
            {
                return new Measurement<long>(_freeCount, new KeyValuePair<string, object?>("stream", m_streamName), new KeyValuePair<string, object?>("operator", m_operatorName));
            });
        }

        public IMemoryOwner<byte> Allocate(int size, int alignment)
        {
            var ptr = NativeMemory.AlignedAlloc((nuint)size, (nuint)alignment);
            RegisterAllocationToMetrics(size);
            return NativeCreatedMemoryOwnerFactory.Get(ptr, size, this);
        }

        public void RegisterAllocationToMetrics(int size)
        {
            Interlocked.Add(ref _allocatedMemory, size);
            Interlocked.Increment(ref _allocationCount);
        }

        public void RegisterFreeToMetrics(int size)
        {
            Interlocked.Add(ref _freedMemory, size);
            Interlocked.Increment(ref _freeCount);
        }
    }
}

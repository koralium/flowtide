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

using FlowtideDotNet.Storage.Memory;
using System.Buffers;

namespace FlowtideDotNet.Storage.Tests.Memory
{
    public class FlowtideMemoryTests
    {
        /// <summary>
        /// Only implements the metric hooks so we get the default methods.
        /// </summary>
        private sealed class CountingAllocator : IMemoryAllocator
        {
            public long AllocatedBytes;
            public long FreedBytes;
            public int AllocationCount;
            public int FreeCount;

            public IMemoryOwner<byte> Allocate(int size, int alignment)
            {
                throw new NotSupportedException();
            }

            public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
            {
                throw new NotSupportedException();
            }

            public void RegisterAllocationToMetrics(int size)
            {
                AllocatedBytes += size;
                AllocationCount++;
            }

            public void RegisterFreeToMetrics(int size)
            {
                FreedBytes += size;
                FreeCount++;
            }
        }

        [Fact]
        public void AllocateReturnsAtLeastRequestedSize()
        {
            IMemoryAllocator allocator = new CountingAllocator();
            var memory = allocator.AllocateMemory(100);

            Assert.False(memory.IsNull);
            Assert.True(memory.Length >= 100);
            Assert.Equal(memory.Length, memory.Span.Length);

            allocator.Free(ref memory);
        }

        [Fact]
        public void AllocateRegistersRoundedLengthToMetrics()
        {
            var allocator = new CountingAllocator();
            var memory = ((IMemoryAllocator)allocator).AllocateMemory(100);

            Assert.Equal(memory.Length, allocator.AllocatedBytes);
            Assert.Equal(1, allocator.AllocationCount);

            ((IMemoryAllocator)allocator).Free(ref memory);
        }

        [Fact]
        public void ReallocFromDefaultAllocates()
        {
            IMemoryAllocator allocator = new CountingAllocator();
            FlowtideMemory memory = default;

            allocator.Realloc(ref memory, 64);

            Assert.False(memory.IsNull);
            Assert.True(memory.Length >= 64);

            allocator.Free(ref memory);
        }

        [Fact]
        public void ReallocGrowPreservesData()
        {
            IMemoryAllocator allocator = new CountingAllocator();
            var memory = allocator.AllocateMemory(64);
            for (int i = 0; i < 64; i++)
            {
                memory.Span[i] = (byte)i;
            }

            // We grow far past the size class so it moves.
            allocator.Realloc(ref memory, 64 * 1024);

            Assert.True(memory.Length >= 64 * 1024);
            for (int i = 0; i < 64; i++)
            {
                Assert.Equal((byte)i, memory.Span[i]);
            }

            allocator.Free(ref memory);
        }

        [Fact]
        public void FreeZeroesTheReference()
        {
            IMemoryAllocator allocator = new CountingAllocator();
            var memory = allocator.AllocateMemory(64);

            allocator.Free(ref memory);

            Assert.True(memory.IsNull);
            Assert.Equal(0, memory.Length);
        }

        [Fact]
        public void DoubleFreeIsANoOp()
        {
            var allocator = new CountingAllocator();
            var memory = ((IMemoryAllocator)allocator).AllocateMemory(64);

            ((IMemoryAllocator)allocator).Free(ref memory);
            ((IMemoryAllocator)allocator).Free(ref memory);

            Assert.Equal(1, allocator.FreeCount);
            Assert.Equal(allocator.AllocatedBytes, allocator.FreedBytes);
        }

        [Fact]
        public void ReallocShrinkPreservesDataAndRegistersFree()
        {
            var allocator = new CountingAllocator();
            IMemoryAllocator iface = allocator;

            var memory = iface.AllocateMemory(200_000);
            for (int i = 0; i < 64; i++)
            {
                memory.Span[i] = (byte)i;
            }
            var previousLength = memory.Length;

            iface.Realloc(ref memory, 100);

            Assert.True(memory.Length >= 100);
            Assert.True(memory.Length < previousLength);
            for (int i = 0; i < 64; i++)
            {
                Assert.Equal((byte)i, memory.Span[i]);
            }
            Assert.Equal(1, allocator.FreeCount);
            Assert.Equal(previousLength - memory.Length, allocator.FreedBytes);

            iface.Free(ref memory);
            Assert.Equal(allocator.AllocatedBytes, allocator.FreedBytes);
        }

        [Fact]
        public void MetricsAreSymmetricOverAllocReallocFree()
        {
            var allocator = new CountingAllocator();
            IMemoryAllocator iface = allocator;

            var memory = iface.AllocateMemory(100);
            iface.Realloc(ref memory, 5000);
            iface.Realloc(ref memory, 200000);
            iface.Free(ref memory);

            Assert.Equal(allocator.AllocatedBytes, allocator.FreedBytes);
        }
    }
}

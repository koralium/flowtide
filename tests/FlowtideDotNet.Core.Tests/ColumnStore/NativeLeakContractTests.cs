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

using Apache.Arrow;
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class NativeLeakContractTests
    {
        private sealed class CountingAllocator : IMemoryAllocator
        {
            public long AllocatedBytes;
            public long FreedBytes;

            public IMemoryOwner<byte> Allocate(int size, int alignment)
            {
                return GlobalMemoryManager.Instance.Allocate(size, alignment);
            }

            public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
            {
                return GlobalMemoryManager.Instance.Realloc(memory, size, alignment);
            }

            public void RegisterAllocationToMetrics(int size)
            {
                AllocatedBytes += size;
            }

            public void RegisterFreeToMetrics(int size)
            {
                FreedBytes += size;
            }
        }

        /// <summary>
        /// Fails the Nth allocation.
        /// </summary>
        private sealed class FailingAllocator : IMemoryAllocator
        {
            public int AllowedAllocations;
            public long AllocatedBytes;
            public long FreedBytes;

            public FlowtideMemory AllocateMemory(int size)
            {
                if (AllowedAllocations-- <= 0)
                {
                    throw new InvalidOperationException("Simulated allocation failure");
                }
                var memory = ((IMemoryAllocator)GlobalMemoryManager.Instance).AllocateMemory(size);
                AllocatedBytes += memory.Length;
                return memory;
            }

            public void Free(ref FlowtideMemory memory)
            {
                FreedBytes += memory.Length;
                ((IMemoryAllocator)GlobalMemoryManager.Instance).Free(ref memory);
            }

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
            }

            public void RegisterFreeToMetrics(int size)
            {
            }
        }

        /// <summary>
        /// Nothing adopted the buffer so the constructor frees it.
        /// </summary>
        [Fact]
        public void IntegerColumnAdoptionCtorFreesMemoryOnUnsupportedBitWidth()
        {
            var allocator = new CountingAllocator();
            var memory = ((IMemoryAllocator)allocator).AllocateMemory(64);

            Assert.Throws<NotImplementedException>(() => new IntegerColumn(allocator, memory, 4, 12));
            Assert.Equal(allocator.AllocatedBytes, allocator.FreedBytes);
        }

        [Fact]
        public void BinaryListCopyFreesFirstBlockWhenSecondAllocationFails()
        {
            BinaryList list = new BinaryList(GlobalMemoryManager.Instance);
            list.Add(new byte[] { 1, 2, 3 }, GlobalMemoryManager.Instance);
            list.Add(new byte[] { 4, 5 }, GlobalMemoryManager.Instance);

            var failing = new FailingAllocator { AllowedAllocations = 1 };
            try
            {
                list.Copy(failing);
                Assert.Fail("Expected the copy to throw");
            }
            catch (InvalidOperationException)
            {
            }
            Assert.Equal(failing.AllocatedBytes, failing.FreedBytes);

            list.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void ArrowToBatchUnionWithUnsupportedChildDoesNotLeak()
        {
            var nullField = new Field("0", NullType.Default, true);
            var stringField = new Field("1", StringType.Default, true);
            var floatField = new Field("2", FloatType.Default, true);
            var unionType = new UnionType(new[] { nullField, stringField, floatField }, new[] { 0, 1, 2 }, UnionMode.Dense);

            using var nullArray = new NullArray(1);
            using var stringArray = new StringArray.Builder().Append("a").Build();
            using var floatArray = new FloatArray.Builder().Append(1.5f).Build();

            using var typeIds = new ArrowBuffer.Builder<byte>().Append(1).Append(2).Build();
            using var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(0).Build();
            using var unionArray = new DenseUnionArray(unionType, 2, new IArrowArray[] { nullArray, stringArray, floatArray }, typeIds, offsets);

            var schema = new Schema.Builder().Field(new Field("u", unionType, true)).Build();
            using var recordBatch = new RecordBatch(schema, new IArrowArray[] { unionArray }, 2);

            var allocator = new CountingAllocator();
            Assert.ThrowsAny<Exception>(() => EventArrowSerializer.ArrowToBatch(recordBatch, allocator));
            Assert.Equal(allocator.AllocatedBytes, allocator.FreedBytes);
        }
    }
}

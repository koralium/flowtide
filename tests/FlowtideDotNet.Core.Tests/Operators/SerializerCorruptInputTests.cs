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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.SurrogateKey;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Core.Operators.Window;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Core.Tests.Operators
{
    public class SerializerCorruptInputTests
    {
        /// <summary>
        /// Tracks live pointers so we also catch an over free.
        /// </summary>
        private sealed unsafe class CountingAllocator : IMemoryAllocator
        {
            private readonly HashSet<nint> _live = new HashSet<nint>();

            public long AllocatedBytes;
            public long FreedBytes;
            public int LiveCount => _live.Count;
            public string? Violation;

            public FlowtideMemory AllocateMemory(int size)
            {
                if (size > 64 * 1024 * 1024)
                {
                    throw new InvalidOperationException("Refusing an implausible allocation");
                }
                var memory = ((IMemoryAllocator)GlobalMemoryManager.Instance).AllocateMemory(size);
                if (!_live.Add((nint)memory.Pointer))
                {
                    Violation ??= $"allocator handed out live pointer 0x{(nint)memory.Pointer:x}";
                }
                AllocatedBytes += memory.Length;
                return memory;
            }

            public void Realloc(ref FlowtideMemory memory, int size)
            {
                if (memory.IsNull)
                {
                    memory = AllocateMemory(size);
                    return;
                }
                var previousPointer = (nint)memory.Pointer;
                var previousLength = memory.Length;
                ((IMemoryAllocator)GlobalMemoryManager.Instance).Realloc(ref memory, size);
                _live.Remove(previousPointer);
                _live.Add((nint)memory.Pointer);
                AllocatedBytes += memory.Length - previousLength;
            }

            public void Free(ref FlowtideMemory memory)
            {
                if (memory.IsNull)
                {
                    return;
                }
                var pointer = (nint)memory.Pointer;
                if (!_live.Remove(pointer))
                {
                    Violation ??= $"free of an untracked or already freed pointer 0x{pointer:x}";
                }
                FreedBytes += memory.Length;
                ((IMemoryAllocator)GlobalMemoryManager.Instance).Free(ref memory);
            }

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
            }

            public void RegisterFreeToMetrics(int size)
            {
            }

            public void AssertClean(string context)
            {
                Assert.True(Violation == null, $"{context}: {Violation}");
                Assert.True(LiveCount == 0, $"{context}: {LiveCount} native block(s) still live ({AllocatedBytes - FreedBytes} bytes)");
            }
        }

        private static void AssertNoLeakAtEveryTruncation(byte[] serialized, Action<CountingAllocator, byte[], int> deserializeAndDispose)
        {
            for (int cut = 0; cut <= serialized.Length; cut++)
            {
                var allocator = new CountingAllocator();
                bool success = false;
                try
                {
                    deserializeAndDispose(allocator, serialized, cut);
                    success = true;
                }
                catch
                {
                }
                if (cut == serialized.Length)
                {
                    Assert.True(success, "Expected the untruncated input to deserialize");
                }
                allocator.AssertClean($"truncation {cut}");
            }
        }

        /// <summary>
        /// Sweeps cannot produce a billion so we plant one.
        /// </summary>
        private static void AssertHostileCountsAreRejected(byte[] serialized, Action<CountingAllocator, byte[]> deserializeAndDispose)
        {
            const long AllocationBudget = 64L * 1024 * 1024;
            ReadOnlySpan<int> hostileCounts = stackalloc int[] { 1342177280, 0x40000000, 100_000_000, -1 };

            for (int offset = 0; offset + 4 <= serialized.Length; offset += 4)
            {
                for (int h = 0; h < hostileCounts.Length; h++)
                {
                    var corrupt = (byte[])serialized.Clone();
                    BinaryPrimitives.WriteInt32LittleEndian(corrupt.AsSpan(offset), hostileCounts[h]);

                    var allocator = new CountingAllocator();
                    var before = GC.GetAllocatedBytesForCurrentThread();
                    try
                    {
                        deserializeAndDispose(allocator, corrupt);
                    }
                    catch
                    {
                    }
                    var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

                    Assert.True(allocated < AllocationBudget,
                        $"count {hostileCounts[h]} at offset {offset} caused {allocated / (1024 * 1024)} MB of managed allocation");
                    allocator.AssertClean($"count {hostileCounts[h]} at offset {offset}");
                }
            }
        }

        [Fact]
        public void StreamEventValueSerializerHostileCountsAreRejected()
        {
            var serialized = SerializeStreamEventContainer();
            AssertHostileCountsAreRejected(serialized, (allocator, bytes) =>
            {
                var target = new StreamEventValueSerializer(allocator);
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes));
                var result = target.Deserialize(ref reader);
                result.Dispose();
            });
        }

        [Fact]
        public void WindowValueContainerSerializerHostileCountsAreRejected()
        {
            var setupSerializer = new WindowValueContainerSerializer(2, GlobalMemoryManager.Instance);
            var container = setupSerializer.CreateEmpty();
            var bufferWriter = new ArrayBufferWriter<byte>();
            IBufferWriter<byte> writer = bufferWriter;
            setupSerializer.Serialize(in writer, in container);
            container.Dispose();

            AssertHostileCountsAreRejected(bufferWriter.WrittenSpan.ToArray(), (allocator, bytes) =>
            {
                var target = new WindowValueContainerSerializer(2, allocator);
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes));
                var result = target.Deserialize(ref reader);
                result.Dispose();
            });
        }

        [Fact]
        public void ColumnAggregateValueSerializerHostileCountsAreRejected()
        {
            var setupSerializer = new ColumnAggregateValueSerializer(1, GlobalMemoryManager.Instance);
            var container = setupSerializer.CreateEmpty();
            var bufferWriter = new ArrayBufferWriter<byte>();
            IBufferWriter<byte> writer = bufferWriter;
            setupSerializer.Serialize(in writer, in container);
            container.Dispose();

            AssertHostileCountsAreRejected(bufferWriter.WrittenSpan.ToArray(), (allocator, bytes) =>
            {
                var target = new ColumnAggregateValueSerializer(1, allocator);
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes));
                var result = target.Deserialize(ref reader);
                result.Dispose();
            });
        }

        private static byte[] SerializeStreamEventContainer()
        {
            var setupAllocator = GlobalMemoryManager.Instance;
            var weights = new PrimitiveList<int>(setupAllocator);
            weights.Add(1);
            weights.Add(-1);
            var iterations = new PrimitiveList<uint>(setupAllocator);
            iterations.Add(0);
            iterations.Add(0);
            Column column = Column.Create(setupAllocator);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            var container = new StreamEventValueContainer(setupAllocator);
            container.Add(new StreamMessage<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData([column]))), 42));

            var serializer = new StreamEventValueSerializer(setupAllocator);
            var bufferWriter = new ArrayBufferWriter<byte>();
            IBufferWriter<byte> writer = bufferWriter;
            serializer.Serialize(in writer, in container);
            container.Dispose();
            return bufferWriter.WrittenSpan.ToArray();
        }

        [Fact]
        public void StreamEventValueSerializerTruncatedInputDoesNotLeak()
        {
            var setupAllocator = GlobalMemoryManager.Instance;
            var weights = new PrimitiveList<int>(setupAllocator);
            weights.Add(1);
            weights.Add(-1);
            var iterations = new PrimitiveList<uint>(setupAllocator);
            iterations.Add(0);
            iterations.Add(0);
            Column column = Column.Create(setupAllocator);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            var container = new StreamEventValueContainer(setupAllocator);
            container.Add(new StreamMessage<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData([column]))), 42));

            var serializer = new StreamEventValueSerializer(setupAllocator);
            var bufferWriter = new ArrayBufferWriter<byte>();
            IBufferWriter<byte> writer = bufferWriter;
            serializer.Serialize(in writer, in container);
            container.Dispose();
            var serialized = bufferWriter.WrittenSpan.ToArray();

            AssertNoLeakAtEveryTruncation(serialized, (allocator, bytes, length) =>
            {
                var target = new StreamEventValueSerializer(allocator);
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes, 0, length));
                var result = target.Deserialize(ref reader);
                result.Dispose();
            });
        }

        [Fact]
        public void WindowValueContainerSerializerTruncatedInputDoesNotLeak()
        {
            var setupSerializer = new WindowValueContainerSerializer(2, GlobalMemoryManager.Instance);
            var container = setupSerializer.CreateEmpty();
            var bufferWriter = new ArrayBufferWriter<byte>();
            IBufferWriter<byte> writer = bufferWriter;
            setupSerializer.Serialize(in writer, in container);
            container.Dispose();
            var serialized = bufferWriter.WrittenSpan.ToArray();

            AssertNoLeakAtEveryTruncation(serialized, (allocator, bytes, length) =>
            {
                var target = new WindowValueContainerSerializer(2, allocator);
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes, 0, length));
                var result = target.Deserialize(ref reader);
                result.Dispose();
            });
        }

        [Fact]
        public void SurrogateKeyValueSerializerTruncatedInputDoesNotLeak()
        {
            var setupSerializer = new SurrogateKeyValueContainerSerializer(GlobalMemoryManager.Instance);
            var container = setupSerializer.CreateEmpty();
            var bufferWriter = new ArrayBufferWriter<byte>();
            IBufferWriter<byte> writer = bufferWriter;
            setupSerializer.Serialize(in writer, in container);
            container.Dispose();
            var serialized = bufferWriter.WrittenSpan.ToArray();

            AssertNoLeakAtEveryTruncation(serialized, (allocator, bytes, length) =>
            {
                var target = new SurrogateKeyValueContainerSerializer(allocator);
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes, 0, length));
                var result = target.Deserialize(ref reader);
                result.Dispose();
            });
        }

        [Fact]
        public void ColumnAggregateValueSerializerTruncatedInputDoesNotLeak()
        {
            var setupSerializer = new ColumnAggregateValueSerializer(1, GlobalMemoryManager.Instance);
            var container = setupSerializer.CreateEmpty();
            var bufferWriter = new ArrayBufferWriter<byte>();
            IBufferWriter<byte> writer = bufferWriter;
            setupSerializer.Serialize(in writer, in container);
            container.Dispose();
            var serialized = bufferWriter.WrittenSpan.ToArray();

            AssertNoLeakAtEveryTruncation(serialized, (allocator, bytes, length) =>
            {
                var target = new ColumnAggregateValueSerializer(1, allocator);
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes, 0, length));
                var result = target.Deserialize(ref reader);
                result.Dispose();
            });
        }
    }
}

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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Buffers.Binary;
using ZstdSharp;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Serialization
{
    public class EventBatchDeserializerCorruptInputTests
    {
        private sealed class BatchCompressor : IBatchCompressor
        {
            private readonly Compressor compressor;
            public BatchCompressor(Compressor compressor)
            {
                this.compressor = compressor;
            }
            public void ColumnChange(int columnIndex)
            {
            }
            public int Wrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return compressor.Wrap(input, output);
            }
        }

        private sealed class BatchDecompressor : IBatchDecompressor
        {
            private readonly Decompressor decompressor;
            public BatchDecompressor(Decompressor decompressor)
            {
                this.decompressor = decompressor;
            }
            public void ColumnChange(int columnIndex)
            {
            }
            public int Unwrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return decompressor.Unwrap(input, output);
            }
        }

        /// <summary>
        /// Tracks live pointers so we also catch an over free.
        /// </summary>
        private sealed unsafe class CountingAllocator : IMemoryAllocator
        {
            private readonly HashSet<nint> _live = new HashSet<nint>();

            public long AllocatedBytes;
            public long FreedBytes;
            public int LiveCount => _live.Count;

            /// <summary>Set on the first over-free.</summary>
            public string? Violation;

            /// <summary>
            /// Refuses sizes that are too large.
            /// </summary>
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

        /// <summary>
        /// Nested columns take their own deserialization paths.
        /// </summary>
        private static byte[] SerializeNestedBatch()
        {
            Column listColumn = Column.Create(GlobalMemoryManager.Instance);
            Column mapColumn = Column.Create(GlobalMemoryManager.Instance);
            Column structColumn = Column.Create(GlobalMemoryManager.Instance);
            Column unionColumn = Column.Create(GlobalMemoryManager.Instance);
            var structHeader = StructHeader.Create("a", "b");
            for (int i = 0; i < 15; i++)
            {
                if (i % 5 == 0)
                {
                    listColumn.Add(NullValue.Instance);
                    mapColumn.Add(NullValue.Instance);
                    structColumn.Add(NullValue.Instance);
                    unionColumn.Add(NullValue.Instance);
                    continue;
                }
                listColumn.Add(new ListValue(new IDataValue[] { new Int64Value(i), new Int64Value(i + 1) }));
                mapColumn.Add(new MapValue(new List<KeyValuePair<IDataValue, IDataValue>>
                {
                    new KeyValuePair<IDataValue, IDataValue>(new StringValue($"k{i}"), new Int64Value(i))
                }));
                structColumn.Add(new StructValue(structHeader, new Int64Value(i), new StringValue($"v{i}")));
                // Mixed types in one column produce a union.
                if (i % 2 == 0)
                {
                    unionColumn.Add(new StringValue($"u{i}"));
                }
                else
                {
                    unionColumn.Add(new Int64Value(i));
                }
            }
            using var batch = new EventBatchData([listColumn, mapColumn, structColumn, unionColumn]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, batch.Count);
            return bufferWriter.WrittenSpan.ToArray();
        }

        /// <summary>
        /// Guards the sweeps from silently testing nothing.
        /// </summary>
        [Fact]
        public void NestedBatchCorpusActuallyContainsNestedColumns()
        {
            var serialized = SerializeNestedBatch();
            Assert.True(serialized.Length > 200, $"corpus is suspiciously small: {serialized.Length} bytes");

            var deserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance);
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(serialized));
            var result = deserializer.DeserializeBatch(ref reader);

            var types = result.EventBatch.Columns.Select(c => c.Type).ToList();
            Assert.Contains(ArrowTypeId.List, types);
            Assert.Contains(ArrowTypeId.Map, types);
            Assert.Contains(ArrowTypeId.Struct, types);
            Assert.Contains(ArrowTypeId.Union, types);

            result.EventBatch.Dispose();
        }

        /// <summary>
        /// Counts size managed arrays before we read any data.
        /// </summary>
        [Theory]
        [InlineData(1342177280)]
        [InlineData(0x40000000)]
        [InlineData(100_000_000)]
        [InlineData(-1)]
        public void HostileCountsDoNotReserveHugeArrays(int hostileCount)
        {
            const long AllocationBudget = 64L * 1024 * 1024;
            var serialized = SerializeNestedBatch();

            for (int offset = 0; offset + 4 <= serialized.Length; offset += 4)
            {
                var corrupt = (byte[])serialized.Clone();
                BinaryPrimitives.WriteInt32LittleEndian(corrupt.AsSpan(offset), hostileCount);

                var allocator = new CountingAllocator();
                var before = GC.GetAllocatedBytesForCurrentThread();
                try
                {
                    var deserializer = new EventBatchDeserializer(allocator);
                    var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(corrupt));
                    var result = deserializer.DeserializeBatch(ref reader);
                    result.EventBatch.Dispose();
                }
                catch
                {
                }
                var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

                Assert.True(allocated < AllocationBudget,
                    $"count {hostileCount} planted at offset {offset} caused {allocated / (1024 * 1024)} MB of managed allocation");
                allocator.AssertClean($"count {hostileCount} planted at offset {offset}");
            }
        }

        [Fact]
        public void NestedColumnsTruncatedInputDoesNotLeakNativeMemory()
        {
            var serialized = SerializeNestedBatch();
            for (int cut = 0; cut < serialized.Length; cut++)
            {
                AssertNoLeak(serialized, cut, compressed: false, expectSuccess: false);
            }
            AssertNoLeak(serialized, serialized.Length, compressed: false, expectSuccess: true);
        }

        [Fact]
        public void NestedColumnsSingleByteCorruptionDoesNotLeakNativeMemory()
        {
            var serialized = SerializeNestedBatch();
            ReadOnlySpan<byte> masks = stackalloc byte[] { 0xFF, 0x01, 0x80 };

            for (int offset = 0; offset < serialized.Length; offset++)
            {
                for (int m = 0; m < masks.Length; m++)
                {
                    var corrupt = (byte[])serialized.Clone();
                    corrupt[offset] ^= masks[m];

                    var allocator = new CountingAllocator();
                    var deserializer = new EventBatchDeserializer(allocator);
                    try
                    {
                        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(corrupt));
                        var result = deserializer.DeserializeBatch(ref reader);
                        result.EventBatch.Dispose();
                    }
                    catch
                    {
                    }
                    allocator.AssertClean($"nested: byte {offset} flipped with mask 0x{masks[m]:X2}");
                }
            }
        }

        /// <summary>
        /// Renaming a struct child in the schema forges a union with two equal
        /// struct headers, which throws inside the union column constructor.
        /// </summary>
        [Fact]
        public void DuplicateUnionStructHeadersDoNotLeakNativeMemory()
        {
            Column unionColumn = Column.Create(GlobalMemoryManager.Instance);
            var firstHeader = StructHeader.Create("k1", "dup1");
            var secondHeader = StructHeader.Create("k1", "dup2");
            for (int i = 0; i < 10; i++)
            {
                if (i % 2 == 0)
                {
                    unionColumn.Add(new StructValue(firstHeader, new Int64Value(i), new Int64Value(i)));
                }
                else
                {
                    unionColumn.Add(new StructValue(secondHeader, new Int64Value(i), new Int64Value(i)));
                }
            }
            using var batch = new EventBatchData([unionColumn]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, batch.Count);
            var serialized = bufferWriter.WrittenSpan.ToArray();

            var sawDuplicateHeader = false;
            ReadOnlySpan<byte> pattern = "dup2"u8;
            for (int offset = 0; offset + pattern.Length <= serialized.Length; offset++)
            {
                if (!serialized.AsSpan(offset, pattern.Length).SequenceEqual(pattern))
                {
                    continue;
                }
                var corrupt = (byte[])serialized.Clone();
                corrupt[offset + 3] = (byte)'1';

                var allocator = new CountingAllocator();
                var deserializer = new EventBatchDeserializer(allocator);
                try
                {
                    var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(corrupt));
                    var result = deserializer.DeserializeBatch(ref reader);
                    result.EventBatch.Dispose();
                }
                catch (ArgumentException)
                {
                    sawDuplicateHeader = true;
                }
                catch
                {
                }
                allocator.AssertClean($"dup2 renamed at offset {offset}");
            }
            Assert.True(sawDuplicateHeader, "no rename triggered the duplicate header path, the corpus no longer exercises it");
        }

        private static byte[] SerializeSampleBatch(bool compress)
        {
            Column stringColumn = Column.Create(GlobalMemoryManager.Instance);
            Column intColumn = Column.Create(GlobalMemoryManager.Instance);
            for (int i = 0; i < 100; i++)
            {
                if (i % 5 == 0)
                {
                    stringColumn.Add(NullValue.Instance);
                }
                else
                {
                    stringColumn.Add(new StringValue($"value {i}"));
                }
                intColumn.Add(new Int64Value(i));
            }
            using var batch = new EventBatchData([stringColumn, intColumn]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            if (compress)
            {
                serializer.SerializeEventBatch(bufferWriter, batch, batch.Count, new BatchCompressor(new Compressor()));
            }
            else
            {
                serializer.SerializeEventBatch(bufferWriter, batch, batch.Count);
            }
            return bufferWriter.WrittenSpan.ToArray();
        }

        private static void AssertNoLeak(byte[] bytes, int length, bool compressed, bool expectSuccess)
        {
            var allocator = new CountingAllocator();
            var deserializer = compressed
                ? new EventBatchDeserializer(allocator, new BatchDecompressor(new Decompressor()))
                : new EventBatchDeserializer(allocator);
            bool success = false;
            try
            {
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes, 0, length));
                var result = deserializer.DeserializeBatch(ref reader);
                result.EventBatch.Dispose();
                success = true;
            }
            catch
            {
            }
            if (expectSuccess)
            {
                Assert.True(success, "Expected the untouched input to deserialize");
            }
            allocator.AssertClean($"input length {length}");
        }

        [Fact]
        public void TruncatedInputDoesNotLeakNativeMemory()
        {
            var serialized = SerializeSampleBatch(compress: false);
            for (int cut = 0; cut < serialized.Length; cut++)
            {
                AssertNoLeak(serialized, cut, compressed: false, expectSuccess: false);
            }
            AssertNoLeak(serialized, serialized.Length, compressed: false, expectSuccess: true);
        }

        [Fact]
        public void TruncatedCompressedInputDoesNotLeakNativeMemory()
        {
            var serialized = SerializeSampleBatch(compress: true);
            for (int cut = 0; cut < serialized.Length; cut++)
            {
                AssertNoLeak(serialized, cut, compressed: true, expectSuccess: false);
            }
            AssertNoLeak(serialized, serialized.Length, compressed: true, expectSuccess: true);
        }

        /// <summary>
        /// Negative or short lengths make the guards always false.
        /// </summary>
        private static void AssertHostileHeaderLengthsDoNotLeak(bool compressed)
        {
            var serialized = SerializeSampleBatch(compressed);
            ReadOnlySpan<long> hostileValues = stackalloc long[] { 1, 4, 7, 8, -1, -8, -63, -64, -1000 };

            for (int offset = 0; offset + 8 <= serialized.Length; offset += 4)
            {
                for (int v = 0; v < hostileValues.Length; v++)
                {
                    var corrupt = (byte[])serialized.Clone();
                    BinaryPrimitives.WriteInt64LittleEndian(corrupt.AsSpan(offset), hostileValues[v]);

                    var allocator = new CountingAllocator();
                    var deserializer = compressed
                        ? new EventBatchDeserializer(allocator, new BatchDecompressor(new Decompressor()))
                        : new EventBatchDeserializer(allocator);
                    try
                    {
                        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(corrupt));
                        var result = deserializer.DeserializeBatch(ref reader);
                        result.EventBatch.Dispose();
                    }
                    catch
                    {
                    }
                    allocator.AssertClean($"length {hostileValues[v]} planted at offset {offset}");
                }
            }
        }

        /// <summary>
        /// Every column carries nulls so each has a validity buffer.
        /// </summary>
        private static byte[] SerializeMixedTypeBatch(int columnCount)
        {
            var columns = new List<Column>();
            for (int c = 0; c < columnCount; c++)
            {
                columns.Add(Column.Create(GlobalMemoryManager.Instance));
            }
            for (int i = 0; i < 20; i++)
            {
                for (int c = 0; c < columns.Count; c++)
                {
                    if ((i + c) % 4 == 0)
                    {
                        columns[c].Add(NullValue.Instance);
                        continue;
                    }
                    switch (c % 5)
                    {
                        case 0: columns[c].Add(new StringValue($"value {i}")); break;
                        case 1: columns[c].Add(new Int64Value(i)); break;
                        case 2: columns[c].Add(new BoolValue(i % 2 == 0)); break;
                        case 3: columns[c].Add(new DoubleValue(i * 1.5)); break;
                        default: columns[c].Add(new BinaryValue(new byte[] { (byte)i, 7, 7 })); break;
                    }
                }
            }
            using var batch = new EventBatchData(columns.Cast<IColumn>().ToArray());
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, batch.Count);
            return bufferWriter.WrittenSpan.ToArray();
        }

        /// <summary>
        /// Finds allocations stranded between a throw and an owner.
        /// </summary>
        [Fact]
        public void SingleByteCorruptionDoesNotLeakNativeMemory()
        {
            AssertSingleByteCorruptionIsClean(columnCount: 5);
        }

        /// <summary>
        /// A corrupt schema once reported over a billion fields.
        /// </summary>
        [Fact]
        public void SingleByteCorruptionOnWideBatchDoesNotLeakNativeMemory()
        {
            AssertSingleByteCorruptionIsClean(columnCount: 11);
        }

        private static void AssertSingleByteCorruptionIsClean(int columnCount)
        {
            var serialized = SerializeMixedTypeBatch(columnCount);
            ReadOnlySpan<byte> masks = stackalloc byte[] { 0xFF, 0x01, 0x80 };

            for (int offset = 0; offset < serialized.Length; offset++)
            {
                for (int m = 0; m < masks.Length; m++)
                {
                    var corrupt = (byte[])serialized.Clone();
                    corrupt[offset] ^= masks[m];

                    var allocator = new CountingAllocator();
                    var deserializer = new EventBatchDeserializer(allocator);
                    try
                    {
                        var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(corrupt));
                        var result = deserializer.DeserializeBatch(ref reader);
                        result.EventBatch.Dispose();
                    }
                    catch
                    {
                    }
                    allocator.AssertClean($"byte {offset} flipped with mask 0x{masks[m]:X2}");
                }
            }
        }

        [Fact]
        public void HostileHeaderLengthsDoNotLeakNativeMemory()
        {
            AssertHostileHeaderLengthsDoNotLeak(compressed: false);
        }

        [Fact]
        public void HostileHeaderLengthsCompressedDoNotLeakNativeMemory()
        {
            AssertHostileHeaderLengthsDoNotLeak(compressed: true);
        }

        [Fact]
        public void CorruptCompressedPayloadDoesNotLeakNativeMemory()
        {
            var serialized = SerializeSampleBatch(compress: true);
            // We flip in the second half to hit compressed payloads.
            for (int i = serialized.Length / 2; i < serialized.Length; i += 7)
            {
                var corrupt = (byte[])serialized.Clone();
                corrupt[i] ^= 0xFF;
                AssertNoLeak(corrupt, corrupt.Length, compressed: true, expectSuccess: false);
            }
        }
    }
}

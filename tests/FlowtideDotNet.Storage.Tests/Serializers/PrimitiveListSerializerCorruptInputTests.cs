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
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;

namespace FlowtideDotNet.Storage.Tests.Serializers
{
    public class PrimitiveListSerializerCorruptInputTests
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
                Assert.True(allocator.AllocatedBytes == allocator.FreedBytes,
                    $"Native memory leak at truncation {cut}: allocated {allocator.AllocatedBytes}, freed {allocator.FreedBytes}");
            }
        }

        [Fact]
        public void ValueContainerTruncatedInputDoesNotLeak()
        {
            var container = new PrimitiveListValueContainer<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < 100; i++)
            {
                container.Insert(i, i);
            }
            var serializer = new PrimitiveListValueContainerSerializer<int>(GlobalMemoryManager.Instance);
            var bufferWriter = new ArrayBufferWriter<byte>();
            IBufferWriter<byte> writer = bufferWriter;
            serializer.Serialize(in writer, in container);
            container.Dispose();
            var serialized = bufferWriter.WrittenSpan.ToArray();

            AssertNoLeakAtEveryTruncation(serialized, (allocator, bytes, length) =>
            {
                var target = new PrimitiveListValueContainerSerializer<int>(allocator);
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes, 0, length));
                var result = target.Deserialize(ref reader);
                result.Dispose();
            });
        }

        [Fact]
        public void KeyContainerTruncatedInputDoesNotLeak()
        {
            var container = new PrimitiveListKeyContainer<long>(GlobalMemoryManager.Instance);
            for (int i = 0; i < 100; i++)
            {
                container.Add(i);
            }
            var serializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance);
            var bufferWriter = new ArrayBufferWriter<byte>();
            IBufferWriter<byte> writer = bufferWriter;
            serializer.Serialize(in writer, in container);
            container.Dispose();
            var serialized = bufferWriter.WrittenSpan.ToArray();

            AssertNoLeakAtEveryTruncation(serialized, (allocator, bytes, length) =>
            {
                var target = new PrimitiveListKeyContainerSerializer<long>(allocator);
                var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes, 0, length));
                var result = target.Deserialize(ref reader);
                result.Dispose();
            });
        }
    }
}

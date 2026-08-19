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

using FlowtideDotNet.Core.Operators.Window;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Core.Tests.Operators.Window
{
    /// <summary>
    /// Native memory from the allocator is only freed by Dispose, it has no finalizer
    /// </summary>
    public class WindowValueContainerSerializerTests
    {
        private sealed class TrackingMemoryAllocator : IMemoryAllocator
        {
            public int ActiveAllocations;
            public long OutstandingBytes;

            public IMemoryOwner<byte> Allocate(int size, int alignment)
            {
                ActiveAllocations++;
                return new TrackingOwner(GlobalMemoryManager.Instance.Allocate(size, alignment), this);
            }

            // The failure paths must throw before anything is wrapped, so nothing can grow
            public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
            {
                throw new NotSupportedException("A deserialize failure path must not reallocate");
            }

            // FlowtideMemory is allocated by the default interface methods, which report
            // every block through these hooks, so a balanced total is the leak check
            public void RegisterAllocationToMetrics(int size) => OutstandingBytes += size;

            public void RegisterFreeToMetrics(int size) => OutstandingBytes -= size;

            private sealed class TrackingOwner : IMemoryOwner<byte>
            {
                private readonly IMemoryOwner<byte> _inner;
                private readonly TrackingMemoryAllocator _parent;
                private bool _disposed;

                public TrackingOwner(IMemoryOwner<byte> inner, TrackingMemoryAllocator parent)
                {
                    _inner = inner;
                    _parent = parent;
                }

                public Memory<byte> Memory => _inner.Memory;

                public void Dispose()
                {
                    if (!_disposed)
                    {
                        _disposed = true;
                        _parent.ActiveAllocations--;
                        _inner.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Declares the two lengths, then supplies fewer bytes than the declared payload.
        /// </summary>
        private static byte[] TruncatedPayload(int weightsLength, int bitmapLength, int suppliedBytes)
        {
            byte[] data = new byte[8 + suppliedBytes];
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(0), weightsLength);
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(4), bitmapLength);
            return data;
        }

        private delegate void DeserializeAction(ref SequenceReader<byte> reader);

        private static void AssertThrowsWithoutLeaking(TrackingMemoryAllocator allocator, byte[] payload, DeserializeAction deserialize)
        {
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(payload));

            bool threw = false;
            try
            {
                deserialize(ref reader);
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            catch (Exception)
            {
                // The corrupt column stream reports a bare Exception, while other paths may throw InvalidOperationException
                threw = true;
            }

            Assert.True(threw, "The invalid payload was not rejected");
            Assert.Equal(0, allocator.ActiveAllocations);
            Assert.Equal(0, allocator.OutstandingBytes);
        }

        [Fact]
        public void BulkDeserializeTruncatedWeightsDoesNotLeak()
        {
            var allocator = new TrackingMemoryAllocator();
            var serializer = new BulkWindowValueContainerSerializer(1, allocator);

            AssertThrowsWithoutLeaking(allocator, TruncatedPayload(64, 64, 8), (ref SequenceReader<byte> r) => serializer.Deserialize(ref r));
        }

        [Fact]
        public void BulkDeserializeTruncatedBitmapDoesNotLeak()
        {
            var allocator = new TrackingMemoryAllocator();
            var serializer = new BulkWindowValueContainerSerializer(1, allocator);

            // The weights payload is complete, the bitmap payload is not
            AssertThrowsWithoutLeaking(allocator, TruncatedPayload(8, 64, 16), (ref SequenceReader<byte> r) => serializer.Deserialize(ref r));
        }

        [Fact]
        public void BulkDeserializeNegativeLengthDoesNotLeak()
        {
            var allocator = new TrackingMemoryAllocator();
            var serializer = new BulkWindowValueContainerSerializer(1, allocator);

            AssertThrowsWithoutLeaking(allocator, TruncatedPayload(8, -1, 8), (ref SequenceReader<byte> r) => serializer.Deserialize(ref r));
        }

        /// <summary>
        /// Both lengths are satisfied, the column stream after them is not.
        /// </summary>
        private static byte[] CorruptColumnStreamPayload()
        {
            byte[] data = new byte[8 + 4 + 4];
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(0), 4);
            BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(4), 4);
            return data;
        }

        [Fact]
        public void BulkDeserializeCorruptColumnStreamReleasesWithoutFinalizer()
        {
            var allocator = new TrackingMemoryAllocator();
            var serializer = new BulkWindowValueContainerSerializer(1, allocator);

            AssertThrowsWithoutLeaking(allocator, CorruptColumnStreamPayload(), (ref SequenceReader<byte> r) => serializer.Deserialize(ref r));
        }

        [Fact]
        public void DeserializeCorruptColumnStreamReleasesWithoutFinalizer()
        {
            var allocator = new TrackingMemoryAllocator();
            var serializer = new WindowValueContainerSerializer(1, allocator);

            AssertThrowsWithoutLeaking(allocator, CorruptColumnStreamPayload(), (ref SequenceReader<byte> r) => serializer.Deserialize(ref r));
        }

        [Fact]
        public void DeserializeTruncatedWeightsDoesNotLeak()
        {
            var allocator = new TrackingMemoryAllocator();
            var serializer = new WindowValueContainerSerializer(1, allocator);

            AssertThrowsWithoutLeaking(allocator, TruncatedPayload(64, 64, 8), (ref SequenceReader<byte> r) => serializer.Deserialize(ref r));
        }

        [Fact]
        public void DeserializeTruncatedBitmapDoesNotLeak()
        {
            var allocator = new TrackingMemoryAllocator();
            var serializer = new WindowValueContainerSerializer(1, allocator);

            AssertThrowsWithoutLeaking(allocator, TruncatedPayload(8, 64, 16), (ref SequenceReader<byte> r) => serializer.Deserialize(ref r));
        }

        [Fact]
        public void DeserializeNegativeLengthDoesNotLeak()
        {
            var allocator = new TrackingMemoryAllocator();
            var serializer = new WindowValueContainerSerializer(1, allocator);

            AssertThrowsWithoutLeaking(allocator, TruncatedPayload(8, -1, 8), (ref SequenceReader<byte> r) => serializer.Deserialize(ref r));
        }

        // Lengths throw InvalidOperationException, a corrupt column stream does not
        private static void AssertRejectedWithoutLeaking(TrackingMemoryAllocator allocator, byte[] payload, DeserializeAction deserialize)
        {
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(payload));

            Exception? caught = null;
            try
            {
                deserialize(ref reader);
            }
            catch (Exception e)
            {
                caught = e;
            }

            Assert.IsType<InvalidOperationException>(caught);
            Assert.Equal(0, allocator.ActiveAllocations);
            Assert.Equal(0, allocator.OutstandingBytes);
        }

        // A length that is not whole ints would truncate the weight count
        [Fact]
        public void BulkDeserializeUnalignedWeightsLengthIsRejected()
        {
            var allocator = new TrackingMemoryAllocator();
            var serializer = new BulkWindowValueContainerSerializer(1, allocator);

            AssertRejectedWithoutLeaking(allocator, TruncatedPayload(6, 4, 16), (ref SequenceReader<byte> r) => serializer.Deserialize(ref r));
        }

        // A length that is not whole ints would truncate the weight count
        [Fact]
        public void DeserializeUnalignedWeightsLengthIsRejected()
        {
            var allocator = new TrackingMemoryAllocator();
            var serializer = new WindowValueContainerSerializer(1, allocator);

            AssertRejectedWithoutLeaking(allocator, TruncatedPayload(6, 4, 16), (ref SequenceReader<byte> r) => serializer.Deserialize(ref r));
        }
    }
}

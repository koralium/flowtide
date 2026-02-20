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

using FlowtideDotNet.Storage.DataStructures;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.DataStructures
{
    public class RoaringBitmapTests
    {
        [Fact]
        public void TestSimpleInsertInOrder()
        {
            RoaringBitmap bitmap = [0, 1];

            Assert.True(bitmap.Contains(0));
            Assert.True(bitmap.Contains(1));
            Assert.False(bitmap.Contains(2));
        }

        [Fact]
        public void TestInsertInDifferentContainers()
        {
            RoaringBitmap bitmap = [1_000_000, 3];

            Assert.True(bitmap.Contains(1_000_000));
            Assert.True(bitmap.Contains(3));
            Assert.False(bitmap.Contains(2));
        }

        [Fact]
        public void InsertInSameContainerOutOfOrder()
        {
            RoaringBitmap bitmap = [4, 1, 9, 7, 72, 63];

            Assert.True(bitmap.Contains(4));
            Assert.True(bitmap.Contains(1));
            Assert.True(bitmap.Contains(9));
            Assert.True(bitmap.Contains(7));
            Assert.True(bitmap.Contains(72));
            Assert.True(bitmap.Contains(63));
        }

        [Fact]
        public void TestSerializeDeserializeArrays()
        {
            RoaringBitmap bitmap = [1, 3, 1_000_000];
            ArrayBufferWriter<byte> bufferWriter = new ArrayBufferWriter<byte>();
            bitmap.Serialize(bufferWriter);
            var mem = bufferWriter.WrittenMemory;
            var sequence = new ReadOnlySequence<byte>(mem);
            var reader = new SequenceReader<byte>(sequence);
            RoaringBitmap deserialized = RoaringBitmap.Deserialize(ref reader);
            Assert.True(deserialized.Contains(1));
            Assert.True(deserialized.Contains(3));
            Assert.True(deserialized.Contains(1_000_000));
            Assert.False(deserialized.Contains(2));
        }

        [Fact]
        public void TestSerializeDeserializeBitmaps()
        {
            RoaringBitmap bitmap = new RoaringBitmap();

            for (int i = 0; i < 5000; i++)
            {
                bitmap.Add(i);
            }

            ArrayBufferWriter<byte> bufferWriter = new ArrayBufferWriter<byte>();
            bitmap.Serialize(bufferWriter);
            var mem = bufferWriter.WrittenMemory;
            var sequence = new ReadOnlySequence<byte>(mem);
            var reader = new SequenceReader<byte>(sequence);
            RoaringBitmap deserialized = RoaringBitmap.Deserialize(ref reader);
            
            for (int i  = 0; i < 5000; i++)
            {
                Assert.True(deserialized.Contains(i));
            }
        }

        [Fact]
        public void IterateEmptyBitmap()
        {
            RoaringBitmap bitmap = new RoaringBitmap();

            foreach(var val in bitmap)
            {
                Assert.Fail("Should not iterate over any values");
            }
        }

        [Fact]
        public void TestSerializeInSequenceNotStartAtZero()
        {
            RoaringBitmap bitmap = [100003, 100004, 100005, 100006, 100007, 100008, 100009];
            ArrayBufferWriter<byte> bufferWriter = new ArrayBufferWriter<byte>();
            bitmap.Serialize(bufferWriter);
            var mem = bufferWriter.WrittenMemory;
            var sequence = new ReadOnlySequence<byte>(mem);
            var reader = new SequenceReader<byte>(sequence);
            RoaringBitmap deserialized = RoaringBitmap.Deserialize(ref reader);

            int start = 100003;

            for (int i = 0; i < 7; i++)
            {
                Assert.True(deserialized.Contains(start + i));
            }

        }
    }
}
